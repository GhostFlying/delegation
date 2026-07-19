package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/GhostFlying/delegation/internal/securefs"
)

var ErrBrokerLeaseHeld = errors.New("broker instance lease is already held")

// BrokerLease prevents two broker processes from owning the same state store.
// The persistent lock file is intentionally never removed; the OS lock is
// released when the process exits or Close releases its file handle.
type BrokerLease struct {
	directory *securefs.Root
	file      *os.File
	closeOnce sync.Once
	closeErr  error
}

func AcquireBrokerLease(statePath string) (*BrokerLease, error) {
	resolved, err := preparePath(statePath)
	if err != nil {
		return nil, err
	}
	statePath = resolved
	directory, err := openStateDirectoryGuard(filepath.Dir(statePath))
	if err != nil {
		return nil, fmt.Errorf("open broker lease directory: %w", err)
	}
	fail := func(err error) (*BrokerLease, error) {
		return nil, errors.Join(err, directory.Close())
	}
	name := filepath.Base(statePath) + ".broker.lock"
	file, err := directory.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fail(fmt.Errorf("open broker instance lease: %w", err))
	}
	failFile := func(err error) (*BrokerLease, error) {
		return nil, errors.Join(err, file.Close(), directory.Close())
	}
	path := statePath + ".broker.lock"
	if err := protectDatabaseFile(path); err != nil {
		return failFile(err)
	}
	opened, err := file.Stat()
	if err != nil {
		return failFile(fmt.Errorf("inspect broker instance lease: %w", err))
	}
	named, err := os.Lstat(path)
	if err != nil {
		return failFile(fmt.Errorf("inspect broker instance lease path: %w", err))
	}
	if !opened.Mode().IsRegular() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, named) {
		return failFile(errors.New("broker instance lease path changed while it was being opened"))
	}
	if err := directory.VerifyPath(); err != nil {
		return failFile(err)
	}
	if err := lockBrokerLease(file); err != nil {
		return failFile(err)
	}
	return &BrokerLease{directory: directory, file: file}, nil
}

func (l *BrokerLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeErr = errors.Join(unlockBrokerLease(l.file), l.file.Close(), l.directory.Close())
	})
	return l.closeErr
}
