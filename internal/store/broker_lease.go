package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/GhostFlying/delegation/internal/securefs"
)

var (
	ErrBrokerLeaseHeld = errors.New("broker instance lease is already held")
	ErrPeerLeaseHeld   = errors.New("peer connector is already running for this state")
)

// InstanceLease prevents two service processes from owning the same state store.
// The persistent lock file is intentionally never removed; the OS lock is
// released when the process exits or Close releases its file handle.
type InstanceLease struct {
	directory *securefs.Root
	file      *os.File
	closeOnce sync.Once
	closeErr  error
}

type BrokerLease = InstanceLease
type PeerLease = InstanceLease

func AcquireBrokerLease(statePath string) (*BrokerLease, error) {
	return acquireInstanceLease(statePath, "broker", ErrBrokerLeaseHeld)
}

func AcquirePeerLease(statePath string) (*PeerLease, error) {
	return acquireInstanceLease(statePath, "peer", ErrPeerLeaseHeld)
}

func acquireInstanceLease(
	statePath, service string,
	heldError error,
) (*InstanceLease, error) {
	resolved, err := preparePath(statePath)
	if err != nil {
		return nil, err
	}
	statePath = resolved
	directory, err := openStateDirectoryGuard(filepath.Dir(statePath))
	if err != nil {
		return nil, fmt.Errorf("open %s lease directory: %w", service, err)
	}
	fail := func(err error) (*InstanceLease, error) {
		return nil, errors.Join(err, directory.Close())
	}
	name := filepath.Base(statePath) + "." + service + ".lock"
	file, err := directory.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fail(fmt.Errorf("open %s instance lease: %w", service, err))
	}
	failFile := func(err error) (*InstanceLease, error) {
		return nil, errors.Join(err, file.Close(), directory.Close())
	}
	path := statePath + "." + service + ".lock"
	if err := protectDatabaseFile(path); err != nil {
		return failFile(err)
	}
	opened, err := file.Stat()
	if err != nil {
		return failFile(fmt.Errorf("inspect %s instance lease: %w", service, err))
	}
	named, err := os.Lstat(path)
	if err != nil {
		return failFile(fmt.Errorf("inspect %s instance lease path: %w", service, err))
	}
	if !opened.Mode().IsRegular() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, named) {
		return failFile(fmt.Errorf("%s instance lease path changed while it was being opened", service))
	}
	if err := directory.VerifyPath(); err != nil {
		return failFile(err)
	}
	if err := lockInstanceLease(file, service, heldError); err != nil {
		return failFile(err)
	}
	return &InstanceLease{directory: directory, file: file}, nil
}

func (l *InstanceLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeErr = errors.Join(unlockInstanceLease(l.file), l.file.Close(), l.directory.Close())
	})
	return l.closeErr
}
