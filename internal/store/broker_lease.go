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
	ErrBrokerLeaseHeld            = errors.New("broker instance lease is already held")
	ErrPeerLeaseHeld              = errors.New("peer connector is already running for this state")
	ErrTailscaleStateDirLeaseHeld = errors.New("tailscale state directory is already in use")
)

// ExclusiveLease prevents two service processes from owning the same resource.
// The persistent lock file is intentionally never removed; the OS lock is
// released when the process exits or Close releases its file handle.
type ExclusiveLease struct {
	directory *securefs.Root
	file      *os.File
	closeOnce sync.Once
	closeErr  error
}

type BrokerLease = ExclusiveLease
type PeerLease = ExclusiveLease
type TailscaleStateDirLease = ExclusiveLease

func AcquireBrokerLease(statePath string) (*BrokerLease, error) {
	return acquireExclusiveLease(statePath+".broker.lock", "broker", ErrBrokerLeaseHeld)
}

func AcquirePeerLease(statePath string) (*PeerLease, error) {
	return acquireExclusiveLease(statePath+".peer.lock", "peer", ErrPeerLeaseHeld)
}

// AcquireTailscaleStateDirLease exclusively owns a configured tsnet state
// directory without creating, changing, or removing that directory.
func AcquireTailscaleStateDirLease(stateDir string) (*TailscaleStateDirLease, error) {
	if !filepath.IsAbs(stateDir) {
		return nil, errors.New("tailscale state directory path must be absolute")
	}
	stateDir = filepath.Clean(stateDir)
	if err := validateTailscaleStateDir(stateDir); err != nil {
		return nil, err
	}
	lease, err := acquireExclusiveLease(
		stateDir+".tailscale.lock",
		"tailscale state directory",
		ErrTailscaleStateDirLeaseHeld,
	)
	if err != nil {
		return nil, err
	}
	if err := validateTailscaleStateDir(stateDir); err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	return lease, nil
}

func validateTailscaleStateDir(stateDir string) error {
	info, err := os.Lstat(stateDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect tailscale state directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("tailscale state directory must be a directory, not a symbolic link")
	}
	if err := validateStateDirectoryLocation(stateDir); err != nil {
		return fmt.Errorf("validate tailscale state directory location: %w", err)
	}
	return nil
}

func acquireExclusiveLease(
	leasePath, description string,
	heldError error,
) (*ExclusiveLease, error) {
	resolved, err := preparePath(leasePath)
	if err != nil {
		return nil, err
	}
	leasePath = resolved
	directory, err := openStateDirectoryGuard(filepath.Dir(leasePath))
	if err != nil {
		return nil, fmt.Errorf("open %s lease directory: %w", description, err)
	}
	fail := func(err error) (*ExclusiveLease, error) {
		return nil, errors.Join(err, directory.Close())
	}
	name := filepath.Base(leasePath)
	file, err := directory.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fail(fmt.Errorf("open %s lease: %w", description, err))
	}
	failFile := func(err error) (*ExclusiveLease, error) {
		return nil, errors.Join(err, file.Close(), directory.Close())
	}
	if err := protectDatabaseFile(leasePath); err != nil {
		return failFile(err)
	}
	opened, err := file.Stat()
	if err != nil {
		return failFile(fmt.Errorf("inspect %s lease: %w", description, err))
	}
	named, err := os.Lstat(leasePath)
	if err != nil {
		return failFile(fmt.Errorf("inspect %s lease path: %w", description, err))
	}
	if !opened.Mode().IsRegular() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, named) {
		return failFile(fmt.Errorf("%s lease path changed while it was being opened", description))
	}
	if err := directory.VerifyPath(); err != nil {
		return failFile(err)
	}
	if err := lockInstanceLease(file, description, heldError); err != nil {
		return failFile(err)
	}
	return &ExclusiveLease{directory: directory, file: file}, nil
}

func (l *ExclusiveLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeErr = errors.Join(unlockInstanceLease(l.file), l.file.Close(), l.directory.Close())
	})
	return l.closeErr
}
