//go:build windows

package store

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

const allBrokerLeaseBytes = ^uint32(0)

func lockBrokerLease(file *os.File) error {
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		allBrokerLeaseBytes,
		allBrokerLeaseBytes,
		&windows.Overlapped{},
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return fmt.Errorf("%w", ErrBrokerLeaseHeld)
	}
	if err != nil {
		return fmt.Errorf("lock broker instance lease: %w", err)
	}
	return nil
}

func unlockBrokerLease(file *os.File) error {
	err := windows.UnlockFileEx(
		windows.Handle(file.Fd()), 0, allBrokerLeaseBytes, allBrokerLeaseBytes, &windows.Overlapped{},
	)
	if err != nil {
		return fmt.Errorf("unlock broker instance lease: %w", err)
	}
	return nil
}
