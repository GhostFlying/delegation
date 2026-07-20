//go:build windows

package store

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

const allLeaseBytes = ^uint32(0)

func lockInstanceLease(file *os.File, description string, heldError error) error {
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		allLeaseBytes,
		allLeaseBytes,
		&windows.Overlapped{},
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return fmt.Errorf("%w", heldError)
	}
	if err != nil {
		return fmt.Errorf("lock %s instance lease: %w", description, err)
	}
	return nil
}

func unlockInstanceLease(file *os.File) error {
	err := windows.UnlockFileEx(
		windows.Handle(file.Fd()), 0, allLeaseBytes, allLeaseBytes, &windows.Overlapped{},
	)
	if err != nil {
		return fmt.Errorf("unlock instance lease: %w", err)
	}
	return nil
}
