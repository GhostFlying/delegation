//go:build linux || darwin

package store

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockInstanceLease(file *os.File, description string, heldError error) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("%w", heldError)
		}
		if err != nil {
			return fmt.Errorf("lock %s instance lease: %w", description, err)
		}
		return nil
	}
}

func unlockInstanceLease(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_UN)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("unlock instance lease: %w", err)
		}
		return nil
	}
}
