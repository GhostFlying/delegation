//go:build linux || darwin

package store

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockBrokerLease(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("%w", ErrBrokerLeaseHeld)
		}
		if err != nil {
			return fmt.Errorf("lock broker instance lease: %w", err)
		}
		return nil
	}
}

func unlockBrokerLease(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_UN)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("unlock broker instance lease: %w", err)
		}
		return nil
	}
}
