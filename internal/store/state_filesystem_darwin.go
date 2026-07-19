//go:build darwin

package store

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

func validateLocalStatePath(path string) error {
	existing, err := nearestExistingPath(path)
	if err != nil {
		return err
	}
	var status unix.Statfs_t
	if err := unix.Statfs(existing, &status); err != nil {
		return fmt.Errorf("inspect broker state filesystem: %w", err)
	}
	if status.Flags&unix.MNT_LOCAL == 0 {
		return errors.New("broker state must not use a network filesystem because SQLite WAL requires local shared memory")
	}
	return nil
}
