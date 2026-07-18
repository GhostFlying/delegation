//go:build linux

package tokenfile

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func installTokenNoReplace(oldPath, newPath string) (bool, error) {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EOPNOTSUPP) {
		return false, err
	}
	if err := os.Link(oldPath, newPath); err != nil {
		return false, fmt.Errorf("filesystem lacks atomic no-replace rename and hard-link fallback: %w", err)
	}
	if err := os.Remove(oldPath); err != nil {
		return true, fmt.Errorf("remove linked temporary token: %w", err)
	}
	return true, nil
}
