//go:build linux

package securefs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func publishNoReplace(root *os.Root, temporary, destination string) (bool, error) {
	directory, err := root.Open(".")
	if err != nil {
		return false, err
	}
	defer directory.Close()
	fd := int(directory.Fd())
	if err := unix.Renameat2(fd, temporary, fd, destination, unix.RENAME_NOREPLACE); err != nil {
		if err == unix.ENOSYS || err == unix.EINVAL || err == unix.EOPNOTSUPP {
			return false, fmt.Errorf("filesystem does not support atomic no-replace rename: %w", err)
		}
		return false, err
	}
	return true, nil
}
