//go:build darwin

package securefs

import (
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
	if err := unix.RenameatxNp(fd, temporary, fd, destination, unix.RENAME_EXCL); err != nil {
		return false, err
	}
	return true, nil
}
