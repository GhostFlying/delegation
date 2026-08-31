//go:build darwin

package securefs

import (
	"os"

	"golang.org/x/sys/unix"
)

func exchange(root *os.Root, first, second string) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	fd := int(directory.Fd())
	return unix.RenameatxNp(fd, first, fd, second, unix.RENAME_SWAP)
}
