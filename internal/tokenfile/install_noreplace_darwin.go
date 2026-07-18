//go:build darwin

package tokenfile

import "golang.org/x/sys/unix"

func installTokenNoReplace(oldPath, newPath string) (bool, error) {
	if err := unix.RenamexNp(oldPath, newPath, unix.RENAME_EXCL); err != nil {
		return false, err
	}
	return true, nil
}
