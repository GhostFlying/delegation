//go:build darwin

package config

import "golang.org/x/sys/unix"

func noReplaceRename(oldPath, newPath string) error {
	return unix.RenamexNp(oldPath, newPath, unix.RENAME_EXCL)
}
