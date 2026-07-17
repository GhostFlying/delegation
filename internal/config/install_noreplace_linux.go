//go:build linux

package config

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

var linuxRenameNoReplace = func(oldPath, newPath string) error {
	return unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
}

var linuxLinkNoReplace = os.Link

var linuxRemoveSource = os.Remove

func noReplaceRename(oldPath, newPath string) error {
	err := linuxRenameNoReplace(oldPath, newPath)
	if err == nil || !renameNoReplaceUnsupported(err) {
		return err
	}
	if err := linuxLinkNoReplace(oldPath, newPath); err != nil {
		return fmt.Errorf("filesystem lacks atomic no-replace rename and hard-link fallback: %w", err)
	}
	if err := linuxRemoveSource(oldPath); err != nil {
		return &CommittedError{Err: fmt.Errorf("remove linked temporary config: %w", err)}
	}
	return nil
}

func renameNoReplaceUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP)
}
