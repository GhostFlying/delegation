//go:build windows

package workerhost

import (
	"errors"
	"os"
)

func openWorkspaceFileForSync(
	root *os.Root,
	name string,
	expected os.FileInfo,
) (*os.File, func() error, error) {
	originalMode := expected.Mode().Perm()
	restored := originalMode&0o200 != 0
	restoreMode := func() error {
		if restored {
			return nil
		}
		if err := root.Chmod(name, originalMode); err != nil {
			return err
		}
		restored = true
		return nil
	}
	if !restored {
		if err := root.Chmod(name, originalMode|0o200); err != nil {
			return nil, restoreMode, err
		}
	}
	file, err := root.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, restoreMode, errors.Join(err, restoreMode())
	}
	return file, restoreMode, nil
}
