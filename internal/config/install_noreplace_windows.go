//go:build windows

package config

import "golang.org/x/sys/windows"

func noReplaceRename(oldPath, newPath string) error {
	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(oldPathPtr, newPathPtr, windows.MOVEFILE_WRITE_THROUGH)
}
