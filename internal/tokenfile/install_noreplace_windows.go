//go:build windows

package tokenfile

import "golang.org/x/sys/windows"

func installTokenNoReplace(oldPath, newPath string) (bool, error) {
	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return false, err
	}
	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return false, err
	}
	if err := windows.MoveFileEx(oldPathPtr, newPathPtr, windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return false, err
	}
	return true, nil
}
