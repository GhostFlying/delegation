//go:build windows

package main

import "golang.org/x/sys/windows"

func commitReleaseDirectory(staging, destination string) error {
	stagingPath, err := windows.UTF16PtrFromString(staging)
	if err != nil {
		return err
	}
	destinationPath, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(stagingPath, destinationPath, 0)
}
