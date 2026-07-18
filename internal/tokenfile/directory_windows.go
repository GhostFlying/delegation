//go:build windows

package tokenfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func validateTokenDirectory(path string) error {
	if strings.HasPrefix(filepath.VolumeName(path), `\\`) {
		return errors.New("token directory must not use a Windows network path")
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := windows.GetFileAttributes(pathPtr)
	if err != nil {
		return err
	}
	if attributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("token directory must be a local directory, not a reparse point")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return errors.New("token directory must be a local directory")
	}
	return nil
}
