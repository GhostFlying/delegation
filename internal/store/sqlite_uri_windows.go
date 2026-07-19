//go:build windows

package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func validateLocalStatePath(path string) error {
	volume := filepath.VolumeName(path)
	if strings.HasPrefix(volume, `\\`) {
		return errors.New("broker state must not use a Windows network path")
	}
	if volume == "" {
		return errors.New("broker state must use a local Windows volume")
	}
	root, err := windows.UTF16PtrFromString(volume + `\`)
	if err != nil {
		return fmt.Errorf("resolve broker state volume: %w", err)
	}
	return validateWindowsDriveType(windows.GetDriveType(root))
}

func validateWindowsDriveType(driveType uint32) error {
	switch driveType {
	case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK:
		return nil
	case windows.DRIVE_REMOTE:
		return errors.New("broker state must not use a mapped network drive because SQLite WAL requires local shared memory")
	default:
		return errors.New("broker state volume must provide local shared-memory semantics for SQLite WAL")
	}
}

func sqliteURIPath(path string) string {
	path = filepath.ToSlash(path)
	if volume := filepath.VolumeName(path); len(volume) == 2 && volume[1] == ':' {
		return "/" + path
	}
	return path
}
