//go:build windows

package winlocalpath

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

const extendedPathPrefix = `\\?\`

// ValidateDirectory rejects opened directories whose resolved Windows volume
// does not provide local filesystem semantics.
func ValidateDirectory(directory *os.File) error {
	path, err := finalDirectoryPath(windows.Handle(directory.Fd()))
	if err != nil {
		return fmt.Errorf("resolve opened directory path: %w", err)
	}
	return validateFinalDirectoryPath(path, windowsDriveType)
}

// ValidateDirectoryPath resolves an existing directory through an opened root
// before applying the local-volume policy.
func ValidateDirectoryPath(path string) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return fmt.Errorf("open directory for local-volume validation: %w", err)
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open directory handle for local-volume validation: %w", err)
	}
	defer directory.Close()
	return ValidateDirectory(directory)
}

func finalDirectoryPath(handle windows.Handle) (string, error) {
	buffer := make([]uint16, 100)
	for {
		length, err := windows.GetFinalPathNameByHandle(handle, &buffer[0], uint32(len(buffer)), 0)
		if err != nil {
			return "", err
		}
		if length < uint32(len(buffer)) {
			return syscall.UTF16ToString(buffer[:length]), nil
		}
		if length > 32*1024 {
			return "", errors.New("resolved directory path exceeds the Windows path limit")
		}
		buffer = make([]uint16, length)
	}
}

func validateFinalDirectoryPath(path string, driveType func(string) uint32) error {
	if strings.HasPrefix(strings.ToUpper(path), strings.ToUpper(extendedPathPrefix+`UNC\`)) {
		return errors.New("resolved directory must not use a Windows network path")
	}
	if len(path) < 7 || !strings.EqualFold(path[:4], extendedPathPrefix) ||
		path[5] != ':' || path[6] != '\\' || !asciiLetter(path[4]) {
		return fmt.Errorf("resolved directory uses an unsupported Windows volume path %q", path)
	}
	root := path[4:7]
	switch driveType(root) {
	case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK:
		return nil
	case windows.DRIVE_REMOTE:
		return errors.New("resolved directory must not use a mapped Windows network drive")
	default:
		return errors.New("resolved directory volume must provide writable local filesystem semantics")
	}
}

func windowsDriveType(root string) uint32 {
	rootPtr, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return windows.DRIVE_UNKNOWN
	}
	return windows.GetDriveType(rootPtr)
}

func asciiLetter(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}
