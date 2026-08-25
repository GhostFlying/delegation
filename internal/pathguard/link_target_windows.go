//go:build windows

package pathguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

const (
	windowsNameSurrogateTag = 0x20000000
	windowsVolumeNameDOS    = 0
	windowsVolumeNameGUID   = 1
	windowsMaximumPath      = 32 * 1024
)

func init() {
	platformPathAliasTarget = windowsPathAliasTarget
	platformCanonicalExistingPath = windowsCanonicalExistingPath
}

func windowsPathAliasTarget(path string, info os.FileInfo) (string, bool, error) {
	if info.Mode()&os.ModeIrregular == 0 {
		return "", false, nil
	}
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", false, err
	}
	var data windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPointer, &data)
	if err != nil {
		return "", false, fmt.Errorf("inspect Windows reparse metadata: %w", err)
	}
	if err := windows.FindClose(handle); err != nil {
		return "", false, fmt.Errorf("close Windows reparse metadata: %w", err)
	}
	return windowsPathAliasTargetFromMetadata(
		path,
		info.Mode(),
		data.FileAttributes,
		data.Reserved0,
		os.Readlink,
	)
}

func windowsPathAliasTargetFromMetadata(
	path string,
	mode os.FileMode,
	attributes, reparseTag uint32,
	readLink func(string) (string, error),
) (string, bool, error) {
	if !windowsNameSurrogate(mode, attributes, reparseTag) {
		return "", false, nil
	}
	target, err := readLink(path)
	return target, true, err
}

func windowsNameSurrogate(mode os.FileMode, attributes, reparseTag uint32) bool {
	return mode&os.ModeIrregular != 0 &&
		attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 &&
		reparseTag&windowsNameSurrogateTag != 0
}

func windowsCanonicalExistingPath(path string) (string, error) {
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", err
	}
	handle, err := windows.CreateFile(
		pathPointer,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return "", fmt.Errorf("open existing path for volume identity: %w", err)
	}
	defer windows.CloseHandle(handle)
	return windowsCanonicalFinalPath(func(volumeName uint32) (string, error) {
		return windowsFinalPathByHandle(handle, volumeName)
	})
}

func windowsCanonicalFinalPath(
	resolveFinalPath func(volumeName uint32) (string, error),
) (string, error) {
	dosPath, err := resolveFinalPath(windowsVolumeNameDOS)
	if err != nil {
		return "", fmt.Errorf("resolve DOS final path: %w", err)
	}
	if strings.HasPrefix(strings.ToUpper(dosPath), `\\?\UNC\`) {
		return filepath.Clean(dosPath), nil
	}
	if !windowsDriveFinalPath(dosPath) && !windowsGUIDFinalPath(dosPath) {
		return "", fmt.Errorf("DOS final path has unsupported volume spelling %q", dosPath)
	}
	guidPath, err := resolveFinalPath(windowsVolumeNameGUID)
	if err != nil {
		return "", fmt.Errorf("resolve volume GUID final path: %w", err)
	}
	if !windowsGUIDFinalPath(guidPath) {
		return "", fmt.Errorf("volume GUID final path has unsupported spelling %q", guidPath)
	}
	return filepath.Clean(guidPath), nil
}

func windowsFinalPathByHandle(handle windows.Handle, volumeName uint32) (string, error) {
	buffer := make([]uint16, 256)
	for {
		length, err := windows.GetFinalPathNameByHandle(
			handle,
			&buffer[0],
			uint32(len(buffer)),
			volumeName,
		)
		if err != nil {
			return "", err
		}
		if length < uint32(len(buffer)) {
			return windows.UTF16ToString(buffer[:length]), nil
		}
		if length > windowsMaximumPath {
			return "", errors.New("Windows final path exceeds the path limit")
		}
		buffer = make([]uint16, length+1)
	}
}

func windowsDriveFinalPath(path string) bool {
	return len(path) >= 7 &&
		strings.EqualFold(path[:4], `\\?\`) &&
		asciiLetter(path[4]) &&
		path[5] == ':' &&
		os.IsPathSeparator(path[6])
}

func windowsGUIDFinalPath(path string) bool {
	const prefix = `\\?\Volume{`
	if len(path) <= len(prefix)+2 || !strings.EqualFold(path[:len(prefix)], prefix) {
		return false
	}
	closingBrace := strings.IndexByte(path[len(prefix):], '}')
	if closingBrace <= 0 {
		return false
	}
	separatorIndex := len(prefix) + closingBrace + 1
	return separatorIndex < len(path) && os.IsPathSeparator(path[separatorIndex])
}

func asciiLetter(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func resolveLinkTarget(parent, target string) (string, error) {
	if filepath.IsAbs(target) {
		return target, nil
	}
	if filepath.VolumeName(target) != "" {
		return "", errors.New("drive-relative symbolic-link targets are unsupported")
	}
	if len(target) > 0 && os.IsPathSeparator(target[0]) {
		volume := filepath.VolumeName(parent)
		if volume == "" {
			return "", errors.New("root-relative symbolic-link target has no parent volume")
		}
		return volume + target, nil
	}
	return filepath.Join(parent, target), nil
}
