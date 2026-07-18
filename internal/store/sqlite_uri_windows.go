//go:build windows

package store

import (
	"errors"
	"path/filepath"
	"strings"
)

func validateLocalStatePath(path string) error {
	if strings.HasPrefix(filepath.VolumeName(path), `\\`) {
		return errors.New("broker state must not use a Windows network path")
	}
	return nil
}

func sqliteURIPath(path string) string {
	path = filepath.ToSlash(path)
	if volume := filepath.VolumeName(path); len(volume) == 2 && volume[1] == ':' {
		return "/" + path
	}
	return path
}
