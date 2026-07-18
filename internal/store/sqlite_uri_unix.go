//go:build !windows

package store

import "path/filepath"

func validateLocalStatePath(string) error {
	return nil
}

func sqliteURIPath(path string) string {
	return filepath.ToSlash(path)
}
