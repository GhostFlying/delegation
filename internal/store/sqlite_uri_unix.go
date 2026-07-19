//go:build !windows

package store

import "path/filepath"

func sqliteURIPath(path string) string {
	return filepath.ToSlash(path)
}
