//go:build windows

package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func unsafeTestDirectory(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}
