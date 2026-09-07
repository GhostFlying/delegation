//go:build linux || darwin

package cli

import (
	"os"
	"path/filepath"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func unsafeTestDirectory(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	return dir
}

func replaceProtectedTestFile(t *testing.T, path string, original, replacement []byte) {
	t.Helper()
	if err := delegationconfig.ReplaceProtectedFile(path, original, replacement); err != nil {
		t.Fatal(err)
	}
}
