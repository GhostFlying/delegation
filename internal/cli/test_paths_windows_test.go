//go:build windows

package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func unsafeTestDirectory(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func replaceProtectedTestFile(t *testing.T, path string, original, replacement []byte) {
	t.Helper()
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(current, original) {
		t.Fatal("protected test file changed before fixture replacement")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(replacement); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := delegationconfig.ValidateProtectedFile(path); err != nil {
		t.Fatalf("validate replaced protected test file: %v", err)
	}
}
