//go:build linux || darwin

package tokenfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteNewRejectsSharedTokenDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteNew(filepath.Join(directory, "device.token"), Token{1}); err == nil {
		t.Fatal("WriteNew accepted a shared token directory")
	}
}

func TestTokenHandleRejectsUnsafeReplacementAfterPathValidation(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	originalHook := afterTokenPathValidation
	t.Cleanup(func() { afterTokenPathValidation = originalHook })
	afterTokenPathValidation = func() {
		if err := os.Rename(directory, directory+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if root, err := holdTokenDirectory(directory); err == nil {
		root.Close()
		t.Fatal("holdTokenDirectory accepted an unsafe replacement")
	}
}
