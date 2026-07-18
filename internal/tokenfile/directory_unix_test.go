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
