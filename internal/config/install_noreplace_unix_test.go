//go:build linux || darwin

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNoReplaceRenameDoesNotReplaceDestination(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old")
	newPath := filepath.Join(dir, "new")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := noReplaceRename(oldPath, newPath); err == nil {
		t.Fatal("noReplaceRename() replaced an existing destination")
	}
	data, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Fatalf("destination data = %q, want new", data)
	}
}
