//go:build !windows

package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenRejectsExistingSharedDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), filepath.Join(directory, "broker.sqlite3")); err == nil {
		t.Fatal("Open accepted a group-readable state directory")
	}
}
