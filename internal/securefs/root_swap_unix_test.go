//go:build linux || darwin

package securefs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRootRemainsBoundAfterDirectoryReplacement(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "authority")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	original := path + ".original"
	if err := os.Rename(path, original); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := root.OpenFile("secret", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("secret"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(original, "secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(path, "secret")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement directory received secret: %v", err)
	}
	if err := root.VerifyPath(); err == nil {
		t.Fatal("VerifyPath accepted a replaced directory")
	}
}
