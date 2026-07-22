//go:build linux || darwin

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareAndValidatePrivateDirectoryModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := PreparePrivateDirectory(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("private directory mode = %o", info.Mode().Perm())
	}
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePrivateDirectory(path); err == nil {
		t.Fatal("ValidatePrivateDirectory accepted permission drift")
	}
}

func TestPreparePrivateDirectoryRejectsSymlinkWithoutChangingTarget(t *testing.T) {
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0o755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	if err := PreparePrivateDirectory(alias); err == nil {
		t.Fatal("PreparePrivateDirectory accepted a symbolic link")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("rejected symlink changed target mode to %o", info.Mode().Perm())
	}
}
