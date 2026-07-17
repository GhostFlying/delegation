//go:build windows

package tokenfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureCreatesTokenWithCurrentUserOnlyDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if _, err := Ensure(path); err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsInheritedWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err == nil {
		t.Fatal("Validate() accepted an inherited Windows DACL")
	}
}

func TestOpenSecureReadRejectsReparsePoint(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "token-link")
	if _, err := Ensure(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("creating symlink requires unavailable Windows privileges: %v", err)
	}
	file, err := openSecureRead(link)
	if err != nil {
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFilePermissions(file, info); err == nil {
		t.Fatal("validateFilePermissions() accepted a reparse point")
	}
}
