//go:build windows

package tokenfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureCreatesTokenWithCurrentUserOnlyDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "token")
	if _, err := Ensure(path); err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err != nil {
		t.Fatal(err)
	}
	if err := validateTokenDirectory(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
}

func TestWriteNewRejectsSharedTokenDirectory(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "token")
	if _, err := WriteNew(path, Token{1}); err == nil {
		t.Fatal("WriteNew accepted an inherited shared token directory")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("rejected token path exists: %v", err)
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
	target := filepath.Join(dir, "private", "target")
	link := filepath.Join(dir, "token-link")
	if _, err := Ensure(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("creating symlink requires unavailable Windows privileges: %v", err)
	}
	if _, err := Read(link); err == nil {
		t.Fatal("Read() accepted a reparse point")
	}
}
