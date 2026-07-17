//go:build linux || darwin

package tokenfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateRejectsBroadUnixPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err == nil {
		t.Fatal("Validate() accepted group-readable token file")
	}
}

func TestValidateRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "token")
	if err := os.WriteFile(target, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := Validate(link); err == nil {
		t.Fatal("Validate() accepted a token symlink")
	}
}

func TestOpenSecureReadDoesNotFollowSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "token")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	file, err := openSecureRead(link)
	if err == nil {
		file.Close()
		t.Fatal("openSecureRead() followed a symlink")
	}
}
