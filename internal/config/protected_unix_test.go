//go:build linux || darwin

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProtectedConfigRejectsSharedDirectoryAndSymlink(t *testing.T) {
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	sharedPath := filepath.Join(shared, "config.json")
	if err := os.WriteFile(sharedPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(sharedPath); err == nil {
		t.Fatal("Read accepted a config in a shared directory")
	}
	if err := WriteNew(sharedPath+".new", protectedTestConfig(t)); err == nil {
		t.Fatal("WriteNew accepted a shared config directory")
	}

	private := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(private, "target.json")
	if err := os.WriteFile(target, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(private, "config.json")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	if _, err := Read(alias); err == nil {
		t.Fatal("Read accepted a symbolic-link config")
	}
}

func TestConfigHandleRejectsUnsafeReplacementAfterPathValidation(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	originalHook := afterConfigPathValidation
	t.Cleanup(func() { afterConfigPathValidation = originalHook })
	afterConfigPathValidation = func() {
		if err := os.Rename(directory, directory+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o777); err != nil {
			t.Fatal(err)
		}
	}
	if root, err := holdConfigDirectory(directory); err == nil {
		root.Close()
		t.Fatal("holdConfigDirectory accepted an unsafe replacement")
	}
}
