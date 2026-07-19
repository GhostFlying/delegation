//go:build windows

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestProtectedConfigRejectsSharedWindowsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := WriteNew(path, protectedTestConfig(t)); err == nil {
		t.Fatal("WriteNew accepted an inherited shared Windows directory")
	}
}

func TestProtectedConfigAcceptsSafeWindowsFileInheritance(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := createConfigDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "config.json")
	data, err := json.Marshal(protectedTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err != nil {
		t.Fatalf("Read rejected safe inherited DACL: %v", err)
	}
}
