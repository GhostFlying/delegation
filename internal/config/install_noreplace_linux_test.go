//go:build linux

package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNoReplaceRenameFallsBackToHardLink(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old")
	newPath := filepath.Join(dir, "new")
	if err := os.WriteFile(oldPath, []byte("config"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalRename := linuxRenameNoReplace
	t.Cleanup(func() { linuxRenameNoReplace = originalRename })
	linuxRenameNoReplace = func(string, string) error { return unix.ENOSYS }

	if err := noReplaceRename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("temporary source still exists: %v", err)
	}
	data, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "config" {
		t.Fatalf("installed data = %q, want config", data)
	}
}

func TestNoReplaceRenameFailsClosedWithoutFallback(t *testing.T) {
	originalRename := linuxRenameNoReplace
	originalLink := linuxLinkNoReplace
	t.Cleanup(func() {
		linuxRenameNoReplace = originalRename
		linuxLinkNoReplace = originalLink
	})
	linuxRenameNoReplace = func(string, string) error { return unix.EOPNOTSUPP }
	linuxLinkNoReplace = func(string, string) error { return errors.New("unsupported") }

	err := noReplaceRename("old", "new")
	if err == nil || !strings.Contains(err.Error(), "lacks atomic no-replace") {
		t.Fatalf("noReplaceRename() error = %v", err)
	}
}

func TestNoReplaceRenameFallbackDoesNotReplaceDestination(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old")
	newPath := filepath.Join(dir, "new")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalRename := linuxRenameNoReplace
	t.Cleanup(func() { linuxRenameNoReplace = originalRename })
	linuxRenameNoReplace = func(string, string) error { return unix.ENOSYS }

	if err := noReplaceRename(oldPath, newPath); err == nil {
		t.Fatal("noReplaceRename() fallback replaced an existing destination")
	}
	data, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Fatalf("destination data = %q, want new", data)
	}
}

func TestNoReplaceRenameReportsCommittedCleanupFailure(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old")
	newPath := filepath.Join(dir, "new")
	if err := os.WriteFile(oldPath, []byte("config"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalRename := linuxRenameNoReplace
	originalRemove := linuxRemoveSource
	t.Cleanup(func() {
		linuxRenameNoReplace = originalRename
		linuxRemoveSource = originalRemove
	})
	linuxRenameNoReplace = func(string, string) error { return unix.ENOSYS }
	linuxRemoveSource = func(string) error { return errors.New("injected remove failure") }

	err := noReplaceRename(oldPath, newPath)
	if !IsCommitted(err) {
		t.Fatalf("noReplaceRename() error = %v, want committed error", err)
	}
	data, readErr := os.ReadFile(newPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(data) != "config" {
		t.Fatalf("installed data = %q, want config", data)
	}
}
