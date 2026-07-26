package rolloutcapture

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

const testThreadID = "123e4567-e89b-42d3-a456-426614174800"

func TestLocateManagedRollout(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(
		home, "sessions", "2026", "07", "26", "rollout-2026-07-26T00-00-00-"+testThreadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("rollout\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Locate(home, testThreadID, path)
	if err != nil {
		t.Fatal(err)
	}
	want := Locator{Path: path, Offset: 8}
	if got != want {
		t.Fatalf("locator = %#v, want %#v", got, want)
	}
}

func TestOpenValidatedKeepsValidatedHandleAfterPathReplacement(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(
		home, "sessions", "rollout-2026-07-26T00-00-00-"+testThreadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("validated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, locator, err := OpenValidated(home, testThreadID, path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := os.Rename(path, path+".replaced"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "validated\n" || locator.Path != path || locator.Offset != int64(len(data)) {
		t.Fatalf("opened rollout = %q, %#v", data, locator)
	}
}

func TestLocateRejectsOutsideOrMismatchedRollout(t *testing.T) {
	home := t.TempDir()
	outside := filepath.Join(t.TempDir(), "rollout-2026-07-26T00-00-00-"+testThreadID+".jsonl")
	if err := os.WriteFile(outside, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	archived := filepath.Join(home, "archived_sessions", "rollout-2026-07-26T00-00-00-"+testThreadID+".jsonl")
	wrongThread := filepath.Join(home, "sessions", "rollout-2026-07-26T00-00-00-123e4567-e89b-42d3-a456-426614174801.jsonl")
	for _, path := range []string{archived, wrongThread} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{outside, archived, wrongThread} {
		if _, err := Locate(home, testThreadID, path); err == nil {
			t.Fatalf("Locate accepted %s", path)
		}
	}
}

func TestLocateRejectsSymbolicLink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires optional Windows privileges")
	}
	home := t.TempDir()
	realDirectory := filepath.Join(home, "real")
	if err := os.MkdirAll(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	realPath := filepath.Join(realDirectory, "rollout-2026-07-26T00-00-00-"+testThreadID+".jsonl")
	if err := os.WriteFile(realPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	sessions := filepath.Join(home, "sessions")
	if err := os.Symlink(realDirectory, sessions); err != nil {
		t.Fatal(err)
	}
	if _, err := Locate(home, testThreadID, filepath.Join(sessions, filepath.Base(realPath))); err == nil {
		t.Fatal("Locate accepted a symbolic-link component")
	}
}

func TestLocateDoesNotTreatBackslashAsUnixSeparator(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("backslash is a path separator on Windows")
	}
	home := t.TempDir()
	path := filepath.Join(
		home,
		"sessions\\sibling",
		"rollout-2026-07-26T00-00-00-"+testThreadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Locate(home, testThreadID, path); err == nil {
		t.Fatal("Locate accepted a Unix directory whose name only begins with sessions")
	}
}
