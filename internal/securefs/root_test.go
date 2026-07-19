package securefs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishNoReplacePreservesDestination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for name, value := range map[string]string{"temporary": "new", "destination": "old"} {
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString(value); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if committed, err := root.PublishNoReplace("temporary", "destination"); committed || !errors.Is(err, os.ErrExist) {
		t.Fatalf("PublishNoReplace() = %v, %v; want false, os.ErrExist", committed, err)
	}
	data, err := os.ReadFile(filepath.Join(path, "destination"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old" {
		t.Fatalf("destination = %q, want old", data)
	}
}

func TestPublishNoReplaceAtomicallyMovesTemporaryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	file, err := root.OpenFile("temporary", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("value"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if committed, err := root.PublishNoReplace("temporary", "destination"); err != nil || !committed {
		t.Fatalf("PublishNoReplace() = %v, %v", committed, err)
	}
	if _, err := root.Lstat("temporary"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary name remains after publish: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(path, "destination"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "value" {
		t.Fatalf("destination = %q, want value", data)
	}
}
