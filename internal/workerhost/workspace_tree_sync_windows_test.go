//go:build windows

package workerhost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncWorkspaceTreeFlushesAndRestoresReadOnlyFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "readonly.txt")
	if err := os.WriteFile(path, []byte("content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := syncWorkspaceTree(root); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o200 != 0 {
		t.Fatalf("read-only file mode after sync = %v", info.Mode().Perm())
	}
}
