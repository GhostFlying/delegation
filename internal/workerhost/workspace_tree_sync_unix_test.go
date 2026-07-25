//go:build !windows

package workerhost

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestSyncWorkspaceTreeRejectsIrregularEntries(t *testing.T) {
	directory := t.TempDir()
	pipe := filepath.Join(directory, "unexpected-pipe")
	if err := syscall.Mkfifo(pipe, 0o600); err != nil {
		t.Skipf("creating a FIFO is unavailable: %v", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := syncWorkspaceTree(root); err == nil || !strings.Contains(err.Error(), "unsupported file type") {
		t.Fatalf("syncWorkspaceTree() = %v, want irregular-entry rejection", err)
	}
}
