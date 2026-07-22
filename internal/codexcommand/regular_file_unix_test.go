//go:build linux || darwin

package codexcommand

import (
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestReadBoundedFileRejectsFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "package.json")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := readBoundedFile(path)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("readBoundedFile accepted a FIFO")
		}
	case <-time.After(time.Second):
		t.Fatal("readBoundedFile blocked opening a FIFO")
	}
}
