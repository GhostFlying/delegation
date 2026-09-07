//go:build linux || darwin

package traexauth

import (
	"os"
	"testing"
)

func makeAuthFileUnsafe(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
}

func unsafeAuthFileError() string {
	return "mode 0600"
}
