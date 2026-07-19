package cli

import (
	"path/filepath"
	"testing"
)

func privateTestDirectory(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "private")
}

func privateTestPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(privateTestDirectory(t), name)
}
