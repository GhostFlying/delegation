//go:build !windows

package localupgrade

import (
	"os"
	"testing"
)

func assertProtectedUpgradeTestFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("protected upgrade file %s = %#v, %v", path, info, err)
	}
}
