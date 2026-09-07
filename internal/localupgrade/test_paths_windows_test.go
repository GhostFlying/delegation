//go:build windows

package localupgrade

import (
	"os"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func assertProtectedUpgradeTestFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("protected upgrade file %s = %#v, %v", path, info, err)
	}
	if err := delegationconfig.ValidateProtectedFile(path); err != nil {
		t.Fatalf("validate protected upgrade file %s: %v", path, err)
	}
}
