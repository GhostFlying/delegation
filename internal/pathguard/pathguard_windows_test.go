//go:build windows

package pathguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateBrokerAuthorityRejectsRootRelativeParentSymlink(t *testing.T) {
	target := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	rootRelative := strings.TrimPrefix(target, filepath.VolumeName(target))
	if err := os.Symlink(rootRelative, alias); err != nil {
		t.Skipf("creating a Windows directory symlink is unavailable: %v", err)
	}
	authority := filepath.Join(target, "authority")
	err := ValidateBrokerAuthority(
		filepath.Join(alias, "authority"),
		filepath.Join(t.TempDir(), "state", "broker.sqlite3"),
		authority,
	)
	if err == nil || !strings.Contains(err.Error(), "master token") {
		t.Fatalf("ValidateBrokerAuthority() error = %v", err)
	}
}
