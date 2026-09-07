package localupgrade

import (
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func privateUpgradeTestDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	return directory
}
