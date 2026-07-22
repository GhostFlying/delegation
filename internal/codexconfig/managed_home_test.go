package codexconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateManagedHomeRejectsUserConfigurationAndAuth(t *testing.T) {
	for _, name := range forbiddenManagedHomeEntries {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			if err := os.WriteFile(filepath.Join(home, name), []byte("ambient"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateManagedHome(home); err == nil {
				t.Fatalf("ValidateManagedHome accepted %s", name)
			}
		})
	}
}
