package codexconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/hostkind"
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

func TestValidateManagedRuntimeHomeAppliesHostContentPolicy(t *testing.T) {
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte("managed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManagedRuntimeHome(hostkind.Codex, home); err == nil ||
		!strings.Contains(err.Error(), "config.toml") {
		t.Fatalf("Codex managed runtime home error = %v", err)
	}
	if err := ValidateManagedRuntimeHome(hostkind.TraeX, home); err != nil {
		t.Fatalf("TraeX managed runtime home error = %v", err)
	}
	if err := ValidateManagedRuntimeHome(hostkind.Kind("unsupported"), home); err == nil {
		t.Fatal("unsupported managed runtime host kind was accepted")
	}
}
