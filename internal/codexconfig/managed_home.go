package codexconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/hostkind"
)

var forbiddenManagedHomeEntries = []string{
	".env",
	"AGENTS.md",
	"AGENTS.override.md",
	"auth.json",
	"config.toml",
	"managed_config.toml",
}

// ValidateManagedHome rejects account and user configuration artifacts that a
// managed app-server must never load from its isolated CODEX_HOME.
func ValidateManagedHome(path string) error {
	for _, name := range forbiddenManagedHomeEntries {
		candidate := filepath.Join(path, name)
		if _, err := os.Lstat(candidate); err == nil {
			return fmt.Errorf("managed CODEX_HOME must not contain %s", name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect managed CODEX_HOME %s: %w", name, err)
		}
	}
	return nil
}

// ValidateManagedRuntimeHome applies the content policy for the CLI that owns
// an isolated managed runtime home.
func ValidateManagedRuntimeHome(kind hostkind.Kind, path string) error {
	if err := kind.Validate(); err != nil {
		return err
	}
	if kind == hostkind.Codex {
		return ValidateManagedHome(path)
	}
	return nil
}
