package codexconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
