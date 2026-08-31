package codexconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/securefs"
)

var forbiddenManagedHomeEntries = []string{
	".env",
	"AGENTS.md",
	"AGENTS.override.md",
	"auth.json",
	"config.toml",
	"managed_config.toml",
}

var forbiddenManagedTraeHomeEntries = []string{
	".env",
	"AGENTS.md",
	"AGENTS.override.md",
	"auth.json",
	"config.toml",
	"hooks",
	"hooks.json",
	"managed_config.toml",
	"model-provider",
	"plugins",
	"rules",
	"traecli.toml",
	"traecli.yaml",
}

// ValidateManagedHome rejects account and user configuration artifacts that a
// managed app-server must never load from its isolated CODEX_HOME.
func ValidateManagedHome(path string) error {
	return validateManagedHomeEntries(path, "CODEX_HOME", forbiddenManagedHomeEntries)
}

func validateManagedHomeEntries(path, homeName string, entries []string) error {
	for _, entry := range entries {
		candidate := filepath.Join(path, entry)
		if _, err := os.Lstat(candidate); err == nil {
			return fmt.Errorf("managed %s must not contain %s", homeName, entry)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect managed %s %s: %w", homeName, entry, err)
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
	switch kind {
	case hostkind.Codex:
		return ValidateManagedHome(path)
	case hostkind.TraeX:
		if err := validateManagedTraeHome(path, "TRAE_HOME"); err != nil {
			return err
		}
		return validateManagedTraeHome(filepath.Join(path, "cli"), "TRAECLI_HOME")
	default:
		return fmt.Errorf("unsupported host kind %q", kind)
	}
}

func validateManagedTraeHome(path, homeName string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect managed %s: %w", homeName, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("managed %s must not be a symbolic link", homeName)
	}
	if !info.IsDir() {
		return fmt.Errorf("managed %s must be a directory", homeName)
	}
	if err := validateManagedHomeEntries(path, homeName, forbiddenManagedTraeHomeEntries); err != nil {
		return err
	}
	if err := validateManagedTraeProfiles(path, homeName); err != nil {
		return err
	}
	return validateManagedTraeSkills(path, homeName)
}

// TraeXRepairEntries returns the managed-home-relative entries that must be
// quarantined before a TraeX worker can run with isolated configuration.
// Ancestors are returned instead of descendants when the ancestor itself has
// an invalid type, so callers can move every result exactly once.
func TraeXRepairEntries(root *securefs.Root) ([]string, error) {
	var result []string
	if root == nil {
		return nil, errors.New("managed TraeX home root is required")
	}
	if err := collectTraeXRepairEntries(root, "", &result); err != nil {
		return nil, err
	}
	info, err := root.Lstat("cli")
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, fmt.Errorf("inspect managed TRAECLI_HOME: %w", err)
	case info.Mode()&os.ModeSymlink != 0 || !info.IsDir():
		result = append(result, "cli")
	default:
		cliRoot, err := root.OpenRoot("cli", nil)
		if err != nil {
			return nil, fmt.Errorf("hold managed TRAECLI_HOME: %w", err)
		}
		if err := collectTraeXRepairEntries(cliRoot, "cli", &result); err != nil {
			_ = cliRoot.Close()
			return nil, err
		}
		if err := cliRoot.Close(); err != nil {
			return nil, err
		}
	}
	slices.Sort(result)
	return result, nil
}

func collectTraeXRepairEntries(root *securefs.Root, prefix string, result *[]string) error {
	for _, entry := range forbiddenManagedTraeHomeEntries {
		if _, err := root.Lstat(entry); err == nil {
			*result = append(*result, filepath.Join(prefix, entry))
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect managed TraeX home entry %s: %w", entry, err)
		}
	}
	entries, err := root.Entries()
	if err != nil {
		return fmt.Errorf("inspect managed TraeX home: %w", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".traecli.toml") {
			*result = append(*result, filepath.Join(prefix, entry.Name()))
		}
	}
	info, err := root.Lstat("skills")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect managed TraeX skills: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		*result = append(*result, filepath.Join(prefix, "skills"))
		return nil
	}
	skillsRoot, err := root.OpenRoot("skills", nil)
	if err != nil {
		return fmt.Errorf("hold managed TraeX skills: %w", err)
	}
	defer skillsRoot.Close()
	skills, err := skillsRoot.Entries()
	if err != nil {
		return fmt.Errorf("inspect managed TraeX skills: %w", err)
	}
	for _, entry := range skills {
		if entry.Name() != ".system" || !entry.IsDir() {
			*result = append(*result, filepath.Join(prefix, "skills", entry.Name()))
		}
	}
	return nil
}

func validateManagedTraeProfiles(path, homeName string) error {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect managed %s profiles: %w", homeName, err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".traecli.toml") {
			return fmt.Errorf("managed %s must not contain %s", homeName, entry.Name())
		}
	}
	return nil
}

func validateManagedTraeSkills(path, homeName string) error {
	skillsPath := filepath.Join(path, "skills")
	info, err := os.Lstat(skillsPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect managed %s skills: %w", homeName, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("managed %s skills must be a directory", homeName)
	}
	entries, err := os.ReadDir(skillsPath)
	if err != nil {
		return fmt.Errorf("inspect managed %s skills: %w", homeName, err)
	}
	for _, entry := range entries {
		if entry.Name() != ".system" {
			return fmt.Errorf("managed %s must not contain skills/%s", homeName, entry.Name())
		}
		if !entry.IsDir() {
			return fmt.Errorf("managed %s skills/.system must be a directory", homeName)
		}
	}
	return nil
}
