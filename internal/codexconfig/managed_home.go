package codexconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
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
	"config.toml",
	"hooks",
	"hooks.json",
	"managed_config.toml",
	"rules",
	"traecli.yaml",
}

var forbiddenManagedTraeCLIHomeEntries = []string{
	".env",
	"AGENTS.md",
	"AGENTS.override.md",
	"config.toml",
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
		if err := validateManagedTraeHome(path, "TRAE_HOME", false); err != nil {
			return err
		}
		return validateManagedTraeHome(filepath.Join(path, "cli"), "TRAECLI_HOME", true)
	default:
		return fmt.Errorf("unsupported host kind %q", kind)
	}
}

func validateManagedTraeHome(path, homeName string, allowManagedAuth bool) error {
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
	forbidden := forbiddenManagedTraeHomeEntries
	if allowManagedAuth {
		forbidden = forbiddenManagedTraeCLIHomeEntries
	}
	if err := validateManagedHomeEntries(path, homeName, forbidden); err != nil {
		return err
	}
	if !allowManagedAuth {
		if err := validateManagedHomeEntries(path, homeName, []string{"auth.json"}); err != nil {
			return err
		}
		root, err := securefs.OpenRoot(path, nil)
		if err != nil {
			return fmt.Errorf("hold managed %s: %w", homeName, err)
		}
		defer root.Close()
		if err := validateGeneratedTraeArtifacts(root); err != nil {
			return fmt.Errorf("managed %s %w", homeName, err)
		}
		if err := validateGeneratedTraeConfig(root); err != nil {
			return fmt.Errorf("managed %s %w", homeName, err)
		}
	} else {
		root, err := securefs.OpenRoot(path, nil)
		if err != nil {
			return fmt.Errorf("hold managed %s: %w", homeName, err)
		}
		defer root.Close()
		if err := validateGeneratedTraeCLIArtifacts(root); err != nil {
			return fmt.Errorf("managed %s %w", homeName, err)
		}
	}
	if err := validateManagedTraeProfiles(path, homeName, !allowManagedAuth); err != nil {
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
	forbidden := forbiddenManagedTraeHomeEntries
	if prefix != "" {
		forbidden = forbiddenManagedTraeCLIHomeEntries
	}
	for _, entry := range forbidden {
		if _, err := root.Lstat(entry); err == nil {
			*result = append(*result, filepath.Join(prefix, entry))
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect managed TraeX home entry %s: %w", entry, err)
		}
	}
	if prefix == "" {
		if _, err := root.Lstat("auth.json"); err == nil {
			*result = append(*result, "auth.json")
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect managed TraeX home entry auth.json: %w", err)
		}
		for _, artifact := range []struct {
			name     string
			validate func(*securefs.Root) error
		}{
			{name: "model-provider", validate: validateGeneratedTraeModelCatalog},
			{name: "plugins", validate: validateGeneratedTraePluginCache},
		} {
			if err := artifact.validate(root); err != nil {
				if _, statErr := root.Lstat(artifact.name); statErr == nil {
					*result = append(*result, artifact.name)
				} else if !errors.Is(statErr, os.ErrNotExist) {
					return fmt.Errorf("inspect managed TraeX home entry %s: %w", artifact.name, statErr)
				}
			}
		}
		if err := validateGeneratedTraeConfig(root); err != nil {
			if _, statErr := root.Lstat("traecli.toml"); statErr == nil {
				*result = append(*result, "traecli.toml")
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return fmt.Errorf("inspect managed TraeX home entry traecli.toml: %w", statErr)
			}
		}
	} else if err := validateGeneratedTraeCLIArtifacts(root); err != nil {
		if _, statErr := root.Lstat("hooks"); statErr == nil {
			*result = append(*result, filepath.Join(prefix, "hooks"))
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("inspect managed TraeX home entry hooks: %w", statErr)
		}
	}
	entries, err := root.Entries()
	if err != nil {
		return fmt.Errorf("inspect managed TraeX home: %w", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".traecli.toml") &&
			!(prefix == "" && strings.EqualFold(entry.Name(), "traecli.toml")) {
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

func validateGeneratedTraeArtifacts(root *securefs.Root) error {
	if err := validateGeneratedTraeModelCatalog(root); err != nil {
		return err
	}
	return validateGeneratedTraePluginCache(root)
}

func validateGeneratedTraeModelCatalog(root *securefs.Root) error {
	provider, found, err := openOptionalGeneratedDirectory(root, "model-provider")
	if err != nil || !found {
		return err
	}
	defer provider.Close()
	if err := requireOnlyGeneratedEntries(provider, "model-provider", "trae"); err != nil {
		return err
	}
	trae, found, err := openOptionalGeneratedDirectory(provider, "trae")
	if err != nil || !found {
		return err
	}
	defer trae.Close()
	return validateGeneratedTraeTree(trae, "model-provider/trae")
}

func validateGeneratedTraePluginCache(root *securefs.Root) error {
	plugins, found, err := openOptionalGeneratedDirectory(root, "plugins")
	if err != nil || !found {
		return err
	}
	defer plugins.Close()
	if err := requireOnlyGeneratedEntries(plugins, "plugins", "cache"); err != nil {
		return err
	}
	cache, found, err := openOptionalGeneratedDirectory(plugins, "cache")
	if err != nil || !found {
		return err
	}
	defer cache.Close()
	if err := requireOnlyGeneratedEntries(cache, "plugins/cache", "traex-bd-plugins"); err != nil {
		return err
	}
	marketplace, found, err := openOptionalGeneratedDirectory(cache, "traex-bd-plugins")
	if err != nil || !found {
		return err
	}
	defer marketplace.Close()
	return validateGeneratedTraeTree(marketplace, "plugins/cache/traex-bd-plugins")
}

func validateGeneratedTraeCLIArtifacts(root *securefs.Root) error {
	hooks, found, err := openOptionalGeneratedDirectory(root, "hooks")
	if err != nil || !found {
		return err
	}
	defer hooks.Close()
	if err := requireOnlyGeneratedEntries(hooks, "hooks", "commit-attribution"); err != nil {
		return err
	}
	attribution, found, err := openOptionalGeneratedDirectory(hooks, "commit-attribution")
	if err != nil || !found {
		return err
	}
	defer attribution.Close()
	return validateGeneratedTraeTree(attribution, "hooks/commit-attribution")
}

func validateGeneratedTraeConfig(root *securefs.Root) error {
	info, err := root.Lstat("traecli.toml")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect traecli.toml: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return errors.New("traecli.toml must be a bounded regular file")
	}
	file, err := root.OpenFile("traecli.toml", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("read traecli.toml: %w", err)
	}
	defer file.Close()
	var document struct {
		Marketplaces map[string]map[string]any `toml:"marketplaces"`
	}
	metadata, err := toml.NewDecoder(file).Decode(&document)
	if err != nil || len(metadata.Undecoded()) != 0 {
		return errors.New("traecli.toml must contain valid marketplace metadata")
	}
	if len(document.Marketplaces) != 1 {
		return errors.New("traecli.toml may contain only the Trae built-in marketplace")
	}
	marketplace, ok := document.Marketplaces["traex-bd-plugins"]
	if !ok {
		return errors.New("traecli.toml may contain only the Trae built-in marketplace")
	}
	allowed := []string{
		"auto_update", "last_revision", "last_updated", "ref", "source", "source_type",
	}
	for key := range marketplace {
		if !slices.Contains(allowed, key) {
			return fmt.Errorf("traecli.toml marketplace metadata contains unsupported key %s", key)
		}
	}
	return nil
}

func openOptionalGeneratedDirectory(
	root *securefs.Root, name string,
) (*securefs.Root, bool, error) {
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, false, fmt.Errorf("generated TraeX entry %s must be a directory", name)
	}
	directory, err := root.OpenRoot(name, nil)
	return directory, err == nil, err
}

func requireOnlyGeneratedEntries(root *securefs.Root, path string, allowed ...string) error {
	entries, err := root.Entries()
	if err != nil {
		return fmt.Errorf("inspect generated TraeX %s: %w", path, err)
	}
	for _, entry := range entries {
		if !slices.Contains(allowed, entry.Name()) {
			return fmt.Errorf("generated TraeX %s contains unsupported entry %s", path, entry.Name())
		}
	}
	return nil
}

func validateGeneratedTraeTree(root *securefs.Root, path string) error {
	entries, err := root.Entries()
	if err != nil {
		return fmt.Errorf("inspect generated TraeX %s: %w", path, err)
	}
	for _, entry := range entries {
		childPath := filepath.Join(path, entry.Name())
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("generated TraeX %s must not contain symbolic links", childPath)
		}
		if entry.IsDir() {
			child, err := root.OpenRoot(entry.Name(), nil)
			if err != nil {
				return err
			}
			err = validateGeneratedTraeTree(child, childPath)
			closeErr := child.Close()
			if err != nil || closeErr != nil {
				return errors.Join(err, closeErr)
			}
			continue
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("generated TraeX %s must contain only regular files and directories", childPath)
		}
	}
	return nil
}

// ResetGeneratedTraeRuntimeArtifacts drops only the reserved caches that TraeX
// recreates before each app-server can accept managed work.
func ResetGeneratedTraeRuntimeArtifacts(path string) error {
	root, err := securefs.OpenRoot(path, nil)
	if err != nil {
		return fmt.Errorf("hold managed TRAE_HOME: %w", err)
	}
	defer root.Close()
	if err := validateGeneratedTraeArtifacts(root); err != nil {
		return err
	}
	if err := validateGeneratedTraeConfig(root); err != nil {
		return err
	}
	var cliRoot *securefs.Root
	cliInfo, err := root.Lstat("cli")
	if err == nil {
		if !cliInfo.IsDir() || cliInfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("managed TRAECLI_HOME must be a directory, not a symbolic link")
		}
		cliRoot, err = root.OpenRoot("cli", nil)
		if err != nil {
			return err
		}
		defer cliRoot.Close()
		if err := validateGeneratedTraeCLIArtifacts(cliRoot); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect managed TRAECLI_HOME: %w", err)
	}
	for _, name := range []string{"model-provider", "plugins", "traecli.toml"} {
		if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("inspect generated TraeX %s: %w", name, err)
		}
		if err := root.RemoveAll(name); err != nil {
			return fmt.Errorf("remove generated TraeX %s: %w", name, err)
		}
	}
	if cliRoot != nil {
		if _, err := cliRoot.Lstat("hooks"); err == nil {
			if err := cliRoot.RemoveAll("hooks"); err != nil {
				return fmt.Errorf("remove generated TraeX CLI hooks: %w", err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect generated TraeX CLI hooks: %w", err)
		}
		if err := cliRoot.Sync(); err != nil {
			return err
		}
	}
	if err := root.Sync(); err != nil {
		return fmt.Errorf("sync managed TRAE_HOME after cache reset: %w", err)
	}
	return root.VerifyPath()
}

func validateManagedTraeProfiles(path, homeName string, allowGeneratedConfig bool) error {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect managed %s profiles: %w", homeName, err)
	}
	for _, entry := range entries {
		if allowGeneratedConfig && strings.EqualFold(entry.Name(), "traecli.toml") {
			continue
		}
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
