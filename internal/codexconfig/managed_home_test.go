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
	for _, test := range []struct {
		name     string
		hostKind hostkind.Kind
		relative string
		want     string
	}{
		{
			name:     "Codex top-level configuration",
			hostKind: hostkind.Codex,
			relative: "config.toml",
			want:     "managed CODEX_HOME must not contain config.toml",
		},
		{
			name:     "TraeX top-level instructions",
			hostKind: hostkind.TraeX,
			relative: "AGENTS.md",
			want:     "managed TRAE_HOME must not contain AGENTS.md",
		},
		{
			name:     "TraeX top-level plugins",
			hostKind: hostkind.TraeX,
			relative: "plugins",
			want:     "managed TRAE_HOME must not contain plugins",
		},
		{
			name:     "TraeX CLI authentication",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("cli", "auth.json"),
			want:     "managed TRAECLI_HOME must not contain auth.json",
		},
		{
			name:     "TraeX CLI hooks",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("cli", "hooks.json"),
			want:     "managed TRAECLI_HOME must not contain hooks.json",
		},
		{
			name:     "TraeX CLI plugins",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("cli", "plugins"),
			want:     "managed TRAECLI_HOME must not contain plugins",
		},
		{
			name:     "TraeX CLI rules",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("cli", "rules"),
			want:     "managed TRAECLI_HOME must not contain rules",
		},
		{
			name:     "TraeX named profile",
			hostKind: hostkind.TraeX,
			relative: "custom.TRAECLI.TOML",
			want:     "managed TRAE_HOME must not contain custom.TRAECLI.TOML",
		},
		{
			name:     "TraeX CLI named profile",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("cli", "custom.traecli.toml"),
			want:     "managed TRAECLI_HOME must not contain custom.traecli.toml",
		},
		{
			name:     "TraeX user skill",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("skills", "custom"),
			want:     "managed TRAE_HOME must not contain skills/custom",
		},
		{
			name:     "TraeX CLI user skill",
			hostKind: hostkind.TraeX,
			relative: filepath.Join("cli", "skills", "custom"),
			want:     "managed TRAECLI_HOME must not contain skills/custom",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, test.relative)
			if filepath.Ext(path) == "" {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("managed"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := ValidateManagedRuntimeHome(test.hostKind, home); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateManagedRuntimeHome() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateManagedRuntimeHomeAllowsTraeXGeneratedArtifacts(t *testing.T) {
	home := t.TempDir()
	for _, directory := range []string{
		".tmp",
		filepath.Join("cli", "memories"),
		filepath.Join("cli", "skills", ".system"),
		filepath.Join("skills", ".system"),
	} {
		if err := os.MkdirAll(filepath.Join(home, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{
		"installation_id",
		"minimum_supported_version.json",
		filepath.Join("cli", "state_5.sqlite"),
		filepath.Join("cli", "state_5.sqlite-wal"),
		filepath.Join("cli", "state_5.sqlite-shm"),
	} {
		path := filepath.Join(home, file)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("managed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateManagedRuntimeHome(hostkind.TraeX, home); err != nil {
		t.Fatalf("TraeX generated managed runtime home error = %v", err)
	}
	if err := ValidateManagedRuntimeHome(hostkind.Kind("unsupported"), home); err == nil {
		t.Fatal("unsupported managed runtime host kind was accepted")
	}
}

func TestValidateManagedRuntimeHomeRejectsAllTraeXForbiddenEntries(t *testing.T) {
	for _, entry := range forbiddenManagedTraeHomeEntries {
		t.Run("TRAE_HOME "+entry, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, entry)
			if err := os.WriteFile(path, []byte("managed"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateManagedRuntimeHome(hostkind.TraeX, home); err == nil ||
				!strings.Contains(err.Error(), entry) {
				t.Fatalf("ValidateManagedRuntimeHome() error = %v, want %q", err, entry)
			}
		})
	}
	for _, entry := range forbiddenManagedTraeHomeEntries {
		t.Run("TRAECLI_HOME "+entry, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, "cli", entry)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("managed"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateManagedRuntimeHome(hostkind.TraeX, home); err == nil ||
				!strings.Contains(err.Error(), entry) {
				t.Fatalf("ValidateManagedRuntimeHome() error = %v, want %q", err, entry)
			}
		})
	}
}
