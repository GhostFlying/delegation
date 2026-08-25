package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/securefs"
)

func TestWriteNewRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
	if err := WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != cfg {
		t.Fatalf("Read() = %#v, want %#v", got, cfg)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := info.Mode().Perm(), os.FileMode(0o600); got != want {
			t.Fatalf("config permissions = %o, want %o", got, want)
		}
	}
}

func TestWriteNewRejectsTailscaleBeforeSideEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	cfg := tailscaleBrokerWriteConfig(t)
	if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err != nil {
		t.Fatalf("test config is not valid with tailscale support: %v", err)
	}

	err := WriteNew(path, cfg)
	if err == nil || !strings.Contains(err.Error(), "not supported by this runtime") {
		t.Fatalf("WriteNew() error = %v, want unsupported runtime", err)
	}
	requireWritePathAbsent(t, path)
}

func TestWriteNewForRuntimeTailscaleRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	cfg := tailscaleBrokerWriteConfig(t)
	capabilities := tailscaleRuntimeCapabilities()

	if err := WriteNewForRuntime(path, cfg, capabilities); err != nil {
		t.Fatal(err)
	}
	got, err := ReadForRuntime(path, capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, cfg) {
		t.Fatalf("ReadForRuntime() = %#v, want %#v", got, cfg)
	}
}

func TestWriteNewForRuntimeRejectsTailscaleWithoutCapability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	cfg := tailscaleBrokerWriteConfig(t)

	err := WriteNewForRuntime(path, cfg, RuntimeCapabilities{})
	if err == nil || !strings.Contains(err.Error(), "not supported by this runtime") {
		t.Fatalf("WriteNewForRuntime() error = %v, want unsupported runtime", err)
	}
	requireWritePathAbsent(t, path)
}

func TestWriteNewRejectsInvalidConfigBeforeSideEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	cfg := protectedTestConfig(t)
	cfg.ControllerID = "not-a-uuid"

	if err := WriteNew(path, cfg); err == nil {
		t.Fatal("WriteNew() accepted invalid config")
	}
	requireWritePathAbsent(t, path)
}

func TestWriteNewForRuntimeRejectsInvalidTailscaleConfigBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *Config)
		want   string
	}{
		{
			name: "malformed tailscale",
			mutate: func(t *testing.T, cfg *Config) {
				cfg.Transport.Tailscale.StateDir = "relative"
			},
			want: "tailscale stateDir",
		},
		{
			name: "wrong role",
			mutate: func(t *testing.T, cfg *Config) {
				cfg.Role = Role("relay")
			},
			want: "unsupported role",
		},
		{
			name: "wrong listen",
			mutate: func(t *testing.T, cfg *Config) {
				cfg.Broker.Listen = "127.0.0.1:8787"
			},
			want: "tailscale broker listen",
		},
		{
			name: "wrong URL",
			mutate: func(t *testing.T, cfg *Config) {
				*cfg = testTailscalePeerConfig(t)
				cfg.Broker.URL = "wss://broker.tailnet.test:8787/v1/connect"
			},
			want: "tailscale broker URL must use ws://",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "config.json")
			cfg := tailscaleBrokerWriteConfig(t)
			test.mutate(t, &cfg)

			err := WriteNewForRuntime(path, cfg, tailscaleRuntimeCapabilities())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("WriteNewForRuntime() error = %v, want %q", err, test.want)
			}
			requireWritePathAbsent(t, path)
		})
	}
}

func TestWriteNewDoesNotReplaceExistingConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	first := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
	second := first
	second.ControllerID = "123e4567-e89b-42d3-a456-426614174099"

	if err := WriteNew(path, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteNew(path, second); err == nil {
		t.Fatal("WriteNew() replaced an existing config")
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != first {
		t.Fatalf("Read() = %#v, want first config %#v", got, first)
	}
}

func TestWriteNewReportsCommittedSyncFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	path := filepath.Join(dir, "config.json")
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
	originalSync := syncInstalledConfig
	originalPublishedSync := syncPublishedConfig
	t.Cleanup(func() {
		syncInstalledConfig = originalSync
		syncPublishedConfig = originalPublishedSync
	})
	syncInstalledConfig = func(string) error { return nil }
	syncPublishedConfig = func(*securefs.Root) error { return errors.New("injected sync failure") }

	err := WriteNew(path, cfg)
	if !IsCommitted(err) {
		t.Fatalf("WriteNew() error = %v, want committed error", err)
	}
	got, readErr := Read(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got != cfg {
		t.Fatalf("Read() = %#v, want %#v", got, cfg)
	}
}

func TestWriteNewSyncsEveryNewDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "one", "two", "config.json")
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
	originalSync := syncInstalledConfig
	originalPublishedSync := syncPublishedConfig
	t.Cleanup(func() {
		syncInstalledConfig = originalSync
		syncPublishedConfig = originalPublishedSync
	})
	var synced []string
	syncInstalledConfig = func(syncPath string) error {
		synced = append(synced, syncPath)
		return nil
	}
	syncPublishedConfig = func(*securefs.Root) error {
		synced = append(synced, filepath.Dir(path))
		return nil
	}

	if err := WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Dir(root), root, filepath.Join(root, "one"), filepath.Join(root, "one", "two")}
	if !slices.Equal(synced, want) {
		t.Fatalf("synced paths = %q, want %q", synced, want)
	}
}

func TestWriteNewRetrySyncsExistingDirectoryAnchor(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "one", "two")
	path := filepath.Join(dir, "config.json")
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
	originalSync := syncInstalledConfig
	t.Cleanup(func() { syncInstalledConfig = originalSync })
	failingParent := filepath.Join(root, "one")
	syncInstalledConfig = func(syncPath string) error {
		if syncPath == failingParent {
			return errors.New("injected sync failure")
		}
		return nil
	}

	err := WriteNew(path, cfg)
	if err == nil || IsCommitted(err) {
		t.Fatalf("first WriteNew() error = %v, want pre-commit sync failure", err)
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		t.Fatalf("created directory missing after injected failure: %v", statErr)
	}

	var synced []string
	syncInstalledConfig = func(syncPath string) error {
		synced = append(synced, syncPath)
		return nil
	}
	if err := WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(synced, failingParent) {
		t.Fatalf("retry synced paths = %q, want existing directory parent %q", synced, failingParent)
	}
}

func tailscaleBrokerWriteConfig(t *testing.T) Config {
	t.Helper()
	cfg := protectedTestConfig(t)
	cfg.Transport = testTailscaleTransport(t)
	cfg.Broker.Listen = ":8787"
	return cfg
}

func requireWritePathAbsent(t *testing.T, path string) {
	t.Helper()
	for _, candidate := range []string{filepath.Dir(path), path} {
		if _, err := os.Lstat(candidate); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("validation created %q or returned unexpected status: %v", candidate, err)
		}
	}
}
