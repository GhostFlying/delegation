package config

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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
