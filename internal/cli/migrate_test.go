package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

const (
	migrateControllerID = "123e4567-e89b-42d3-a456-426614174800"
	migrateDeviceID     = "123e4567-e89b-42d3-a456-426614174801"
)

func TestMigrateBrokerConfigPreservesAuthorityWithoutOpeningState(t *testing.T) {
	root := privateTestDirectory(t)
	source := filepath.Join(root, "legacy-broker.json")
	destination := filepath.Join(root, "broker.json")
	statePath := filepath.Join(root, "state", "broker.sqlite3")
	masterPath := filepath.Join(root, "secrets", "broker.token")
	if _, err := tokenfile.Ensure(masterPath); err != nil {
		t.Fatal(err)
	}
	legacy := delegationconfig.Config{
		SchemaVersion: delegationconfig.LegacySchemaVersion,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  migrateControllerID,
		Broker: delegationconfig.BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: statePath,
			Auth: delegationconfig.AuthConfig{
				Mode: delegationconfig.AuthModeToken, TokenFile: masterPath,
			},
		},
	}
	sourceBefore := writeLegacyConfig(t, source, legacy)
	stdout, stderr, code := runCLIForMigration([]string{
		"migrate", "config", "--from", source, "--to", destination, "--json",
	})
	if code != 0 {
		t.Fatalf("migrate broker code = %d, stderr = %q", code, stderr)
	}
	var result migrateConfigResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatal(err)
	}
	if result.Role != delegationconfig.RoleBroker || result.ConfigPath != destination || result.TokenFile != masterPath {
		t.Fatalf("broker migration result = %#v", result)
	}
	got, err := delegationconfig.Read(destination)
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != delegationconfig.CurrentSchemaVersion || got.ControllerID != legacy.ControllerID ||
		got.Broker.StateFile != statePath || got.Broker.Auth.TokenFile != masterPath {
		t.Fatalf("migrated broker = %#v", got)
	}
	assertMigrationSourceUnchanged(t, source, sourceBefore)
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("config migration opened broker state: %v", err)
	}
}

func TestMigrateBrokerConfigRequiresUsableMasterToken(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, string)
	}{
		{name: "missing"},
		{
			name: "malformed",
			prepare: func(t *testing.T, path string) {
				t.Helper()
				if _, err := tokenfile.Ensure(path); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("not-a-token\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symbolic link",
			prepare: func(t *testing.T, path string) {
				t.Helper()
				target := filepath.Join(filepath.Dir(path), "real-broker.token")
				if _, err := tokenfile.Ensure(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Skipf("creating a token symlink is unavailable: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := privateTestDirectory(t)
			anchor := filepath.Join(root, "anchor.token")
			if _, err := tokenfile.Ensure(anchor); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(anchor); err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(root, "legacy-broker.json")
			destination := filepath.Join(root, "broker.json")
			masterPath := filepath.Join(root, "secrets", "broker.token")
			if test.prepare != nil {
				test.prepare(t, masterPath)
			}
			legacy := delegationconfig.Config{
				SchemaVersion: delegationconfig.LegacySchemaVersion,
				Role:          delegationconfig.RoleBroker,
				ControllerID:  migrateControllerID,
				Broker: delegationconfig.BrokerConfig{
					Listen:    "127.0.0.1:8787",
					StateFile: filepath.Join(root, "state", "broker.sqlite3"),
					Auth: delegationconfig.AuthConfig{
						Mode: delegationconfig.AuthModeToken, TokenFile: masterPath,
					},
				},
			}
			writeLegacyConfig(t, source, legacy)
			_, stderr, code := runCLIForMigration([]string{
				"migrate", "config", "--from", source, "--to", destination,
			})
			if code == 0 || !strings.Contains(stderr, "validate retained broker master token") {
				t.Fatalf("unusable master token code = %d, stderr = %q", code, stderr)
			}
			if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed migration created destination: %v", err)
			}
		})
	}
}

func TestMigrateLegacyControllerRetainsCredentialAsPeer(t *testing.T) {
	root := privateTestDirectory(t)
	source := filepath.Join(root, "legacy-controller.json")
	destination := filepath.Join(root, "peer.json")
	tokenPath := filepath.Join(root, "secrets", "controller.token")
	if _, err := tokenfile.Ensure(tokenPath); err != nil {
		t.Fatal(err)
	}
	legacy := legacyPeerConfig(delegationconfig.LegacyRoleController, tokenPath)
	sourceBefore := writeLegacyConfig(t, source, legacy)
	_, stderr, code := runCLIForMigration([]string{
		"migrate", "config", "--from", source, "--to", destination,
	})
	if code != 0 {
		t.Fatalf("migrate controller code = %d, stderr = %q", code, stderr)
	}
	got, err := delegationconfig.Read(destination)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != delegationconfig.RolePeer || got.DeviceID != migrateDeviceID ||
		got.Broker.Auth.TokenFile != tokenPath || got.Broker.URL != "wss://broker.example.test/v2/connect" {
		t.Fatalf("migrated controller peer = %#v", got)
	}
	assertMigrationSourceUnchanged(t, source, sourceBefore)
}

func TestMigrateLegacyDeviceRequiresFreshPeerCredential(t *testing.T) {
	root := privateTestDirectory(t)
	source := filepath.Join(root, "legacy-device.json")
	destination := filepath.Join(root, "peer.json")
	legacyTokenPath := filepath.Join(root, "secrets", "device.token")
	freshTokenPath := filepath.Join(root, "secrets", "peer.token")
	if _, err := tokenfile.Ensure(legacyTokenPath); err != nil {
		t.Fatal(err)
	}
	if _, err := tokenfile.Ensure(freshTokenPath); err != nil {
		t.Fatal(err)
	}
	legacy := legacyPeerConfig(delegationconfig.LegacyRoleDevice, legacyTokenPath)
	sourceBefore := writeLegacyConfig(t, source, legacy)
	_, stderr, code := runCLIForMigration([]string{
		"migrate", "config", "--from", source, "--to", destination,
	})
	if code == 0 || !strings.Contains(stderr, "freshly issued peer credential") {
		t.Fatalf("missing fresh token code = %d, stderr = %q", code, stderr)
	}
	_, stderr, code = runCLIForMigration([]string{
		"migrate", "config", "--from", source, "--to", destination,
		"--token-file", legacyTokenPath,
	})
	if code == 0 || !strings.Contains(stderr, "different file") {
		t.Fatalf("reused legacy token code = %d, stderr = %q", code, stderr)
	}
	_, stderr, code = runCLIForMigration([]string{
		"migrate", "config", "--from", source, "--to", destination,
		"--token-file", freshTokenPath,
	})
	if code != 0 {
		t.Fatalf("migrate device code = %d, stderr = %q", code, stderr)
	}
	got, err := delegationconfig.Read(destination)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != delegationconfig.RolePeer || got.DeviceID != migrateDeviceID ||
		got.Broker.Auth.TokenFile != freshTokenPath {
		t.Fatalf("migrated device peer = %#v", got)
	}
	assertMigrationSourceUnchanged(t, source, sourceBefore)
	_, stderr, code = runCLIForMigration([]string{
		"migrate", "config", "--from", source, "--to", destination,
		"--token-file", freshTokenPath,
	})
	if code == 0 || !strings.Contains(stderr, "config already exists") {
		t.Fatalf("destination replacement code = %d, stderr = %q", code, stderr)
	}
}

func legacyPeerConfig(role delegationconfig.Role, tokenPath string) delegationconfig.Config {
	return delegationconfig.Config{
		SchemaVersion: delegationconfig.LegacySchemaVersion,
		Role:          role,
		ControllerID:  migrateControllerID,
		DeviceID:      migrateDeviceID,
		DeviceName:    "peer",
		Broker: delegationconfig.BrokerConfig{
			URL: "wss://broker.example.test/v1/connect",
			Auth: delegationconfig.AuthConfig{
				Mode: delegationconfig.AuthModeToken, TokenFile: tokenPath,
			},
		},
	}
}

func writeLegacyConfig(t *testing.T, path string, cfg delegationconfig.Config) []byte {
	t.Helper()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(data)
}

func assertMigrationSourceUnchanged(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("config migration changed the source file")
	}
}

func runCLIForMigration(args []string) (string, string, int) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}
