package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func TestConnectorServiceStopsBeforeBindingWhenPreCanceled(t *testing.T) {
	configPath := privateTestPath(t, "config.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "worker",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	var stderr bytes.Buffer
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runConnectorService(ctx, configPath, cfg, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "authentication is disabled") {
		t.Fatalf("pre-canceled connector warning = %q", stderr.String())
	}
}

func TestServiceRunRejectsInvalidConfiguration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "missing.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "run", "--config", configPath}, &stdout, &stderr)

	if code == exitUnavailable || code == 0 {
		t.Fatalf("service run code = %d, want configuration failure", code)
	}
}

func TestNamedInstanceServiceInstallFailsClosedWithoutArtifact(t *testing.T) {
	home := privateTestDirectory(t)
	configPath := filepath.Join(home, "instances", "second", "broker.json")
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		InstanceID:    "second",
		HostKind:      "codex",
		Role:          delegationconfig.RoleBroker,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174000",
		Broker: delegationconfig.BrokerConfig{
			Listen:       "127.0.0.1:18787",
			StatusListen: "127.0.0.1:18788",
			StateFile:    filepath.Join(home, "instances", "second", "state", "broker.sqlite3"),
			Auth:         delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "systemd"))
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", configPath}, &stdout, &stderr)

	if code == 0 || !strings.Contains(stderr.String(), "requires instance-scoped native service support") {
		t.Fatalf("service install code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(home, "systemd", "systemd", "user", "delegation-broker.service")); !os.IsNotExist(err) {
		t.Fatalf("service install created a fixed-name artifact: %v", err)
	}
}
