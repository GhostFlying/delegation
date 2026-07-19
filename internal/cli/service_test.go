package cli

import (
	"bytes"
	"context"
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
