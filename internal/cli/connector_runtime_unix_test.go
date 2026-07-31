//go:build linux || darwin

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConnectorRejectsManagedDirectoryPermissionDriftBeforeStateOpen(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t,
		runtimeDeviceID,
		"permission-drift",
		"ws://127.0.0.1:1",
	)
	if err := os.Chmod(cfg.Peer.CodexHome, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runConnectorService(ctx, configPath, cfg, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "mode 0700") {
		t.Fatalf("runConnectorService() error = %v", err)
	}
	if _, statErr := os.Lstat(cfg.Peer.StateFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("connector opened state after authority failure: %v", statErr)
	}
}

func TestDoctorRejectsTraeCLIHomePermissionDrift(t *testing.T) {
	root := privateTestDirectory(t)
	configPath := filepath.Join(root, "peer.json")
	managedHome := filepath.Join(root, "managed-trae")
	executable := testCodexBinary(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--instance", "traex-main",
		"--host-kind", "traex",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--cli-command", executable,
		"--cli-launcher", executable,
		"--codex-home", managedHome,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cliHome := filepath.Join(managedHome, "cli")
	if err := os.Mkdir(cliHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cliHome, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(cliHome, 0o700) })
	stdout.Reset()
	stderr.Reset()
	code := Run([]string{"doctor", "--config", configPath}, &stdout, &stderr)
	if code == 0 ||
		!strings.Contains(stderr.String(), "validate managed TRAECLI_HOME") ||
		!strings.Contains(stderr.String(), "mode 0700") {
		t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
	}
}
