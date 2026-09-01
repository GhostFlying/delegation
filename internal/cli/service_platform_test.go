package cli

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestServiceRuntimePersistsUnsupportedWindowsTraeXHost(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t, "123e4567-e89b-42d3-a456-426614174724", "windows-traex",
		"wss://broker.example.test/v1/connect",
	)
	cfg.HostKind = hostkind.TraeX
	cfg.Peer.CLI = &delegationconfig.CLIConfig{
		Command:  testCodexBinary(t),
		Launcher: &clilaunch.Spec{Executable: testCodexBinary(t)},
	}
	cfg.Peer.CodexBinary = ""
	writePlatformServiceConfig(t, configPath, cfg)

	_, err := readServiceRuntimeConfig(configPath, "", "windows")
	if err == nil || !strings.Contains(err.Error(), "unsupported on Windows") {
		t.Fatalf("Windows TraeX startup error = %v", err)
	}
	if !strings.Contains(err.Error(), "state=intervention_required") ||
		!strings.Contains(err.Error(), "failureCode=unsupported_host") {
		t.Fatalf("Windows TraeX startup log = %v", err)
	}
	state, err := store.OpenPeer(context.Background(), cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	readiness, err := state.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if readiness.State != protocol.WorkerReadinessInterventionRequired ||
		readiness.FailureCode != protocol.WorkerHostUnsupported {
		t.Fatalf("unsupported host readiness = %#v", readiness)
	}
}

func writePlatformServiceConfig(t *testing.T, path string, cfg delegationconfig.Config) {
	t.Helper()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
