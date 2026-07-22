//go:build integration && live && linux

package codex_peer_e2e

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/codexcommand"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func TestManagedWorkerLiveProviderSmoke(t *testing.T) {
	delegationBinary := requiredExecutable(t, "DELEGATION_E2E_BINARY")
	codexBinary := requiredExecutable(t, "CODEX_BINARY")
	if _, found := os.LookupEnv(codexconfig.EnvironmentVariable); !found {
		t.Fatalf("%s is required for the live smoke", codexconfig.EnvironmentVariable)
	}
	codexOverrides, err := codexconfig.Load(os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	codexLaunch, err := codexcommand.Resolve(codexBinary)
	if err != nil {
		t.Fatal(err)
	}

	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	// Keep the test HOME short enough for the portable Unix socket path limit.
	root, err := os.MkdirTemp(userHome, ".dmwl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	t.Setenv("HOME", filepath.Join(root, "home"))
	if err := os.Mkdir(os.Getenv("HOME"), 0o700); err != nil {
		t.Fatal(err)
	}

	controllerID := newIdentity(t)
	deviceID := newIdentity(t)
	treeID := newIdentity(t)
	parentAgentID := newIdentity(t)
	agentID := newIdentity(t)
	delegationHome := filepath.Join(root, "delegation")
	configPath := filepath.Join(delegationHome, "peer.json")
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	statePath := filepath.Join(delegationHome, "state", "peer.sqlite3")
	run(t, os.Environ(), delegationBinary,
		"setup", "peer", "--config", configPath,
		"--controller-id", controllerID, "--device-id", deviceID,
		"--device-name", "managed-worker-live", "--broker-url", "ws://127.0.0.1:1",
		"--auth-mode", "none", "--codex-binary", codexBinary,
		"--codex-home", codexHome, "--workspace-root", workspaceRoot,
		"--state", statePath, "--max-worker-slots", "1", "--json",
	)
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	host, err := workerhost.New(context.Background(), workerhost.Options{
		ControllerID: cfg.ControllerID, DeviceID: cfg.DeviceID,
		PeerConfigPath: configPath, DelegationBinary: delegationBinary,
		CodexBinary: codexLaunch.NativePath, CodexHome: cfg.Peer.CodexHome,
		CodexEnvironment:      codexLaunch.Environment,
		CodexUnsetEnvironment: codexLaunch.UnsetEnvironment,
		WorkspaceRoot:         cfg.Peer.WorkspaceRoot, MaxWorkerSlots: cfg.Peer.MaxWorkerSlots,
		CodexConfig: codexOverrides, Store: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close worker host: %v", err)
		}
	}()

	started, err := host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID: treeID, AgentID: agentID, ParentAgentID: parentAgentID,
		TaskName: "live provider smoke",
		Prompt:   "Reply with exactly delegation-live-ok. Do not call tools.",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSuccessfulLiveTurn(t, state, started.Worker.WorkerKey)
	if _, err := os.Stat(filepath.Join(codexHome, "config.toml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed CODEX_HOME contains config.toml: %v", err)
	}
}

func newIdentity(t *testing.T) string {
	t.Helper()
	value, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func waitForSuccessfulLiveTurn(t *testing.T, state *store.PeerStore, key store.WorkerKey) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		worker, err := state.GetWorker(context.Background(), key)
		if err == nil {
			switch worker.Status {
			case store.WorkerIdle:
				return
			case store.WorkerFailed:
				t.Fatalf("managed live turn failed with %s", worker.FailureCode)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	worker, err := state.GetWorker(context.Background(), key)
	t.Fatalf("managed live turn did not complete: %#v, %v", worker, err)
}
