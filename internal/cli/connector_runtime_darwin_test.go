//go:build darwin

package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/serviceenv"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/traexauth"
)

func TestConnectorPersistsUnsafeTraeXSandboxPathInCurrentEpoch(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t, runtimeDeviceID, "traex-auth-unsafe-sandbox-path", "wss://broker.example.test",
	)
	cfg.HostKind = hostkind.TraeX
	authDirectory, err := os.MkdirTemp("/tmp", "delegation-traex-auth-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(authDirectory) })
	if err := os.Chmod(authDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg.Peer.TraeAuthFile = filepath.Join(authDirectory, "auth.json")
	if err := os.WriteFile(cfg.Peer.TraeAuthFile, []byte(testTraeAuth), 0o600); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		err := runConnectorServiceWithProviderEnvironment(
			context.Background(), configPath, cfg, "",
			func() (serviceenv.Resolved, error) { return serviceenv.Resolved{}, nil },
			io.Discard, connectorRuntimeOptions{},
		)
		if !errors.Is(err, traexauth.ErrUnsafeSandboxPath) ||
			!strings.Contains(err.Error(), "state=intervention_required") ||
			!strings.Contains(err.Error(), "failureCode=authentication_invalid") {
			t.Fatalf("unsafe TraeX authentication path startup %d error = %v", attempt+1, err)
		}
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
	if readiness.Epoch != 1 || readiness.State != protocol.WorkerReadinessInterventionRequired ||
		readiness.FailureCode != protocol.WorkerAuthenticationInvalid {
		t.Fatalf("unsafe TraeX authentication path readiness = %#v", readiness)
	}
}
