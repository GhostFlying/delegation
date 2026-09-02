//go:build integration && live && darwin

package codex_peer_e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/traexauth"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func TestSetupPeerRejectsTraeXAuthUnderDarwinTemporaryRoot(t *testing.T) {
	delegationBinary := optionalLiveExecutable(t, "DELEGATION_E2E_BINARY")
	traeXBinary := optionalLiveExecutable(t, "TRAE_X_BINARY")
	warmpoolBinary := optionalLiveExecutable(t, "WARMPOOL_BINARY")
	temporaryRoot, err := os.MkdirTemp("/tmp", "delegation-traex-auth-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporaryRoot) })
	if err := os.Chmod(temporaryRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	authSource := filepath.Join(temporaryRoot, "auth.json")
	dummyAuth := []byte(`{"auth_mode":"trae","trae":{"access_token":"dummy-non-secret-account-token","credential_kind":"cloud_cli_jwt","login_method":"probe","region":"probe","version":1}}`)
	if err := os.WriteFile(authSource, dummyAuth, 0o600); err != nil {
		t.Fatal(err)
	}

	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	safeRoot, err := os.MkdirTemp(userHome, ".delegation-traex-reject-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(safeRoot) })
	configPath := filepath.Join(safeRoot, "peer.json")
	managedHome := filepath.Join(safeRoot, "managed-trae")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, delegationBinary,
		"setup", "peer", "--config", configPath,
		"--host-kind", "traex",
		"--controller-id", newTraeXLiveIdentity(t),
		"--device-id", newTraeXLiveIdentity(t),
		"--device-name", "managed-worker-traex-temp-rejection",
		"--broker-url", "ws://127.0.0.1:1", "--auth-mode", "none",
		"--cli-command", traeXBinary,
		"--cli-launcher", warmpoolBinary,
		"--cli-launcher-prefix-argument=run",
		"--cli-launcher-prefix-argument=--",
		"--trae-auth-file", authSource,
		"--codex-home", managedHome,
		"--workspace-root", filepath.Join(safeRoot, "workspaces"),
		"--state", filepath.Join(safeRoot, "state", "peer.sqlite3"),
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err = command.Run()
	if ctx.Err() != nil {
		t.Fatalf("temporary-root rejection timed out: %v", ctx.Err())
	}
	if err == nil {
		t.Fatalf("setup accepted TraeX auth beneath /tmp; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "readable by the macOS worker sandbox") ||
		!strings.Contains(stderr.String(), "credential source") {
		t.Fatalf("setup temporary-root error = %q", stderr.String())
	}
	if _, err := os.Lstat(configPath); !os.IsNotExist(err) {
		t.Fatalf("setup created configuration before rejection: %v", err)
	}
	if _, err := os.Lstat(managedHome); !os.IsNotExist(err) {
		t.Fatalf("setup created managed home before rejection: %v", err)
	}
}

func TestManagedWorkerTraeXSandboxBoundaryDarwin(t *testing.T) {
	delegationBinary := optionalLiveExecutable(t, "DELEGATION_E2E_BINARY")
	traeXBinary := optionalLiveExecutable(t, "TRAE_X_BINARY")
	warmpoolBinary := optionalLiveExecutable(t, "WARMPOOL_BINARY")
	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(userHome, ".dmts-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}

	isolationHome := liveEmptyDirectory(t, root, "home")
	t.Setenv("HOME", isolationHome)
	t.Setenv("CODEX_HOME", liveEmptyDirectory(t, root, "ambient-codex"))
	t.Setenv("TRAE_HOME", liveEmptyDirectory(t, root, "ambient-trae"))
	t.Setenv("TRAECLI_HOME", liveEmptyDirectory(t, root, "ambient-trae-cli"))
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	const providerEnvironment = "DELEGATION_TRAEX_PROBE_PROVIDER"
	const providerCredential = "dummy-provider-credential"
	t.Setenv(providerEnvironment, providerCredential)
	mock := &managedResponsesMock{
		calls:                 make(map[string]int),
		expectedAuthorization: "Bearer " + providerCredential,
		shellTool:             "Bash",
		allowWarmup:           true,
		runningStarted:        make(chan struct{}),
		runningDisconnected:   make(chan struct{}),
	}
	modelServer := httptest.NewServer(mock)
	t.Cleanup(modelServer.Close)

	controllerID := newTraeXLiveIdentity(t)
	deviceID := newTraeXLiveIdentity(t)
	treeID := newTraeXLiveIdentity(t)
	parentAgentID := newTraeXLiveIdentity(t)
	agentID := newTraeXLiveIdentity(t)
	delegationHome := filepath.Join(root, "delegation")
	configPath := filepath.Join(delegationHome, "peer.json")
	managedTraeHome := filepath.Join(root, "managed-trae")
	workspaceRoot := filepath.Join(root, "workspaces")
	statePath := filepath.Join(delegationHome, "state", "peer.sqlite3")
	authDirectory := filepath.Join(root, "source")
	if err := delegationconfig.PreparePrivateDirectory(authDirectory); err != nil {
		t.Fatal(err)
	}
	authSource := filepath.Join(authDirectory, "auth.json")
	dummyAuth := []byte(`{"auth_mode":"trae","trae":{"access_token":"dummy-non-secret-account-token","credential_kind":"cloud_cli_jwt","login_method":"probe","region":"probe","version":1}}`)
	if err := os.WriteFile(authSource, dummyAuth, 0o600); err != nil {
		t.Fatal(err)
	}
	managedAuthPath, err := traexauth.Sync(dummyAuth, managedTraeHome)
	if err != nil {
		t.Fatal(err)
	}
	runTraeXLive(t, os.Environ(), delegationBinary,
		"setup", "peer", "--config", configPath,
		"--host-kind", "traex",
		"--controller-id", controllerID, "--device-id", deviceID,
		"--device-name", "managed-worker-traex-sandbox",
		"--broker-url", "ws://127.0.0.1:1", "--auth-mode", "none",
		"--cli-command", traeXBinary,
		"--cli-launcher", warmpoolBinary,
		"--cli-launcher-prefix-argument=run",
		"--cli-launcher-prefix-argument=--",
		"--trae-auth-file", authSource,
		"--codex-home", managedTraeHome, "--workspace-root", workspaceRoot,
		"--state", statePath, "--max-worker-slots", "1", "--json",
	)

	providerConfig := map[string]any{
		"model":          "gpt-5.2",
		"model_provider": "delegation_mock",
		"model_providers.delegation_mock": map[string]any{
			"name": "Delegation TraeX sandbox mock", "base_url": modelServer.URL + "/v1",
			"wire_api": "responses", "env_key": providerEnvironment,
			"requires_openai_auth": false,
		},
	}
	serviceEnvironmentDirectory := filepath.Join(root, "service-environment")
	if err := delegationconfig.PreparePrivateDirectory(serviceEnvironmentDirectory); err != nil {
		t.Fatal(err)
	}
	serviceEnvironmentPath := filepath.Join(serviceEnvironmentDirectory, "peer.env")
	if err := os.WriteFile(
		serviceEnvironmentPath,
		[]byte(fmt.Sprintf("%s={}\n%s=%s\n", codexconfig.EnvironmentVariable, providerEnvironment, providerCredential)),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	workspace := filepath.Join(workspaceRoot, treeID+"-"+agentID)
	command := fmt.Sprintf(
		"rm -f source-alias managed-alias; "+
			"if cat %s >/dev/null 2>&1; then echo SOURCE=readable; else echo SOURCE=blocked; fi; "+
			"if cat %s >/dev/null 2>&1; then echo MANAGED=readable; else echo MANAGED=blocked; fi; "+
			"ln -s %s source-alias; ln -s %s managed-alias; "+
			"if cat source-alias >/dev/null 2>&1; then echo SOURCE_ALIAS=readable; else echo SOURCE_ALIAS=blocked; fi; "+
			"if cat managed-alias >/dev/null 2>&1; then echo MANAGED_ALIAS=readable; else echo MANAGED_ALIAS=blocked; fi; "+
			"printf 'SANDBOX=%%s\n' \"${CODEX_SANDBOX:-missing}\"; "+
			"printf 'PROFILE=%%s\n' \"${CODEX_PERMISSION_PROFILE:-missing}\"; "+
			"if printenv TRAECLI_HOME >/dev/null 2>&1; then echo TRAECLI_HOME=visible; else echo TRAECLI_HOME=hidden; fi",
		managedPOSIXShellLiteral(authSource), managedPOSIXShellLiteral(managedAuthPath),
		managedPOSIXShellLiteral(authSource), managedPOSIXShellLiteral(managedAuthPath),
	)
	mock.probeCommand = command
	mock.probeMarkers = []string{
		"SOURCE=blocked", "MANAGED=blocked",
		"SOURCE_ALIAS=blocked", "MANAGED_ALIAS=blocked",
		"SANDBOX=seatbelt", "PROFILE=missing", "TRAECLI_HOME=hidden",
	}
	mock.protectedOutput = []managedProtectedValue{
		{label: "dummy account token", value: "dummy-non-secret-account-token"},
		{label: "dummy provider credential", value: providerCredential},
	}

	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Errorf("close TraeX sandbox peer state: %v", err)
		}
	})
	resultPackages, err := resultpackagefiles.New(context.Background(), resultpackagefiles.Options{
		ControllerID: controllerID, DeviceID: deviceID, WorkspaceRoot: workspaceRoot, Store: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	reported := make(chan error, 64)
	host, err := workerhost.New(context.Background(), workerhost.Options{
		ControllerID: controllerID, DeviceID: deviceID, HostKind: hostkind.TraeX,
		PeerConfigPath: configPath, DelegationBinary: delegationBinary,
		CLILaunch: clilaunch.Spec{
			Executable: warmpoolBinary, PrefixArguments: []string{"run", "--", traeXBinary},
		},
		CLIRuntimeExecutable: traeXBinary, GitBinary: resolveLiveExecutable(t, "git"),
		CodexHome: managedTraeHome, TraeAuthSourceFile: authSource,
		ManagedTraeAuthFile: managedAuthPath, WorkspaceRoot: workspaceRoot, MaxWorkerSlots: 1,
		CodexEnvironment:        map[string]string{providerEnvironment: providerCredential},
		ProviderEnvironmentFile: serviceEnvironmentPath, CodexConfig: providerConfig,
		Store: state, ResultPackages: resultPackages,
		ReportError: func(err error) {
			select {
			case reported <- err:
			default:
			}
		},
	})
	if err != nil {
		_ = resultPackages.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close TraeX sandbox worker host: %v", err)
		}
		if err := resultPackages.Close(); err != nil {
			t.Errorf("close TraeX sandbox result packages: %v", err)
		}
	})

	started, err := host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID: treeID, AgentID: agentID, ParentAgentID: parentAgentID,
		TaskName: "TraeX sandbox boundary", Prompt: "managed-worker-case=first Run the requested probe.",
	})
	if err != nil {
		select {
		case reportedErr := <-reported:
			t.Fatal(fmt.Errorf("spawn TraeX sandbox worker: %w", reportedErr))
		default:
			t.Fatal(err)
		}
	}
	waitForWorkerStateWithResultAck(
		t, &managedTestHost{Host: host, resultPackages: resultPackages}, state,
		started.Worker.WorkerKey, store.WorkerIdle, mock.diagnostics,
	)
	if _, err := os.Stat(workspace); err != nil {
		t.Fatal(err)
	}
	mock.verifyTraeXSandbox(t)
}

func (m *managedResponsesMock) verifyTraeXSandbox(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errors) != 0 {
		t.Fatalf("TraeX sandbox Responses errors: %v", m.errors)
	}
	if m.calls["first"] != 2 {
		t.Fatalf("TraeX sandbox Responses calls = %#v, want first=2", m.calls)
	}
}
