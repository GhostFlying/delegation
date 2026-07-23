//go:build integration

package codex_peer_e2e

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/codexcommand"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/serviceenv"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

const (
	managedNetworkID       = "123e4567-e89b-42d3-a456-426614174950"
	managedDeviceID        = "123e4567-e89b-42d3-a456-426614174951"
	managedTreeID          = "123e4567-e89b-42d3-a456-426614174952"
	managedParentID        = "123e4567-e89b-42d3-a456-426614174953"
	managedAgentID         = "123e4567-e89b-42d3-a456-426614174954"
	managedCrashID         = "123e4567-e89b-42d3-a456-426614174955"
	managedFollowupID      = "123e4567-e89b-42d3-a456-426614174956"
	managedCrashFollowupID = "123e4567-e89b-42d3-a456-426614174957"

	managedCrashHelperEnvironment   = "DELEGATION_MANAGED_CRASH_HELPER"
	managedCrashConfigEnvironment   = "DELEGATION_MANAGED_CRASH_CONFIG"
	managedCrashRuntimeEnvironment  = "DELEGATION_MANAGED_CRASH_RUNTIME"
	managedCrashCodexEnvironment    = "DELEGATION_MANAGED_CRASH_CODEX"
	managedCrashProviderEnvironment = "DELEGATION_MANAGED_CRASH_PROVIDER"
	managedCrashStateEnvironment    = "DELEGATION_MANAGED_CRASH_STATE"
	managedCrashReadyEnvironment    = "DELEGATION_MANAGED_CRASH_READY"
	managedCrashHelperEnabled       = "1"
	managedCrashRunningCase         = "restart-running"
	managedCrashResumeCase          = "restart-resume"
)

func TestManagedWorkerUsesEnvOnlyProviderAndColdResume(t *testing.T) {
	delegationBinary := requiredManagedExecutable(t, "DELEGATION_E2E_BINARY")
	codexBinary := requiredManagedExecutable(t, "CODEX_BINARY")
	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(userHome, ".dmw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", filepath.Join(root, "home"))
	if err := os.Mkdir(os.Getenv("HOME"), 0o700); err != nil {
		t.Fatal(err)
	}

	const providerEnvironment = "DELEGATION_E2E_PROVIDER_VALUE"
	const providerCredential = "managed-provider-test-value"
	t.Setenv(providerEnvironment, "ambient-provider-must-not-be-used")
	t.Setenv(codexconfig.EnvironmentVariable, "{}")
	ambientSQLiteHome := filepath.Join(root, "ambient-sqlite")
	t.Setenv("CODEX_SQLITE_HOME", ambientSQLiteHome)
	credentialPath := filepath.Join(root, "outside-credential.txt")
	if err := os.WriteFile(credentialPath, []byte("outside-credential-test-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	delegationHome := filepath.Join(root, "delegation")
	configPath := filepath.Join(delegationHome, "peer.json")
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	statePath := filepath.Join(delegationHome, "state", "peer.sqlite3")
	protectedRuntimeDirectory := filepath.Join(root, "protected-runtime")
	if err := os.Mkdir(protectedRuntimeDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	managedRuntimeName := "delegation"
	if runtime.GOOS == "windows" {
		managedRuntimeName += ".exe"
	}
	managedDelegationBinary := filepath.Join(protectedRuntimeDirectory, managedRuntimeName)
	if err := copyExecutable(delegationBinary, managedDelegationBinary); err != nil {
		t.Fatal(err)
	}
	serviceEnvironmentDirectory := filepath.Join(root, "service-environment")
	if err := delegationconfig.PreparePrivateDirectory(serviceEnvironmentDirectory); err != nil {
		t.Fatal(err)
	}
	serviceEnvironmentPath := filepath.Join(serviceEnvironmentDirectory, "peer.env")
	mock := &managedResponsesMock{
		calls:                 make(map[string]int),
		expectedAuthorization: "Bearer " + providerCredential,
		runningStarted:        make(chan struct{}),
		runningDisconnected:   make(chan struct{}),
	}
	mock.probeCommand, mock.probeMarkers = managedSandboxProbe(
		credentialPath,
		serviceEnvironmentPath,
		configPath,
		managedDelegationBinary,
		statePath,
		providerEnvironment,
	)
	mock.protectedOutput = []managedProtectedValue{
		{label: "connector configuration authority path", value: configPath},
		{label: "Delegation executable path", value: managedDelegationBinary},
		{label: "peer state path", value: statePath},
		{label: "service environment path", value: serviceEnvironmentPath},
		{label: "provider credential", value: providerCredential},
		{label: "external fixture credential", value: "outside-credential-test-value"},
	}
	modelServer := httptest.NewServer(mock)
	t.Cleanup(modelServer.Close)
	providerJSON := fmt.Sprintf(
		`{"model":"gpt-5.2","model_provider":"delegation_mock","model_providers.delegation_mock":{"name":"Delegation managed-worker mock","base_url":%q,"wire_api":"responses","env_key":%q,"requires_openai_auth":false}}`,
		modelServer.URL+"/v1",
		providerEnvironment,
	)
	serviceEnvironment := fmt.Sprintf(
		"%s=%s\n%s=%s\n",
		codexconfig.EnvironmentVariable,
		providerJSON,
		providerEnvironment,
		providerCredential,
	)
	if err := os.WriteFile(serviceEnvironmentPath, []byte(serviceEnvironment), 0o600); err != nil {
		t.Fatal(err)
	}
	runManagedCommand(t, os.Environ(), delegationBinary,
		"setup", "peer", "--config", configPath,
		"--controller-id", managedNetworkID, "--device-id", managedDeviceID,
		"--device-name", "managed-worker-e2e", "--broker-url", "ws://127.0.0.1:1",
		"--auth-mode", "none", "--codex-binary", codexBinary,
		"--codex-home", codexHome, "--workspace-root", workspaceRoot,
		"--state", statePath, "--max-worker-slots", "1", "--json",
	)
	managedWorkspace := filepath.Join(workspaceRoot, managedTreeID+"-"+managedAgentID)
	if err := os.Mkdir(managedWorkspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(managedWorkspace, "workspace-marker.txt"), []byte("readable"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspaceCodexDirectory := filepath.Join(managedWorkspace, ".codex")
	if err := os.Mkdir(workspaceCodexDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	workspaceConfig := `[mcp_servers.workspace_injected]
command = "delegation-workspace-config-must-not-start"
`
	if err := os.WriteFile(filepath.Join(workspaceCodexDirectory, "config.toml"), []byte(workspaceConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Errorf("close managed peer state: %v", err)
		}
	})
	hosts := make([]*workerhost.Host, 0, 3)
	newHost := func() *workerhost.Host {
		host := openManagedTestHost(
			t,
			configPath,
			managedDelegationBinary,
			codexBinary,
			serviceEnvironmentPath,
			state,
		)
		hosts = append(hosts, host)
		return host
	}
	t.Cleanup(func() {
		for _, host := range hosts {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := host.Close(ctx)
			cancel()
			if err != nil {
				t.Errorf("close managed worker host: %v", err)
			}
		}
	})
	closeHost := func(host *workerhost.Host) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}

	host := newHost()
	started, err := host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID: managedTreeID, AgentID: managedAgentID, ParentAgentID: managedParentID,
		TaskName: "managed worker E2E", Prompt: "managed-worker-case=first Return probe-ok.",
	})
	if err != nil {
		closeHost(host)
		t.Fatal(err)
	}
	waitForWorkerState(t, state, started.Worker.WorkerKey, store.WorkerIdle, mock.diagnostics)
	closeHost(host)

	host = newHost()
	resumed, err := host.Followup(context.Background(), workerhost.FollowupRequest{
		OperationID: managedFollowupID, Key: started.Worker.WorkerKey,
		Message: "managed-worker-case=resume Return probe-ok.",
	})
	if err != nil {
		closeHost(host)
		t.Fatal(err)
	}
	if resumed.Worker.CodexThreadID != started.Worker.CodexThreadID {
		closeHost(host)
		t.Fatalf("resumed thread = %q, want %q", resumed.Worker.CodexThreadID, started.Worker.CodexThreadID)
	}
	waitForWorkerState(t, state, resumed.Worker.WorkerKey, store.WorkerIdle, mock.diagnostics)
	closeHost(host)

	crashReadyPath := filepath.Join(root, "managed-crash-ready.json")
	helper := startManagedCrashHelper(t, managedCrashHelperOptions{
		ConfigPath: configPath, DelegationBinary: managedDelegationBinary,
		CodexBinary: codexBinary, ProviderEnvironmentFile: serviceEnvironmentPath,
		StatePath: statePath, ReadyPath: crashReadyPath,
	})
	ready := waitForManagedCrashReady(t, crashReadyPath)
	waitForManagedSignal(t, mock.runningStarted, "running model request")
	if err := helper.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := helper.command.Wait(); err == nil {
		t.Fatal("managed connector helper did not report its hard termination")
	}
	helper.waited = true
	waitForManagedSignal(t, mock.runningDisconnected, "hard-killed app-server disconnect")

	host = newHost()
	crashedKey := store.WorkerKey{
		ControllerID: managedNetworkID,
		TreeID:       managedTreeID,
		AgentID:      managedCrashID,
	}
	interrupted := waitForWorkerState(t, state, crashedKey, store.WorkerInterrupted, mock.diagnostics)
	if interrupted.CodexThreadID != ready.ThreadID || interrupted.ActiveTurnID != ready.TurnID ||
		interrupted.FailureCode != "turn_interrupted" {
		closeHost(host)
		t.Fatalf("recovered running worker = %#v, ready = %#v", interrupted, ready)
	}
	restarted, err := host.Followup(context.Background(), workerhost.FollowupRequest{
		OperationID: managedCrashFollowupID, Key: crashedKey,
		Message: "managed-worker-case=" + managedCrashResumeCase + " Return probe-ok.",
	})
	if err != nil {
		closeHost(host)
		t.Fatal(err)
	}
	if restarted.Worker.CodexThreadID != ready.ThreadID {
		closeHost(host)
		t.Fatalf("restart thread = %q, want %q", restarted.Worker.CodexThreadID, ready.ThreadID)
	}
	waitForWorkerState(t, state, crashedKey, store.WorkerIdle, mock.diagnostics)
	closeHost(host)
	if _, err := os.Stat(filepath.Join(codexHome, "config.toml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed CODEX_HOME contains config.toml: %v", err)
	}
	if _, err := os.Stat(ambientSQLiteHome); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed app-server used ambient CODEX_SQLITE_HOME: %v", err)
	}
	managedState, err := filepath.Glob(filepath.Join(codexHome, "state*.sqlite*"))
	if err != nil || len(managedState) == 0 {
		t.Fatalf("managed CODEX_HOME state files = %#v, %v", managedState, err)
	}
	mock.verify(t)
}

func TestManagedWorkerCrashHelperProcess(t *testing.T) {
	if os.Getenv(managedCrashHelperEnvironment) != managedCrashHelperEnabled {
		t.Skip("managed connector crash helper")
	}
	state, err := store.OpenPeer(context.Background(), os.Getenv(managedCrashStateEnvironment))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	host := openManagedTestHost(
		t,
		os.Getenv(managedCrashConfigEnvironment),
		os.Getenv(managedCrashRuntimeEnvironment),
		os.Getenv(managedCrashCodexEnvironment),
		os.Getenv(managedCrashProviderEnvironment),
		state,
	)
	started, err := host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID: managedTreeID, AgentID: managedCrashID, ParentAgentID: managedParentID,
		TaskName: "managed worker hard restart",
		Prompt:   "managed-worker-case=" + managedCrashRunningCase + " Wait for completion.",
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := json.Marshal(managedCrashReady{
		ThreadID: started.Worker.CodexThreadID,
		TurnID:   started.Worker.ActiveTurnID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(managedCrashReadyEnvironment), ready, 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func openManagedTestHost(
	t *testing.T,
	configPath, delegationBinary, codexBinary, providerEnvironmentFile string,
	state *store.PeerStore,
) *workerhost.Host {
	t.Helper()
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := serviceenv.LoadProtectedFile(providerEnvironmentFile)
	if err != nil {
		t.Fatal(err)
	}
	codexLaunch, err := codexcommand.Resolve(codexBinary)
	if err != nil {
		t.Fatal(err)
	}
	codexEnvironment := make(map[string]string, len(codexLaunch.Environment)+len(provider.Environment))
	for name, value := range codexLaunch.Environment {
		codexEnvironment[name] = value
	}
	for name, value := range provider.Environment {
		codexEnvironment[name] = value
	}
	host, err := workerhost.New(context.Background(), workerhost.Options{
		ControllerID: cfg.ControllerID, DeviceID: cfg.DeviceID,
		PeerConfigPath: configPath, DelegationBinary: delegationBinary,
		CodexBinary: codexLaunch.NativePath, CodexHome: cfg.Peer.CodexHome,
		GitBinary:               cfg.Peer.GitBinary,
		CodexEnvironment:        codexEnvironment,
		CodexUnsetEnvironment:   codexLaunch.UnsetEnvironment,
		ProviderEnvironmentFile: providerEnvironmentFile,
		WorkspaceRoot:           cfg.Peer.WorkspaceRoot, MaxWorkerSlots: cfg.Peer.MaxWorkerSlots,
		CodexConfig: provider.Config, Store: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

type managedCrashHelperOptions struct {
	ConfigPath              string
	DelegationBinary        string
	CodexBinary             string
	ProviderEnvironmentFile string
	StatePath               string
	ReadyPath               string
}

type managedCrashHelper struct {
	command *exec.Cmd
	waited  bool
}

func startManagedCrashHelper(t *testing.T, options managedCrashHelperOptions) *managedCrashHelper {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestManagedWorkerCrashHelperProcess$", "-test.count=1")
	command.Env = append(os.Environ(),
		managedCrashHelperEnvironment+"="+managedCrashHelperEnabled,
		managedCrashConfigEnvironment+"="+options.ConfigPath,
		managedCrashRuntimeEnvironment+"="+options.DelegationBinary,
		managedCrashCodexEnvironment+"="+options.CodexBinary,
		managedCrashProviderEnvironment+"="+options.ProviderEnvironmentFile,
		managedCrashStateEnvironment+"="+options.StatePath,
		managedCrashReadyEnvironment+"="+options.ReadyPath,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	helper := &managedCrashHelper{command: command}
	t.Cleanup(func() {
		if helper.waited {
			return
		}
		_ = command.Process.Kill()
		_ = command.Wait()
		if t.Failed() && (stdout.Len() != 0 || stderr.Len() != 0) {
			t.Logf("managed crash helper stdout: %s\nstderr: %s", stdout.String(), stderr.String())
		}
	})
	return helper
}

type managedCrashReady struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
}

func waitForManagedCrashReady(t *testing.T, path string) managedCrashReady {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var ready managedCrashReady
			if err = json.Unmarshal(data, &ready); err == nil && ready.ThreadID != "" && ready.TurnID != "" {
				return ready
			}
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("managed crash helper did not become ready: %v", lastErr)
	return managedCrashReady{}
}

func waitForManagedSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func requiredManagedExecutable(t *testing.T, variable string) string {
	t.Helper()
	path := os.Getenv(variable)
	if path == "" {
		t.Fatalf("%s is required", variable)
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", variable, err)
	}
	return resolved
}

func runManagedCommand(t *testing.T, environment []string, binary string, args ...string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = environment
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatalf("command timed out: %s %s\nstdout: %s\nstderr: %s", binary, strings.Join(args, " "), stdout.String(), stderr.String())
	}
	if err != nil {
		t.Fatalf("command failed: %s %s: %v\nstdout: %s\nstderr: %s", binary, strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

func copyExecutable(source string, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open source executable: %w", err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		return fmt.Errorf("create protected executable: %w", err)
	}
	if _, err := io.Copy(output, input); err != nil {
		return errors.Join(fmt.Errorf("copy protected executable: %w", err), output.Close())
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("close protected executable: %w", err)
	}
	return nil
}

func waitForWorkerState(
	t *testing.T,
	state *store.PeerStore,
	key store.WorkerKey,
	status store.WorkerStatus,
	diagnostics ...func() string,
) store.WorkerReservation {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		worker, err := state.GetWorker(context.Background(), key)
		if err == nil && worker.Status == status {
			return worker
		}
		if err == nil && worker.Status == store.WorkerFailed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	worker, err := state.GetWorker(context.Background(), key)
	detail := ""
	if len(diagnostics) != 0 && diagnostics[0] != nil {
		detail = diagnostics[0]()
	}
	t.Fatalf("worker state = %#v, %v; want %s; diagnostics: %s", worker, err, status, detail)
	return store.WorkerReservation{}
}

type managedResponsesMock struct {
	mu                    sync.Mutex
	calls                 map[string]int
	errors                []string
	expectedAuthorization string
	probeCommand          string
	probeMarkers          []string
	protectedOutput       []managedProtectedValue
	runningStarted        chan struct{}
	runningDisconnected   chan struct{}
	runningStartedOnce    sync.Once
	runningDisconnectOnce sync.Once
}

type managedProtectedValue struct {
	label string
	value string
}

func managedSandboxProbe(
	credentialPath, serviceEnvironmentPath, configPath, delegationBinary, statePath, providerEnvironment string,
) (string, []string) {
	markers := []string{
		"WORKSPACE=readable", "OUTSIDE=blocked", "SERVICE_ENV=blocked", "CONNECTOR_CONFIG=blocked",
		"DELEGATION_BINARY=blocked", "PEER_STATE=blocked", "PROVIDER_ENV=hidden",
		"CONFIG_ENV=hidden", "SQLITE_ENV=hidden",
	}
	if runtime.GOOS == "windows" {
		markers = []string{
			"WORKSPACE=readable", "OUTSIDE=readable", "SERVICE_ENV=readable", "CONNECTOR_CONFIG=readable",
			"DELEGATION_BINARY=readable", "PEER_STATE=readable", "PROVIDER_ENV=hidden",
			"CONFIG_ENV=hidden", "SQLITE_ENV=hidden",
		}
		literal := managedPowerShellLiteral
		return fmt.Sprintf(`$ErrorActionPreference = 'Stop'
function Get-ReadState([string]$Path) {
  try {
    $share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, $share)
    $stream.Dispose()
    return 'readable'
  } catch {
    return 'blocked'
  }
}
function Get-EnvironmentState([string]$Name) {
  if ($null -ne [System.Environment]::GetEnvironmentVariable($Name, [System.EnvironmentVariableTarget]::Process)) {
    return 'visible'
  }
  return 'hidden'
}
try {
  $workspace = [System.IO.File]::ReadAllText('workspace-marker.txt').Trim()
} catch {
  $workspace = 'blocked'
}
Write-Output ('WORKSPACE=' + $workspace)
Write-Output ('OUTSIDE=' + (Get-ReadState %s))
Write-Output ('SERVICE_ENV=' + (Get-ReadState %s))
Write-Output ('CONNECTOR_CONFIG=' + (Get-ReadState %s))
Write-Output ('DELEGATION_BINARY=' + (Get-ReadState %s))
Write-Output ('PEER_STATE=' + (Get-ReadState %s))
Write-Output ('PROVIDER_ENV=' + (Get-EnvironmentState %s))
Write-Output ('CONFIG_ENV=' + (Get-EnvironmentState %s))
Write-Output ('SQLITE_ENV=' + (Get-EnvironmentState 'CODEX_SQLITE_HOME'))`,
			literal(credentialPath),
			literal(serviceEnvironmentPath),
			literal(configPath),
			literal(delegationBinary),
			literal(statePath),
			literal(providerEnvironment),
			literal(codexconfig.EnvironmentVariable),
		), markers
	}

	literal := managedPOSIXShellLiteral
	command := fmt.Sprintf(`set -eu
read_state() {
  if cat "$1" >/dev/null 2>&1; then printf readable; else printf blocked; fi
}
environment_state() {
  if printenv "$1" >/dev/null 2>&1; then printf visible; else printf hidden; fi
}
printf 'WORKSPACE=%%s\n' "$(cat workspace-marker.txt)"
printf 'OUTSIDE=%%s\n' "$(read_state %s)"
printf 'SERVICE_ENV=%%s\n' "$(read_state %s)"
printf 'CONNECTOR_CONFIG=%%s\n' "$(read_state %s)"
printf 'DELEGATION_BINARY=%%s\n' "$(read_state %s)"
printf 'PEER_STATE=%%s\n' "$(read_state %s)"
printf 'PROVIDER_ENV=%%s\n' "$(environment_state %s)"
printf 'CONFIG_ENV=%%s\n' "$(environment_state %s)"
printf 'SQLITE_ENV=%%s\n' "$(environment_state CODEX_SQLITE_HOME)"`,
		literal(credentialPath),
		literal(serviceEnvironmentPath),
		literal(configPath),
		literal(delegationBinary),
		literal(statePath),
		literal(providerEnvironment),
		literal(codexconfig.EnvironmentVariable),
	)
	if runtime.GOOS == "linux" {
		command += fmt.Sprintf(`
if tr '\000' '\n' </proc/$PPID/environ 2>/dev/null | grep -q '^%s='; then
  echo PARENT_ENV=visible
else
  echo PARENT_ENV=hidden
fi`, providerEnvironment)
		markers = append(markers, "PARENT_ENV=hidden")
	}
	return command, markers
}

func managedPOSIXShellLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func managedPowerShellLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func (m *managedResponsesMock) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/v1/responses" {
		m.fail(writer, fmt.Errorf("unexpected managed model request %s %s", request.Method, request.URL.Path))
		return
	}
	if request.Header.Get("Authorization") != m.expectedAuthorization {
		m.fail(writer, errors.New("managed provider authorization header is missing or incorrect"))
		return
	}
	body, err := decodeManagedRequest(request)
	if err != nil {
		m.fail(writer, err)
		return
	}
	encoded, _ := json.Marshal(body)
	testCase := ""
	for _, candidate := range []string{managedCrashResumeCase, managedCrashRunningCase, "resume", "first"} {
		if strings.Contains(string(encoded), "managed-worker-case="+candidate) {
			testCase = candidate
			break
		}
	}
	if testCase == "" {
		m.fail(writer, errors.New("managed model request has no test case"))
		return
	}
	tools, _ := json.Marshal(body["tools"])
	if strings.Contains(string(tools), "spawn_agent") || strings.Contains(string(tools), "multi_agent") {
		m.record(fmt.Errorf("managed worker exposes recursive delegation tools: %s", tools))
	}
	if !strings.Contains(string(tools), "tool_search") &&
		!strings.Contains(string(tools), "mcp__delegation_worker") {
		m.record(fmt.Errorf("managed worker exposes no worker-tool discovery path: %s", tools))
	}
	shellTool, shellArguments := managedShellTool(m.probeCommand)
	if !strings.Contains(string(tools), `"name":"`+shellTool+`"`) {
		m.record(fmt.Errorf("managed worker exposes no %s tool: %s", shellTool, tools))
	}
	m.mu.Lock()
	m.calls[testCase]++
	call := m.calls[testCase]
	m.mu.Unlock()
	if testCase == managedCrashRunningCase {
		if call != 1 {
			m.fail(writer, fmt.Errorf("managed case %s received call %d", testCase, call))
			return
		}
		m.runningStartedOnce.Do(func() { close(m.runningStarted) })
		<-request.Context().Done()
		m.runningDisconnectOnce.Do(func() { close(m.runningDisconnected) })
		return
	}
	if testCase == "first" && call == 1 && m.probeCommand != "" {
		arguments, _ := json.Marshal(shellArguments)
		writeManagedSSE(writer,
			map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-managed-probe"}},
			map[string]any{"type": "response.output_item.done", "item": map[string]any{
				"type": "function_call", "id": "fc-managed-probe", "call_id": "call-managed-probe",
				"name": shellTool, "arguments": string(arguments),
			}},
			managedCompletedEvent("resp-managed-probe"),
		)
		return
	}
	if testCase == "first" && m.probeCommand != "" && call == 2 {
		probeOutput := functionCallOutput(body["input"], "call-managed-probe")
		for _, protected := range m.protectedOutput {
			if protected.value != "" && strings.Contains(probeOutput, protected.value) {
				m.fail(writer, fmt.Errorf("managed sandbox probe exposed %s", protected.label))
				return
			}
		}
		for _, marker := range m.probeMarkers {
			if !strings.Contains(probeOutput, marker) {
				m.fail(writer, fmt.Errorf(
					"managed sandbox probe is missing marker %s (%s)",
					marker,
					managedProbeSummary(probeOutput, m.probeMarkers),
				))
				return
			}
		}
	} else if (testCase != "first" && testCase != "resume" && testCase != managedCrashResumeCase) || call != 1 {
		m.fail(writer, fmt.Errorf("managed case %s received call %d", testCase, call))
		return
	}
	writeManagedSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-managed-" + testCase}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "message", "role": "assistant", "id": "msg-managed-" + testCase,
			"content": []map[string]any{{"type": "output_text", "text": "probe-ok"}},
		}},
		managedCompletedEvent("resp-managed-"+testCase),
	)
}

func managedShellTool(command string) (string, map[string]any) {
	if runtime.GOOS == "windows" {
		return "shell_command", map[string]any{
			"command":    command,
			"timeout_ms": 10_000,
		}
	}
	return "exec_command", map[string]any{
		"cmd":               command,
		"yield_time_ms":     10_000,
		"max_output_tokens": 2_000,
	}
}

func managedProbeSummary(output string, markers []string) string {
	states := make([]string, 0, len(markers))
	for _, marker := range markers {
		name, expected, found := strings.Cut(marker, "=")
		if !found {
			continue
		}
		state := "absent"
		for _, candidate := range []string{expected, "readable", "blocked", "visible", "hidden"} {
			if strings.Contains(output, name+"="+candidate) {
				state = candidate
				break
			}
		}
		states = append(states, name+"="+state)
	}
	return strings.Join(states, ",")
}

func functionCallOutput(value any, callID string) string {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if output := functionCallOutput(item, callID); output != "" {
				return output
			}
		}
	case map[string]any:
		if typed["type"] == "function_call_output" && typed["call_id"] == callID {
			if output, ok := typed["output"].(string); ok {
				return output
			}
			encoded, _ := json.Marshal(typed["output"])
			return string(encoded)
		}
		for _, item := range typed {
			if output := functionCallOutput(item, callID); output != "" {
				return output
			}
		}
	}
	return ""
}

func (m *managedResponsesMock) fail(writer http.ResponseWriter, err error) {
	m.record(err)
	http.Error(writer, err.Error(), http.StatusBadRequest)
}

func (m *managedResponsesMock) record(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, err.Error())
}

func (m *managedResponsesMock) verify(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errors) != 0 {
		t.Fatalf("managed Responses errors: %s", strings.Join(m.errors, "\n"))
	}
	firstCalls := 1
	if m.probeCommand != "" {
		firstCalls = 2
	}
	if m.calls["first"] != firstCalls || m.calls["resume"] != 1 ||
		m.calls[managedCrashRunningCase] != 1 || m.calls[managedCrashResumeCase] != 1 {
		t.Fatalf("managed Responses calls = %#v", m.calls)
	}
}

func (m *managedResponsesMock) diagnostics() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fmt.Sprintf("calls=%v errors=%s", m.calls, strings.Join(m.errors, "; "))
}

func decodeManagedRequest(request *http.Request) (map[string]any, error) {
	var reader io.Reader = io.LimitReader(request.Body, 16<<20)
	if request.Header.Get("Content-Encoding") == "gzip" {
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("open compressed model request: %w", err)
		}
		defer compressed.Close()
		reader = compressed
	}
	var body map[string]any
	if err := json.NewDecoder(reader).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode model request: %w", err)
	}
	return body, nil
}

func managedCompletedEvent(id string) map[string]any {
	return map[string]any{"type": "response.completed", "response": map[string]any{
		"id": id,
		"usage": map[string]any{
			"input_tokens": 0, "input_tokens_details": nil, "output_tokens": 0,
			"output_tokens_details": nil, "total_tokens": 0,
		},
	}}
}

func writeManagedSSE(writer http.ResponseWriter, events ...map[string]any) {
	writer.Header().Set("Content-Type", "text/event-stream")
	buffer := bufio.NewWriter(writer)
	for _, event := range events {
		data, _ := json.Marshal(event)
		_, _ = fmt.Fprintf(buffer, "event: %s\ndata: %s\n\n", event["type"], data)
	}
	_ = buffer.Flush()
}
