package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/userservice"
	"github.com/GhostFlying/delegation/internal/workerprofile"
	"github.com/GhostFlying/delegation/internal/workerreadiness"
)

const upgradeTestTransactionID = "123e4567-e89b-42d3-a456-426614174388"

func TestPeerServiceUpgradeRequiresBootstrap(t *testing.T) {
	configPath, _ := writeStatusTestConfig(t, delegationconfig.RolePeer)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgrade([]string{
		"--config", configPath, "--target-version", "0.1.0-alpha.8",
		"--environment-file", privateTestPath(t, "peer.env"),
	}, &stdout, &stderr)
	if code == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "--bootstrap") {
		t.Fatalf("upgrade = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestServiceUpgradeRejectsTimeoutOverThirtyMinutes(t *testing.T) {
	configPath, _ := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgrade([]string{
		"--config", configPath, "--target-version", "0.1.0-alpha.8", "--timeout", "30m1ns",
	}, &stdout, &stderr)
	if code == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "between 1ns and 30m") {
		t.Fatalf("oversized timeout = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestServiceUpgradeRejectsInvalidRoleFlagCombinations(t *testing.T) {
	peerConfig, _ := writeStatusTestConfig(t, delegationconfig.RolePeer)
	brokerConfig, _ := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	environmentFile := privateTestPath(t, "peer.env")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "peer environment required",
			args: []string{"--config", peerConfig, "--target-version", "0.1.0-alpha.8", "--bootstrap"},
			want: "peer service upgrade requires --environment-file",
		},
		{
			name: "broker environment forbidden",
			args: []string{
				"--config", brokerConfig, "--target-version", "0.1.0-alpha.8",
				"--environment-file", environmentFile, "--bootstrap",
			},
			want: "broker service upgrade must not use --environment-file",
		},
		{
			name: "cancel excludes prepare flags",
			args: []string{
				"--cancel", "--config", peerConfig, "--transaction-id", upgradeTestTransactionID,
				"--environment-file", environmentFile,
			},
			want: "--cancel requires --transaction-id and excludes",
		},
		{
			name: "prepare excludes transaction",
			args: []string{
				"--config", brokerConfig, "--target-version", "0.1.0-alpha.8",
				"--transaction-id", upgradeTestTransactionID, "--bootstrap",
			},
			want: "upgrade requires --target-version and excludes --transaction-id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runServiceUpgrade(test.args, &stdout, &stderr)
			if code == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("upgrade = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestServiceUpgradeBootstrapDoesNotRequireLegacyLocalBridge(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	environmentFile := privateTestPath(t, "peer.env")
	called := 0
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgradeWithDependencies([]string{
		"--config", configPath, "--environment-file", environmentFile,
		"--target-version", "0.1.0-alpha.8", "--bootstrap", "--json",
		"--timeout", "5s",
	}, &stdout, &stderr, serviceUpgradeCommandDependencies{
		bootstrap: func(
			_ context.Context, gotConfig delegationconfig.Config, gotConfigPath, gotEnvironment, gotTarget string,
		) (localbridge.UpgradeSnapshot, error) {
			called++
			if gotConfig.Role != cfg.Role || gotConfigPath != configPath ||
				gotEnvironment != environmentFile || gotTarget != "0.1.0-alpha.8" {
				t.Fatalf("bootstrap inputs = %#v, %q, %q, %q", gotConfig, gotConfigPath, gotEnvironment, gotTarget)
			}
			result := upgradeTestSnapshot(localupgrade.StateCommitted)
			result.CommitAuthorized = true
			return result, nil
		},
	})
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("upgrade = %d, stderr %q", code, stderr.String())
	}
	var result localbridge.UpgradeSnapshot
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != string(localupgrade.StateCommitted) || !result.CommitAuthorized {
		t.Fatalf("upgrade result = %#v", result)
	}
	if called != 1 {
		t.Fatalf("bootstrap calls = %d, want 1", called)
	}
}

func TestServiceUpgradeBootstrapReadsAlpha4Schema3Config(t *testing.T) {
	configPath, wantConfig := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	current, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(current, &document); err != nil {
		t.Fatal(err)
	}
	document["schemaVersion"] = json.RawMessage(`3`)
	delete(document, "transport")
	legacy, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	legacy = append(legacy, '\n')
	replaceProtectedTestFile(t, configPath, current, legacy)

	called := 0
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgradeWithDependencies([]string{
		"--config", configPath, "--target-version", "0.1.0-alpha.8",
		"--bootstrap", "--json", "--timeout", "5s",
	}, &stdout, &stderr, serviceUpgradeCommandDependencies{
		bootstrap: func(
			_ context.Context, got delegationconfig.Config, gotPath, environment, target string,
		) (localbridge.UpgradeSnapshot, error) {
			called++
			if !reflect.DeepEqual(got, wantConfig) || gotPath != configPath ||
				environment != "" || target != "0.1.0-alpha.8" {
				t.Fatalf("bootstrap inputs = %#v, %q, %q, %q", got, gotPath, environment, target)
			}
			result := upgradeTestSnapshot(localupgrade.StateCommitted)
			result.CommitAuthorized = true
			return result, nil
		},
	})
	if code != 0 || stderr.Len() != 0 || called != 1 {
		t.Fatalf("schema-3 bootstrap = %d, calls %d, stderr %q", code, called, stderr.String())
	}
}

func TestQualifyLocalUpgradeRequiresFreshReadyLocalPeer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("qualification endpoint isolation is covered by Windows local-bridge tests")
	}
	home, err := os.MkdirTemp("/tmp", "du-qualify-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)

	configPath := filepath.Join(home, "peer.json")
	environmentPath := filepath.Join(home, "peer.env")
	if err := os.WriteFile(configPath, []byte("target config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(environmentPath, []byte("TOKEN=value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimePath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	runtimeDigest, err := workerreadiness.RuntimeDigest(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	configDigest, err := workerreadiness.ConfigDigest(configPath, environmentPath)
	if err != nil {
		t.Fatal(err)
	}
	journal := localupgrade.Journal{
		Role: delegationconfig.RolePeer, InstanceID: delegationconfig.DefaultInstanceID,
		ControllerID: statusTestControllerID, DeviceID: statusTestDeviceID,
		SourceVersion: "0.1.0-alpha.6", TargetVersion: buildinfo.Version,
		TargetRuntimeDigest: runtimeDigest, ConfigDigest: configDigest, SourceReadinessEpoch: 7,
		Invocation: localupgrade.Invocation{
			TargetBinaryPath: runtimePath, ConfigPath: configPath, EnvironmentFile: environmentPath,
		},
	}
	ready := protocol.WorkerReadiness{
		Epoch: 8, State: protocol.WorkerReadinessReady, AttemptCount: 1,
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest,
		EpochStartedAt: 1, LastAttemptAt: 1, UpdatedAt: 1,
	}
	baseStatus := localbridge.StatusSnapshot{
		TransportStatus: delegationconfig.TransportStatus{Transport: "tcp"},
		Version:         buildinfo.Version, ControllerID: journal.ControllerID, DeviceID: journal.DeviceID,
		DeviceName: "upgrade-peer", ServiceRunning: true,
		ConnectionState: localbridge.ConnectionReady, Connected: true,
		WorkerRevision: 1, BrokerWorkerRevision: 1, WorkerSyncReady: true,
		WorkerReady: true, Dispatchable: true, WorkerReadiness: ready, MaxWorkerSlots: 1,
	}
	tests := []struct {
		name   string
		mutate func(*localbridge.StatusSnapshot, *localbridge.ServiceIdentity)
		want   string
	}{
		{name: "ready"},
		{name: "disconnected before broker upgrade", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.ConnectionState = localbridge.ConnectionConnecting
			status.Connected = false
			status.WorkerRevision = 0
			status.BrokerWorkerRevision = 0
			status.WorkerSyncReady = false
			status.Dispatchable = false
		}},
		{name: "stale epoch", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.WorkerReadiness.Epoch = journal.SourceReadinessEpoch
		}, want: "new execution-readiness epoch"},
		{name: "status version", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.Version = "0.1.0-alpha.6"
		}, want: "not running the target runtime"},
		{name: "pending", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.WorkerReadiness.State = protocol.WorkerReadinessPending
			status.WorkerReadiness.AttemptCount = 0
			status.WorkerReadiness.LastAttemptAt = 0
			status.WorkerReadiness.NextAttemptAt = 1
			status.WorkerReady = false
			status.Dispatchable = false
		}, want: "new execution-readiness epoch"},
		{name: "intervention required", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.WorkerReadiness.State = protocol.WorkerReadinessInterventionRequired
			status.WorkerReadiness.FailureCode = protocol.WorkerManagedHomeInvalid
			status.WorkerReady = false
			status.Dispatchable = false
		}, want: "new execution-readiness epoch"},
		{name: "runtime digest", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.WorkerReadiness.RuntimeDigest = strings.Repeat("c", 64)
		}, want: "new execution-readiness epoch"},
		{name: "config digest", mutate: func(status *localbridge.StatusSnapshot, _ *localbridge.ServiceIdentity) {
			status.WorkerReadiness.ConfigDigest = strings.Repeat("d", 64)
		}, want: "new execution-readiness epoch"},
		{name: "identity mismatch", mutate: func(_ *localbridge.StatusSnapshot, identity *localbridge.ServiceIdentity) {
			identity.DeviceID = statusTestOtherID
		}, want: "local bridge identity mismatch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := baseStatus
			identity := localbridge.ServiceIdentity{
				Role: journal.Role, InstanceID: journal.InstanceID,
				ControllerID: journal.ControllerID, DeviceID: journal.DeviceID,
			}
			if test.mutate != nil {
				test.mutate(&status, &identity)
			}
			stop := startQualificationBridge(t, journal, identity, status)
			err := qualifyLocalUpgrade(context.Background(), journal)
			stop()
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("qualification error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestQualifyLocalUpgradeBindsBrokerRuntimeConfigAndIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("qualification endpoint isolation is covered by Windows local-bridge tests")
	}
	home, err := os.MkdirTemp("/tmp", "du-broker-qualify-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	configPath := filepath.Join(home, "broker.json")
	if err := os.WriteFile(configPath, []byte("target config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimePath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	runtimeDigest, err := workerreadiness.RuntimeDigest(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	configDigest, err := workerreadiness.ConfigDigest(configPath)
	if err != nil {
		t.Fatal(err)
	}
	journal := localupgrade.Journal{
		Role: delegationconfig.RoleBroker, InstanceID: delegationconfig.DefaultInstanceID,
		ControllerID: statusTestControllerID, SourceVersion: "0.1.0-alpha.6",
		TargetVersion: buildinfo.Version, TargetRuntimeDigest: runtimeDigest,
		ConfigDigest: configDigest, Invocation: localupgrade.Invocation{
			TargetBinaryPath: runtimePath, ConfigPath: configPath,
		},
	}
	identity := localbridge.ServiceIdentity{
		Role: delegationconfig.RoleBroker, InstanceID: journal.InstanceID,
		ControllerID: journal.ControllerID,
	}
	stop := startQualificationBridge(t, journal, identity, localbridge.StatusSnapshot{})
	defer stop()
	if err := qualifyLocalUpgrade(context.Background(), journal); err != nil {
		t.Fatal(err)
	}

	journal.ConfigDigest = strings.Repeat("e", 64)
	if err := qualifyLocalUpgrade(context.Background(), journal); err == nil ||
		!strings.Contains(err.Error(), "configuration does not match") {
		t.Fatalf("broker config mismatch error = %v", err)
	}
}

func TestValidateUpgradeActivatorRuntimeAcceptsSymlinkedTargetPath(t *testing.T) {
	physicalRoot := t.TempDir()
	logicalRoot := filepath.Join(t.TempDir(), "logical-home")
	if err := os.Symlink(physicalRoot, logicalRoot); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	runtimePath := filepath.Join(physicalRoot, "bin", "delegation")
	if err := os.MkdirAll(filepath.Dir(runtimePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(runtimePath, []byte("target runtime\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	digest, err := workerreadiness.RuntimeDigest(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	journal := localupgrade.Journal{
		TargetVersion: buildinfo.Version, TargetRuntimeDigest: digest,
		Invocation: localupgrade.Invocation{
			TargetBinaryPath: filepath.Join(logicalRoot, "bin", "delegation"),
		},
	}
	if err := validateUpgradeActivatorRuntimePath(journal, runtimePath); err != nil {
		t.Fatalf("symlinked target runtime = %v", err)
	}

	otherPath := filepath.Join(physicalRoot, "bin", "other")
	if err := os.WriteFile(otherPath, []byte("target runtime\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validateUpgradeActivatorRuntimePath(journal, otherPath); err == nil ||
		!strings.Contains(err.Error(), "identity does not match") {
		t.Fatalf("different resolved runtime error = %v", err)
	}

	journal.TargetRuntimeDigest = strings.Repeat("f", 64)
	if err := validateUpgradeActivatorRuntimePath(journal, runtimePath); err == nil ||
		!strings.Contains(err.Error(), "identity does not match") {
		t.Fatalf("runtime digest mismatch error = %v", err)
	}
}

func TestServiceUpgradePrefersCurrentProtectedLocalBridge(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the shared Windows test environment cannot replace the process user profile")
	}
	home, err := os.MkdirTemp("/tmp", "du-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	environmentFile := privateTestPath(t, "peer.env")
	manager := &cliUpgradeManager{snapshot: upgradeTestSnapshot(localupgrade.StatePrepared)}
	stop := startCLIUpgradeBridge(t, cfg, manager)
	defer stop()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgrade([]string{
		"--config", configPath, "--environment-file", environmentFile,
		"--target-version", "0.1.0-alpha.8", "--bootstrap", "--json",
		"--timeout", "5s",
	}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("upgrade = %d, stderr %q", code, stderr.String())
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.prepareCalls != 1 || manager.armCalls != 1 || manager.activateCalls != 1 {
		t.Fatalf("upgrade calls = %#v", manager)
	}
}

func TestServiceUpgradeCancelUsesProtectedControllerJournal(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	called := 0
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgradeWithDependencies([]string{
		"--cancel", "--config", configPath, "--transaction-id",
		upgradeTestTransactionID, "--json", "--timeout", "5s",
	}, &stdout, &stderr, serviceUpgradeCommandDependencies{
		cancelCoordinated: func(
			_ context.Context, gotConfig delegationconfig.Config, gotTransactionID string,
		) (localbridge.ControllerUpgradeSnapshot, error) {
			called++
			if gotConfig.Role != cfg.Role || gotTransactionID != upgradeTestTransactionID {
				t.Fatalf("cancel inputs = %#v, %q", gotConfig, gotTransactionID)
			}
			return controllerUpgradeTestSnapshot("canceled"), nil
		},
	})
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("cancel = %d, stderr %q", code, stderr.String())
	}
	var result localbridge.ControllerUpgradeSnapshot
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "canceled" || result.GlobalCommit {
		t.Fatalf("cancel result = %#v", result)
	}
	if called != 1 {
		t.Fatalf("cancel calls = %d, want 1", called)
	}
}

func TestBrokerServiceUpgradeUsesProtectedControllerManagement(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	called := 0
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgradeWithDependencies([]string{
		"--config", configPath, "--target-version", "0.1.0-alpha.8",
		"--timeout", "5s", "--json",
	}, &stdout, &stderr, serviceUpgradeCommandDependencies{
		coordinate: func(
			_ context.Context, gotConfig delegationconfig.Config, target string, timeout time.Duration,
		) (localbridge.ControllerUpgradeSnapshot, error) {
			called++
			if gotConfig.Role != cfg.Role || target != "0.1.0-alpha.8" || timeout != 5*time.Second {
				t.Fatalf("coordinate inputs = %#v, %q, %v", gotConfig, target, timeout)
			}
			return controllerUpgradeTestSnapshot("completed"), nil
		},
	})
	if code != 0 || stderr.Len() != 0 || called != 1 {
		t.Fatalf("upgrade = %d, stderr %q, calls %d", code, stderr.String(), called)
	}
	var result localbridge.ControllerUpgradeSnapshot
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "completed" || !result.GlobalCommit {
		t.Fatalf("controller result = %#v", result)
	}
}

func TestCoordinatedServiceUpgradeRetriesLostStartRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	startCalls := 0
	readCalls := 0
	result, err := coordinateServiceUpgradeWithClient(
		ctx, "0.1.0-alpha.8", 5_000,
		func(context.Context, string, int64) (localbridge.ControllerUpgradeSnapshot, error) {
			startCalls++
			if startCalls == 1 {
				return localbridge.ControllerUpgradeSnapshot{}, errors.New("request lost")
			}
			return controllerUpgradeTestSnapshot("completed"), nil
		},
		func(context.Context) (*localbridge.ControllerUpgradeSnapshot, error) {
			readCalls++
			return nil, nil
		},
	)
	if err != nil || result.State != "completed" || startCalls != 2 || readCalls == 0 {
		t.Fatalf("lost start recovery = %#v, %v, starts=%d reads=%d",
			result, err, startCalls, readCalls)
	}
}

func TestCoordinatedServiceUpgradeRecoversLostActivationResponseFromStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want := controllerUpgradeTestSnapshot("completed_with_errors")
	startCalls := 0
	result, err := coordinateServiceUpgradeWithClient(
		ctx, want.TargetVersion, 5_000,
		func(context.Context, string, int64) (localbridge.ControllerUpgradeSnapshot, error) {
			startCalls++
			return localbridge.ControllerUpgradeSnapshot{}, errors.New("activation response lost")
		},
		func(context.Context) (*localbridge.ControllerUpgradeSnapshot, error) { return &want, nil },
	)
	if err != nil || !reflect.DeepEqual(result, want) || startCalls != 1 {
		t.Fatalf("lost response recovery = %#v, %v, starts=%d", result, err, startCalls)
	}
}

func TestLegacyBootstrapDiscoversAlpha4RuntimeWithoutBridge(t *testing.T) {
	testLegacyBootstrapDiscoversRuntimeWithoutBridge(t, "0.1.0-alpha.4")
}

func TestLegacyBootstrapDiscoversAlpha7RuntimeWithoutBridge(t *testing.T) {
	testLegacyBootstrapDiscoversRuntimeWithoutBridge(t, "0.1.0-alpha.7")
}

func testLegacyBootstrapDiscoversRuntimeWithoutBridge(t *testing.T, currentVersion string) {
	t.Helper()
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	environmentFile := privateTestPath(t, "peer.env")
	legacyBinary := filepath.Join(t.TempDir(), "delegation-legacy")
	manager := &cliUpgradeManager{absent: true, snapshot: upgradeTestSnapshot(localupgrade.StatePrepared)}
	managerFactoryCalls := 0
	discoverCalls := 0
	probeCalls := 0
	result, err := bootstrapLegacyServiceUpgradeWithDependencies(
		context.Background(), cfg, configPath, environmentFile, "0.1.0-alpha.8",
		legacyBootstrapDependencies{
			discoverSource: func(
				_ context.Context, role userservice.ServiceRole, expected userservice.Invocation,
			) (userservice.Invocation, error) {
				discoverCalls++
				if role != userservice.ServiceRolePeer || expected.BinaryPath != "" ||
					expected.ConfigPath != configPath || expected.EnvironmentFile != environmentFile {
					t.Fatalf("discovery request = %q, %#v", role, expected)
				}
				expected.BinaryPath = legacyBinary
				return expected, nil
			},
			probeVersion: func(_ context.Context, path string) (string, error) {
				probeCalls++
				if path != legacyBinary {
					t.Fatalf("version probe path = %q", path)
				}
				return currentVersion, nil
			},
			newManager: func(
				_ delegationconfig.Config, gotConfig, gotEnvironment, source, version string,
			) (localUpgradeControl, error) {
				managerFactoryCalls++
				if gotConfig != configPath || gotEnvironment != environmentFile {
					t.Fatalf("manager paths = %q, %q", gotConfig, gotEnvironment)
				}
				if managerFactoryCalls == 1 {
					if source != "" || version != "" {
						t.Fatalf("resume manager source = %q, %q", source, version)
					}
					return manager, nil
				}
				if source != legacyBinary || version != currentVersion {
					t.Fatalf("legacy manager source = %q, %q", source, version)
				}
				manager.absent = false
				return manager, nil
			},
		},
	)
	if err != nil || result.State != string(localupgrade.StateCommitted) || !result.CommitAuthorized {
		t.Fatalf("bootstrap = %#v, %v", result, err)
	}
	if managerFactoryCalls != 2 || discoverCalls != 1 || probeCalls != 1 {
		t.Fatalf("calls = manager %d, discover %d, probe %d", managerFactoryCalls, discoverCalls, probeCalls)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.prepareCalls != 1 || manager.armCalls != 1 || manager.activateCalls != 1 {
		t.Fatalf("manager calls = %#v", manager)
	}
}

func TestLegacyBootstrapResumesJournalWithoutRediscoveringSource(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	manager := &cliUpgradeManager{snapshot: upgradeTestSnapshot(localupgrade.StatePrepared)}
	result, err := bootstrapLegacyServiceUpgradeWithDependencies(
		context.Background(), cfg, configPath, "", "0.1.0-alpha.8",
		legacyBootstrapDependencies{
			discoverSource: func(
				context.Context, userservice.ServiceRole, userservice.Invocation,
			) (userservice.Invocation, error) {
				t.Fatal("same-target resume rediscovered the legacy service")
				return userservice.Invocation{}, nil
			},
			probeVersion: func(context.Context, string) (string, error) {
				t.Fatal("same-target resume re-executed the source runtime")
				return "", nil
			},
			newManager: func(
				delegationconfig.Config, string, string, string, string,
			) (localUpgradeControl, error) {
				return manager, nil
			},
		},
	)
	if err != nil || result.State != string(localupgrade.StateCommitted) {
		t.Fatalf("resume = %#v, %v", result, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.prepareCalls != 1 || manager.armCalls != 1 || manager.activateCalls != 1 {
		t.Fatalf("resume calls = %#v", manager)
	}
}

func TestLegacyBootstrapFailsClosedOnSourceVersionProbe(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	manager := &cliUpgradeManager{absent: true, snapshot: upgradeTestSnapshot(localupgrade.StatePrepared)}
	result, err := bootstrapLegacyServiceUpgradeWithDependencies(
		context.Background(), cfg, configPath, "", "0.1.0-alpha.8",
		legacyBootstrapDependencies{
			discoverSource: func(
				_ context.Context, _ userservice.ServiceRole, expected userservice.Invocation,
			) (userservice.Invocation, error) {
				expected.BinaryPath = filepath.Join(t.TempDir(), "legacy")
				return expected, nil
			},
			probeVersion: func(context.Context, string) (string, error) {
				return "", errors.New("version mismatch")
			},
			newManager: func(
				delegationconfig.Config, string, string, string, string,
			) (localUpgradeControl, error) {
				return manager, nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "verify legacy service runtime version") ||
		result.TransactionID != "" {
		t.Fatalf("bootstrap = %#v, %v", result, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.prepareCalls != 0 || manager.armCalls != 0 || manager.activateCalls != 0 {
		t.Fatalf("failed probe mutated transaction: %#v", manager)
	}
}

func TestServiceUpgradeCompatibilityRequiresInternalContract(t *testing.T) {
	brokerConfig, _ := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	peerConfig, _ := writeStatusTestConfig(t, delegationconfig.RolePeer)
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "JSON required", args: []string{"--config", brokerConfig}, want: "--config and --json are required"},
		{name: "peer environment required", args: []string{"--config", peerConfig, "--json"}, want: "peer compatibility probe requires --environment-file"},
		{name: "broker environment forbidden", args: []string{"--config", brokerConfig, "--environment-file", privateTestPath(t, "peer.env"), "--json"}, want: "broker compatibility probe must not use --environment-file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := runServiceUpgradeCompatibility(test.args, &stdout, &stderr); code == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("compatibility = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
		})
	}

	for _, test := range []struct {
		name        string
		args        []string
		wantKind    string
		wantProfile int
	}{
		{name: "broker", args: []string{"--config", brokerConfig, "--json"}, wantKind: "broker"},
		{name: "peer", args: []string{
			"--config", peerConfig, "--environment-file", privateTestPath(t, "peer.env"), "--json",
		}, wantKind: "peer", wantProfile: workerprofile.CurrentVersion},
	} {
		t.Run(test.name+" identity", func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := runServiceUpgradeCompatibility(test.args, &stdout, &stderr); code != 0 || stderr.Len() != 0 {
				t.Fatalf("compatibility = %d, stderr %q", code, stderr.String())
			}
			var document map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &document); err != nil {
				t.Fatal(err)
			}
			if document["schemaVersion"] != float64(localupgrade.CompatibilitySchemaVersion) ||
				document["databaseKind"] != test.wantKind ||
				document["workerProfileVersion"] != float64(test.wantProfile) {
				t.Fatalf("compatibility JSON = %#v", document)
			}
			var compatibility localupgrade.Compatibility
			if err := json.Unmarshal(stdout.Bytes(), &compatibility); err != nil {
				t.Fatal(err)
			}
			if err := compatibility.Validate(); err != nil {
				t.Fatalf("compatibility = %#v: %v", compatibility, err)
			}
		})
	}
}

func TestServiceUpgradeActivatorRejectsUntrustedInputWithoutCreatingIt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing", "upgrade")
	var stderr bytes.Buffer
	code := runServiceUpgradeActivator([]string{
		"--upgrade-root", root, "--transaction-id", upgradeTestTransactionID,
	}, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "validate protected upgrade root") {
		t.Fatalf("activator = %d, stderr %q", code, stderr.String())
	}
	if _, err := os.Lstat(filepath.Dir(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("activator created input path: %v", err)
	}
}

type cliUpgradeManager struct {
	mu              sync.Mutex
	snapshot        localbridge.UpgradeSnapshot
	prepareCalls    int
	armCalls        int
	activateCalls   int
	cancelCalls     int
	targetVersion   string
	configPath      string
	environmentFile string
	transactionID   string
	absent          bool
	runtimeIdentity localbridge.UpgradeRuntimeIdentity
}

func (m *cliUpgradeManager) PrepareLocalUpgrade(
	_ context.Context, targetVersion, configPath, environmentFile string,
) (localbridge.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prepareCalls++
	m.targetVersion = targetVersion
	m.configPath = configPath
	m.environmentFile = environmentFile
	m.snapshot.State = string(localupgrade.StatePrepared)
	return m.snapshot, nil
}

func (m *cliUpgradeManager) ArmLocalUpgrade(
	_ context.Context, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.armCalls++
	m.transactionID = transactionID
	m.snapshot.State = string(localupgrade.StateArmed)
	return m.snapshot, nil
}

func (m *cliUpgradeManager) ActivateLocalUpgrade(
	_ context.Context, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activateCalls++
	m.transactionID = transactionID
	m.snapshot.State = string(localupgrade.StateCommitted)
	m.snapshot.CommitAuthorized = true
	m.snapshot.UpdatedAt++
	return m.snapshot, nil
}

func (m *cliUpgradeManager) CancelLocalUpgrade(
	_ context.Context, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelCalls++
	m.transactionID = transactionID
	m.snapshot.State = string(localupgrade.StateRolledBack)
	m.snapshot.CommitAuthorized = false
	m.snapshot.UpdatedAt++
	return m.snapshot, nil
}

func (m *cliUpgradeManager) LocalUpgrade(context.Context) (*localbridge.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.absent {
		return nil, nil
	}
	result := m.snapshot
	return &result, nil
}

func (m *cliUpgradeManager) LocalUpgradeRuntime(context.Context) (localbridge.UpgradeRuntimeIdentity, error) {
	if m.runtimeIdentity.Version != "" {
		return m.runtimeIdentity, nil
	}
	return localbridge.UpgradeRuntimeIdentity{
		Version: "0.1.0-alpha.8", Digest: strings.Repeat("a", 64),
	}, nil
}

func startQualificationBridge(
	t *testing.T, journal localupgrade.Journal, identity localbridge.ServiceIdentity,
	status localbridge.StatusSnapshot,
) func() {
	t.Helper()
	endpoint, _, err := localUpgradeEndpoint(delegationconfig.Config{
		Role: journal.Role, InstanceID: journal.InstanceID, ControllerID: journal.ControllerID,
		DeviceID: journal.DeviceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := &cliUpgradeManager{runtimeIdentity: localbridge.UpgradeRuntimeIdentity{
		Version: journal.TargetVersion, Digest: journal.TargetRuntimeDigest,
	}}
	server, err := localbridge.ListenWithUpgradeManagement(
		endpoint, identity, statusTestBackend{}, nil, staticLocalStatus{status: status},
		nil, nil, nil, manager,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close qualification bridge: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("serve qualification bridge: %v", err)
		}
	}
}

func upgradeTestSnapshot(state localupgrade.State) localbridge.UpgradeSnapshot {
	return localbridge.UpgradeSnapshot{
		TransactionID: upgradeTestTransactionID, State: string(state),
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8", UpdatedAt: 1,
	}
}

func controllerUpgradeTestSnapshot(state string) localbridge.ControllerUpgradeSnapshot {
	return localbridge.ControllerUpgradeSnapshot{
		TransactionID: upgradeTestTransactionID, State: state,
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8",
		GlobalCommit: state != "canceled", Deadline: 2, UpdatedAt: 1,
		Participants: localbridge.ControllerUpgradeCounts{},
	}
}

func startCLIUpgradeBridge(
	t *testing.T, cfg delegationconfig.Config, manager localbridge.UpgradeManager,
) func() {
	t.Helper()
	endpoint, identity, err := localUpgradeEndpoint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := localbridge.ListenWithUpgradeManagement(
		endpoint, identity, nil, nil, nil, nil, nil, nil, manager,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close upgrade bridge: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("serve upgrade bridge: %v", err)
		}
	}
}
