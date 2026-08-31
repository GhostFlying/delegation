package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/userservice"
)

const upgradeTestTransactionID = "123e4567-e89b-42d3-a456-426614174388"

func TestServiceUpgradeRequiresBootstrapUntilCoordinationIsAvailable(t *testing.T) {
	for _, role := range []delegationconfig.Role{
		delegationconfig.RoleBroker, delegationconfig.RolePeer,
	} {
		t.Run(string(role), func(t *testing.T) {
			configPath, _ := writeStatusTestConfig(t, role)
			args := []string{
				"--config", configPath, "--target-version", "0.1.0-alpha.8",
			}
			if role == delegationconfig.RolePeer {
				args = append(args, "--environment-file", privateTestPath(t, "peer.env"))
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runServiceUpgrade(args, &stdout, &stderr)
			if code == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "--bootstrap") {
				t.Fatalf("upgrade = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
		})
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

func TestServiceUpgradeCancelUsesProtectedLocalJournal(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	called := 0
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runServiceUpgradeWithDependencies([]string{
		"--cancel", "--config", configPath, "--transaction-id",
		upgradeTestTransactionID, "--json", "--timeout", "5s",
	}, &stdout, &stderr, serviceUpgradeCommandDependencies{
		cancel: func(
			_ context.Context, gotConfig delegationconfig.Config, gotConfigPath, gotTransactionID string,
		) (localbridge.UpgradeSnapshot, error) {
			called++
			if gotConfig.Role != cfg.Role || gotConfigPath != configPath || gotTransactionID != upgradeTestTransactionID {
				t.Fatalf("cancel inputs = %#v, %q, %q", gotConfig, gotConfigPath, gotTransactionID)
			}
			return upgradeTestSnapshot(localupgrade.StateRolledBack), nil
		},
	})
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("cancel = %d, stderr %q", code, stderr.String())
	}
	var result localbridge.UpgradeSnapshot
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != string(localupgrade.StateRolledBack) || result.CommitAuthorized {
		t.Fatalf("cancel result = %#v", result)
	}
	if called != 1 {
		t.Fatalf("cancel calls = %d, want 1", called)
	}
}

func TestLegacyBootstrapDiscoversAlpha4RuntimeWithoutBridge(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	environmentFile := privateTestPath(t, "peer.env")
	legacyBinary := filepath.Join(t.TempDir(), "delegation-alpha4")
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
				return "0.1.0-alpha.4", nil
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
				if source != legacyBinary || version != "0.1.0-alpha.4" {
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runServiceUpgradeCompatibility(
		[]string{"--config", brokerConfig, "--json"}, &stdout, &stderr,
	); code != 0 || stderr.Len() != 0 {
		t.Fatalf("compatibility = %d, stderr %q", code, stderr.String())
	}
	var compatibility localupgrade.Compatibility
	if err := json.Unmarshal(stdout.Bytes(), &compatibility); err != nil {
		t.Fatal(err)
	}
	if err := compatibility.Validate(); err != nil {
		t.Fatalf("compatibility = %#v: %v", compatibility, err)
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
	return localbridge.UpgradeRuntimeIdentity{
		Version: "0.1.0-alpha.8", Digest: strings.Repeat("a", 64),
	}, nil
}

func upgradeTestSnapshot(state localupgrade.State) localbridge.UpgradeSnapshot {
	return localbridge.UpgradeSnapshot{
		TransactionID: upgradeTestTransactionID, State: string(state),
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8", UpdatedAt: 1,
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
