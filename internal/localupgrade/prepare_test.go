package localupgrade

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/releaseverify"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/userservice"
)

func TestPrepareCreatesProtectedJournalAndResumesBeforeAcquisition(t *testing.T) {
	options := prepareFixture(t)
	calls := 0
	options.Dependencies.AcquireRelease = func(context.Context, string, string) (releaseverify.Result, error) {
		calls++
		return releaseverify.Result{Version: options.TargetVersion}, nil
	}
	first, err := Prepare(context.Background(), options)
	if err != nil || first.Resumed {
		t.Fatalf("Prepare() = %#v, %v", first, err)
	}
	if first.Journal.State != StatePrepared || first.Journal.TargetVersion != options.TargetVersion ||
		first.Journal.Definition.OldDigest == first.Journal.Definition.NewDigest || calls != 1 {
		t.Fatalf("prepared journal = %#v, acquire calls = %d", first.Journal, calls)
	}
	second, err := Prepare(context.Background(), options)
	if err != nil || !second.Resumed || second.Journal.TransactionID != first.Journal.TransactionID || calls != 1 {
		t.Fatalf("resume = %#v, %v, acquire calls = %d", second, err, calls)
	}
	for _, path := range []string{first.Journal.Definition.OldPath, first.Journal.Definition.NewPath} {
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("definition material %s = %#v, %v", path, info, statErr)
		}
	}
}

func TestPrepareUsesReservedCoordinatedTransactionIdentity(t *testing.T) {
	options := prepareFixture(t)
	options.TransactionID = "123e4567-e89b-42d3-a456-426614174897"
	options.ControllerTransactionID = "123e4567-e89b-42d3-a456-426614174898"
	options.Dependencies.NewID = func() (string, error) {
		t.Fatal("Prepare generated an ID despite a reserved transaction identity")
		return "", nil
	}
	first, err := Prepare(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if first.Journal.TransactionID != options.TransactionID ||
		first.Journal.ControllerTransactionID != options.ControllerTransactionID {
		t.Fatalf("coordinated identity = %#v", first.Journal)
	}
	second, err := Prepare(context.Background(), options)
	if err != nil || !second.Resumed || second.Journal.TransactionID != options.TransactionID {
		t.Fatalf("reserved identity resume = %#v, %v", second, err)
	}
	different := options
	different.TransactionID = "123e4567-e89b-42d3-a456-426614174896"
	if _, err := Prepare(context.Background(), different); err == nil ||
		!strings.Contains(err.Error(), "reserved transaction identity") {
		t.Fatalf("different reserved identity error = %v", err)
	}
}

func TestPrepareRejectsBusyStateBeforeServiceInspection(t *testing.T) {
	options := prepareFixture(t)
	serviceCalls := 0
	options.Dependencies.ReadBlockers = func(context.Context) (store.UpgradeBlockers, error) {
		return store.UpgradeBlockers{OccupiedWorkers: 1}, nil
	}
	options.Dependencies.PrepareService = func(context.Context, userservice.ServiceRole, userservice.Invocation, userservice.Invocation) (userservice.UpgradePlan, error) {
		serviceCalls++
		return userservice.UpgradePlan{}, nil
	}
	if _, err := Prepare(context.Background(), options); err == nil || !strings.Contains(err.Error(), "active durable work") {
		t.Fatalf("Prepare() error = %v", err)
	}
	if serviceCalls != 0 {
		t.Fatalf("service inspection calls = %d", serviceCalls)
	}
}

func TestPrepareRejectsCompatibilityAndConfigurationDrift(t *testing.T) {
	options := prepareFixture(t)
	options.Dependencies.ProbeTarget = func(context.Context, string, string, string) (Compatibility, error) {
		compatibility := testCompatibility(t, delegationconfig.RolePeer, options.TargetVersion)
		compatibility.TailscaleGeneration++
		return compatibility, compatibility.Validate()
	}
	if _, err := Prepare(context.Background(), options); err == nil || !strings.Contains(err.Error(), "Tailscale") {
		t.Fatalf("compatibility drift error = %v", err)
	}

	options = prepareFixture(t)
	originalPrepare := options.Dependencies.PrepareService
	options.Dependencies.PrepareService = func(ctx context.Context, role userservice.ServiceRole, source, target userservice.Invocation) (userservice.UpgradePlan, error) {
		plan, err := originalPrepare(ctx, role, source, target)
		if err == nil {
			err = os.WriteFile(options.ConfigPath, []byte("changed"), 0o600)
		}
		return plan, err
	}
	if _, err := Prepare(context.Background(), options); err == nil || !strings.Contains(err.Error(), "configuration changed") {
		t.Fatalf("configuration drift error = %v", err)
	}
}

func TestPrepareAcceptsExactAlpha4PeerConfigAndDatabase(t *testing.T) {
	options := prepareFixture(t)
	currentConfig, err := os.ReadFile(options.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(currentConfig, &document); err != nil {
		t.Fatal(err)
	}
	document["schemaVersion"] = json.RawMessage(`3`)
	delete(document, "transport")
	legacyConfig, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	legacyConfig = append(legacyConfig, '\n')
	if err := os.WriteFile(options.ConfigPath, legacyConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", options.Config.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`DROP TABLE worker_readiness; PRAGMA user_version = 15`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	options.Dependencies.ReadBlockers = nil
	options.Dependencies.ReadReadinessEpoch = nil
	options.Dependencies.ProbeTarget = func(
		_ context.Context, _ string, targetConfigPath, _ string,
	) (Compatibility, error) {
		targetConfig, err := delegationconfig.ReadForRuntime(
			targetConfigPath, delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true},
		)
		if err != nil || targetConfig != options.Config {
			t.Fatalf("target probe config = %#v, %v", targetConfig, err)
		}
		return testCompatibility(t, delegationconfig.RolePeer, options.TargetVersion), nil
	}

	result, err := Prepare(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	journal := result.Journal
	if journal.Configuration.SourceDigest == journal.Configuration.TargetDigest ||
		journal.SourceConfigDigest == journal.ConfigDigest || journal.SourceReadinessEpoch != 0 ||
		journal.Database.SourceIdentity.SchemaVersion != 15 ||
		journal.Database.TargetIdentity.SchemaVersion != 16 {
		t.Fatalf("alpha.4 preparation journal = %#v", journal)
	}
	if source, err := os.ReadFile(journal.Configuration.SourcePath); err != nil ||
		!strings.Contains(string(source), `"schemaVersion": 3`) ||
		strings.Contains(string(source), `"transport"`) {
		t.Fatalf("source config = %q, %v", source, err)
	}
	if _, err := delegationconfig.Read(journal.Configuration.TargetPath); err != nil {
		t.Fatalf("target config is not current: %v", err)
	}
}

func prepareFixture(t *testing.T) PrepareOptions {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{root, filepath.Join(root, "codex-home"), filepath.Join(root, "workspaces")} {
		if err := os.Chmod(directory, 0o700); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	configPath := filepath.Join(root, "peer.json")
	environmentPath := filepath.Join(root, "peer.env")
	sourceBinary := filepath.Join(root, "source-delegation")
	targetBinary := filepath.Join(root, "target-delegation")
	for path, data := range map[string][]byte{
		environmentPath: []byte("TOKEN=value\n"),
		sourceBinary:    []byte("source"), targetBinary: []byte("target"),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	statePath := filepath.Join(root, "peer.sqlite3")
	peer, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	transactionStore, err := OpenStore(filepath.Join(root, "upgrade"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion, InstanceID: delegationconfig.DefaultInstanceID,
		HostKind: hostkind.Codex, Role: delegationconfig.RolePeer,
		ControllerID: "123e4567-e89b-42d3-a456-426614174801",
		DeviceID:     "123e4567-e89b-42d3-a456-426614174802", DeviceName: "upgrade-peer",
		Transport: delegationconfig.TransportConfig{Mode: delegationconfig.TransportModeTCP},
		Broker:    delegationconfig.BrokerConfig{URL: "wss://broker.example.test", Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone}},
		Peer: delegationconfig.PeerConfig{
			CodexBinary: filepath.Join(root, "codex"), GitBinary: filepath.Join(root, "git"),
			CodexHome: filepath.Join(root, "codex-home"), WorkspaceRoot: filepath.Join(root, "workspaces"),
			StateFile: statePath, MaxWorkerSlots: 1,
		},
	}
	configData, err := delegationconfig.EncodeForRuntime(
		cfg, delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readBlockers := func(context.Context) (store.UpgradeBlockers, error) { return store.UpgradeBlockers{}, nil }
	return PrepareOptions{
		Store: transactionStore, Home: root, Config: cfg, ConfigPath: configPath,
		EnvironmentFile: environmentPath, SourceBinary: sourceBinary,
		CurrentVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8",
		Dependencies: PrepareDependencies{
			AcquireRelease: func(context.Context, string, string) (releaseverify.Result, error) {
				return releaseverify.Result{Version: "0.1.0-alpha.8"}, nil
			},
			InstallRuntime: func(context.Context, string, releaseverify.Result) (releaseverify.RuntimeMaterial, error) {
				return releaseverify.RuntimeMaterial{BinaryPath: targetBinary, BinarySHA256: digestBytes([]byte("target")), Directory: root}, nil
			},
			ProbeTarget: func(context.Context, string, string, string) (Compatibility, error) {
				return testCompatibility(t, delegationconfig.RolePeer, "0.1.0-alpha.8"), nil
			},
			PrepareService: func(_ context.Context, _ userservice.ServiceRole, source, target userservice.Invocation) (userservice.UpgradePlan, error) {
				kind := userservice.KindSystemd
				processGroup := "/user.slice/delegation"
				if runtime.GOOS == "darwin" {
					kind, processGroup = userservice.KindLaunchAgent, ""
				} else if runtime.GOOS == "windows" {
					kind, processGroup = userservice.KindScheduledTask, ""
				}
				return userservice.UpgradePlan{
					Role: userservice.ServiceRolePeer, Kind: kind, NativeName: "delegation-peer",
					Artifact: filepath.Join(root, "service.definition"), UserIdentity: "current-user",
					SourceInvocation: source, TargetInvocation: target,
					OldDefinition: []byte("source definition"), NewDefinition: []byte("target definition"),
					ProcessIDs: []int{42}, ProcessGroup: processGroup,
				}, nil
			},
			ReadBlockers: readBlockers, NewID: func() (string, error) {
				return "123e4567-e89b-42d3-a456-426614174899", nil
			},
			ReadReadinessEpoch: func(context.Context) (uint64, error) { return 3, nil },
			Now:                func() time.Time { return time.Unix(100, 0) },
		},
	}
}

func testCompatibility(t *testing.T, role delegationconfig.Role, version string) Compatibility {
	t.Helper()
	result, err := CurrentCompatibility(role, version)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
