package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	statusTestControllerID = "123e4567-e89b-42d3-a456-426614174800"
	statusTestDeviceID     = "123e4567-e89b-42d3-a456-426614174801"
	statusTestOtherID      = "123e4567-e89b-42d3-a456-426614174802"
)

type staticConnectorStatus struct {
	status connector.Status
}

func (s staticConnectorStatus) Status() connector.Status {
	return s.status
}

type staticPeerStatusStore struct {
	status    store.PeerStatusSnapshot
	readiness protocol.WorkerReadiness
	err       error
}

func (s staticPeerStatusStore) WorkerReadiness(context.Context) (protocol.WorkerReadiness, error) {
	return s.readiness, s.err
}

func (s staticPeerStatusStore) ReadPeerStatusSnapshot(
	context.Context,
	string,
	string,
) (store.PeerStatusSnapshot, error) {
	return s.status, s.err
}

type staticLocalStatus struct {
	status localbridge.StatusSnapshot
	err    error
}

func (s staticLocalStatus) LocalStatus(context.Context) (localbridge.StatusSnapshot, error) {
	return s.status, s.err
}

type statusTestBackend struct{}

func (statusTestBackend) Call(
	context.Context,
	string,
	string,
	*control.PrincipalIdentity,
	any,
	any,
) error {
	return errors.New("unexpected status test backend call")
}

func TestPeerLocalStatusProviderCombinesLiveAndDurableState(t *testing.T) {
	readiness := readyStatusTestReadiness()
	durable := store.PeerStatusSnapshot{
		WorkerRevision: 77,
		Workers: store.PeerStatusWorkerCounts{
			Total: 10, Reserved: 1, Pending: 1, Starting: 1, Preflight: 1,
			Ready: 1, Running: 1, Finalizing: 1, Idle: 1, Interrupted: 1,
			Failed: 1, Occupied: 6,
		},
		Artifacts: store.PeerStatusArtifactCounts{
			CaptureBacklog: 2, PublishBacklog: 3, Retained: 4, RetainedBytes: 8192,
		},
		Results: store.PeerStatusResultCounts{
			OutboxCapturePending: 1, OutboxPublishPending: 2,
			OutboxDeliveryPending: 3, OutboxDelivered: 4,
			OutboxReleasePending: 5,
			OutboxRetainedBytes:  16384,
			InboxReceiving:       5, InboxAvailable: 6, InboxEvictionPending: 7,
			InboxEvicted:         8,
			InboxRetainedBytes:   32768,
			RolloutCaptureFailed: 2, WorkspaceCaptureFailed: 1,
		},
	}
	provider := peerLocalStatusProvider{
		client: staticConnectorStatus{status: connector.Status{
			Connected: true, RegistryRevision: 42, WorkerRevision: 77,
		}},
		state:          staticPeerStatusStore{status: durable, readiness: readiness},
		transport:      delegationconfig.TransportStatus{Transport: "tcp"},
		controllerID:   statusTestControllerID,
		deviceID:       statusTestDeviceID,
		deviceName:     "status-peer",
		maxWorkerSlots: 8,
	}

	got, err := provider.LocalStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := localbridge.StatusSnapshot{
		TransportStatus:      delegationconfig.TransportStatus{Transport: "tcp"},
		Version:              buildinfo.Version,
		ControllerID:         statusTestControllerID,
		DeviceID:             statusTestDeviceID,
		DeviceName:           "status-peer",
		ServiceRunning:       true,
		ConnectionState:      localbridge.ConnectionReady,
		Connected:            true,
		RegistryRevision:     42,
		WorkerRevision:       77,
		BrokerWorkerRevision: 77,
		WorkerSyncReady:      true,
		WorkerReady:          true,
		Dispatchable:         true,
		WorkerReadiness:      readiness,
		MaxWorkerSlots:       8,
		Workers: localbridge.WorkerCounts{
			Total: 10, Reserved: 1, Pending: 1, Starting: 1, Preflight: 1,
			Ready: 1, Running: 1, Finalizing: 1, Idle: 1, Interrupted: 1,
			Failed: 1, Occupied: 6,
		},
		Artifacts: localbridge.ArtifactCounts{
			CapturePending: 2, PublishPending: 3, Retained: 4, RetainedBytes: 8192,
		},
		Results: localbridge.ResultCounts{
			OutboxCapturePending: 1, OutboxPublishPending: 2,
			OutboxDeliveryPending: 3, OutboxDelivered: 4,
			OutboxReleasePending: 5,
			OutboxRetainedBytes:  16384,
			InboxReceiving:       5, InboxAvailable: 6, InboxEvictionPending: 7,
			InboxEvicted:         8,
			InboxRetainedBytes:   32768,
			RolloutCaptureFailed: 2, WorkspaceCaptureFailed: 1,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LocalStatus() = %#v, want %#v", got, want)
	}

	provider.client = staticConnectorStatus{status: connector.Status{
		Connected: true, RegistryRevision: 43, WorkerRevision: 76,
	}}
	got, err = provider.LocalStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.ConnectionState != localbridge.ConnectionSynchronizing || got.WorkerSyncReady ||
		got.BrokerWorkerRevision != 76 || got.WorkerRevision != 77 {
		t.Fatalf("unsynchronized status = %#v", got)
	}

	provider.client = staticConnectorStatus{status: connector.Status{
		ConnectionErrorCode:   protocol.PeerStateRollbackCode,
		StateRecoveryRequired: true, RecoveryPeerWorkerRevision: 9,
		RecoveryBrokerWorkerRevision: 91,
	}}
	provider.state = staticPeerStatusStore{
		status: store.PeerStatusSnapshot{WorkerRevision: 100}, readiness: readiness,
	}
	got, err = provider.LocalStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.ConnectionState != localbridge.ConnectionStateRecoveryRequired ||
		got.ConnectionErrorCode != protocol.PeerStateRollbackCode || got.ServiceRunning != true ||
		got.Connected || got.WorkerSyncReady || got.WorkerRevision != 100 ||
		got.RecoveryPeerWorkerRevision != 9 || got.BrokerWorkerRevision != 91 {
		t.Fatalf("worker revision rollback status = %#v", got)
	}
	var human bytes.Buffer
	var humanError bytes.Buffer
	if code := writePeerStatus(&human, &humanError, got, false); code != 0 {
		t.Fatalf("writePeerStatus() code = %d, stderr = %q", code, humanError.String())
	}
	for _, want := range []string{
		"worker revision: 100\n",
		"broker worker revision: 91\n",
		"rejected peer worker revision: 9\n",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("rollback status output %q does not contain %q", human.String(), want)
		}
	}
}

func TestStatusCommandRendersStablePeerOutput(t *testing.T) {
	if runtime.GOOS != "windows" {
		home, err := os.MkdirTemp("/tmp", "ds-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(home) })
		t.Setenv("HOME", home)
	}
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	status := statusTestSnapshot(cfg)
	stop := startStatusTestBridge(t, status)
	defer stop()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "human",
			args: []string{"status", "--config", configPath},
			want: `delegation peer status
version: 0.2.0-test
transport: tcp
device: status-peer
service running: true
broker connection: ready
connected: true
worker sync ready: true
worker ready: true
dispatchable: true
readiness epoch: 1
readiness state: ready
readiness attempts: 1/5
readiness runtime digest: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
readiness config digest: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
readiness epoch started at: 1
readiness next attempt at: 0
readiness last attempt at: 1
readiness updated at: 1
registry revision: 42
worker revision: 77
broker worker revision: 77
worker slots: 6/8 occupied
workers:
  total: 10
  reserved: 1
  pending: 1
  starting: 1
  preflight: 1
  ready: 1
  running: 1
  finalizing: 1
  idle: 1
  interrupted: 1
  failed: 1
artifacts:
  capture pending: 2
  publish pending: 3
  retained: 4
  retained bytes: 8192
results:
  outbox capture pending: 1
  outbox publish pending: 2
  outbox delivery pending: 3
  outbox delivered: 4
  outbox release pending: 5
  outbox retained bytes: 16384
  inbox receiving: 5
  inbox available: 6
  inbox eviction pending: 7
  inbox evicted lifetime: 8
  inbox retained bytes: 32768
  rollout capture failed: 2
  workspace capture failed: 1
`,
		},
		{
			name: "JSON",
			args: []string{"status", "--config", configPath, "--json"},
			want: `{"transport":"tcp","version":"0.2.0-test","controllerId":"123e4567-e89b-42d3-a456-426614174800","deviceId":"123e4567-e89b-42d3-a456-426614174801","deviceName":"status-peer","serviceRunning":true,"connectionState":"ready","connectionErrorCode":"","connected":true,"registryRevision":42,"workerRevision":77,"brokerWorkerRevision":77,"recoveryPeerWorkerRevision":0,"workerSyncReady":true,"workerReady":true,"dispatchable":true,"workerReadiness":{"epoch":1,"state":"ready","attemptCount":1,"runtimeDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","configDigest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","epochStartedAt":1,"nextAttemptAt":0,"lastAttemptAt":1,"failureCode":"","updatedAt":1},"maxWorkerSlots":8,"workers":{"total":10,"reserved":1,"pending":1,"starting":1,"preflight":1,"ready":1,"running":1,"finalizing":1,"idle":1,"interrupted":1,"failed":1,"occupied":6},"artifacts":{"capturePending":2,"publishPending":3,"retained":4,"retainedBytes":8192},"results":{"outboxCapturePending":1,"outboxPublishPending":2,"outboxDeliveryPending":3,"outboxDelivered":4,"outboxReleasePending":5,"outboxRetainedBytes":16384,"inboxReceiving":5,"inboxAvailable":6,"inboxEvictionPending":7,"inboxEvicted":8,"inboxRetainedBytes":32768,"rolloutCaptureFailed":2,"workspaceCaptureFailed":1}}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := Run(test.args, &stdout, &stderr); code != 0 {
				t.Fatalf("Run() code = %d, stderr = %q", code, stderr.String())
			}
			if stdout.String() != test.want {
				t.Fatalf("stdout = %q, want %q", stdout.String(), test.want)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestStatusCommandReturnsBoundedRoleAndPeerErrors(t *testing.T) {
	peerConfig, peerCfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	brokerConfig, _ := writeStatusTestConfig(t, delegationconfig.RoleBroker)
	hugeError := errors.New(strings.Repeat("private failure ", maximumStatusOutput))

	tests := []struct {
		name     string
		config   string
		read     statusReader
		wantCode int
		wantErr  string
	}{
		{
			name: "peer unavailable", config: peerConfig,
			read: func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
				return localbridge.StatusSnapshot{}, hugeError
			},
			wantCode: exitUnavailable, wantErr: peerStatusUnavailableError,
		},
		{
			name: "broker not integrated", config: brokerConfig,
			read: func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
				t.Fatal("broker status called peer reader")
				return localbridge.StatusSnapshot{}, nil
			},
			wantCode: exitUnavailable, wantErr: brokerStatusUnavailableError,
		},
		{
			name: "peer identity mismatch", config: peerConfig,
			read: func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
				status := statusTestSnapshot(peerCfg)
				status.DeviceID = statusTestOtherID
				return status, nil
			},
			wantCode: exitUnavailable, wantErr: peerStatusUnavailableError,
		},
		{
			name: "peer transport mismatch", config: peerConfig,
			read: func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
				status := statusTestSnapshot(peerCfg)
				status.TransportStatus = delegationconfig.TransportStatus{
					Transport:         "tailscale",
					TailscaleHostname: "other-peer",
				}
				return status, nil
			},
			wantCode: exitUnavailable, wantErr: peerStatusUnavailableError,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runStatusWithReader(
				[]string{"--config", test.config}, &stdout, &stderr, test.read,
			)
			if code != test.wantCode {
				t.Fatalf("runStatusWithReader() code = %d, want %d", code, test.wantCode)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if stderr.String() != test.wantErr {
				t.Fatalf("stderr = %q, want %q", stderr.String(), test.wantErr)
			}
			if strings.Contains(stderr.String(), "private failure") {
				t.Fatal("status error leaked provider details")
			}
		})
	}
}

func TestStatusCommandRendersStoppedProfileInvalidPeerReadiness(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	cfg.Peer.CLI = &delegationconfig.CLIConfig{
		Command: testCodexBinary(t), Arguments: []string{"--profile", "legacy"},
	}
	cfg.Peer.CodexBinary = ""
	rewriteStatusTestConfig(t, configPath, cfg)
	persistStatusTestReadinessFailure(t, cfg, protocol.WorkerProfileUnsupported)

	readCalls := 0
	read := func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
		readCalls++
		return localbridge.StatusSnapshot{}, errors.New("service stopped")
	}
	for _, test := range []struct {
		name       string
		jsonOutput bool
	}{
		{name: "text"},
		{name: "JSON", jsonOutput: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []string{"--config", configPath}
			if test.jsonOutput {
				args = append(args, "--json")
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := runStatusWithReader(args, &stdout, &stderr, read); code != 0 {
				t.Fatalf("runStatusWithReader() code = %d, stderr = %q", code, stderr.String())
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
			if test.jsonOutput {
				var status localbridge.StatusSnapshot
				if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
					t.Fatal(err)
				}
				if status.ServiceRunning || status.Connected || status.WorkerSyncReady ||
					status.Dispatchable || status.ConnectionState != localbridge.ConnectionConnecting ||
					status.WorkerReadiness.State != protocol.WorkerReadinessInterventionRequired ||
					status.WorkerReadiness.FailureCode != protocol.WorkerProfileUnsupported {
					t.Fatalf("stopped JSON status = %#v", status)
				}
				return
			}
			for _, want := range []string{
				"service running: false\n",
				"connected: false\n",
				"worker sync ready: false\n",
				"dispatchable: false\n",
				"readiness state: intervention_required\n",
				"readiness failure: profile_arguments_unsupported\n",
			} {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout %q does not contain %q", stdout.String(), want)
				}
			}
		})
	}
	if readCalls != 2 {
		t.Fatalf("bridge read calls = %d, want 2", readCalls)
	}
}

func TestStatusCommandRendersOtherStoppedPeerInterventionFailures(t *testing.T) {
	for _, failureCode := range []string{
		protocol.WorkerManagedHomeInvalid,
		protocol.WorkerHostUnsupported,
	} {
		t.Run(failureCode, func(t *testing.T) {
			configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
			persistStatusTestReadinessFailure(t, cfg, failureCode)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runStatusWithReader(
				[]string{"--config", configPath, "--json"}, &stdout, &stderr,
				func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
					return localbridge.StatusSnapshot{}, errors.New("service stopped")
				},
			)
			if code != 0 {
				t.Fatalf("runStatusWithReader() code = %d, stderr = %q", code, stderr.String())
			}
			var status localbridge.StatusSnapshot
			if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if status.ServiceRunning || status.Dispatchable ||
				status.WorkerReadiness.FailureCode != failureCode {
				t.Fatalf("stopped status = %#v", status)
			}
		})
	}
}

func TestStatusCommandDoesNotFallbackAfterReachableIdentityMismatch(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	persistStatusTestReadinessFailure(t, cfg, protocol.WorkerManagedHomeInvalid)
	for _, test := range []struct {
		name string
		read statusReader
	}{
		{
			name: "returned snapshot",
			read: func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
				status := statusTestSnapshot(cfg)
				status.DeviceID = statusTestOtherID
				return status, nil
			},
		},
		{
			name: "identity probe error",
			read: func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
				return localbridge.StatusSnapshot{}, fmt.Errorf(
					"probe: %w", localbridge.ErrServiceIdentityMismatch,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runStatusWithReader(
				[]string{"--config", configPath}, &stdout, &stderr, test.read,
			)
			if code != exitUnavailable || stdout.Len() != 0 ||
				stderr.String() != peerStatusUnavailableError {
				t.Fatalf("status = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestStatusCommandRejectsReachableForeignBridgeWithoutOfflineFallback(t *testing.T) {
	if runtime.GOOS != "windows" {
		home, err := os.MkdirTemp("/tmp", "ds-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(home) })
		t.Setenv("HOME", home)
	}
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	persistStatusTestReadinessFailure(t, cfg, protocol.WorkerManagedHomeInvalid)
	endpoint, err := localbridge.EndpointForInstance(
		cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	foreignIdentity := localbridge.ServiceIdentity{
		InstanceID: cfg.EffectiveInstanceID(), ControllerID: cfg.ControllerID, DeviceID: statusTestOtherID,
	}
	foreignStatus := statusTestSnapshot(cfg)
	foreignStatus.DeviceID = statusTestOtherID
	server, err := localbridge.ListenWithStatus(
		endpoint, foreignIdentity, statusTestBackend{}, nil,
		staticLocalStatus{status: foreignStatus},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		if err := <-done; err != nil {
			t.Errorf("serve foreign bridge: %v", err)
		}
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{"status", "--config", configPath}, &stdout, &stderr)
	if code != exitUnavailable || stdout.Len() != 0 || stderr.String() != peerStatusUnavailableError {
		t.Fatalf("status = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestStatusCommandRejectsUnrelatedValidationErrorBeforeBridgeOrOfflineState(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	cfg.Peer.MaxWorkerSlots = 0
	rewriteStatusTestConfig(t, configPath, cfg)
	bridgeCalls := 0
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStatusWithReader(
		[]string{"--config", configPath}, &stdout, &stderr,
		func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
			bridgeCalls++
			return localbridge.StatusSnapshot{}, errors.New("unexpected bridge call")
		},
	)
	if code != 1 || stdout.Len() != 0 || bridgeCalls != 0 ||
		!strings.Contains(stderr.String(), "peer maxWorkerSlots must be from 1") {
		t.Fatalf(
			"status = %d, bridge calls %d, stdout %q, stderr %q",
			code, bridgeCalls, stdout.String(), stderr.String(),
		)
	}
	if _, err := os.Lstat(cfg.Peer.StateFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid config created or read peer state: %v", err)
	}
}

func TestStatusCommandDoesNotReadOfflineStateWhilePeerLeaseIsHeld(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, delegationconfig.RolePeer)
	persistStatusTestReadinessFailure(t, cfg, protocol.WorkerManagedHomeInvalid)
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStatusWithReader(
		[]string{"--config", configPath}, &stdout, &stderr,
		func(context.Context, string, localbridge.ServiceIdentity) (localbridge.StatusSnapshot, error) {
			return localbridge.StatusSnapshot{}, errors.New("bridge unavailable")
		},
	)
	if code != exitUnavailable || stdout.Len() != 0 || stderr.String() != peerStatusUnavailableError {
		t.Fatalf("status = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestWritePeerStatusReturnsBoundedOutputError(t *testing.T) {
	status := statusTestSnapshot(delegationconfig.Config{
		ControllerID: statusTestControllerID,
		DeviceID:     statusTestDeviceID,
		DeviceName:   "status-peer",
		Peer:         delegationconfig.PeerConfig{MaxWorkerSlots: 8},
	})
	var stderr bytes.Buffer
	code := writePeerStatus(statusFailingWriter{}, &stderr, status, true)
	if code != 1 {
		t.Fatalf("writePeerStatus() code = %d, want 1", code)
	}
	if stderr.String() != statusOutputError {
		t.Fatalf("stderr = %q, want %q", stderr.String(), statusOutputError)
	}
}

type statusFailingWriter struct{}

func (statusFailingWriter) Write([]byte) (int, error) {
	return 0, errors.New("private output failure")
}

func writeStatusTestConfig(
	t *testing.T,
	role delegationconfig.Role,
) (string, delegationconfig.Config) {
	t.Helper()
	directory := privateTestDirectory(t)
	configPath := filepath.Join(directory, string(role)+".json")
	binary, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          role,
		ControllerID:  statusTestControllerID,
		DeviceID:      statusTestDeviceID,
		DeviceName:    "status-peer",
		Broker: delegationconfig.BrokerConfig{
			URL:  "ws://127.0.0.1:1",
			Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
		Peer: delegationconfig.PeerConfig{
			CodexBinary: binary, GitBinary: binary,
			CodexHome:      filepath.Join(directory, "codex-home"),
			WorkspaceRoot:  filepath.Join(directory, "workspaces"),
			StateFile:      filepath.Join(directory, "state", "peer.sqlite3"),
			MaxWorkerSlots: 8,
		},
	}
	if role == delegationconfig.RoleBroker {
		cfg.DeviceID = ""
		cfg.DeviceName = ""
		cfg.Broker = delegationconfig.BrokerConfig{
			Listen: "127.0.0.1:8787", StatusListen: "127.0.0.1:8788",
			StateFile: filepath.Join(directory, "state", "broker.sqlite3"),
			Auth:      delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		}
		cfg.Peer = delegationconfig.PeerConfig{}
	}
	if err := delegationconfig.WriteNew(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	return configPath, cfg
}

func rewriteStatusTestConfig(t *testing.T, path string, cfg delegationconfig.Config) {
	t.Helper()
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	replacement = append(replacement, '\n')
	if err := delegationconfig.ReplaceProtectedFile(path, original, replacement); err != nil {
		t.Fatal(err)
	}
}

func persistStatusTestReadinessFailure(
	t *testing.T, cfg delegationconfig.Config, failureCode string,
) {
	t.Helper()
	state, err := store.OpenPeer(context.Background(), cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if _, err := state.EnsureWorkerReadinessEpoch(
		context.Background(), strings.Repeat("a", 64), strings.Repeat("b", 64), 1,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := state.FailWorkerReadiness(context.Background(), failureCode, 2); err != nil {
		t.Fatal(err)
	}
}

func statusTestSnapshot(cfg delegationconfig.Config) localbridge.StatusSnapshot {
	readiness := readyStatusTestReadiness()
	return localbridge.StatusSnapshot{
		TransportStatus:      delegationconfig.TransportStatus{Transport: "tcp"},
		Version:              "0.2.0-test",
		ControllerID:         cfg.ControllerID,
		DeviceID:             cfg.DeviceID,
		DeviceName:           cfg.DeviceName,
		ServiceRunning:       true,
		ConnectionState:      localbridge.ConnectionReady,
		Connected:            true,
		RegistryRevision:     42,
		WorkerRevision:       77,
		BrokerWorkerRevision: 77,
		WorkerSyncReady:      true,
		WorkerReady:          true,
		Dispatchable:         true,
		WorkerReadiness:      readiness,
		MaxWorkerSlots:       cfg.Peer.MaxWorkerSlots,
		Workers: localbridge.WorkerCounts{
			Total: 10, Reserved: 1, Pending: 1, Starting: 1, Preflight: 1,
			Ready: 1, Running: 1, Finalizing: 1, Idle: 1, Interrupted: 1,
			Failed: 1, Occupied: 6,
		},
		Artifacts: localbridge.ArtifactCounts{
			CapturePending: 2, PublishPending: 3, Retained: 4, RetainedBytes: 8192,
		},
		Results: localbridge.ResultCounts{
			OutboxCapturePending: 1, OutboxPublishPending: 2,
			OutboxDeliveryPending: 3, OutboxDelivered: 4,
			OutboxReleasePending: 5,
			OutboxRetainedBytes:  16384,
			InboxReceiving:       5, InboxAvailable: 6, InboxEvictionPending: 7,
			InboxEvicted:         8,
			InboxRetainedBytes:   32768,
			RolloutCaptureFailed: 2, WorkspaceCaptureFailed: 1,
		},
	}
}

func readyStatusTestReadiness() protocol.WorkerReadiness {
	return protocol.WorkerReadiness{
		Epoch: 1, State: protocol.WorkerReadinessReady, AttemptCount: 1,
		RuntimeDigest: strings.Repeat("a", 64), ConfigDigest: strings.Repeat("b", 64),
		EpochStartedAt: 1, LastAttemptAt: 1, UpdatedAt: 1,
	}
}

func startStatusTestBridge(t *testing.T, status localbridge.StatusSnapshot) func() {
	t.Helper()
	endpoint, err := localbridge.Endpoint(status.ControllerID, status.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	server, err := localbridge.ListenWithStatus(
		endpoint,
		localbridge.ServiceIdentity{
			ControllerID: status.ControllerID,
			DeviceID:     status.DeviceID,
		},
		statusTestBackend{},
		nil,
		staticLocalStatus{status: status},
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
			t.Errorf("close status test bridge: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("serve status test bridge: %v", err)
		}
	}
}
