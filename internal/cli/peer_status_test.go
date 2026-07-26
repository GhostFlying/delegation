package cli

import (
	"bytes"
	"context"
	"errors"
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
	status store.PeerStatusSnapshot
	err    error
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
			OutboxRetainedBytes: 16384,
			InboxReceiving:      5, InboxAvailable: 6, InboxEvictionPending: 7,
			InboxRetainedBytes:   32768,
			RolloutCaptureFailed: 2, WorkspaceCaptureFailed: 1,
		},
	}
	provider := peerLocalStatusProvider{
		client: staticConnectorStatus{status: connector.Status{
			Connected: true, RegistryRevision: 42, WorkerRevision: 77,
		}},
		state:          staticPeerStatusStore{status: durable},
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
		Version:              buildinfo.Version,
		ControllerID:         statusTestControllerID,
		DeviceID:             statusTestDeviceID,
		DeviceName:           "status-peer",
		Connected:            true,
		RegistryRevision:     42,
		WorkerRevision:       77,
		BrokerWorkerRevision: 77,
		WorkerSyncReady:      true,
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
			OutboxRetainedBytes: 16384,
			InboxReceiving:      5, InboxAvailable: 6, InboxEvictionPending: 7,
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
	if got.WorkerSyncReady || got.BrokerWorkerRevision != 76 || got.WorkerRevision != 77 {
		t.Fatalf("unsynchronized status = %#v", got)
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
device: status-peer
connected: true
worker sync ready: true
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
  outbox retained bytes: 16384
  inbox receiving: 5
  inbox available: 6
  inbox eviction pending: 7
  inbox retained bytes: 32768
  rollout capture failed: 2
  workspace capture failed: 1
`,
		},
		{
			name: "JSON",
			args: []string{"status", "--config", configPath, "--json"},
			want: `{"version":"0.2.0-test","controllerId":"123e4567-e89b-42d3-a456-426614174800","deviceId":"123e4567-e89b-42d3-a456-426614174801","deviceName":"status-peer","connected":true,"registryRevision":42,"workerRevision":77,"brokerWorkerRevision":77,"workerSyncReady":true,"maxWorkerSlots":8,"workers":{"total":10,"reserved":1,"pending":1,"starting":1,"preflight":1,"ready":1,"running":1,"finalizing":1,"idle":1,"interrupted":1,"failed":1,"occupied":6},"artifacts":{"capturePending":2,"publishPending":3,"retained":4,"retainedBytes":8192},"results":{"outboxCapturePending":1,"outboxPublishPending":2,"outboxDeliveryPending":3,"outboxDelivered":4,"outboxRetainedBytes":16384,"inboxReceiving":5,"inboxAvailable":6,"inboxEvictionPending":7,"inboxRetainedBytes":32768,"rolloutCaptureFailed":2,"workspaceCaptureFailed":1}}` + "\n",
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
			read: func(context.Context, string) (localbridge.StatusSnapshot, error) {
				return localbridge.StatusSnapshot{}, hugeError
			},
			wantCode: exitUnavailable, wantErr: peerStatusUnavailableError,
		},
		{
			name: "broker not integrated", config: brokerConfig,
			read: func(context.Context, string) (localbridge.StatusSnapshot, error) {
				t.Fatal("broker status called peer reader")
				return localbridge.StatusSnapshot{}, nil
			},
			wantCode: exitUnavailable, wantErr: brokerStatusUnavailableError,
		},
		{
			name: "peer identity mismatch", config: peerConfig,
			read: func(context.Context, string) (localbridge.StatusSnapshot, error) {
				status := statusTestSnapshot(peerCfg)
				status.DeviceID = statusTestOtherID
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

func statusTestSnapshot(cfg delegationconfig.Config) localbridge.StatusSnapshot {
	return localbridge.StatusSnapshot{
		Version:              "0.2.0-test",
		ControllerID:         cfg.ControllerID,
		DeviceID:             cfg.DeviceID,
		DeviceName:           cfg.DeviceName,
		Connected:            true,
		RegistryRevision:     42,
		WorkerRevision:       77,
		BrokerWorkerRevision: 77,
		WorkerSyncReady:      true,
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
			OutboxRetainedBytes: 16384,
			InboxReceiving:      5, InboxAvailable: 6, InboxEvictionPending: 7,
			InboxRetainedBytes:   32768,
			RolloutCaptureFailed: 2, WorkspaceCaptureFailed: 1,
		},
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
