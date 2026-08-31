package localbridge

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
)

type staticStatusProvider struct {
	status StatusSnapshot
	err    error
}

type staticReadinessManager struct {
	readiness protocol.WorkerReadiness
	err       error
	wait      chan protocol.WorkerReadiness
}

func (m staticReadinessManager) WorkerReadiness(context.Context) (protocol.WorkerReadiness, error) {
	return m.readiness, m.err
}

func (m staticReadinessManager) RecheckWorkerReadiness(context.Context) (protocol.WorkerReadiness, error) {
	return m.readiness, m.err
}

func (m staticReadinessManager) WaitWorkerIntervention(
	ctx context.Context, _ protocol.WorkerReadinessCursor,
) (protocol.WorkerReadiness, error) {
	if m.wait == nil {
		return protocol.WorkerReadiness{}, m.err
	}
	select {
	case <-ctx.Done():
		return protocol.WorkerReadiness{}, ctx.Err()
	case readiness := <-m.wait:
		return readiness, nil
	}
}

func (p staticStatusProvider) LocalStatus(context.Context) (StatusSnapshot, error) {
	return p.status, p.err
}

func TestReadStatusReturnsValidatedLocalSnapshot(t *testing.T) {
	identity := ServiceIdentity{ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID}
	want := StatusSnapshot{
		TransportStatus: config.TransportStatus{Transport: "tcp"},
		Version:         "0.1.0-test", ControllerID: identity.ControllerID, DeviceID: identity.DeviceID,
		DeviceName: "test-peer", ServiceRunning: true, ConnectionState: ConnectionReady,
		Connected: true, RegistryRevision: 7, WorkerRevision: 5,
		BrokerWorkerRevision: 5, WorkerSyncReady: true,
		WorkerReady: true, Dispatchable: true, WorkerReadiness: readyBridgeTestReadiness(),
		MaxWorkerSlots: 8,
		Workers: WorkerCounts{
			Total: 10, Reserved: 1, Pending: 1, Starting: 1, Preflight: 1,
			Ready: 1, Running: 1, Finalizing: 1, Idle: 1, Interrupted: 1,
			Failed: 1, Occupied: 6,
		},
		Artifacts: ArtifactCounts{
			CapturePending: 1, PublishPending: 2, Retained: 3, RetainedBytes: 4096,
		},
		Results: ResultCounts{
			OutboxCapturePending: 1, OutboxPublishPending: 2,
			OutboxDeliveryPending: 3, OutboxDelivered: 4,
			OutboxRetainedBytes: 8192,
			InboxReceiving:      1, InboxAvailable: 2, InboxEvictionPending: 3,
			InboxEvicted: 4, InboxRetainedBytes: 16384,
			RolloutCaptureFailed: 2, WorkspaceCaptureFailed: 1,
		},
	}
	endpoint := testEndpoint(t)
	server, err := ListenWithStatus(
		endpoint, identity, &fakeBackend{}, nil, staticStatusProvider{status: want},
	)
	if err != nil {
		t.Fatalf("ListenWithStatus() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})

	got, err := ReadStatus(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("ReadStatus() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadStatus() = %#v, want %#v", got, want)
	}
}

func TestReadStatusForIdentityRejectsReachableForeignBridge(t *testing.T) {
	foreign := ServiceIdentity{
		ControllerID: bridgeTestControllerID,
		DeviceID:     bridgeTestDeviceID,
	}
	status := StatusSnapshot{
		TransportStatus: config.TransportStatus{Transport: "tcp"},
		Version:         "0.1.0-test",
		ControllerID:    foreign.ControllerID,
		DeviceID:        foreign.DeviceID,
		DeviceName:      "test-peer",
		ServiceRunning:  true,
		ConnectionState: ConnectionConnecting,
		WorkerReadiness: pendingBridgeTestReadiness(),
		MaxWorkerSlots:  4,
	}
	endpoint := testEndpoint(t)
	server, err := ListenWithStatus(
		endpoint, foreign, &fakeBackend{}, nil, staticStatusProvider{status: status},
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
		<-done
	})
	expected := foreign
	expected.DeviceID = "123e4567-e89b-42d3-a456-426614174399"
	if _, err := ReadStatusForIdentity(context.Background(), endpoint, expected); !errors.Is(err, ErrServiceIdentityMismatch) {
		t.Fatalf("ReadStatusForIdentity() error = %v, want identity mismatch", err)
	}
}

func TestRecheckWorkerUsesProtectedManagementEndpoint(t *testing.T) {
	identity := ServiceIdentity{ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID}
	want := pendingBridgeTestReadiness()
	want.Epoch = 2
	server, err := ListenWithManagement(
		testEndpoint(t), identity, &fakeBackend{}, nil, nil, nil, nil,
		staticReadinessManager{readiness: want},
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
		<-done
	})

	got, err := RecheckWorker(context.Background(), server.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RecheckWorker() = %#v, want %#v", got, want)
	}
}

func TestWorkerInterventionWaitUsesProtectedWaitAdmission(t *testing.T) {
	identity := ServiceIdentity{ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID}
	baseline := pendingBridgeTestReadiness()
	terminal := baseline
	terminal.State = protocol.WorkerReadinessInterventionRequired
	terminal.AttemptCount = 1
	terminal.LastAttemptAt = baseline.UpdatedAt + 1
	terminal.UpdatedAt = baseline.UpdatedAt + 2
	terminal.NextAttemptAt = 0
	terminal.FailureCode = protocol.WorkerManagedHomeInvalid
	wait := make(chan protocol.WorkerReadiness, 1)
	endpoint := testEndpoint(t)
	server, err := ListenWithManagement(
		endpoint, identity, &fakeBackend{}, nil, nil, nil, nil,
		staticReadinessManager{readiness: baseline, wait: wait},
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
		<-done
	})
	gotBaseline, err := ReadWorkerReadiness(context.Background(), endpoint)
	if err != nil || !reflect.DeepEqual(gotBaseline, baseline) {
		t.Fatalf("ReadWorkerReadiness() = %#v, %v", gotBaseline, err)
	}
	result := make(chan protocol.WorkerReadiness, 1)
	errors := make(chan error, 1)
	go func() {
		got, err := WaitWorkerIntervention(context.Background(), endpoint, baseline.Cursor())
		if err != nil {
			errors <- err
			return
		}
		result <- got
	}()
	wait <- terminal
	select {
	case err := <-errors:
		t.Fatal(err)
	case got := <-result:
		if !reflect.DeepEqual(got, terminal) {
			t.Fatalf("WaitWorkerIntervention() = %#v, want %#v", got, terminal)
		}
	case <-time.After(time.Second):
		t.Fatal("worker intervention wait did not complete")
	}
}

func TestStatusSnapshotRejectsInconsistentCountsAndSynchronization(t *testing.T) {
	valid := StatusSnapshot{
		TransportStatus: config.TransportStatus{Transport: "tcp"},
		Version:         "0.1.0-test", ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID,
		DeviceName: "test-peer", ServiceRunning: true, ConnectionState: ConnectionReady,
		Connected: true, WorkerRevision: 8,
		BrokerWorkerRevision: 8, WorkerSyncReady: true, MaxWorkerSlots: 4,
		WorkerReady: true, Dispatchable: true, WorkerReadiness: readyBridgeTestReadiness(),
		Workers: WorkerCounts{Total: 2, Ready: 1, Idle: 1, Occupied: 1},
	}
	tests := []struct {
		name   string
		mutate func(*StatusSnapshot)
	}{
		{
			name: "phase total",
			mutate: func(status *StatusSnapshot) {
				status.Workers.Total++
			},
		},
		{
			name: "occupied phases",
			mutate: func(status *StatusSnapshot) {
				status.Workers.Occupied = 0
			},
		},
		{
			name: "disconnected sync ready",
			mutate: func(status *StatusSnapshot) {
				status.Connected = false
			},
		},
		{
			name: "mismatched revision sync ready",
			mutate: func(status *StatusSnapshot) {
				status.BrokerWorkerRevision--
			},
		},
		{
			name: "non-printing version",
			mutate: func(status *StatusSnapshot) {
				status.Version = "version\nsecret"
			},
		},
		{
			name: "negative result count",
			mutate: func(status *StatusSnapshot) {
				status.Results.InboxAvailable = -1
			},
		},
		{
			name: "outbox bytes without packages",
			mutate: func(status *StatusSnapshot) {
				status.Results.OutboxRetainedBytes = 1
			},
		},
		{
			name: "capture failures without captured outbox",
			mutate: func(status *StatusSnapshot) {
				status.Results.RolloutCaptureFailed = 1
			},
		},
		{
			name: "inbox package without bytes",
			mutate: func(status *StatusSnapshot) {
				status.Results.InboxReceiving = 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := valid
			test.mutate(&status)
			if err := status.Validate(); err == nil {
				t.Fatal("Validate() accepted inconsistent status")
			}
		})
	}
}

func TestStatusSnapshotAcceptsConnectionStates(t *testing.T) {
	base := StatusSnapshot{
		TransportStatus: config.TransportStatus{Transport: "tcp"},
		Version:         "0.1.0-test", ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID,
		DeviceName: "test-peer", ServiceRunning: true, MaxWorkerSlots: 4,
		WorkerReadiness: pendingBridgeTestReadiness(),
	}
	tests := map[string]StatusSnapshot{
		"connecting": func() StatusSnapshot {
			status := base
			status.ConnectionState = ConnectionConnecting
			status.ConnectionErrorCode = "broker_unavailable"
			return status
		}(),
		"synchronizing": func() StatusSnapshot {
			status := base
			status.ConnectionState = ConnectionSynchronizing
			status.Connected = true
			status.WorkerRevision = 2
			status.BrokerWorkerRevision = 1
			return status
		}(),
		"ready": func() StatusSnapshot {
			status := base
			status.ConnectionState = ConnectionReady
			status.Connected = true
			status.WorkerRevision = 2
			status.BrokerWorkerRevision = 2
			status.WorkerSyncReady = true
			return status
		}(),
		"state recovery required": func() StatusSnapshot {
			status := base
			status.ConnectionState = ConnectionStateRecoveryRequired
			status.ConnectionErrorCode = protocol.PeerStateRollbackCode
			status.WorkerRevision = 2
			status.RecoveryPeerWorkerRevision = 2
			status.BrokerWorkerRevision = 3
			return status
		}(),
	}
	for name, status := range tests {
		t.Run(name, func(t *testing.T) {
			if err := status.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatusSnapshotAcceptsStoppedPeerWithDurableReadiness(t *testing.T) {
	readiness := pendingBridgeTestReadiness()
	readiness.State = protocol.WorkerReadinessInterventionRequired
	readiness.NextAttemptAt = 0
	readiness.FailureCode = protocol.WorkerProfileUnsupported
	status := StatusSnapshot{
		TransportStatus: config.TransportStatus{Transport: "tcp"},
		Version:         "0.1.0-test",
		ControllerID:    bridgeTestControllerID,
		DeviceID:        bridgeTestDeviceID,
		DeviceName:      "test-peer",
		ServiceRunning:  false,
		ConnectionState: ConnectionConnecting,
		WorkerRevision:  8,
		WorkerReadiness: readiness,
		MaxWorkerSlots:  4,
	}
	if err := status.Validate(); err != nil {
		t.Fatal(err)
	}
	status.RegistryRevision = 1
	if err := status.Validate(); err == nil {
		t.Fatal("Validate() accepted a stopped peer with a live registry revision")
	}
}

func pendingBridgeTestReadiness() protocol.WorkerReadiness {
	return protocol.NewPendingWorkerReadiness(strings.Repeat("a", 64), strings.Repeat("b", 64), 1)
}

func readyBridgeTestReadiness() protocol.WorkerReadiness {
	return protocol.WorkerReadiness{
		Epoch: 1, State: protocol.WorkerReadinessReady, AttemptCount: 1,
		RuntimeDigest: strings.Repeat("a", 64), ConfigDigest: strings.Repeat("b", 64),
		EpochStartedAt: 1, LastAttemptAt: 1, UpdatedAt: 1,
	}
}

func TestReadStatusFailsClosedWithoutProvider(t *testing.T) {
	identity := ServiceIdentity{ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID}
	endpoint := testEndpoint(t)
	server, err := Listen(endpoint, identity, &fakeBackend{})
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})

	if _, err := ReadStatus(context.Background(), endpoint); err == nil {
		t.Fatal("ReadStatus() succeeded without a local status provider")
	}
}
