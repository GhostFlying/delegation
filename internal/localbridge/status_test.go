package localbridge

import (
	"context"
	"reflect"
	"testing"
)

type staticStatusProvider struct {
	status StatusSnapshot
	err    error
}

func (p staticStatusProvider) LocalStatus(context.Context) (StatusSnapshot, error) {
	return p.status, p.err
}

func TestReadStatusReturnsValidatedLocalSnapshot(t *testing.T) {
	identity := ServiceIdentity{ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID}
	want := StatusSnapshot{
		Version: "0.1.0-test", ControllerID: identity.ControllerID, DeviceID: identity.DeviceID,
		DeviceName: "test-peer", Connected: true, RegistryRevision: 7, WorkerRevision: 5,
		BrokerWorkerRevision: 5, WorkerSyncReady: true,
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

func TestStatusSnapshotRejectsInconsistentCountsAndSynchronization(t *testing.T) {
	valid := StatusSnapshot{
		Version: "0.1.0-test", ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID,
		DeviceName: "test-peer", Connected: true, WorkerRevision: 8,
		BrokerWorkerRevision: 8, WorkerSyncReady: true, MaxWorkerSlots: 4,
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
