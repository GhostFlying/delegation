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
		MaxWorkerSlots: 4,
		Workers:        WorkerCounts{Total: 6, Running: 1, Idle: 3, Failed: 2, Occupied: 1},
		Artifacts:      ArtifactCounts{Available: 2, RetainedBytes: 4096},
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
