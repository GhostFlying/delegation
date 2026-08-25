package connector

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"net/http/httptest"
)

type rollbackConnectorRegistry struct {
	*store.Store
	claimCalls     atomic.Int64
	brokerRevision uint64
	claimStarted   chan store.WorkerLifecycleSessionClaim
	releaseClaim   chan struct{}
}

func (r *rollbackConnectorRegistry) ClaimWorkerLifecycleSession(
	_ context.Context,
	claim store.WorkerLifecycleSessionClaim,
) (uint64, error) {
	r.claimCalls.Add(1)
	if r.claimStarted != nil {
		r.claimStarted <- claim
		<-r.releaseClaim
	}
	if claim.WorkerRevision < r.brokerRevision {
		return r.brokerRevision, store.ErrWorkerLifecyclePeerBehind
	}
	return r.brokerRevision, nil
}

type startupRollbackLifecycleSource struct {
	*lifecycleTestSource
	startupRevision uint64
}

func (s *startupRollbackLifecycleSource) StartupWorkerRevision() uint64 {
	return s.startupRevision
}

func TestClientStatusRetainsExplicitPeerStateRollbackDiagnosis(t *testing.T) {
	client := newTestClient(t, "ws://127.0.0.1:1/v1/connect", config.AuthModeNone, nil)
	details, err := json.Marshal(protocol.PeerStateRollbackErrorData{
		Code:                 protocol.PeerStateRollbackCode,
		PeerWorkerRevision:   0,
		BrokerWorkerRevision: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !client.recordSessionError(&RPCError{
		Code: protocol.ErrorConflict, Message: "peer worker revision is behind broker state", Data: details,
		helloWorkerBaselineRevision: 0, helloResponse: true,
	}) {
		t.Fatal("rollback error did not require state recovery")
	}
	status := client.Status()
	if !status.StateRecoveryRequired || status.ConnectionErrorCode != protocol.PeerStateRollbackCode ||
		status.RecoveryPeerWorkerRevision != 0 || status.RecoveryBrokerWorkerRevision != 9 || status.Connected {
		t.Fatalf("rollback status = %#v", status)
	}

	if client.recordSessionError(context.DeadlineExceeded) {
		t.Fatal("transient error requested state recovery")
	}
	if status = client.Status(); !status.StateRecoveryRequired ||
		status.ConnectionErrorCode != protocol.PeerStateRollbackCode {
		t.Fatalf("transient failure erased rollback diagnosis: %#v", status)
	}

	client.publish(nil, protocol.HelloResult{WorkerAppliedRevision: 9})
	status = client.Status()
	if status.StateRecoveryRequired || status.ConnectionErrorCode != "" || !status.Connected {
		t.Fatalf("successful synchronization did not clear rollback diagnosis: %#v", status)
	}
}

func TestConnectorRetainsStartupRollbackWhenLocalRevisionAdvances(t *testing.T) {
	registry, err := store.Open(context.Background(), t.TempDir()+"/state/broker.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	claimStarted := make(chan store.WorkerLifecycleSessionClaim, 1)
	releaseClaim := make(chan struct{})
	wrapped := &rollbackConnectorRegistry{
		Store: registry, brokerRevision: 12, claimStarted: claimStarted, releaseClaim: releaseClaim,
	}
	server, err := broker.New(broker.Options{
		ControllerID: connectorTestControllerID,
		AuthMode:     config.AuthModeNone,
		Registry:     wrapped,
		Transport:    config.TransportStatus{Transport: "tcp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		if err := registry.Close(); err != nil {
			t.Errorf("close broker store: %v", err)
		}
	})

	lifecycle := &startupRollbackLifecycleSource{
		lifecycleTestSource: newLifecycleTestSource(10, nil),
		startupRevision:     9,
	}
	client, err := New(Options{
		BrokerURL: websocketURL(httpServer.URL), ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "0.1.0-alpha.0.m1.1", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: lifecycle,
		ChangesArtifactSource: testWorkerSpawner{}, WorkspaceManager: testWorkerSpawner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	claim := <-claimStarted
	if claim.WorkerRevision != 9 {
		t.Fatalf("hello worker revision = %d, want startup revision 9", claim.WorkerRevision)
	}
	lifecycle.update(11, nil)
	close(releaseClaim)
	waitForRollbackStatus(t, client)
	status := client.Status()
	if !status.StateRecoveryRequired || status.ConnectionErrorCode != protocol.PeerStateRollbackCode ||
		status.RecoveryPeerWorkerRevision != 9 || status.RecoveryBrokerWorkerRevision != 12 ||
		status.Connected || lifecycle.WorkerRevision() != 11 {
		t.Fatalf("rollback status after local revision advance = %#v", status)
	}
	if wrapped.claimCalls.Load() != 1 {
		t.Fatalf("rollback hello claims = %d, want 1", wrapped.claimCalls.Load())
	}
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorStopsReconnectLoopAfterPeerStateRollback(t *testing.T) {
	registry, err := store.Open(context.Background(), t.TempDir()+"/state/broker.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &rollbackConnectorRegistry{Store: registry, brokerRevision: 9}
	server, err := broker.New(broker.Options{
		ControllerID: connectorTestControllerID,
		AuthMode:     config.AuthModeNone,
		Registry:     wrapped,
		Transport:    config.TransportStatus{Transport: "tcp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		if err := registry.Close(); err != nil {
			t.Errorf("close broker store: %v", err)
		}
	})

	client := newTestClient(t, websocketURL(httpServer.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitForRollbackStatus(t, client)
	status := client.Status()
	if !status.StateRecoveryRequired || status.ConnectionErrorCode != protocol.PeerStateRollbackCode ||
		status.Connected || status.RecoveryPeerWorkerRevision != 0 ||
		status.RecoveryBrokerWorkerRevision != 9 {
		t.Fatalf("rollback status = %#v", status)
	}
	before, err := registry.DescribeDevice(
		context.Background(), connectorTestControllerID, connectorTestDeviceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if before.Device.Online {
		t.Fatalf("rollback device remained online: %#v", before.Device)
	}
	select {
	case runErr := <-done:
		t.Fatalf("connector stopped before an explicit service restart: %v", runErr)
	case <-time.After(50 * time.Millisecond):
	}
	after, err := registry.DescribeDevice(
		context.Background(), connectorTestControllerID, connectorTestDeviceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.claimCalls.Load() != 1 || after.Device.Revision != before.Device.Revision || after.Device.Online {
		t.Fatalf(
			"rollback reconnects = %d, device before = %#v, after = %#v",
			wrapped.claimCalls.Load(),
			before.Device,
			after.Device,
		)
	}
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func waitForRollbackStatus(t *testing.T, client *Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !client.Status().StateRecoveryRequired && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}
