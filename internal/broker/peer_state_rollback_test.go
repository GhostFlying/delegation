package broker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type rollbackLifecycleRegistry struct {
	*store.Store
	brokerRevision uint64
}

type readyGateLifecycleRegistry struct {
	*store.Store
	brokerRevision uint64
	claims         chan store.WorkerLifecycleSessionClaim
}

func (r *readyGateLifecycleRegistry) ClaimWorkerLifecycleSession(
	_ context.Context,
	claim store.WorkerLifecycleSessionClaim,
) (uint64, error) {
	r.claims <- claim
	return r.brokerRevision, nil
}

func (r *readyGateLifecycleRegistry) ApplyWorkerLifecyclePage(
	_ context.Context,
	request store.WorkerLifecyclePageApply,
) (protocol.SyncWorkerLifecycleResult, error) {
	applied, err := request.Page.AppliedRevision()
	return protocol.SyncWorkerLifecycleResult{AppliedRevision: applied}, err
}

func (r *rollbackLifecycleRegistry) ClaimWorkerLifecycleSession(
	context.Context,
	store.WorkerLifecycleSessionClaim,
) (uint64, error) {
	return r.brokerRevision, store.ErrWorkerLifecyclePeerBehind
}

func TestHelloReportsStructuredPeerStateRollback(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeToken, defaultHeartbeatInterval)
	harness.replaceRegistry(&rollbackLifecycleRegistry{Store: harness.registry, brokerRevision: 11})
	connection, _, err := dialBroker(harness, &harness.deviceToken)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()

	peerHello := hello()
	peerHello.WorkerBaselineRevision = 10
	peerHello.WorkerRevision = 11
	response := writeAndRead(t, connection, request(t, protocol.MethodHello, peerHello))
	if response.Error == nil || response.Error.Code != protocol.ErrorConflict {
		t.Fatalf("rollback hello response = %#v", response)
	}
	var details protocol.PeerStateRollbackErrorData
	if err := json.Unmarshal(response.Error.Data, &details); err != nil {
		t.Fatal(err)
	}
	want := protocol.PeerStateRollbackErrorData{
		Code: protocol.PeerStateRollbackCode, PeerWorkerRevision: 10, BrokerWorkerRevision: 11,
	}
	if details != want {
		t.Fatalf("rollback error details = %#v, want %#v", details, want)
	}
}

func TestHelloUsesCurrentWorkerRevisionForReadiness(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	registry := &readyGateLifecycleRegistry{
		Store: harness.registry, brokerRevision: 10,
		claims: make(chan store.WorkerLifecycleSessionClaim, 1),
	}
	harness.replaceRegistry(registry)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()

	peerHello := hello()
	peerHello.WorkerBaselineRevision = 10
	peerHello.WorkerRevision = 11
	response := writeAndRead(t, connection, request(t, protocol.MethodHello, peerHello))
	if response.Error != nil {
		t.Fatalf("hello response = %#v", response.Error)
	}
	claim := <-registry.claims
	if claim.WorkerRevision != 10 {
		t.Fatalf("rollback comparison revision = %d, want 10", claim.WorkerRevision)
	}
	waitForBrokerConnectionState(t, harness.server, brokerTestDeviceID, false)

	response = writeAndRead(t, connection, request(
		t,
		protocol.MethodSyncWorkerLifecycle,
		protocol.SyncWorkerLifecycleParams{
			BaseRevision: 10, ThroughRevision: 11, Complete: true,
		},
	))
	if response.Error != nil {
		t.Fatalf("lifecycle sync response = %#v", response.Error)
	}
	waitForBrokerConnectionState(t, harness.server, brokerTestDeviceID, true)
}
