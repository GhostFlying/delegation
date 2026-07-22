package broker

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/coder/websocket"
)

type blockingLifecycleClaimRegistry struct {
	*store.Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingLifecycleClaimRegistry) ClaimWorkerLifecycleSession(
	ctx context.Context,
	claim store.WorkerLifecycleSessionClaim,
) (uint64, error) {
	r.once.Do(func() { close(r.started) })
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.release:
	}
	return r.Store.ClaimWorkerLifecycleSession(ctx, claim)
}

const (
	lifecycleTargetDeviceID = "123e4567-e89b-42d3-a456-426614174150"
	lifecycleSpawnID        = "123e4567-e89b-42d3-a456-426614174151"
	lifecycleAgentID        = "123e4567-e89b-42d3-a456-426614174152"
)

func TestWorkerLifecycleSyncGatesDispatchAndRenewsHeartbeatLease(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)

	targetDescriptor := hello().Descriptor()
	targetDescriptor.DeviceID = lifecycleTargetDeviceID
	targetDescriptor.Name = "lifecycle-target"
	if _, err := harness.registry.RegisterTrustedDevice(
		context.Background(), targetDescriptor, time.Unix(10, 0),
	); err != nil {
		t.Fatal(err)
	}
	receipt, err := harness.registry.BeginAgentSpawn(
		context.Background(),
		store.AgentSpawnIntent{
			Source:         root.Identity(),
			SpawnID:        lifecycleSpawnID,
			AgentID:        lifecycleAgentID,
			TargetDeviceID: lifecycleTargetDeviceID,
			TaskName:       "lifecycle_worker",
			PromptDigest:   sha256.Sum256([]byte("lifecycle worker prompt")),
		},
		time.Unix(11, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.registry.MarkAgentSpawnStarted(
		context.Background(),
		store.AgentSpawnKey{
			ControllerID: root.ControllerID, TreeID: root.TreeID,
			SourceAgentID: root.AgentID, SpawnID: lifecycleSpawnID,
		},
		time.Unix(12, 0),
	); err != nil {
		t.Fatal(err)
	}

	targetConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer targetConnection.Close(websocket.StatusNormalClosure, "done")
	targetHello := hello()
	targetHello.DeviceID = lifecycleTargetDeviceID
	targetHello.DeviceName = "lifecycle-target"
	targetHello.WorkerRevision = 1
	helloResponse := writeAndRead(
		t, targetConnection, request(t, protocol.MethodHello, targetHello),
	)
	if helloResponse.Error != nil {
		t.Fatalf("target hello = %#v", helloResponse.Error)
	}
	helloResult := decodeResult[protocol.HelloResult](t, helloResponse)
	if helloResult.WorkerAppliedRevision != 0 {
		t.Fatalf("initial worker applied revision = %d", helloResult.WorkerAppliedRevision)
	}
	waitForBrokerConnectionState(t, harness.server, lifecycleTargetDeviceID, false)

	first := protocol.SyncWorkerLifecycleParams{
		BaseRevision: 0, ThroughRevision: 1, Complete: true,
		Workers: []protocol.WorkerLifecycleSnapshot{{
			TreeID: root.TreeID, AgentID: receipt.Agent.Principal.AgentID,
			Revision: 1, Phase: protocol.WorkerLifecycleRunning,
		}},
	}
	response := writeAndRead(
		t, targetConnection, request(t, protocol.MethodSyncWorkerLifecycle, first),
	)
	if response.Error != nil {
		t.Fatalf("initial lifecycle sync = %#v", response.Error)
	}
	if result := decodeResult[protocol.SyncWorkerLifecycleResult](t, response); result.AppliedRevision != 1 {
		t.Fatalf("initial lifecycle result = %#v", result)
	}
	waitForBrokerConnectionState(t, harness.server, lifecycleTargetDeviceID, true)
	page, err := harness.registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(),
		store.AgentLifecyclePageRequest{Limit: protocol.MaximumAgentPage},
	)
	if err != nil || len(page.Activities) != 1 ||
		page.Activities[0].Phase != protocol.WorkerLifecycleRunning || page.Highwater != 1 {
		t.Fatalf("running lifecycle page = %#v, error %v", page, err)
	}

	heartbeat := writeAndRead(
		t, targetConnection, request(t, protocol.MethodHeartbeat, protocol.Heartbeat{}),
	)
	if heartbeat.Error != nil {
		t.Fatalf("target heartbeat = %#v", heartbeat.Error)
	}
	second := protocol.SyncWorkerLifecycleParams{
		BaseRevision: 1, ThroughRevision: 2, Complete: true,
		Workers: []protocol.WorkerLifecycleSnapshot{{
			TreeID: root.TreeID, AgentID: receipt.Agent.Principal.AgentID,
			Revision: 2, Phase: protocol.WorkerLifecycleIdle,
		}},
	}
	response = writeAndRead(
		t, targetConnection, request(t, protocol.MethodSyncWorkerLifecycle, second),
	)
	if response.Error != nil {
		t.Fatalf("post-heartbeat lifecycle sync = %#v", response.Error)
	}
	page, err = harness.registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(),
		store.AgentLifecyclePageRequest{Limit: protocol.MaximumAgentPage},
	)
	if err != nil || len(page.Activities) != 1 ||
		page.Activities[0].Phase != protocol.WorkerLifecycleIdle || page.Highwater != 2 {
		t.Fatalf("idle lifecycle page = %#v, error %v", page, err)
	}
}

func TestReplacementHandshakeImmediatelyFencesDispatchToPriorSession(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	prior, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer prior.CloseNow()
	sendHello(t, prior)
	if harness.server.connection(brokerTestDeviceID) == nil {
		t.Fatal("prior session was not dispatchable before replacement")
	}

	blocked := &blockingLifecycleClaimRegistry{
		Store: harness.registry, started: make(chan struct{}), release: make(chan struct{}),
	}
	harness.server.registry = blocked
	replacement, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.CloseNow()
	writeEnvelope(t, replacement, request(t, protocol.MethodHello, hello()))
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement handshake did not reach lifecycle claim")
	}
	if target := harness.server.connection(brokerTestDeviceID); target != nil {
		t.Fatal("prior session remained dispatchable after replacement registered its lease")
	}
	readContext, cancelRead := context.WithTimeout(context.Background(), time.Second)
	_, _, readErr := prior.Read(readContext)
	cancelRead()
	if readErr == nil {
		t.Fatal("prior session remained open after replacement fencing")
	}

	close(blocked.release)
	response := readBrokerResponse(t, replacement)
	if response.Error != nil {
		t.Fatalf("replacement hello = %#v", response.Error)
	}
	waitForBrokerConnectionState(t, harness.server, brokerTestDeviceID, true)
}

func TestActivateRejectsHandshakeOlderThanLatestRegisteredLease(t *testing.T) {
	server := &Server{
		connections:     make(map[string]*session),
		latestRevisions: map[string]uint64{brokerTestDeviceID: 2},
	}
	stale := &session{deviceID: brokerTestDeviceID}
	stale.revision.Store(1)
	if _, active := server.activate(stale); active {
		t.Fatal("stale handshake activated after a newer lease was registered")
	}
}

func waitForBrokerConnectionState(
	t *testing.T,
	server *Server,
	deviceID string,
	wantReady bool,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		current := server.connections[deviceID]
		active := current != nil
		ready := active && current.workerReady.Load()
		server.mu.Unlock()
		if active && ready == wantReady {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("broker connection %s ready state did not become %t", deviceID, wantReady)
}
