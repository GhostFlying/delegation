package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	lifecycleCodexThreadID = "123e4567-e89b-42d3-a456-426614174225"
	lifecycleActiveTurnID  = "123e4567-e89b-42d3-a456-426614174226"
)

type lifecycleTestSource struct {
	mu        sync.Mutex
	revision  uint64
	snapshots []protocol.WorkerLifecycleSnapshot
	changes   chan struct{}
}

type mutableReadinessSource struct {
	mu        sync.Mutex
	readiness protocol.WorkerReadiness
}

func (s *mutableReadinessSource) WorkerReadiness(
	context.Context,
) (protocol.WorkerReadiness, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readiness, nil
}

func (s *mutableReadinessSource) set(readiness protocol.WorkerReadiness) {
	s.mu.Lock()
	s.readiness = readiness
	s.mu.Unlock()
}

func newLifecycleTestSource(
	revision uint64,
	snapshots []protocol.WorkerLifecycleSnapshot,
) *lifecycleTestSource {
	return &lifecycleTestSource{
		revision: revision, snapshots: snapshots, changes: make(chan struct{}, 1),
	}
}

func (s *lifecycleTestSource) WorkerRevision() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}

func (s *lifecycleTestSource) WorkerReadiness(context.Context) (protocol.WorkerReadiness, error) {
	return defaultTestWorkerReadiness(), nil
}

func (s *lifecycleTestSource) WorkerLifecycleChanges() <-chan struct{} {
	return s.changes
}

func (s *lifecycleTestSource) ListWorkerLifecycles(
	context.Context,
) ([]protocol.WorkerLifecycleSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.WorkerLifecycleSnapshot(nil), s.snapshots...), nil
}

func (s *lifecycleTestSource) update(
	revision uint64,
	snapshots []protocol.WorkerLifecycleSnapshot,
) {
	s.mu.Lock()
	s.revision = revision
	s.snapshots = append([]protocol.WorkerLifecycleSnapshot(nil), snapshots...)
	s.mu.Unlock()
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

func TestConnectorDoesNotPublishReadyBeforeInitialLifecycleAck(t *testing.T) {
	source := newLifecycleTestSource(2, []protocol.WorkerLifecycleSnapshot{
		lifecycleSnapshot(connectorTestWorkerID, 1, protocol.WorkerLifecycleRunning),
		lifecycleSnapshot(connectorTestMessageID, 2, protocol.WorkerLifecycleIdle),
	})
	syncSeen := make(chan protocol.SyncWorkerLifecycleParams, 1)
	releaseAck := make(chan struct{})
	defer func() {
		select {
		case <-releaseAck:
		default:
			close(releaseAck)
		}
	}()
	hold := make(chan struct{})
	server := newLifecycleBroker(t, func(connection *websocket.Conn, hello protocol.Hello) {
		if hello.WorkerRevision != 2 {
			t.Errorf("hello worker revision = %d", hello.WorkerRevision)
		}
		request := readTestEnvelope(t, connection)
		params, err := protocol.DecodePayload[protocol.SyncWorkerLifecycleParams](request.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		syncSeen <- params
		<-releaseAck
		applied, err := params.AppliedRevision()
		if err != nil {
			t.Error(err)
			return
		}
		writeTestResult(t, connection, request, protocol.SyncWorkerLifecycleResult{AppliedRevision: applied})
		<-hold
	}, 0)
	defer server.Close()
	defer close(hold)
	client := newLifecycleClient(t, websocketURL(server.URL), source)
	runContext, cancelRun := context.WithCancel(context.Background())
	done := runClient(client, runContext)

	var params protocol.SyncWorkerLifecycleParams
	select {
	case params = <-syncSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("initial lifecycle sync did not reach broker")
	}
	if params.BaseRevision != 0 || params.ThroughRevision != 2 || !params.Complete ||
		len(params.Workers) != 2 {
		t.Fatalf("initial lifecycle page = %#v", params)
	}
	if params.Workers[0].CodexThreadID != lifecycleCodexThreadID ||
		params.Workers[0].ActiveTurnID != lifecycleActiveTurnID ||
		params.Workers[1].CodexThreadID != lifecycleCodexThreadID ||
		params.Workers[1].ActiveTurnID != "" {
		t.Fatalf("initial lifecycle page lost thread or turn identity: %#v", params.Workers)
	}
	notReadyContext, cancelNotReady := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err := client.WaitReady(notReadyContext)
	cancelNotReady()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connector published before lifecycle ACK: %v", err)
	}
	close(releaseAck)
	waitReady(t, client)
	if status := client.Status(); status.WorkerRevision != 2 ||
		!containsFeature(status.Features, protocol.FeatureWorkerLifecycle) {
		t.Fatalf("connector lifecycle status = %#v", status)
	}
	cancelRun()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorUsesStartupBaselineButSynchronizesCurrentRevision(t *testing.T) {
	source := &startupRollbackLifecycleSource{
		lifecycleTestSource: newLifecycleTestSource(11, nil),
		startupRevision:     10,
	}
	syncSeen := make(chan protocol.SyncWorkerLifecycleParams, 1)
	releaseAck := make(chan struct{})
	defer func() {
		select {
		case <-releaseAck:
		default:
			close(releaseAck)
		}
	}()
	hold := make(chan struct{})
	server := newLifecycleBroker(t, func(connection *websocket.Conn, hello protocol.Hello) {
		if hello.WorkerBaselineRevision != 10 || hello.WorkerRevision != 11 {
			t.Errorf("hello revisions = baseline %d, current %d", hello.WorkerBaselineRevision, hello.WorkerRevision)
		}
		request := readTestEnvelope(t, connection)
		params, err := protocol.DecodePayload[protocol.SyncWorkerLifecycleParams](request.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		syncSeen <- params
		<-releaseAck
		writeTestResult(t, connection, request, protocol.SyncWorkerLifecycleResult{AppliedRevision: 11})
		<-hold
	}, 10)
	defer server.Close()
	defer close(hold)
	client := newLifecycleClient(t, websocketURL(server.URL), source)
	runContext, cancelRun := context.WithCancel(context.Background())
	done := runClient(client, runContext)

	select {
	case params := <-syncSeen:
		if params.BaseRevision != 10 || params.ThroughRevision != 11 || !params.Complete ||
			len(params.Workers) != 0 {
			t.Fatalf("initial lifecycle page = %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initial lifecycle sync did not reach broker")
	}
	notReadyContext, cancelNotReady := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err := client.WaitReady(notReadyContext)
	cancelNotReady()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connector published before current revision ACK: %v", err)
	}
	close(releaseAck)
	waitReady(t, client)
	if status := client.Status(); status.WorkerRevision != 11 {
		t.Fatalf("connector lifecycle status = %#v", status)
	}
	cancelRun()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorReconnectRetainsStartupBaselineUntilLifecycleAck(t *testing.T) {
	source := &startupRollbackLifecycleSource{
		lifecycleTestSource: newLifecycleTestSource(1, []protocol.WorkerLifecycleSnapshot{
			lifecycleSnapshot(connectorTestWorkerID, 1, protocol.WorkerLifecycleRunning),
		}),
		startupRevision: 0,
	}
	firstSync := make(chan struct{})
	secondHello := make(chan struct{})
	hold := make(chan struct{})
	var connections atomic.Int32
	server := newLifecycleBrokerDynamic(t, func(
		connection *websocket.Conn,
		helloRequest protocol.Envelope,
		hello protocol.Hello,
	) {
		sequence := connections.Add(1)
		if hello.WorkerBaselineRevision != 0 || hello.WorkerRevision != 1 {
			t.Errorf(
				"connection %d revisions = baseline %d, current %d",
				sequence,
				hello.WorkerBaselineRevision,
				hello.WorkerRevision,
			)
		}
		writeLifecycleHello(t, connection, helloRequest, 0)
		request := readTestEnvelope(t, connection)
		params, err := protocol.DecodePayload[protocol.SyncWorkerLifecycleParams](request.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		if params.BaseRevision != 0 || params.ThroughRevision != 1 {
			t.Errorf("connection %d lifecycle page = %#v", sequence, params)
		}
		if sequence == 1 {
			close(firstSync)
			return
		}
		writeTestResult(t, connection, request, protocol.SyncWorkerLifecycleResult{AppliedRevision: 1})
		close(secondHello)
		<-hold
	})
	defer server.Close()
	defer close(hold)
	client := newLifecycleClient(t, websocketURL(server.URL), source)
	runContext, cancelRun := context.WithCancel(context.Background())
	done := runClient(client, runContext)
	select {
	case <-firstSync:
	case <-time.After(2 * time.Second):
		t.Fatal("first lifecycle sync was not sent")
	}
	select {
	case <-secondHello:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not reconnect after lost lifecycle ACK")
	}
	waitReady(t, client)
	if status := client.Status(); status.WorkerRevision != 1 {
		t.Fatalf("reconnected lifecycle status = %#v", status)
	}
	cancelRun()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorPublishesTerminalReadinessOncePerActiveConnection(t *testing.T) {
	lifecycle := newLifecycleTestSource(0, nil)
	pending := protocol.NewPendingWorkerReadiness(
		strings.Repeat("a", 64), strings.Repeat("b", 64), 1,
	)
	readiness := &mutableReadinessSource{readiness: pending}
	updateSeen := make(chan protocol.WorkerReadiness, 2)
	hold := make(chan struct{})
	server := newLifecycleBrokerDynamic(t, func(
		connection *websocket.Conn, helloRequest protocol.Envelope, hello protocol.Hello,
	) {
		writeLifecycleHelloWithReadiness(t, connection, helloRequest, 0, hello.WorkerReadiness)
		request := readTestEnvelope(t, connection)
		if request.Method != protocol.MethodUpdateWorkerReadiness {
			t.Errorf("terminal update method = %q", request.Method)
			return
		}
		params, err := protocol.DecodePayload[protocol.UpdateWorkerReadinessParams](request.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		updateSeen <- params.Readiness
		writeTestResult(t, connection, request, protocol.UpdateWorkerReadinessResult{
			Readiness: params.Readiness,
		})
		<-hold
	})
	defer server.Close()
	defer close(hold)
	client := newLifecycleClientWithReadiness(
		t, websocketURL(server.URL), lifecycle, readiness,
	)
	runContext, cancelRun := context.WithCancel(context.Background())
	done := runClient(client, runContext)
	waitReady(t, client)
	terminal := pending
	terminal.State = protocol.WorkerReadinessReady
	terminal.AttemptCount = 1
	terminal.NextAttemptAt = 0
	terminal.LastAttemptAt = 2
	terminal.UpdatedAt = 2
	readiness.set(terminal)
	if err := client.UpdateWorkerReadiness(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	if err := client.UpdateWorkerReadiness(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-updateSeen:
		if got != terminal {
			t.Fatalf("terminal update = %#v, want %#v", got, terminal)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal readiness update was not sent")
	}
	select {
	case duplicate := <-updateSeen:
		t.Fatalf("duplicate terminal readiness update = %#v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
	cancelRun()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorLostTerminalAcknowledgementUsesReconnectHelloWithoutResend(t *testing.T) {
	lifecycle := newLifecycleTestSource(0, nil)
	pending := protocol.NewPendingWorkerReadiness(
		strings.Repeat("c", 64), strings.Repeat("d", 64), 1,
	)
	readiness := &mutableReadinessSource{readiness: pending}
	firstUpdate := make(chan struct{})
	secondHello := make(chan protocol.Hello, 1)
	hold := make(chan struct{})
	var connections atomic.Int32
	server := newLifecycleBrokerDynamic(t, func(
		connection *websocket.Conn, helloRequest protocol.Envelope, hello protocol.Hello,
	) {
		sequence := connections.Add(1)
		writeLifecycleHelloWithReadiness(t, connection, helloRequest, 0, hello.WorkerReadiness)
		if sequence == 1 {
			request := readTestEnvelope(t, connection)
			if request.Method != protocol.MethodUpdateWorkerReadiness {
				t.Errorf("terminal update method = %q", request.Method)
			}
			close(firstUpdate)
			return
		}
		secondHello <- hello
		readContext, cancelRead := context.WithTimeout(context.Background(), 75*time.Millisecond)
		defer cancelRead()
		if _, _, err := connection.Read(readContext); err == nil {
			t.Error("connector resent terminal readiness after reconnect")
		}
		<-hold
	})
	defer server.Close()
	defer close(hold)
	client := newLifecycleClientWithReadiness(
		t, websocketURL(server.URL), lifecycle, readiness,
	)
	runContext, cancelRun := context.WithCancel(context.Background())
	done := runClient(client, runContext)
	waitReady(t, client)
	terminal := pending
	terminal.State = protocol.WorkerReadinessInterventionRequired
	terminal.AttemptCount = 1
	terminal.NextAttemptAt = 0
	terminal.LastAttemptAt = 2
	terminal.FailureCode = protocol.WorkerManagedHomeInvalid
	terminal.UpdatedAt = 2
	readiness.set(terminal)
	updateErr := make(chan error, 1)
	go func() {
		updateErr <- client.UpdateWorkerReadiness(context.Background(), terminal)
	}()
	select {
	case <-firstUpdate:
	case <-time.After(2 * time.Second):
		t.Fatal("first terminal readiness update was not sent")
	}
	select {
	case err := <-updateErr:
		if err == nil {
			t.Fatal("lost terminal acknowledgement unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost terminal acknowledgement did not unblock caller")
	}
	select {
	case hello := <-secondHello:
		if hello.WorkerReadiness != terminal {
			t.Fatalf("reconnect hello readiness = %#v, want %#v", hello.WorkerReadiness, terminal)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not reconnect with durable terminal readiness")
	}
	cancelRun()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorSynchronizesLifecycleChangesAfterReadiness(t *testing.T) {
	source := newLifecycleTestSource(0, nil)
	synced := make(chan protocol.SyncWorkerLifecycleParams, 1)
	hold := make(chan struct{})
	server := newLifecycleBroker(t, func(connection *websocket.Conn, _ protocol.Hello) {
		request := readTestEnvelope(t, connection)
		params, err := protocol.DecodePayload[protocol.SyncWorkerLifecycleParams](request.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		writeTestResult(t, connection, request, protocol.SyncWorkerLifecycleResult{
			AppliedRevision: params.ThroughRevision,
		})
		synced <- params
		<-hold
	}, 0)
	defer server.Close()
	defer close(hold)
	client := newLifecycleClient(t, websocketURL(server.URL), source)
	runContext, cancelRun := context.WithCancel(context.Background())
	done := runClient(client, runContext)
	waitReady(t, client)
	source.update(1, []protocol.WorkerLifecycleSnapshot{
		lifecycleSnapshot(connectorTestWorkerID, 1, protocol.WorkerLifecycleRunning),
	})
	select {
	case params := <-synced:
		if params.BaseRevision != 0 || params.ThroughRevision != 1 || len(params.Workers) != 1 {
			t.Fatalf("background lifecycle page = %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background lifecycle change was not synchronized")
	}
	deadline := time.Now().Add(2 * time.Second)
	for client.Status().WorkerRevision != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := client.Status(); status.WorkerRevision != 1 {
		t.Fatalf("background lifecycle status = %#v", status)
	}
	cancelRun()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerLifecyclePagesAreStrictlyRevisionOrderedAndBounded(t *testing.T) {
	snapshots := make([]protocol.WorkerLifecycleSnapshot, 0, protocol.MaximumWorkerLifecyclePage+1)
	for index := 1; index <= protocol.MaximumWorkerLifecyclePage+1; index++ {
		snapshots = append(snapshots, lifecycleSnapshot(
			fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", index),
			uint64(protocol.MaximumWorkerLifecyclePage+2-index),
			protocol.WorkerLifecycleIdle,
		))
	}
	source := newLifecycleTestSource(uint64(len(snapshots)), snapshots)
	client := &Client{workerLifecycle: source}
	session := &session{client: client}
	first, err := session.nextWorkerLifecyclePage(context.Background(), 0, uint64(len(snapshots)))
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || len(first.Workers) != protocol.MaximumWorkerLifecyclePage ||
		first.Workers[0].Revision != 1 ||
		first.Workers[len(first.Workers)-1].Revision != protocol.MaximumWorkerLifecyclePage {
		t.Fatalf("first lifecycle page = %#v", first)
	}
	second, err := session.nextWorkerLifecyclePage(
		context.Background(), protocol.MaximumWorkerLifecyclePage, uint64(len(snapshots)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete || len(second.Workers) != 1 ||
		second.Workers[0].Revision != uint64(len(snapshots)) {
		t.Fatalf("second lifecycle page = %#v", second)
	}
}

func newLifecycleClient(
	t *testing.T,
	brokerURL string,
	source WorkerLifecycleSource,
) *Client {
	return newLifecycleClientWithReadiness(
		t, brokerURL, source, testWorkerSpawner{},
	)
}

func newLifecycleClientWithReadiness(
	t *testing.T, brokerURL string, source WorkerLifecycleSource, readiness WorkerReadinessSource,
) *Client {
	t.Helper()
	manager := testWorkerSpawner{}
	client, err := New(Options{
		BrokerURL: brokerURL, ControllerID: connectorTestControllerID, DeviceID: connectorTestDeviceID,
		DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "lifecycle-test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: manager, WorkerController: manager, WorkerLifecycleSource: source,
		WorkerReadinessSource: readiness,
		ChangesArtifactSource: manager,
		WorkspaceManager:      manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newLifecycleBroker(
	t *testing.T,
	afterHello func(*websocket.Conn, protocol.Hello),
	appliedRevision uint64,
) *httptest.Server {
	return newLifecycleBrokerDynamic(t, func(
		connection *websocket.Conn,
		helloRequest protocol.Envelope,
		hello protocol.Hello,
	) {
		writeLifecycleHello(t, connection, helloRequest, appliedRevision)
		afterHello(connection, hello)
	})
}

func newLifecycleBrokerDynamic(
	t *testing.T,
	handle func(*websocket.Conn, protocol.Envelope, protocol.Hello),
) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept lifecycle broker connection: %v", err)
			return
		}
		defer connection.CloseNow()
		helloEnvelope := readTestEnvelope(t, connection)
		hello, err := protocol.DecodePayload[protocol.Hello](helloEnvelope.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		handle(connection, helloEnvelope, hello)
	}))
}

func writeLifecycleHello(
	t *testing.T,
	connection *websocket.Conn,
	request protocol.Envelope,
	appliedRevision uint64,
) {
	writeLifecycleHelloWithReadiness(
		t, connection, request, appliedRevision, defaultTestWorkerReadiness(),
	)
}

func writeLifecycleHelloWithReadiness(
	t *testing.T, connection *websocket.Conn, request protocol.Envelope,
	appliedRevision uint64, readiness protocol.WorkerReadiness,
) {
	writeTestResult(t, connection, request, protocol.HelloResult{
		ConnectionID: connectorTestConnectionID,
		HostKind:     hostkind.Codex,
		Features: []string{
			protocol.FeatureChangesArtifact,
			protocol.FeatureDeviceRegistry,
			protocol.FeatureFullDuplexRPC,
			protocol.FeatureMailbox,
			protocol.FeatureWorkerDispatch,
			protocol.FeaturePeerRoot,
			protocol.FeatureResultApply,
			protocol.FeatureResultPackage,
			protocol.FeatureWorkerLifecycle,
			protocol.FeatureWorkerReadiness,
			protocol.FeatureWorkspaceSync,
			protocol.FeatureWorkspaceTransfer,
		},
		HeartbeatIntervalMS:   time.Hour.Milliseconds(),
		Revision:              1,
		WorkerAppliedRevision: appliedRevision,
		WorkerReadiness:       readiness,
	})
}

func lifecycleSnapshot(
	agentID string,
	revision uint64,
	phase protocol.WorkerLifecyclePhase,
) protocol.WorkerLifecycleSnapshot {
	snapshot := protocol.WorkerLifecycleSnapshot{
		TreeID: connectorTestThreadID, AgentID: agentID, Revision: revision, Phase: phase,
		CodexThreadID: lifecycleCodexThreadID,
	}
	if phase == protocol.WorkerLifecycleRunning || phase == protocol.WorkerLifecycleFinalizing {
		snapshot.ActiveTurnID = lifecycleActiveTurnID
	}
	return snapshot
}

func containsFeature(features []string, wanted string) bool {
	for _, feature := range features {
		if feature == wanted {
			return true
		}
	}
	return false
}
