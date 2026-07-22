package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

type lifecycleTestSource struct {
	mu        sync.Mutex
	revision  uint64
	snapshots []protocol.WorkerLifecycleSnapshot
	changes   chan struct{}
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

func TestConnectorReconnectUsesDurableLifecycleCursorAfterLostAck(t *testing.T) {
	source := newLifecycleTestSource(1, []protocol.WorkerLifecycleSnapshot{
		lifecycleSnapshot(connectorTestWorkerID, 1, protocol.WorkerLifecycleRunning),
	})
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
		if hello.WorkerRevision != 1 {
			t.Errorf("connection %d worker revision = %d", sequence, hello.WorkerRevision)
		}
		applied := uint64(0)
		if sequence > 1 {
			applied = 1
		}
		writeLifecycleHello(t, connection, helloRequest, applied)
		if sequence == 1 {
			request := readTestEnvelope(t, connection)
			if request.Method != protocol.MethodSyncWorkerLifecycle {
				t.Errorf("first post-hello method = %q", request.Method)
			}
			close(firstSync)
			return
		}
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
	t.Helper()
	manager := testWorkerSpawner{}
	client, err := New(Options{
		BrokerURL: brokerURL, ControllerID: connectorTestControllerID, DeviceID: connectorTestDeviceID,
		DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "lifecycle-test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: manager, WorkerController: manager, WorkerLifecycleSource: source,
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
	writeTestResult(t, connection, request, protocol.HelloResult{
		ConnectionID: connectorTestConnectionID,
		Features: []string{
			protocol.FeatureDeviceRegistry,
			protocol.FeatureFullDuplexRPC,
			protocol.FeatureMailbox,
			protocol.FeatureWorkerDispatch,
			protocol.FeaturePeerRoot,
			protocol.FeatureWorkerLifecycle,
		},
		HeartbeatIntervalMS:   time.Hour.Milliseconds(),
		Revision:              1,
		WorkerAppliedRevision: appliedRevision,
	})
}

func lifecycleSnapshot(
	agentID string,
	revision uint64,
	phase protocol.WorkerLifecyclePhase,
) protocol.WorkerLifecycleSnapshot {
	return protocol.WorkerLifecycleSnapshot{
		TreeID: connectorTestThreadID, AgentID: agentID, Revision: revision, Phase: phase,
	}
}

func containsFeature(features []string, wanted string) bool {
	for _, feature := range features {
		if feature == wanted {
			return true
		}
	}
	return false
}
