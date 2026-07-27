package connector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

type resultPackageTestSource struct {
	mu        sync.Mutex
	pending   []ResultPackagePublication
	changes   chan struct{}
	acked     chan ResultPackagePublication
	listErr   error
	listCalls atomic.Int64
}

func newResultPackageTestSource(
	publications ...ResultPackagePublication,
) *resultPackageTestSource {
	return &resultPackageTestSource{
		pending: append([]ResultPackagePublication(nil), publications...),
		changes: make(chan struct{}, 1),
		acked:   make(chan ResultPackagePublication, 4),
	}
}

func (s *resultPackageTestSource) ResultPackageChanges() <-chan struct{} {
	return s.changes
}

func (s *resultPackageTestSource) ListPendingResultPackagePublications(
	context.Context,
) ([]ResultPackagePublication, error) {
	s.listCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]ResultPackagePublication(nil), s.pending...), nil
}

func (s *resultPackageTestSource) AcknowledgeResultPackageMetadata(
	_ context.Context,
	publication ResultPackagePublication,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, pending := range s.pending {
		if pending.Source == publication.Source &&
			protocol.SamePublishResultPackageParams(pending.Params, publication.Params) {
			s.pending = append(s.pending[:index], s.pending[index+1:]...)
			s.acked <- publication
			return nil
		}
	}
	return errors.New("result package publication is not pending")
}

func (s *resultPackageTestSource) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

func TestConnectorReplaysExactResultPackageMetadataAfterLostResponse(t *testing.T) {
	publication := testResultPackagePublication(t)
	source := newResultPackageTestSource(publication)
	requests := make(chan ResultPackagePublication, 2)
	holdConnection := make(chan struct{})
	var connections atomic.Int64
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readResultPackagePublication(t, connection)
		requests <- request.publication
		if connections.Add(1) == 1 {
			connection.CloseNow()
			return
		}
		writeTestResult(t, connection, request.envelope, protocol.PublishResultPackageResult{
			PackageID: resultPackageTestPackageID,
		})
		<-holdConnection
	})
	defer server.Close()
	client := newResultPackageSourceClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)

	first := waitResultPackagePublication(t, requests)
	if source.pendingCount() != 1 {
		t.Fatalf("lost response acknowledged local outbox; pending = %d", source.pendingCount())
	}
	second := waitResultPackagePublication(t, requests)
	assertSameResultPackagePublication(t, first, publication)
	assertSameResultPackagePublication(t, second, publication)
	assertSameResultPackagePublication(t, second, first)
	select {
	case acknowledgement := <-source.acked:
		assertSameResultPackagePublication(t, acknowledgement, publication)
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not acknowledge replayed result package metadata")
	}
	if source.pendingCount() != 0 {
		t.Fatalf("replayed result package remained pending: %d", source.pendingCount())
	}
	waitReady(t, client)

	cancel()
	close(holdConnection)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRejectsResultPackageManifestOutsideWorkerAuthority(t *testing.T) {
	publication := testResultPackagePublication(t)
	manifest, err := publication.Params.Metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.SourceDeviceID = connectorTestMessageID
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	publication.Params.Metadata = protocol.ResultPackageMetadata{
		Manifest: data, ManifestDescriptor: descriptor,
	}
	s := &session{client: &Client{hello: protocol.Hello{
		ControllerID: connectorTestControllerID, DeviceID: connectorTestDeviceID,
	}}}
	if _, err := s.validateResultPackagePublication(publication); err == nil {
		t.Fatal("connector accepted result metadata outside the worker principal")
	}
}

func TestResultPackagePublicationPermanentErrorClassification(t *testing.T) {
	localSource := newResultPackageTestSource()
	localSource.listErr = permanentResultPackageError(errors.New("durable invariant failed"))
	s := &session{client: &Client{
		resultSource: localSource, artifactCallLimit: time.Second,
	}}
	if err := s.publishPendingResultPackages(context.Background()); !errors.Is(err, ErrPermanentResultPackagePublication) || localSource.listCalls.Load() != 1 {
		t.Fatalf("permanent local publication error = %v, calls = %d", err, localSource.listCalls.Load())
	}

	for _, test := range []struct {
		name      string
		errorCode int
		permanent bool
	}{
		{name: "unavailable", errorCode: protocol.ErrorUnavailable, permanent: false},
		{name: "conflict", errorCode: protocol.ErrorConflict, permanent: true},
		{name: "forbidden", errorCode: protocol.ErrorForbidden, permanent: true},
		{name: "invalid params", errorCode: protocol.ErrorInvalidParams, permanent: true},
		{name: "method not found", errorCode: protocol.ErrorMethodNotFound, permanent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := permanentResultPackageRPCError(&RPCError{
				Code: test.errorCode, Message: test.name,
			})
			if got != test.permanent {
				t.Fatalf("permanent RPC classification = %t, want %t", got, test.permanent)
			}
		})
	}
}

func TestConnectorTreatsClosedResultPackageNotificationChannelAsSessionFailure(t *testing.T) {
	source := newResultPackageTestSource()
	close(source.changes)
	holdConnection := make(chan struct{})
	connectionStarted := make(chan struct{}, 1)
	var connections atomic.Int64
	server := newFakeBroker(t, func(*websocket.Conn) {
		connections.Add(1)
		select {
		case connectionStarted <- struct{}{}:
		default:
		}
		<-holdConnection
	})
	defer server.Close()
	client := newResultPackageSourceClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runClient(client, ctx)

	err := waitClient(done)
	if !errors.Is(err, errResultPackageNotificationsClosed) {
		t.Fatalf("connector run error = %v", err)
	}
	waitSignal(t, connectionStarted, "result package connector session")
	if client.Status().Connected || connections.Load() != 1 {
		t.Fatalf("closed result channel status=%#v connections=%d", client.Status(), connections.Load())
	}
	close(holdConnection)
}

func TestConnectorFailsClosedOnPermanentLocalResultPackageError(t *testing.T) {
	source := newResultPackageTestSource()
	source.listErr = permanentResultPackageError(errors.New("durable outbox invariant failed"))
	holdConnection := make(chan struct{})
	defer close(holdConnection)
	connectionStarted := make(chan struct{}, 1)
	var connections atomic.Int64
	server := newFakeBroker(t, func(*websocket.Conn) {
		connections.Add(1)
		select {
		case connectionStarted <- struct{}{}:
		default:
		}
		<-holdConnection
	})
	defer server.Close()
	client := newResultPackageSourceClient(t, websocketURL(server.URL), source)
	done := runClient(client, context.Background())

	err := waitClient(done)
	if !errors.Is(err, ErrPermanentResultPackagePublication) {
		t.Fatalf("connector run error = %v", err)
	}
	waitSignal(t, connectionStarted, "result package connector session")
	if client.Status().Connected || connections.Load() != 1 || source.listCalls.Load() != 1 {
		t.Fatalf(
			"permanent local error status=%#v connections=%d listCalls=%d",
			client.Status(), connections.Load(), source.listCalls.Load(),
		)
	}
}

func TestConnectorFailsClosedOnPermanentBrokerResultPackageError(t *testing.T) {
	publication := testResultPackagePublication(t)
	source := newResultPackageTestSource(publication)
	holdConnection := make(chan struct{})
	defer close(holdConnection)
	var requests atomic.Int64
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readTestEnvelope(t, connection)
		requests.Add(1)
		writeTestEnvelope(t, connection, protocol.Envelope{
			ProtocolVersion: protocol.Version,
			Kind:            protocol.KindResponse,
			RequestID:       testRequestID(t, protocol.DirectionBroker),
			ReplyTo:         request.RequestID,
			ControllerID:    connectorTestControllerID,
			TreeID:          request.TreeID,
			Error: &protocol.Error{
				Code: protocol.ErrorConflict, Message: "result package conflicts with broker state",
			},
		})
		<-holdConnection
	})
	defer server.Close()
	client := newResultPackageSourceClient(t, websocketURL(server.URL), source)
	done := runClient(client, context.Background())

	err := waitClient(done)
	if !errors.Is(err, ErrPermanentResultPackagePublication) {
		t.Fatalf("connector run error = %v", err)
	}
	if client.Status().Connected || requests.Load() != 1 || source.pendingCount() != 1 {
		t.Fatalf(
			"permanent broker error status=%#v requests=%d pending=%d",
			client.Status(), requests.Load(), source.pendingCount(),
		)
	}
}

type resultPackageBrokerRequest struct {
	envelope    protocol.Envelope
	publication ResultPackagePublication
}

func testResultPackagePublication(t *testing.T) ResultPackagePublication {
	t.Helper()
	return ResultPackagePublication{
		Source: resultPackageWorker(),
		Params: protocol.PublishResultPackageParams{Metadata: newResultPackageRPCFixture(t).metadata},
	}
}

func newResultPackageSourceClient(
	t *testing.T,
	brokerURL string,
	source ResultPackageSource,
) *Client {
	t.Helper()
	manager := testWorkerSpawner{}
	client, err := New(Options{
		BrokerURL: brokerURL, ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "result-package-source-test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: manager, WorkerController: manager, WorkerLifecycleSource: manager,
		ChangesArtifactSource: manager, ResultPackageSource: source,
		WorkspaceManager: manager, ResultPackageManager: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func readResultPackagePublication(
	t *testing.T,
	connection *websocket.Conn,
) resultPackageBrokerRequest {
	t.Helper()
	request := readTestEnvelope(t, connection)
	if request.Kind != protocol.KindRequest || request.Method != protocol.MethodPublishResultPackage ||
		request.Source == nil || request.TreeID != request.Source.TreeID {
		t.Errorf("result package request envelope = %#v", request)
		return resultPackageBrokerRequest{}
	}
	params, err := protocol.DecodePayload[protocol.PublishResultPackageParams](request.Payload)
	if err != nil {
		t.Error(err)
		return resultPackageBrokerRequest{}
	}
	return resultPackageBrokerRequest{
		envelope:    request,
		publication: ResultPackagePublication{Source: *request.Source, Params: params},
	}
}

func waitResultPackagePublication(
	t *testing.T,
	publications <-chan ResultPackagePublication,
) ResultPackagePublication {
	t.Helper()
	select {
	case publication := <-publications:
		return publication
	case <-time.After(2 * time.Second):
		t.Fatal("fake broker did not receive result package publication")
		return ResultPackagePublication{}
	}
}

func assertSameResultPackagePublication(
	t *testing.T,
	got, want ResultPackagePublication,
) {
	t.Helper()
	if got.Source != want.Source || !protocol.SamePublishResultPackageParams(got.Params, want.Params) {
		t.Fatalf("result package publication = %#v, want %#v", got, want)
	}
}
