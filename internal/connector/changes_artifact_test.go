package connector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	connectorTestArtifactID  = "123e4567-e89b-42d3-a456-426614174206"
	connectorTestTurnID      = "123e4567-e89b-42d3-a456-426614174207"
	connectorTestWorkspaceID = "123e4567-e89b-42d3-a456-426614174208"
)

type changesArtifactAcknowledgement struct {
	publication ChangesArtifactPublication
	sequence    uint64
}

type changesArtifactBrokerRequest struct {
	envelope    protocol.Envelope
	publication ChangesArtifactPublication
}

type changesArtifactTestSource struct {
	mu      sync.Mutex
	pending []ChangesArtifactPublication
	changes chan struct{}
	acked   chan changesArtifactAcknowledgement
}

func newChangesArtifactTestSource(
	publications ...ChangesArtifactPublication,
) *changesArtifactTestSource {
	return &changesArtifactTestSource{
		pending: cloneChangesArtifactPublications(publications),
		changes: make(chan struct{}, 1),
		acked:   make(chan changesArtifactAcknowledgement, 4),
	}
}

func (s *changesArtifactTestSource) ArtifactChanges() <-chan struct{} { return s.changes }

func (s *changesArtifactTestSource) ListPendingChangesPublications(
	context.Context,
) ([]ChangesArtifactPublication, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneChangesArtifactPublications(s.pending), nil
}

func (s *changesArtifactTestSource) AcknowledgeChangesArtifact(
	_ context.Context,
	publication ChangesArtifactPublication,
	sequence uint64,
) error {
	s.mu.Lock()
	index := -1
	for candidateIndex, candidate := range s.pending {
		if sameChangesArtifactPublication(candidate, publication) {
			index = candidateIndex
			break
		}
	}
	if index < 0 {
		s.mu.Unlock()
		return errors.New("changes artifact publication is not pending")
	}
	s.pending = append(s.pending[:index], s.pending[index+1:]...)
	s.mu.Unlock()
	s.acked <- changesArtifactAcknowledgement{publication: publication, sequence: sequence}
	return nil
}

func (s *changesArtifactTestSource) add(publication ChangesArtifactPublication) {
	s.mu.Lock()
	s.pending = append(s.pending, cloneChangesArtifactPublication(publication))
	s.mu.Unlock()
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

func (s *changesArtifactTestSource) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

func TestConnectorPublishesPendingChangesBeforeReadiness(t *testing.T) {
	publication := testChangesArtifactPublication()
	source := newChangesArtifactTestSource(publication)
	requestSeen := make(chan ChangesArtifactPublication, 1)
	releaseResponse := make(chan struct{})
	holdConnection := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readChangesArtifactPublication(t, connection)
		requestSeen <- request.publication
		<-releaseResponse
		writeTestResult(t, connection, request.envelope, protocol.PublishChangesArtifactResult{
			ArtifactID: publication.Params.ArtifactID, Sequence: 17,
		})
		<-holdConnection
	})
	defer server.Close()
	client := newChangesArtifactClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)

	received := waitChangesArtifactPublication(t, requestSeen)
	assertSameChangesArtifactPublication(t, received, publication)
	readyContext, cancelReady := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := client.WaitReady(readyContext)
	cancelReady()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connector became ready before changes artifact acknowledgement: %v", err)
	}
	close(releaseResponse)
	acknowledgement := waitChangesArtifactAcknowledgement(t, source.acked)
	assertSameChangesArtifactPublication(t, acknowledgement.publication, publication)
	if acknowledgement.sequence != 17 || source.pendingCount() != 0 {
		t.Fatalf("changes artifact acknowledgement = %#v, pending = %d", acknowledgement, source.pendingCount())
	}
	waitReady(t, client)

	cancel()
	close(holdConnection)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorPublishesChangesAfterNotification(t *testing.T) {
	publication := testChangesArtifactPublication()
	source := newChangesArtifactTestSource()
	requestSeen := make(chan ChangesArtifactPublication, 1)
	holdConnection := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readChangesArtifactPublication(t, connection)
		requestSeen <- request.publication
		writeTestResult(t, connection, request.envelope, protocol.PublishChangesArtifactResult{
			ArtifactID: publication.Params.ArtifactID, Sequence: 23,
		})
		<-holdConnection
	})
	defer server.Close()
	client := newChangesArtifactClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)

	source.add(publication)
	received := waitChangesArtifactPublication(t, requestSeen)
	assertSameChangesArtifactPublication(t, received, publication)
	acknowledgement := waitChangesArtifactAcknowledgement(t, source.acked)
	if acknowledgement.sequence != 23 || source.pendingCount() != 0 {
		t.Fatalf("changes artifact acknowledgement = %#v, pending = %d", acknowledgement, source.pendingCount())
	}

	cancel()
	close(holdConnection)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorReplaysExactChangesArtifactAfterLostResponse(t *testing.T) {
	publication := testChangesArtifactPublication()
	source := newChangesArtifactTestSource(publication)
	requests := make(chan ChangesArtifactPublication, 2)
	holdConnection := make(chan struct{})
	var connections atomic.Int64
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readChangesArtifactPublication(t, connection)
		requests <- request.publication
		if connections.Add(1) == 1 {
			// The fake broker has durably committed the request, but its response is lost.
			connection.CloseNow()
			return
		}
		writeTestResult(t, connection, request.envelope, protocol.PublishChangesArtifactResult{
			ArtifactID: publication.Params.ArtifactID, Sequence: 29,
		})
		<-holdConnection
	})
	defer server.Close()
	client := newChangesArtifactClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)

	first := waitChangesArtifactPublication(t, requests)
	if source.pendingCount() != 1 {
		t.Fatalf("lost response acknowledged local outbox; pending = %d", source.pendingCount())
	}
	second := waitChangesArtifactPublication(t, requests)
	assertSameChangesArtifactPublication(t, first, publication)
	assertSameChangesArtifactPublication(t, second, publication)
	assertSameChangesArtifactPublication(t, second, first)
	acknowledgement := waitChangesArtifactAcknowledgement(t, source.acked)
	if acknowledgement.sequence != 29 || source.pendingCount() != 0 {
		t.Fatalf("replayed acknowledgement = %#v, pending = %d", acknowledgement, source.pendingCount())
	}
	select {
	case duplicate := <-source.acked:
		t.Fatalf("changes artifact was acknowledged twice: %#v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
	waitReady(t, client)

	cancel()
	close(holdConnection)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRejectsMismatchedChangesArtifactResult(t *testing.T) {
	publication := testChangesArtifactPublication()
	source := newChangesArtifactTestSource(publication)
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readChangesArtifactPublication(t, connection)
		writeTestResult(t, connection, request.envelope, protocol.PublishChangesArtifactResult{
			ArtifactID: connectorTestMessageID, Sequence: 31,
		})
	})
	defer server.Close()
	client := newChangesArtifactClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	healthy, err := client.runSession(ctx)
	if err == nil || !strings.Contains(err.Error(), "mismatched changes artifact") || healthy {
		t.Fatalf("mismatched publication result = healthy %v, error %v", healthy, err)
	}
	if source.pendingCount() != 1 {
		t.Fatalf("mismatched result acknowledged local outbox; pending = %d", source.pendingCount())
	}
	select {
	case acknowledgement := <-source.acked:
		t.Fatalf("mismatched result was acknowledged: %#v", acknowledgement)
	default:
	}
}

func TestConnectorShutdownPreservesUnacknowledgedChangesArtifact(t *testing.T) {
	publication := testChangesArtifactPublication()
	source := newChangesArtifactTestSource(publication)
	requestSeen := make(chan ChangesArtifactPublication, 1)
	releaseBroker := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readChangesArtifactPublication(t, connection)
		requestSeen <- request.publication
		<-releaseBroker
	})
	defer server.Close()
	client := newChangesArtifactClient(t, websocketURL(server.URL), source)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)

	received := waitChangesArtifactPublication(t, requestSeen)
	assertSameChangesArtifactPublication(t, received, publication)
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
	if source.pendingCount() != 1 {
		t.Fatalf("shutdown removed unacknowledged outbox record; pending = %d", source.pendingCount())
	}
	select {
	case acknowledgement := <-source.acked:
		t.Fatalf("shutdown acknowledged an unconfirmed publication: %#v", acknowledgement)
	default:
	}
	close(releaseBroker)
}

func newChangesArtifactClient(
	t *testing.T,
	brokerURL string,
	source ChangesArtifactSource,
) *Client {
	t.Helper()
	manager := testWorkerSpawner{}
	client, err := New(Options{
		BrokerURL: brokerURL, ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "changes-artifact-test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: manager, WorkerController: manager, WorkerLifecycleSource: manager,
		ChangesArtifactSource: source, WorkspaceManager: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testChangesArtifactPublication() ChangesArtifactPublication {
	return ChangesArtifactPublication{
		Source: control.PrincipalIdentity{
			ControllerID: connectorTestControllerID, TreeID: connectorTestThreadID,
			AgentID: connectorTestWorkerID, ParentAgentID: connectorTestMessageID,
			DeviceID: connectorTestDeviceID,
		},
		Params: protocol.PublishChangesArtifactParams{
			ArtifactID: connectorTestArtifactID, TurnID: connectorTestTurnID,
			WorkspaceID: connectorTestWorkspaceID, Status: protocol.ChangesArtifactAvailable,
			BaseHeadOID: strings.Repeat("a", 40), BaseManifestHash: strings.Repeat("b", 64),
			BaseSnapshotHash: strings.Repeat("c", 64), ResultHeadOID: strings.Repeat("d", 40),
			ResultSnapshotHash: strings.Repeat("e", 64), ResultClean: true,
			Parts: []protocol.WorkspaceArtifactDescriptor{{
				Kind: protocol.WorkspaceArtifactBundle, Size: 32, SHA256: strings.Repeat("f", 64),
			}},
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
	}
}

func readChangesArtifactPublication(
	t *testing.T,
	connection *websocket.Conn,
) changesArtifactBrokerRequest {
	t.Helper()
	request := readTestEnvelope(t, connection)
	if request.Kind != protocol.KindRequest || request.Method != protocol.MethodPublishChangesArtifact ||
		request.Source == nil || request.TreeID != request.Source.TreeID {
		t.Errorf("changes artifact request envelope = %#v", request)
		return changesArtifactBrokerRequest{}
	}
	params, err := protocol.DecodePayload[protocol.PublishChangesArtifactParams](request.Payload)
	if err != nil {
		t.Error(err)
		return changesArtifactBrokerRequest{}
	}
	return changesArtifactBrokerRequest{
		envelope:    request,
		publication: ChangesArtifactPublication{Source: *request.Source, Params: params},
	}
}

func waitChangesArtifactPublication(
	t *testing.T,
	publications <-chan ChangesArtifactPublication,
) ChangesArtifactPublication {
	t.Helper()
	select {
	case publication := <-publications:
		return publication
	case <-time.After(2 * time.Second):
		t.Fatal("fake broker did not receive changes artifact publication")
		return ChangesArtifactPublication{}
	}
}

func waitChangesArtifactAcknowledgement(
	t *testing.T,
	acknowledgements <-chan changesArtifactAcknowledgement,
) changesArtifactAcknowledgement {
	t.Helper()
	select {
	case acknowledgement := <-acknowledgements:
		return acknowledgement
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not acknowledge changes artifact publication")
		return changesArtifactAcknowledgement{}
	}
}

func assertSameChangesArtifactPublication(
	t *testing.T,
	got, want ChangesArtifactPublication,
) {
	t.Helper()
	if !sameChangesArtifactPublication(got, want) {
		t.Fatalf("changes artifact publication = %#v, want %#v", got, want)
	}
}

func sameChangesArtifactPublication(left, right ChangesArtifactPublication) bool {
	return left.Source == right.Source &&
		protocol.SameChangesArtifactParams(left.Params, right.Params)
}

func cloneChangesArtifactPublications(
	publications []ChangesArtifactPublication,
) []ChangesArtifactPublication {
	cloned := make([]ChangesArtifactPublication, len(publications))
	for index, publication := range publications {
		cloned[index] = cloneChangesArtifactPublication(publication)
	}
	return cloned
}

func cloneChangesArtifactPublication(publication ChangesArtifactPublication) ChangesArtifactPublication {
	publication.Params.Parts = append([]protocol.WorkspaceArtifactDescriptor(nil), publication.Params.Parts...)
	publication.Params.BaseWarnings = append([]string(nil), publication.Params.BaseWarnings...)
	publication.Params.ResultWarnings = append([]string(nil), publication.Params.ResultWarnings...)
	return publication
}
