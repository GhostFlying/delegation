package broker

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/coder/websocket"
)

const (
	brokerMailboxWorkerAgentID = "123e4567-e89b-42d3-a456-426614174120"
	brokerMailboxMessageID1    = "123e4567-e89b-42d3-a456-426614174121"
	brokerMailboxMessageID2    = "123e4567-e89b-42d3-a456-426614174122"
	brokerMailboxMessageID3    = "123e4567-e89b-42d3-a456-426614174123"
	brokerMailboxMessageID4    = "123e4567-e89b-42d3-a456-426614174124"
	brokerMailboxMessageID5    = "123e4567-e89b-42d3-a456-426614174125"
)

func TestMailboxRPCIsBidirectionalAndDoesNotBlockTheConnectionReadLoop(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)

	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	worker, err := harness.registry.CreateWorkerPrincipal(
		context.Background(),
		root.ControllerID,
		root.TreeID,
		brokerMailboxWorkerAgentID,
		root.AgentID,
		brokerTestSecondDeviceID,
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	waitRequest := principalRequest(t, protocol.MethodWaitMailbox, protocol.WaitMailboxParams{
		TimeoutMillis: 2_000,
		Limit:         1,
	}, worker)
	writeEnvelope(t, workerConnection, waitRequest)
	heartbeatRequest := request(t, protocol.MethodHeartbeat, protocol.Heartbeat{})
	writeEnvelope(t, workerConnection, heartbeatRequest)
	heartbeatResponse := readBrokerResponse(t, workerConnection)
	if heartbeatResponse.ReplyTo != heartbeatRequest.RequestID || heartbeatResponse.Error != nil {
		t.Fatalf("heartbeat while mailbox wait was pending = %#v", heartbeatResponse)
	}

	sendResponse := writeAndRead(t, rootConnection, principalRequest(
		t,
		protocol.MethodSendMessage,
		protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID1,
			Target: protocol.MessageTarget{
				Kind:    protocol.MessageTargetAgent,
				AgentID: worker.AgentID,
			},
			Message: "root to worker",
		},
		root,
	))
	if sendResponse.Error != nil {
		t.Fatalf("root send error = %#v", sendResponse.Error)
	}
	receipt := decodeResult[protocol.SendMessageResult](t, sendResponse)
	if receipt.Sequence != 1 {
		t.Fatalf("root send receipt = %#v", receipt)
	}

	waitResponse := readBrokerResponse(t, workerConnection)
	if waitResponse.ReplyTo != waitRequest.RequestID || waitResponse.Error != nil {
		t.Fatalf("worker mailbox response = %#v", waitResponse)
	}
	delivery := decodeResult[protocol.WaitMailboxResult](t, waitResponse)
	if len(delivery.Messages) != 1 || delivery.NextCursor != 1 ||
		delivery.Messages[0].Source != root.Identity() ||
		delivery.Messages[0].Message != "root to worker" {
		t.Fatalf("worker mailbox delivery = %#v", delivery)
	}

	workerSend := writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodSendMessage,
		protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID2,
			Target:    protocol.MessageTarget{Kind: protocol.MessageTargetParent},
			Message:   "worker to parent",
		},
		worker,
	))
	if workerSend.Error != nil {
		t.Fatalf("worker send error = %#v", workerSend.Error)
	}
	rootWait := writeAndRead(t, rootConnection, principalRequest(
		t,
		protocol.MethodWaitMailbox,
		protocol.WaitMailboxParams{TimeoutMillis: 0, Limit: 1},
		root,
	))
	if rootWait.Error != nil {
		t.Fatalf("root wait error = %#v", rootWait.Error)
	}
	rootDelivery := decodeResult[protocol.WaitMailboxResult](t, rootWait)
	if len(rootDelivery.Messages) != 1 || rootDelivery.NextCursor != 1 ||
		rootDelivery.Messages[0].Source != worker.Identity() ||
		rootDelivery.Messages[0].Message != "worker to parent" {
		t.Fatalf("root mailbox delivery = %#v", rootDelivery)
	}
}

func TestMailboxRPCIdempotencyAndStableQuotaError(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	registry := &mailboxTestRegistry{Store: harness.registry}
	harness.server.registry = registry
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)
	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	worker, err := harness.registry.CreateWorkerPrincipal(
		context.Background(), root.ControllerID, root.TreeID, brokerMailboxWorkerAgentID,
		root.AgentID, brokerTestSecondDeviceID, time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	target := protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: worker.AgentID}
	firstResponse := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodSendMessage, protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID1, Target: target, Message: "quota message",
		}, root,
	))
	if firstResponse.Error != nil {
		t.Fatalf("initial send = %#v", firstResponse.Error)
	}
	first := decodeResult[protocol.SendMessageResult](t, firstResponse)

	retry := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodSendMessage, protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID1, Target: target, Message: "quota message",
		}, root,
	))
	if retry.Error != nil {
		t.Fatalf("idempotent retry = %#v", retry.Error)
	}
	receipt := decodeResult[protocol.SendMessageResult](t, retry)
	if receipt != first {
		t.Fatalf("dedup retry receipt = %#v, want %#v", receipt, first)
	}

	conflict := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodSendMessage, protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID1, Target: target, Message: "conflicting retry",
		}, root,
	))
	if conflict.Error == nil || conflict.Error.Code != protocol.ErrorConflict ||
		conflict.Error.Message != "messageId conflicts with an existing message" {
		t.Fatalf("conflicting messageId response = %#v", conflict)
	}

	registry.setSendError(store.ErrMailboxFull)
	overflow := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodSendMessage, protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID2, Target: target, Message: "over quota",
		}, root,
	))
	if overflow.Error == nil || overflow.Error.Code != protocol.ErrorUnavailable ||
		overflow.Error.Message != "mailbox pending message quota exceeded" {
		t.Fatalf("over-quota response = %#v", overflow)
	}
}

type mailboxTestRegistry struct {
	*store.Store
	mu      sync.RWMutex
	sendErr error
}

func (r *mailboxTestRegistry) SendMailboxMessage(
	ctx context.Context,
	source control.Principal,
	target protocol.MessageTarget,
	messageID, message string,
	createdAt time.Time,
) (store.MailboxDelivery, error) {
	r.mu.RLock()
	err := r.sendErr
	r.mu.RUnlock()
	if err != nil {
		return store.MailboxDelivery{}, err
	}
	return r.Store.SendMailboxMessage(ctx, source, target, messageID, message, createdAt)
}

func (r *mailboxTestRegistry) setSendError(err error) {
	r.mu.Lock()
	r.sendErr = err
	r.mu.Unlock()
}

func TestMailboxRPCRejectsForgedAuthorityAndWorkerSiblingTarget(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)

	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	worker, err := harness.registry.CreateWorkerPrincipal(
		context.Background(),
		root.ControllerID,
		root.TreeID,
		brokerMailboxWorkerAgentID,
		root.AgentID,
		brokerTestSecondDeviceID,
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	forged := worker.Identity()
	forged.DeviceID = brokerTestDeviceID
	response := writeAndRead(t, workerConnection, identityRequest(
		t,
		protocol.MethodWaitMailbox,
		protocol.WaitMailboxParams{TimeoutMillis: 0, Limit: 1},
		forged,
	))
	if response.Error == nil || response.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("forged device wait response = %#v", response)
	}

	response = writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodSendMessage,
		protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID3,
			Target: protocol.MessageTarget{
				Kind:    protocol.MessageTargetAgent,
				AgentID: worker.AgentID,
			},
			Message: "forbidden arbitrary target",
		},
		worker,
	))
	if response.Error == nil || response.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("worker arbitrary target response = %#v", response)
	}

	response = writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodWaitMailbox,
		protocol.WaitMailboxParams{Cursor: 1, TimeoutMillis: 0, Limit: 1},
		worker,
	))
	if response.Error == nil || response.Error.Code != protocol.ErrorConflict {
		t.Fatalf("future cursor response = %#v", response)
	}
}

func TestMailboxRPCBoundsPendingWaitsPerSession(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)

	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	worker, err := harness.registry.CreateWorkerPrincipal(
		context.Background(),
		root.ControllerID,
		root.TreeID,
		brokerMailboxWorkerAgentID,
		root.AgentID,
		brokerTestSecondDeviceID,
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	harness.server.mu.Lock()
	workerSession := harness.server.connections[brokerTestSecondDeviceID]
	if workerSession != nil {
		workerSession.asyncSem = make(chan struct{}, 1)
	}
	harness.server.mu.Unlock()
	if workerSession == nil {
		t.Fatal("worker broker session was not active")
	}

	firstWait := principalRequest(t, protocol.MethodWaitMailbox, protocol.WaitMailboxParams{
		TimeoutMillis: 2_000,
		Limit:         1,
	}, worker)
	secondWait := principalRequest(t, protocol.MethodWaitMailbox, protocol.WaitMailboxParams{
		TimeoutMillis: 2_000,
		Limit:         1,
	}, worker)
	writeEnvelope(t, workerConnection, firstWait)
	writeEnvelope(t, workerConnection, secondWait)
	busy := readBrokerResponse(t, workerConnection)
	if busy.ReplyTo != secondWait.RequestID || busy.Error == nil ||
		busy.Error.Code != protocol.ErrorUnavailable {
		t.Fatalf("excess mailbox wait response = %#v", busy)
	}

	send := writeAndRead(t, rootConnection, principalRequest(
		t,
		protocol.MethodSendMessage,
		protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID4,
			Target: protocol.MessageTarget{
				Kind:    protocol.MessageTargetAgent,
				AgentID: worker.AgentID,
			},
			Message: "release admitted wait",
		},
		root,
	))
	if send.Error != nil {
		t.Fatalf("release message response = %#v", send)
	}
	admitted := readBrokerResponse(t, workerConnection)
	if admitted.ReplyTo != firstWait.RequestID || admitted.Error != nil {
		t.Fatalf("admitted mailbox wait response = %#v", admitted)
	}
	result := decodeResult[protocol.WaitMailboxResult](t, admitted)
	if len(result.Messages) != 1 || result.Messages[0].Message != "release admitted wait" {
		t.Fatalf("admitted mailbox wait result = %#v", result)
	}
}

func TestMailboxRPCWorkerWaitCapacityPreservesControlPlane(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rootConnection.CloseNow() })
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)
	rootSession := activeBrokerSession(t, harness.server, brokerTestDeviceID)
	if capacity := cap(rootSession.asyncSem); capacity != config.MaximumWorkerSlots+rootMailboxWaitHeadroom {
		t.Fatalf("broker mailbox wait capacity = %d", capacity)
	}

	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	worker, err := harness.registry.CreateWorkerPrincipal(
		context.Background(),
		root.ControllerID,
		root.TreeID,
		brokerMailboxWorkerAgentID,
		root.AgentID,
		brokerTestSecondDeviceID,
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	key := mailboxKey{
		controllerID: root.ControllerID,
		treeID:       root.TreeID,
		agentID:      root.AgentID,
	}
	for range config.MaximumWorkerSlots {
		writeEnvelope(t, rootConnection, principalRequest(
			t,
			protocol.MethodWaitMailbox,
			protocol.WaitMailboxParams{
				TimeoutMillis: protocol.MaximumMailboxWaitMillis,
				Limit:         1,
			},
			root,
		))
	}
	waitForPendingMailboxWaitCount(
		t, harness.server, rootSession, key, config.MaximumWorkerSlots,
	)

	listRequest := principalRequest(
		t,
		protocol.MethodListDevices,
		protocol.ListDevicesParams{Limit: 10},
		root,
	)
	writeEnvelope(t, rootConnection, listRequest)
	listResponse := readBrokerResponse(t, rootConnection)
	if listResponse.ReplyTo != listRequest.RequestID || listResponse.Error != nil {
		t.Fatalf("device list while worker waits occupied = %#v", listResponse)
	}

	sendRequest := principalRequest(
		t,
		protocol.MethodSendMessage,
		protocol.SendMessageParams{
			MessageID: brokerMailboxMessageID5,
			Target: protocol.MessageTarget{
				Kind:    protocol.MessageTargetAgent,
				AgentID: worker.AgentID,
			},
			Message: "control plane remains available",
		},
		root,
	)
	writeEnvelope(t, rootConnection, sendRequest)
	sendResponse := readBrokerResponse(t, rootConnection)
	if sendResponse.ReplyTo != sendRequest.RequestID || sendResponse.Error != nil {
		t.Fatalf("message send while worker waits occupied = %#v", sendResponse)
	}

	workerDelivery := writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodWaitMailbox,
		protocol.WaitMailboxParams{TimeoutMillis: 0, Limit: 1},
		worker,
	))
	if workerDelivery.Error != nil ||
		len(decodeResult[protocol.WaitMailboxResult](t, workerDelivery).Messages) != 1 {
		t.Fatalf("control-plane message delivery = %#v", workerDelivery)
	}
	if err := rootConnection.CloseNow(); err != nil {
		t.Fatal(err)
	}
	assertMailboxWaitCleanedUp(t, harness.server, rootSession)
}

func TestMailboxRPCPeerDisconnectCancelsMaximumWait(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	sendHello(t, connection)
	root := ensureRootPrincipal(t, connection)
	session := activeBrokerSession(t, harness.server, brokerTestDeviceID)
	key := mailboxKey{controllerID: root.ControllerID, treeID: root.TreeID, agentID: root.AgentID}

	writeEnvelope(t, connection, principalRequest(
		t,
		protocol.MethodWaitMailbox,
		protocol.WaitMailboxParams{TimeoutMillis: protocol.MaximumMailboxWaitMillis, Limit: 1},
		root,
	))
	waitForPendingMailboxWait(t, harness.server, session, key)

	if err := connection.CloseNow(); err != nil {
		t.Fatal(err)
	}
	assertMailboxWaitCleanedUp(t, harness.server, session)
}

func TestMailboxRPCBrokerShutdownCancelsMaximumWait(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	sendHello(t, connection)
	root := ensureRootPrincipal(t, connection)
	session := activeBrokerSession(t, harness.server, brokerTestDeviceID)
	key := mailboxKey{controllerID: root.ControllerID, treeID: root.TreeID, agentID: root.AgentID}

	writeEnvelope(t, connection, principalRequest(
		t,
		protocol.MethodWaitMailbox,
		protocol.WaitMailboxParams{TimeoutMillis: protocol.MaximumMailboxWaitMillis, Limit: 1},
		root,
	))
	waitForPendingMailboxWait(t, harness.server, session, key)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := harness.server.Close(ctx); err != nil {
		t.Fatal(err)
	}
	assertMailboxWaitCleanedUp(t, harness.server, session)
}

func activeBrokerSession(t *testing.T, server *Server, deviceID string) *session {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	current := server.connections[deviceID]
	if current == nil {
		t.Fatalf("broker session for %s is not active", deviceID)
	}
	return current
}

func waitForPendingMailboxWait(
	t *testing.T,
	server *Server,
	session *session,
	key mailboxKey,
) {
	waitForPendingMailboxWaitCount(t, server, session, key, 1)
}

func waitForPendingMailboxWaitCount(
	t *testing.T,
	server *Server,
	session *session,
	key mailboxKey,
	expected int,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		server.mailboxNotifier.mu.Lock()
		watch := server.mailboxNotifier.watches[key]
		waiters := 0
		if watch != nil {
			waiters = watch.waiters
		}
		server.mailboxNotifier.mu.Unlock()
		if len(session.asyncSem) == expected && waiters == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%d mailbox waits did not enter the asynchronous notifier path", expected)
}

func assertMailboxWaitCleanedUp(t *testing.T, server *Server, session *session) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		session.async.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("mailbox wait remained active instead of being canceled promptly")
	}
	if pending := len(session.asyncSem); pending != 0 {
		t.Fatalf("mailbox async slots after cancellation = %d, want 0", pending)
	}
	server.mailboxNotifier.mu.Lock()
	watches := len(server.mailboxNotifier.watches)
	server.mailboxNotifier.mu.Unlock()
	if watches != 0 {
		t.Fatalf("mailbox notifier retained %d subscriptions after cancellation", watches)
	}
}

func connectBrokerMailboxPeer(
	t *testing.T,
	harness brokerHarness,
	deviceID string,
) *websocket.Conn {
	t.Helper()
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload := hello()
	payload.DeviceID = deviceID
	payload.DeviceName = "mailbox-worker"
	response := writeAndRead(t, connection, request(t, protocol.MethodHello, payload))
	if response.Error != nil {
		connection.CloseNow()
		t.Fatalf("worker hello error = %#v", response.Error)
	}
	return connection
}

func readBrokerResponse(t *testing.T, connection *websocket.Conn) protocol.Envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messageType, data, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("response message type = %v, want text", messageType)
	}
	response, err := protocol.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return response
}
