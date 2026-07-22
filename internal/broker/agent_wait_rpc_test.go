package broker

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/coder/websocket"
)

const (
	agentWaitWorkerID   = "123e4567-e89b-42d3-a456-426614174160"
	agentWaitMessageID  = "123e4567-e89b-42d3-a456-426614174161"
	agentWaitMessageID2 = "123e4567-e89b-42d3-a456-426614174162"
	agentWaitMessageID3 = "123e4567-e89b-42d3-a456-426614174163"
)

func TestAgentWaitReturnsWorkerMessageWithIndependentCursors(t *testing.T) {
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
		agentWaitWorkerID,
		root.AgentID,
		brokerTestSecondDeviceID,
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	waitRequest := principalRequest(t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
		TimeoutMillis: 2_000,
		MessageLimit:  protocol.MaximumAgentWaitMessages,
		ActivityLimit: protocol.MaximumAgentWaitActivities,
	}, root)
	writeEnvelope(t, rootConnection, waitRequest)
	workerSend := writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodSendMessage,
		protocol.SendMessageParams{
			MessageID: agentWaitMessageID,
			Target:    protocol.MessageTarget{Kind: protocol.MessageTargetParent},
			Message:   "worker completed validation",
		},
		worker,
	))
	if workerSend.Error != nil {
		t.Fatalf("worker send error = %#v", workerSend.Error)
	}

	waitResponse := readBrokerResponse(t, rootConnection)
	if waitResponse.ReplyTo != waitRequest.RequestID || waitResponse.Error != nil {
		t.Fatalf("root agent wait response = %#v", waitResponse)
	}
	result := decodeResult[protocol.WaitAgentResult](t, waitResponse)
	if len(result.Messages) != 1 || len(result.Activities) != 0 ||
		result.Messages[0].Source != worker.Identity() ||
		result.Messages[0].Message != "worker completed validation" ||
		result.NextMailboxCursor != 1 || result.NextLifecycleCursor != 0 {
		t.Fatalf("root agent wait result = %#v", result)
	}
	for _, message := range []struct {
		id   string
		text string
	}{
		{id: agentWaitMessageID2, text: "second worker update"},
		{id: agentWaitMessageID3, text: "third worker update"},
	} {
		response := writeAndRead(t, workerConnection, principalRequest(
			t,
			protocol.MethodSendMessage,
			protocol.SendMessageParams{
				MessageID: message.id,
				Target:    protocol.MessageTarget{Kind: protocol.MessageTargetParent},
				Message:   message.text,
			},
			worker,
		))
		if response.Error != nil {
			t.Fatalf("queue %s = %#v", message.text, response.Error)
		}
	}
	continued := writeAndRead(t, rootConnection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{
			MailboxCursor: result.NextMailboxCursor,
			MessageLimit:  1, ActivityLimit: 1,
		},
		root,
	))
	continuedResult := decodeResult[protocol.WaitAgentResult](t, continued)
	if continued.Error != nil || len(continuedResult.Messages) != 1 ||
		continuedResult.Messages[0].MessageID != agentWaitMessageID2 ||
		continuedResult.NextMailboxCursor != 2 || !continuedResult.MoreMessages {
		t.Fatalf("continued agent wait result = %#v, error %#v", continuedResult, continued.Error)
	}
	drained := writeAndRead(t, rootConnection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{
			MailboxCursor: continuedResult.NextMailboxCursor,
			MessageLimit:  1, ActivityLimit: 1,
		},
		root,
	))
	drainedResult := decodeResult[protocol.WaitAgentResult](t, drained)
	if drained.Error != nil || len(drainedResult.Messages) != 1 ||
		drainedResult.Messages[0].MessageID != agentWaitMessageID3 ||
		drainedResult.NextMailboxCursor != 3 || drainedResult.MoreMessages {
		t.Fatalf("drained agent wait result = %#v, error %#v", drainedResult, drained.Error)
	}

	workerBypass := writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{MessageLimit: 1, ActivityLimit: 1},
		worker,
	))
	if workerBypass.Error == nil || workerBypass.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("worker agent wait bypass = %#v", workerBypass)
	}
	crossDeviceRoot := writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{MessageLimit: 1, ActivityLimit: 1},
		root,
	))
	if crossDeviceRoot.Error == nil || crossDeviceRoot.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("cross-device root agent wait bypass = %#v", crossDeviceRoot)
	}
	ahead := writeAndRead(t, rootConnection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{
			MailboxCursor: 1, LifecycleCursor: 1,
			MessageLimit: 1, ActivityLimit: 1,
		},
		root,
	))
	if ahead.Error == nil || ahead.Error.Code != protocol.ErrorConflict {
		t.Fatalf("ahead lifecycle cursor = %#v", ahead)
	}
}

func TestAgentWaitWakesOnWorkerLifecycleNotification(t *testing.T) {
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
			Source: root.Identity(), SpawnID: lifecycleSpawnID, AgentID: lifecycleAgentID,
			TargetDeviceID: lifecycleTargetDeviceID, TaskName: "lifecycle_wait_worker",
			PromptDigest: sha256.Sum256([]byte("lifecycle wait worker prompt")),
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
	if response := writeAndRead(
		t, targetConnection, request(t, protocol.MethodHello, targetHello),
	); response.Error != nil {
		t.Fatalf("target hello = %#v", response.Error)
	}

	waitRequest := principalRequest(t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
		TimeoutMillis: 2_000,
		MessageLimit:  protocol.MaximumAgentWaitMessages, ActivityLimit: protocol.MaximumAgentWaitActivities,
	}, root)
	writeEnvelope(t, rootConnection, waitRequest)
	rootSession := activeBrokerSession(t, harness.server, brokerTestDeviceID)
	waitForPendingAgentWait(t, harness.server, rootSession, root, waitRequest.RequestID)
	syncResponse := writeAndRead(
		t,
		targetConnection,
		request(t, protocol.MethodSyncWorkerLifecycle, protocol.SyncWorkerLifecycleParams{
			BaseRevision: 0, ThroughRevision: 1, Complete: true,
			Workers: []protocol.WorkerLifecycleSnapshot{{
				TreeID: root.TreeID, AgentID: receipt.Agent.Principal.AgentID,
				Revision: 1, Phase: protocol.WorkerLifecycleRunning,
			}},
		}),
	)
	if syncResponse.Error != nil {
		t.Fatalf("worker lifecycle sync = %#v", syncResponse.Error)
	}
	waitResponse := readBrokerResponse(t, rootConnection)
	if waitResponse.ReplyTo != waitRequest.RequestID || waitResponse.Error != nil {
		t.Fatalf("root lifecycle wait response = %#v", waitResponse)
	}
	result := decodeResult[protocol.WaitAgentResult](t, waitResponse)
	if len(result.Messages) != 0 || len(result.Activities) != 1 ||
		result.Activities[0].AgentID != receipt.Agent.Principal.AgentID ||
		result.Activities[0].Phase != protocol.WorkerLifecycleRunning ||
		result.NextMailboxCursor != 0 || result.NextLifecycleCursor != 1 {
		t.Fatalf("root lifecycle wait result = %#v", result)
	}
}

func TestAgentWaitCancellationReleasesCapacityAndSubscriptions(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, connection)
	root := ensureRootPrincipal(t, connection)
	session := activeBrokerSession(t, harness.server, brokerTestDeviceID)

	waitRequest := principalRequest(t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
		TimeoutMillis: protocol.MaximumAgentWaitMillis,
		MessageLimit:  protocol.MaximumAgentWaitMessages, ActivityLimit: protocol.MaximumAgentWaitActivities,
	}, root)
	writeEnvelope(t, connection, waitRequest)
	waitForPendingAgentWait(t, harness.server, session, root, waitRequest.RequestID)
	cancelRequest := request(t, protocol.MethodCancelRequest, protocol.CancelRequestParams{
		RequestID: waitRequest.RequestID,
	})
	cancelRequest.Kind = protocol.KindNotification
	writeEnvelope(t, connection, cancelRequest)
	assertAgentWaitCleanedUp(t, harness.server, session, waitRequest.RequestID)

	immediate := writeAndRead(t, connection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{MessageLimit: 1, ActivityLimit: 1},
		root,
	))
	result := decodeResult[protocol.WaitAgentResult](t, immediate)
	if immediate.Error != nil || len(result.Messages) != 0 || len(result.Activities) != 0 {
		t.Fatalf("agent wait after cancellation = %#v, error %#v", result, immediate.Error)
	}
}

func waitForPendingAgentWait(
	t *testing.T,
	server *Server,
	session *session,
	root control.Principal,
	requestID string,
) {
	t.Helper()
	mailbox := mailboxKey{
		controllerID: root.ControllerID, treeID: root.TreeID, agentID: root.AgentID,
	}
	lifecycle := treeKey{controllerID: root.ControllerID, treeID: root.TreeID}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		server.mailboxNotifier.mu.Lock()
		mailboxWatch := server.mailboxNotifier.watches[mailbox]
		mailboxWaiters := 0
		if mailboxWatch != nil {
			mailboxWaiters = mailboxWatch.waiters
		}
		server.mailboxNotifier.mu.Unlock()
		server.lifecycleNotifier.mu.Lock()
		lifecycleWatch := server.lifecycleNotifier.watches[lifecycle]
		lifecycleWaiters := 0
		if lifecycleWatch != nil {
			lifecycleWaiters = lifecycleWatch.waiters
		}
		server.lifecycleNotifier.mu.Unlock()
		session.asyncMu.Lock()
		_, cancellable := session.asyncCancels[requestID]
		session.asyncMu.Unlock()
		if len(session.asyncSem) == 1 && mailboxWaiters == 1 && lifecycleWaiters == 1 && cancellable {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("agent wait did not enter both notifier paths")
}

func assertAgentWaitCleanedUp(
	t *testing.T,
	server *Server,
	session *session,
	requestID string,
) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		session.async.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent wait remained active after cancellation")
	}
	session.asyncMu.Lock()
	_, cancellable := session.asyncCancels[requestID]
	session.asyncMu.Unlock()
	server.mailboxNotifier.mu.Lock()
	mailboxWatches := len(server.mailboxNotifier.watches)
	server.mailboxNotifier.mu.Unlock()
	server.lifecycleNotifier.mu.Lock()
	lifecycleWatches := len(server.lifecycleNotifier.watches)
	server.lifecycleNotifier.mu.Unlock()
	if len(session.asyncSem) != 0 || cancellable || mailboxWatches != 0 || lifecycleWatches != 0 {
		t.Fatalf(
			"agent wait cleanup = slots %d, cancellable %v, mailbox watches %d, lifecycle watches %d",
			len(session.asyncSem), cancellable, mailboxWatches, lifecycleWatches,
		)
	}
}
