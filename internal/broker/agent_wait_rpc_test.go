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
	agentWaitWorkerID         = "123e4567-e89b-42d3-a456-426614174160"
	agentWaitMessageID        = "123e4567-e89b-42d3-a456-426614174161"
	agentWaitMessageID2       = "123e4567-e89b-42d3-a456-426614174162"
	agentWaitMessageID3       = "123e4567-e89b-42d3-a456-426614174163"
	agentWaitResultSpawnID1   = "123e4567-e89b-42d3-a456-426614174164"
	agentWaitResultAgentID1   = "123e4567-e89b-42d3-a456-426614174165"
	agentWaitResultThreadID1  = "123e4567-e89b-42d3-a456-426614174166"
	agentWaitResultTurnID1    = "123e4567-e89b-42d3-a456-426614174167"
	agentWaitResultPackageID1 = "123e4567-e89b-42d3-a456-426614174168"
	agentWaitResultSpawnID2   = "123e4567-e89b-42d3-a456-426614174169"
	agentWaitResultAgentID2   = "123e4567-e89b-42d3-a456-42661417416a"
	agentWaitResultThreadID2  = "123e4567-e89b-42d3-a456-42661417416b"
	agentWaitResultTurnID2    = "123e4567-e89b-42d3-a456-42661417416c"
	agentWaitResultPackageID2 = "123e4567-e89b-42d3-a456-42661417416d"
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
		ArtifactLimit: protocol.MaximumAgentWaitArtifacts,
		ResultLimit:   protocol.MaximumAgentWaitResults,
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
			MessageLimit:  1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
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
			MessageLimit:  1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
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
		protocol.WaitAgentParams{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		worker,
	))
	if workerBypass.Error == nil || workerBypass.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("worker agent wait bypass = %#v", workerBypass)
	}
	crossDeviceRoot := writeAndRead(t, workerConnection, principalRequest(
		t,
		protocol.MethodWaitAgent,
		protocol.WaitAgentParams{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
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
			MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
		},
		root,
	))
	if ahead.Error == nil || ahead.Error.Code != protocol.ErrorConflict {
		t.Fatalf("ahead lifecycle cursor = %#v", ahead)
	}
}

func TestAgentWaitPaginatesDeliveredResultsWithIndependentCursor(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)

	targetConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer targetConnection.Close(websocket.StatusNormalClosure, "done")
	targetHello := hello()
	targetHello.DeviceID = brokerTestSecondDeviceID
	targetHello.DeviceName = "result-target"
	targetHello.WorkerRevision = 2
	if response := writeAndRead(
		t, targetConnection, request(t, protocol.MethodHello, targetHello),
	); response.Error != nil {
		t.Fatalf("target hello = %#v", response.Error)
	}

	workers := []struct {
		spawnID, agentID, taskName, threadID, turnID, packageID string
		revision                                                uint64
	}{
		{
			spawnID: agentWaitResultSpawnID1, agentID: agentWaitResultAgentID1,
			taskName: "result_wait_worker_1",
			threadID: agentWaitResultThreadID1, turnID: agentWaitResultTurnID1,
			packageID: agentWaitResultPackageID1, revision: 1,
		},
		{
			spawnID: agentWaitResultSpawnID2, agentID: agentWaitResultAgentID2,
			taskName: "result_wait_worker_2",
			threadID: agentWaitResultThreadID2, turnID: agentWaitResultTurnID2,
			packageID: agentWaitResultPackageID2, revision: 2,
		},
	}
	receipts := make([]store.AgentSpawnReceipt, 0, len(workers))
	snapshots := make([]protocol.WorkerLifecycleSnapshot, 0, len(workers))
	for index, worker := range workers {
		receipt, err := harness.registry.BeginAgentSpawn(
			context.Background(),
			store.AgentSpawnIntent{
				Source: root.Identity(), SpawnID: worker.spawnID, AgentID: worker.agentID,
				TargetDeviceID: brokerTestSecondDeviceID, TaskName: worker.taskName,
				PromptDigest: sha256.Sum256([]byte(worker.agentID)),
			},
			time.Unix(10+int64(worker.revision), 0),
		)
		if err != nil {
			t.Fatalf("begin worker %d (%s): %v", index, worker.taskName, err)
		}
		if _, err := harness.registry.MarkAgentSpawnStarted(
			context.Background(),
			store.AgentSpawnKey{
				ControllerID: root.ControllerID, TreeID: root.TreeID,
				SourceAgentID: root.AgentID, SpawnID: worker.spawnID,
			},
			time.Unix(20+int64(worker.revision), 0),
		); err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
		snapshots = append(snapshots, protocol.WorkerLifecycleSnapshot{
			TreeID: root.TreeID, AgentID: worker.agentID, Revision: worker.revision,
			Phase: protocol.WorkerLifecycleFinalizing, CodexThreadID: worker.threadID,
			ActiveTurnID: worker.turnID,
		})
	}
	response := writeAndRead(
		t,
		targetConnection,
		request(t, protocol.MethodSyncWorkerLifecycle, protocol.SyncWorkerLifecycleParams{
			BaseRevision: 0, ThroughRevision: 2, Complete: true, Workers: snapshots,
		}),
	)
	if response.Error != nil {
		t.Fatalf("worker lifecycle sync = %#v", response.Error)
	}
	for index, worker := range workers {
		manifest := protocol.ResultManifest{
			Version: protocol.ResultManifestVersion, PackageID: worker.packageID,
			ControllerID: root.ControllerID, TreeID: root.TreeID,
			SourceAgentID: worker.agentID, SourceDeviceID: brokerTestSecondDeviceID,
			ManagedThreadID: worker.threadID, TurnID: worker.turnID,
			LifecycleRevision: worker.revision,
			Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
			CapturedAt:        30 + int64(worker.revision),
			Rollout: protocol.ResultRolloutComponent{
				Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
			},
			Workspace: protocol.ResultWorkspaceComponent{
				Status:       protocol.ResultWorkspaceNotManaged,
				BaseWarnings: []string{}, ResultWarnings: []string{},
			},
			Parts: []protocol.ResultPackagePartDescriptor{},
		}
		manifestBytes, manifestDescriptor, err := protocol.EncodeResultManifest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := harness.registry.PublishResultPackage(
			context.Background(), brokerTestSecondDeviceID, receipts[index].Agent.Principal,
			protocol.PublishResultPackageParams{Metadata: protocol.ResultPackageMetadata{
				Manifest: manifestBytes, ManifestDescriptor: manifestDescriptor,
			}},
			time.Unix(40+int64(worker.revision), 0),
		); err != nil {
			t.Fatal(err)
		}
		delivered, err := harness.registry.MarkResultPackageDelivered(
			context.Background(), root.DeviceID, root.Identity(), worker.packageID,
			time.Unix(50+int64(worker.revision), 0),
		)
		if err != nil || delivered.Sequence != uint64(index+1) {
			t.Fatalf("deliver result package %d = %#v, error %v", index, delivered, err)
		}
	}

	first := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
			LifecycleCursor: 2,
			MessageLimit:    1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
		}, root,
	))
	if first.Error != nil {
		t.Fatalf("first result page error = %#v", first.Error)
	}
	firstResult := decodeResult[protocol.WaitAgentResult](t, first)
	if len(firstResult.Results) != 1 ||
		firstResult.Results[0].Manifest.PackageID != agentWaitResultPackageID1 ||
		firstResult.Results[0].Sequence != 1 ||
		firstResult.Results[0].Availability != protocol.ResultPackageUnverified ||
		firstResult.NextResultCursor != 1 || !firstResult.MoreResults ||
		firstResult.NextMailboxCursor != 0 || firstResult.NextLifecycleCursor != 2 ||
		firstResult.NextArtifactCursor != 0 {
		t.Fatalf("first result page = %#v", firstResult)
	}
	second := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
			LifecycleCursor: 2, ResultCursor: firstResult.NextResultCursor,
			MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
		}, root,
	))
	if second.Error != nil {
		t.Fatalf("second result page error = %#v", second.Error)
	}
	secondResult := decodeResult[protocol.WaitAgentResult](t, second)
	if len(secondResult.Results) != 1 ||
		secondResult.Results[0].Manifest.PackageID != agentWaitResultPackageID2 ||
		secondResult.Results[0].Sequence != 2 ||
		secondResult.Results[0].Availability != protocol.ResultPackageUnverified ||
		secondResult.NextResultCursor != 2 || secondResult.MoreResults ||
		secondResult.NextMailboxCursor != 0 || secondResult.NextLifecycleCursor != 2 ||
		secondResult.NextArtifactCursor != 0 {
		t.Fatalf("second result page = %#v", secondResult)
	}
	ahead := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
			LifecycleCursor: 2, ResultCursor: 3,
			MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
		}, root,
	))
	if ahead.Error == nil || ahead.Error.Code != protocol.ErrorConflict {
		t.Fatalf("ahead result cursor = %#v", ahead)
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
		ArtifactLimit: protocol.MaximumAgentWaitArtifacts,
		ResultLimit:   protocol.MaximumAgentWaitResults,
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
				CodexThreadID: lifecycleCodexThreadID, ActiveTurnID: lifecycleTurnID,
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
		ArtifactLimit: protocol.MaximumAgentWaitArtifacts,
		ResultLimit:   protocol.MaximumAgentWaitResults,
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
		protocol.WaitAgentParams{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
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
	artifact := lifecycle
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
		server.artifactNotifier.mu.Lock()
		artifactWatch := server.artifactNotifier.watches[artifact]
		artifactWaiters := 0
		if artifactWatch != nil {
			artifactWaiters = artifactWatch.waiters
		}
		server.artifactNotifier.mu.Unlock()
		server.resultNotifier.mu.Lock()
		resultWatch := server.resultNotifier.watches[artifact]
		resultWaiters := 0
		if resultWatch != nil {
			resultWaiters = resultWatch.waiters
		}
		server.resultNotifier.mu.Unlock()
		session.asyncMu.Lock()
		_, cancellable := session.asyncCancels[requestID]
		session.asyncMu.Unlock()
		if len(session.asyncSem) == 1 && mailboxWaiters == 1 && lifecycleWaiters == 1 &&
			artifactWaiters == 1 && resultWaiters == 1 && cancellable {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("agent wait did not enter all notifier paths")
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
	server.artifactNotifier.mu.Lock()
	artifactWatches := len(server.artifactNotifier.watches)
	server.artifactNotifier.mu.Unlock()
	server.resultNotifier.mu.Lock()
	resultWatches := len(server.resultNotifier.watches)
	server.resultNotifier.mu.Unlock()
	if len(session.asyncSem) != 0 || cancellable || mailboxWatches != 0 ||
		lifecycleWatches != 0 || artifactWatches != 0 || resultWatches != 0 {
		t.Fatalf(
			"agent wait cleanup = slots %d, cancellable %v, mailbox watches %d, lifecycle watches %d, artifact watches %d, result watches %d",
			len(session.asyncSem), cancellable, mailboxWatches, lifecycleWatches, artifactWatches, resultWatches,
		)
	}
}
