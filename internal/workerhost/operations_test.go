package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestLifecycleControlWireMatchesCodex01441(t *testing.T) {
	steer, err := json.Marshal(turnSteerParams{
		ThreadID: "thread-id", ClientUserMessageID: "message-id",
		Input:          []textInput{{Type: "text", Text: "hello", TextElements: []any{}}},
		ExpectedTurnID: "turn-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantSteer := `{"threadId":"thread-id","clientUserMessageId":"message-id","input":[{"type":"text","text":"hello","text_elements":[]}],"expectedTurnId":"turn-id"}`
	if string(steer) != wantSteer {
		t.Fatalf("turn/steer params = %s, want %s", steer, wantSteer)
	}
	interrupt, err := json.Marshal(turnInterruptParams{ThreadID: "thread-id", TurnID: "turn-id"})
	if err != nil {
		t.Fatal(err)
	}
	wantInterrupt := `{"threadId":"thread-id","turnId":"turn-id"}`
	if string(interrupt) != wantInterrupt {
		t.Fatalf("turn/interrupt params = %s, want %s", interrupt, wantInterrupt)
	}
}

func TestHostAppServerInheritsHostAuthenticationButWorkerShellExcludesIt(t *testing.T) {
	application := newFakeApplication()
	host, _, paths := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174465", "host-auth")
	for name, want := range map[string]string{
		"CODEX_ACCESS_TOKEN": "host-auth",
		"CODEX_API_KEY":      "ambient-codex-auth",
		"OPENAI_API_KEY":     "ambient-openai-auth",
	} {
		if got := paths.launchOptions.Environment[name]; got != want {
			t.Fatalf("managed app-server %s = %q, want inherited host value", name, got)
		}
		if slices.Contains(paths.launchOptions.UnsetEnvironment, name) {
			t.Fatalf("managed app-server unexpectedly unsets %s", name)
		}
	}
	config := application.snapshot().starts[0].Config
	policy, ok := config["shell_environment_policy"].(map[string]any)
	if !ok {
		t.Fatalf("managed shell policy = %#v", config["shell_environment_policy"])
	}
	excluded, ok := policy["exclude"].([]string)
	if !ok {
		t.Fatalf("managed shell exclusions = %#v", policy["exclude"])
	}
	for _, name := range hostAuthEnvironment {
		if !slices.Contains(excluded, name) {
			t.Fatalf("managed worker shell does not exclude %s: %#v", name, excluded)
		}
	}
	if started.Worker.Status != store.WorkerRunning {
		t.Fatalf("worker with inherited host auth = %#v", started.Worker)
	}
}

func TestHostSendSteersRunningWorkerAndReplaysReceipt(t *testing.T) {
	application := newFakeApplication()
	host, _, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174460", "steer")
	messageID := newTestID()
	request := SendRequest{Key: started.Worker.WorkerKey, MessageID: messageID, Message: "steer input"}
	result, err := host.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.OperationID != messageID || result.Receipt.Action != store.WorkerOperationSend ||
		result.Receipt.Status != store.WorkerOperationSucceeded ||
		result.Receipt.Outcome != store.WorkerOutcomeSteered ||
		result.Worker.ActiveTurnID != started.Worker.ActiveTurnID {
		t.Fatalf("steer result = %#v", result)
	}
	wantSteer := turnSteerParams{
		ThreadID: started.Worker.CodexThreadID, ClientUserMessageID: messageID,
		Input:          []textInput{{Type: "text", Text: request.Message, TextElements: []any{}}},
		ExpectedTurnID: started.Worker.ActiveTurnID,
	}
	record := application.snapshot()
	if len(record.steers) != 1 || !reflect.DeepEqual(record.steers[0], wantSteer) {
		t.Fatalf("turn/steer calls = %#v, want %#v", record.steers, wantSteer)
	}

	replayed, err := host.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != result {
		t.Fatalf("steer replay = %#v, want %#v", replayed, result)
	}
	if got := len(application.snapshot().steers); got != 1 {
		t.Fatalf("turn/steer calls after exact replay = %d, want 1", got)
	}
	request.Message = "changed input"
	if _, err := host.Send(context.Background(), request); !errors.Is(err, store.ErrWorkerOperationConflict) {
		t.Fatalf("changed send replay error = %v, want ErrWorkerOperationConflict", err)
	}
}

func TestHostSendQueuesWhenTurnCompletedBeforeSteer(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174461", "steer-race")
	application.turnSteerHook = func(steer turnSteerParams) {
		application.notifyCompletion(steer.ThreadID, steer.ExpectedTurnID, "completed")
	}
	application.turnSteerErr = &appserver.RPCError{Code: -32600, Message: "no active turn to steer"}
	result, err := host.Send(context.Background(), SendRequest{
		Key: started.Worker.WorkerKey, MessageID: newTestID(), Message: "queue after completion",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.Outcome != store.WorkerOutcomeQueued {
		t.Fatalf("completion-race send = %#v", result)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
}

func TestHostPendingSendReplayDoesNotRepeatAmbiguousSteer(t *testing.T) {
	application := newFakeApplication()
	responseLost := errors.New("turn/steer response lost")
	application.turnSteerErr = responseLost
	host, _, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174462", "steer-loss")
	request := SendRequest{
		Key: started.Worker.WorkerKey, MessageID: newTestID(), Message: "ambiguous steer",
	}
	result, err := host.Send(context.Background(), request)
	if !errors.Is(err, responseLost) {
		t.Fatalf("ambiguous send error = %v, want response loss", err)
	}
	if result.Receipt.Status != store.WorkerOperationPending ||
		result.Receipt.Outcome != store.WorkerOutcomePending ||
		result.Worker.Status != store.WorkerInterrupted {
		t.Fatalf("ambiguous send result = %#v", result)
	}
	replayed, err := host.Send(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Receipt != result.Receipt || replayed.Worker != result.Worker {
		t.Fatalf("pending replay = %#v, want %#v", replayed, result)
	}
	if got := len(application.snapshot().steers); got != 1 {
		t.Fatalf("turn/steer calls after pending replay = %d, want 1", got)
	}
}

func TestHostInterruptAcknowledgesBeforeCompletionAndReplays(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174463", "interrupt")
	request := InterruptRequest{OperationID: newTestID(), Key: started.Worker.WorkerKey}
	result, err := host.Interrupt(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.Outcome != store.WorkerOutcomeInterrupted ||
		result.Worker.Status != store.WorkerRunning {
		t.Fatalf("interrupt acknowledgement = %#v", result)
	}
	wantInterrupt := turnInterruptParams{
		ThreadID: started.Worker.CodexThreadID,
		TurnID:   started.Worker.ActiveTurnID,
	}
	record := application.snapshot()
	if len(record.interrupts) != 1 || record.interrupts[0] != wantInterrupt {
		t.Fatalf("turn/interrupt calls = %#v, want %#v", record.interrupts, wantInterrupt)
	}
	replayed, err := host.Interrupt(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != result || len(application.snapshot().interrupts) != 1 {
		t.Fatalf("interrupt replay = %#v, calls = %#v", replayed, application.snapshot().interrupts)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID,
		started.Worker.ActiveTurnID,
		"interrupted",
	)
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
}

func TestHostChangesCoalesceAroundGlobalWorkerRevision(t *testing.T) {
	application := newFakeApplication()
	host, _, _ := newTestHost(t, 1, application)
	select {
	case <-host.Changes():
		t.Fatal("empty startup emitted a worker change")
	default:
	}
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174464", "changes")
	select {
	case <-host.Changes():
	case <-time.After(time.Second):
		t.Fatal("spawn did not emit a coalesced worker change")
	}
	workers, err := host.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].Revision != host.WorkerRevision() {
		t.Fatalf("worker snapshot = %#v, high watermark = %d", workers, host.WorkerRevision())
	}
	beforeCompletion := workers[0].Revision
	application.notifyCompletion(
		started.Worker.CodexThreadID,
		started.Worker.ActiveTurnID,
		"completed",
	)
	select {
	case <-host.Changes():
	case <-time.After(time.Second):
		t.Fatal("completion did not emit a worker change")
	}
	workers, err = host.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if workers[0].Status != store.WorkerIdle || workers[0].Revision <= beforeCompletion ||
		workers[0].Revision != host.WorkerRevision() {
		t.Fatalf("completed snapshot = %#v, high watermark = %d", workers, host.WorkerRevision())
	}
}

func TestHostStartupRecoveryEmitsWorkerChange(t *testing.T) {
	var revisionBeforeRecovery uint64
	host, _, _ := newTestHostWithStateSetup(
		t,
		1,
		"",
		func(state *store.PeerStore, workspaceRoot string) {
			key := store.WorkerKey{
				ControllerID: testControllerID,
				TreeID:       testTreeID,
				AgentID:      "123e4567-e89b-42d3-a456-426614174466",
			}
			worker, err := state.ReserveWorker(
				context.Background(),
				store.WorkerReservation{
					WorkerKey: key, ParentAgentID: testParentID, DeviceID: testDeviceID,
					TaskName: "startup recovery", PromptDigest: promptDigest("startup recovery"),
					WorkspacePath:  filepath.Join(workspaceRoot, workspaceName(key)),
					ProfileVersion: workerProfileVersion,
				},
				1,
				time.Unix(100, 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			worker, err = state.BeginWorkerStart(
				context.Background(), worker.WorkerKey, 1, time.Unix(101, 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			worker, err = state.AttachWorkerThread(
				context.Background(), worker.WorkerKey, newTestID(), time.Unix(102, 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			worker, err = state.MarkWorkerReady(
				context.Background(), worker.WorkerKey, time.Unix(103, 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			worker, err = state.MarkWorkerRunning(
				context.Background(), worker.WorkerKey, newTestID(), time.Unix(104, 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			revisionBeforeRecovery = worker.Revision
		},
	)
	select {
	case <-host.Changes():
	case <-time.After(time.Second):
		t.Fatal("startup recovery did not emit a worker change")
	}
	workers, err := host.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].Status != store.WorkerInterrupted ||
		workers[0].Revision <= revisionBeforeRecovery || workers[0].Revision != host.WorkerRevision() {
		t.Fatalf(
			"startup recovery snapshot = %#v, previous revision = %d, high watermark = %d",
			workers,
			revisionBeforeRecovery,
			host.WorkerRevision(),
		)
	}
}
