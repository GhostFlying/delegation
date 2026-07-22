package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	testControllerID = "123e4567-e89b-42d3-a456-426614174400"
	testDeviceID     = "123e4567-e89b-42d3-a456-426614174401"
	testTreeID       = "123e4567-e89b-42d3-a456-426614174402"
	testParentID     = "123e4567-e89b-42d3-a456-426614174403"
)

func TestHostUsesOneAppServerAndEnforcesWorkerSlots(t *testing.T) {
	application := newFakeApplication()
	host, state, paths := newTestHost(t, 2, application)
	first := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174410", "first")
	second := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174411", "second")
	if first.Worker.Status != store.WorkerRunning || second.Worker.Status != store.WorkerRunning {
		t.Fatalf("spawned workers = %#v / %#v", first.Worker, second.Worker)
	}
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174412",
		ParentAgentID: testParentID, TaskName: "third", Prompt: "third prompt",
	})
	if !errors.Is(err, store.ErrWorkerBusy) {
		t.Fatalf("third Spawn() error = %v, want ErrWorkerBusy", err)
	}

	record := application.snapshot()
	if len(record.starts) != 2 || len(record.turns) != 2 || record.preflights != 2 {
		t.Fatalf("app-server calls = %#v", record)
	}
	assertManagedProfile(t, record.starts[0].Config, paths, first.Worker)
	if _, err := os.Stat(first.Worker.WorkspacePath); err != nil {
		t.Fatal(err)
	}
	workers, err := state.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 2 {
		t.Fatalf("stored workers = %#v", workers)
	}
}

func TestHostRejectsSpawnRetryWithDifferentPrompt(t *testing.T) {
	application := newFakeApplication()
	host, _, _ := newTestHost(t, 1, application)
	agentID := "123e4567-e89b-42d3-a456-426614174413"
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "digest", Prompt: "first prompt",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "digest", Prompt: "different prompt",
	}); !errors.Is(err, store.ErrWorkerReservationConflict) {
		t.Fatalf("changed prompt Spawn() error = %v, want reservation conflict", err)
	}
}

func TestHostBoundsSpawnAndFollowupPromptItems(t *testing.T) {
	host, _, _ := newTestHost(t, 1, newFakeApplication())
	oversized := strings.Repeat("x", maximumPromptBytes+1)
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174414",
		ParentAgentID: testParentID, TaskName: "oversized", Prompt: oversized,
	}); err == nil {
		t.Fatal("Spawn accepted an oversized model-visible item")
	}
	if _, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(),
		Key: store.WorkerKey{
			ControllerID: testControllerID,
			TreeID:       testTreeID,
			AgentID:      "123e4567-e89b-42d3-a456-426614174414",
		},
		Message: oversized,
	}); err == nil {
		t.Fatal("Followup accepted an oversized model-visible item")
	}
}

func TestHostCallerCancellationDoesNotRetireSharedAppServer(t *testing.T) {
	application := newFakeApplication()
	application.turnStartGate = make(chan struct{})
	application.turnStartStarted = make(chan struct{})
	host, _, _ := newTestHost(t, 1, application)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		started StartedTurn
		err     error
	}, 1)
	go func() {
		started, err := host.Spawn(ctx, SpawnRequest{
			TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174419",
			ParentAgentID: testParentID, TaskName: "detached", Prompt: "detached prompt",
		})
		done <- struct {
			started StartedTurn
			err     error
		}{started: started, err: err}
	}()
	select {
	case <-application.turnStartStarted:
	case <-time.After(time.Second):
		t.Fatal("turn/start did not begin")
	}
	cancel()
	select {
	case result := <-done:
		t.Fatalf("Spawn returned on caller cancellation: %#v, %v", result.started, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("caller cancellation retired shared app-server %d times", got)
	}
	close(application.turnStartGate)
	select {
	case result := <-done:
		if result.err != nil || result.started.Worker.Status != store.WorkerRunning {
			t.Fatalf("detached Spawn() = %#v, %v", result.started, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("detached Spawn did not finish")
	}
}

func TestHostSerializesEarlyCompletionAndColdResumesAfterCrash(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	host, state, paths := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174420", "fast")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	firstApplication.crash(errors.New("test app-server crash"))
	waitForClientRetirement(t, host, firstApplication)
	followupRequest := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "follow-up",
	}
	followup, err := host.Followup(context.Background(), followupRequest)
	if err != nil {
		t.Fatal(err)
	}
	if followup.Worker.Status != store.WorkerRunning ||
		followup.Worker.CodexThreadID != started.Worker.CodexThreadID ||
		followup.Receipt.Outcome != store.WorkerOutcomeStarted {
		t.Fatalf("follow-up worker = %#v", followup.Worker)
	}
	replayed, err := host.Followup(context.Background(), followupRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != followup {
		t.Fatalf("follow-up replay = %#v, want %#v", replayed, followup)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || record.resumes[0].ThreadID != started.Worker.CodexThreadID ||
		!record.resumes[0].ExcludeTurns || record.preflights != 1 || len(record.turns) != 1 {
		t.Fatalf("cold-resume calls = %#v", record)
	}
	assertManagedProfile(t, record.resumes[0].Config, paths, followup.Worker)
	if _, err := os.Stat(filepath.Join(paths.codexHome, "config.toml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed Codex config.toml exists: %v", err)
	}
}

func TestHostDrainsCompletionBeforeRecoveringClosedClient(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.crashAfterComplete = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174423", "drain-crash")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
}

func TestHostCompletionFencePrecedesRecovery(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	firstApplication.crashAfterComplete = true
	firstApplication.closeGate = make(chan struct{})
	firstApplication.closeStarted = make(chan struct{})
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	completionStarted := make(chan struct{})
	releaseCompletion := make(chan struct{})
	realApply := host.applyCompletion
	host.applyCompletion = func(completed turnCompletedNotification) error {
		close(completionStarted)
		<-releaseCompletion
		return realApply(completed)
	}
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174424", "fenced")
	select {
	case <-completionStarted:
	case <-time.After(time.Second):
		t.Fatal("completion processing did not start")
	}
	select {
	case <-firstApplication.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("client retirement did not reach Close")
	}
	close(releaseCompletion)
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	blockedContext, blockedCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, blockedErr := host.Followup(blockedContext, FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "while recovery is fenced",
	})
	blockedCancel()
	if !errors.Is(blockedErr, context.DeadlineExceeded) {
		t.Fatalf("Followup while recovery was fenced = %v, want deadline exceeded", blockedErr)
	}
	if got := secondApplication.snapshot(); len(got.resumes) != 0 {
		t.Fatalf("replacement app-server started before recovery was released: %#v", got)
	}
	close(firstApplication.closeGate)
	waitForClientRetirement(t, host, firstApplication)
	_, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "after fence",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerRunning)
}

func TestHostSpawnReturnsOnCallerDeadlineWhileRecoveryContinues(t *testing.T) {
	application := newFakeApplication()
	transportErr := errors.New("injected app-server transport failure")
	application.threadStartErr = transportErr
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, _, _ := newTestHost(t, 1, application)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err := host.Spawn(ctx, SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174425",
		ParentAgentID: testParentID, TaskName: "caller-deadline", Prompt: "caller deadline prompt",
	})
	cancel()
	if !errors.Is(err, transportErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Spawn() error = %v, want transport error and caller deadline", err)
	}
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("app-server recovery did not continue after caller deadline")
	}
	close(application.closeGate)
	waitForClientRetirement(t, host, application)
}

func TestHostRetriesDeferredCompletionWithoutConsumerDeadlock(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	firstAttempt := make(chan struct{})
	retryStarted := make(chan struct{})
	releaseRetry := make(chan struct{})
	realApply := host.applyCompletion
	var attempts atomic.Int32
	host.applyCompletion = func(completed turnCompletedNotification) error {
		switch attempts.Add(1) {
		case 1:
			close(firstAttempt)
			return errors.New("injected completion write failure")
		case 2:
			close(retryStarted)
			<-releaseRetry
			return realApply(completed)
		default:
			return realApply(completed)
		}
	}
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174425", "retry")
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("first completion attempt did not run")
	}
	select {
	case <-retryStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("deferred completion retry did not run")
	}
	followupDone := make(chan error, 1)
	go func() {
		_, err := host.Followup(context.Background(), FollowupRequest{
			OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "after retry",
		})
		followupDone <- err
	}()
	select {
	case err := <-followupDone:
		t.Fatalf("Followup returned while completion retry was blocked: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseRetry)
	select {
	case err := <-followupDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Followup remained blocked after completion retry")
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("completion attempts = %d, want at least 2", got)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerRunning)
}

func TestHostFailsClosedWhenAppServerExitIsUnconfirmed(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.closeErr = errors.Join(
		appserver.ErrCloseTimeout,
		appserver.ErrProcessExitUnconfirmed,
	)
	secondApplication := newFakeApplication()
	host, _, paths := newTestHost(t, 1, firstApplication, secondApplication)
	paths.allowCloseError.Store(true)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174426", "unconfirmed")
	firstApplication.crash(errors.New("test app-server crash"))
	select {
	case <-host.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("host did not fail after an unconfirmed app-server exit")
	}
	if !errors.Is(host.Err(), appserver.ErrProcessExitUnconfirmed) {
		t.Fatalf("host error = %v, want ErrProcessExitUnconfirmed", host.Err())
	}
	_, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "must not restart",
	})
	if !errors.Is(err, appserver.ErrProcessExitUnconfirmed) {
		t.Fatalf("Followup error = %v, want ErrProcessExitUnconfirmed", err)
	}
	if got := secondApplication.snapshot(); len(got.resumes) != 0 || len(got.starts) != 0 {
		t.Fatalf("replacement app-server started after unconfirmed exit: %#v", got)
	}
}

func TestHostCloseDrainsAcceptedCompletion(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174421", "drain")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatal(err)
	}
	worker, err := state.GetWorker(context.Background(), started.Worker.WorkerKey)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status != store.WorkerIdle {
		t.Fatalf("worker after drained Close() = %#v", worker)
	}
}

func TestHostCloseContinuesAfterCallerTimeout(t *testing.T) {
	application := newFakeApplication()
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, _, _ := newTestHost(t, 1, application)
	spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174422", "close-timeout")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := host.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() error = %v, want deadline exceeded", err)
	}
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("managed app-server Close did not start")
	}
	close(application.closeGate)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestHostFailsClosedWhenWorkerMCPInventoryIsWrong(t *testing.T) {
	tests := map[string]func(*fakeApplication){
		"unexpected tool": func(application *fakeApplication) {
			application.tools = []string{"send_message", "spawn_agent", "wait_agent"}
		},
		"extra server": func(application *fakeApplication) {
			application.extraServers = []mcpServerStatus{{Name: "delegation"}}
		},
		"wrong auth": func(application *fakeApplication) {
			application.authStatus = "authenticated"
		},
		"resource": func(application *fakeApplication) {
			application.resources = []json.RawMessage{json.RawMessage(`{"uri":"file:///unexpected"}`)}
		},
		"resource template": func(application *fakeApplication) {
			application.resourceTemplates = []json.RawMessage{json.RawMessage(`{"uriTemplate":"file:///{path}"}`)}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			blockedApplication := newFakeApplication()
			mutate(blockedApplication)
			cleanApplication := newFakeApplication()
			host, state, _ := newTestHost(t, 1, blockedApplication, cleanApplication)
			started, err := host.Spawn(context.Background(), SpawnRequest{
				TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174430",
				ParentAgentID: testParentID, TaskName: "blocked", Prompt: "blocked prompt",
			})
			if !errors.Is(err, ErrMCPInjectionBlocked) {
				t.Fatalf("blocked Spawn() error = %v, want ErrMCPInjectionBlocked", err)
			}
			failed, err := state.GetWorker(context.Background(), started.Worker.WorkerKey)
			if err != nil {
				t.Fatal(err)
			}
			if failed.Status != store.WorkerFailed || failed.FailureCode != "mcp_injection_blocked" {
				t.Fatalf("failed worker = %#v", failed)
			}
			if got := blockedApplication.closeCount(); got != 1 {
				t.Fatalf("blocked app-server Close calls = %d, want 1", got)
			}
			if _, err := host.Spawn(context.Background(), SpawnRequest{
				TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174431",
				ParentAgentID: testParentID, TaskName: "replacement", Prompt: "replacement prompt",
			}); err != nil {
				t.Fatalf("clean replacement Spawn() error = %v", err)
			}
			if got := cleanApplication.snapshot(); len(got.starts) != 1 || got.preflights != 1 || len(got.turns) != 1 {
				t.Fatalf("clean replacement calls = %#v", got)
			}
		})
	}
}

func TestHostInterruptsAmbiguousInitialTurnForExplicitFollowup(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.turnStartErr = context.DeadlineExceeded
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174435",
		ParentAgentID: testParentID, TaskName: "ambiguous", Prompt: "must not be dropped",
	}
	started, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Spawn() error = %v, want deadline exceeded", err)
	}
	interrupted := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.FailureCode != "turn_start_interrupted" || interrupted.ActiveTurnID != "" {
		t.Fatalf("interrupted worker = %#v", interrupted)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, ErrWorkerInterrupted) {
		t.Fatalf("retry Spawn() error = %v, want ErrWorkerInterrupted", err)
	}
	if got := len(firstApplication.snapshot().turns); got != 1 {
		t.Fatalf("turn/start calls = %d, want 1", got)
	}
	followup, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: interrupted.WorkerKey,
		Message: "continue only after explicit confirmation",
	})
	if err != nil || followup.Worker.Status != store.WorkerRunning {
		t.Fatalf("explicit Followup() = %#v, %v", followup, err)
	}
}

func TestHostFailsClosedAfterAmbiguousThreadStart(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadStartErr = context.DeadlineExceeded
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174448",
		ParentAgentID: testParentID, TaskName: "ambiguous thread", Prompt: "must run once",
	}
	started, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Spawn() error = %v, want deadline exceeded", err)
	}
	failed := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFailed)
	if failed.FailureCode != "thread_start_ambiguous" || failed.CodexThreadID != "" {
		t.Fatalf("ambiguous thread worker = %#v", failed)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, ErrWorkerFailed) {
		t.Fatalf("retry Spawn() error = %v, want ErrWorkerFailed", err)
	}
	if got := len(firstApplication.snapshot().starts); got != 1 {
		t.Fatalf("first app-server thread/start calls = %d, want 1", got)
	}
	if got := len(secondApplication.snapshot().starts); got != 0 {
		t.Fatalf("replacement app-server thread/start calls = %d, want 0", got)
	}
}

func TestHostPersistsFailedTurnOutcome(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.completionStatus = "failed"
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174436", "failed-turn")
	failed := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFailed)
	if failed.FailureCode != "turn_failed" {
		t.Fatalf("failed worker = %#v", failed)
	}
}

func TestHostContainsUnknownTurnStatusToWorker(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.completionStatus = "future-status"
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174437", "unknown-status")
	failed := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFailed)
	if failed.FailureCode != "unsupported_turn_status" {
		t.Fatalf("failed worker = %#v", failed)
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unknown turn status retired shared app-server %d times", got)
	}
	application.completeBeforeReturn = false
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174438",
		ParentAgentID: testParentID, TaskName: "after unknown status", Prompt: "after unknown status",
	}); err != nil {
		t.Fatalf("Spawn after unknown status error = %v", err)
	}
}

func TestHostDoesNotRetireSharedAppServerForUnsentRequest(t *testing.T) {
	application := newFakeApplication()
	application.threadStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174439",
		ParentAgentID: testParentID, TaskName: "canceled", Prompt: "canceled",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Spawn() error = %v", err)
	}
	restored := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if restored.CodexThreadID != "" || restored.FailureCode != "" {
		t.Fatalf("restored worker = %#v", restored)
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unsent request retired shared app-server %d times", got)
	}
	application.threadStartErr = nil
	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry Spawn after unsent request = %#v, %v", retried, err)
	}
	if got := len(application.snapshot().starts); got != 2 {
		t.Fatalf("thread/start calls = %d, want 2", got)
	}
}

func TestHostUnsentThreadStartDoesNotRetainSlot(t *testing.T) {
	application := newFakeApplication()
	application.threadStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	first, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174446",
		ParentAgentID: testParentID, TaskName: "abandoned start", Prompt: "abandoned start",
	})
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("first Spawn() error = %v", err)
	}
	waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	application.threadStartErr = nil
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174447",
		ParentAgentID: testParentID, TaskName: "replacement", Prompt: "replacement",
	}); err != nil {
		t.Fatalf("replacement Spawn() while first worker is pending = %v", err)
	}
}

func TestHostRetriesInitialTurnWhenRequestWasNotWritten(t *testing.T) {
	application := newFakeApplication()
	application.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417443c",
		ParentAgentID: testParentID, TaskName: "unsent initial turn", Prompt: "unsent initial turn",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("initial Spawn() error = %v", err)
	}
	pending := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if pending.CodexThreadID == "" {
		t.Fatalf("pending worker = %#v", pending)
	}
	application.turnStartErr = nil
	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry initial Spawn() = %#v, %v", retried, err)
	}
	record := application.snapshot()
	if len(record.starts) != 1 || len(record.turns) != 2 || record.preflights != 2 {
		t.Fatalf("retried initial calls = %#v", record)
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unsent initial turn retired shared app-server %d times", got)
	}
}

func TestHostRetriesInitialPreflightWhenRequestWasNotWritten(t *testing.T) {
	application := newFakeApplication()
	application.mcpStatusErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417444b",
		ParentAgentID: testParentID, TaskName: "unsent preflight", Prompt: "unsent preflight",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("initial Spawn() error = %v", err)
	}
	pending := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if pending.CodexThreadID == "" {
		t.Fatalf("pending worker = %#v", pending)
	}
	application.mcpStatusErr = nil
	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry initial Spawn() = %#v, %v", retried, err)
	}
	record := application.snapshot()
	if len(record.starts) != 1 || len(record.resumes) != 1 ||
		!record.resumes[0].ExcludeTurns || record.preflights != 2 || len(record.turns) != 1 {
		t.Fatalf("retried preflight calls = %#v", record)
	}
}

func TestHostUnsentInitialTurnDoesNotRetainSlot(t *testing.T) {
	application := newFakeApplication()
	application.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	first, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174448",
		ParentAgentID: testParentID, TaskName: "abandoned turn", Prompt: "abandoned turn",
	})
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("first Spawn() error = %v", err)
	}
	pending := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if pending.CodexThreadID == "" {
		t.Fatalf("pending worker = %#v", pending)
	}
	application.turnStartErr = nil
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174449",
		ParentAgentID: testParentID, TaskName: "replacement", Prompt: "replacement",
	}); err != nil {
		t.Fatalf("replacement Spawn() while first worker is pending = %v", err)
	}
}

func TestHostColdResumesPendingInitialTurnAfterAppServerRestart(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417444a",
		ParentAgentID: testParentID, TaskName: "cold pending", Prompt: "cold pending",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("first Spawn() error = %v", err)
	}
	waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	firstApplication.crash(errors.New("force cold pending retry"))
	waitForClientRetirement(t, host, firstApplication)

	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("cold pending retry = %#v, %v", retried, err)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || len(record.starts) != 0 || record.preflights != 1 || len(record.turns) != 1 {
		t.Fatalf("cold pending retry calls = %#v", record)
	}
}

func TestHostRecoversRunningWorkerWhenAppServerDies(t *testing.T) {
	firstApplication := newFakeApplication()
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174440", "running")
	firstApplication.crash(errors.New("lost process"))
	interrupted := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.ActiveTurnID == "" || interrupted.FailureCode != "turn_interrupted" {
		t.Fatalf("interrupted worker = %#v", interrupted)
	}
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: started.Worker.AgentID, ParentAgentID: testParentID,
		TaskName: "running", Prompt: "running prompt",
	}); !errors.Is(err, ErrWorkerInterrupted) {
		t.Fatalf("interrupted Spawn retry error = %v, want ErrWorkerInterrupted", err)
	}
	resumed, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "resume after loss",
	})
	if err != nil || resumed.Worker.Status != store.WorkerRunning {
		t.Fatalf("Followup() = %#v, %v", resumed, err)
	}
}

func TestHostKeepsColdResumeRetryableAfterInterruption(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	interruptedResume := newFakeApplication()
	interruptedResume.resumeErr = context.DeadlineExceeded
	retryApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, interruptedResume, retryApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174442", "resume")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.crash(errors.New("force cold resume"))
	waitForClientRetirement(t, host, firstApplication)

	_, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "interrupted follow-up",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("interrupted Followup() error = %v, want deadline exceeded", err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	retried, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "retry follow-up",
	})
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry Followup() = %#v, %v", retried, err)
	}
	if got := len(retryApplication.snapshot().resumes); got != 1 {
		t.Fatalf("retry resume calls = %d, want 1", got)
	}
}

func TestHostRetriesUnsentColdResumeOnSameAppServer(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	secondApplication.resumeErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417443a", "unsent-resume")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.crash(errors.New("force cold resume"))
	waitForClientRetirement(t, host, firstApplication)

	request := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "retry cold resume",
	}
	if _, err := host.Followup(context.Background(), request); !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("unsent cold Followup() error = %v", err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	if got := secondApplication.closeCount(); got != 0 {
		t.Fatalf("unsent cold resume retired shared app-server %d times", got)
	}
	secondApplication.resumeErr = nil
	request.OperationID = newTestID()
	retried, err := host.Followup(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry cold Followup() = %#v, %v", retried, err)
	}
	if got := len(secondApplication.snapshot().resumes); got != 2 {
		t.Fatalf("thread/resume calls = %d, want 2", got)
	}
}

func TestHostRetriesUnsentLoadedTurnOnSameAppServer(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417443b", "unsent-turn")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	application.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)

	request := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "retry loaded turn",
	}
	if _, err := host.Followup(context.Background(), request); !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("unsent loaded Followup() error = %v", err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unsent loaded turn retired shared app-server %d times", got)
	}
	application.turnStartErr = nil
	request.OperationID = newTestID()
	retried, err := host.Followup(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry loaded Followup() = %#v, %v", retried, err)
	}
	if got := len(application.snapshot().turns); got != 3 {
		t.Fatalf("turn/start calls = %d, want 3", got)
	}
}

func TestHostRecoversRunningWorkerWhenThreadCloses(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174443", "thread-closed")
	application.notifyThreadClosed(started.Worker.CodexThreadID)
	interrupted := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.ActiveTurnID == "" || interrupted.FailureCode != "turn_interrupted" {
		t.Fatalf("thread-closed worker = %#v", interrupted)
	}
}

func TestHostPreservesOtherWorkerCompletionWhileRetiringClient(t *testing.T) {
	application := newFakeApplication()
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, state, _ := newTestHost(t, 2, application)
	first := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417444a", "closed-thread")
	second := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417444b", "completed-thread")

	application.notifyThreadClosed(first.Worker.CodexThreadID)
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("client retirement did not start")
	}
	application.notifyCompletion(
		second.Worker.CodexThreadID,
		second.Worker.ActiveTurnID,
		"completed",
	)
	close(application.closeGate)

	interrupted := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.ActiveTurnID == "" || interrupted.FailureCode != "turn_interrupted" {
		t.Fatalf("thread-closed worker = %#v", interrupted)
	}
	completed := waitWorkerStatus(t, state, second.Worker.WorkerKey, store.WorkerIdle)
	if completed.ActiveTurnID != "" || completed.FailureCode != "" {
		t.Fatalf("completed worker = %#v", completed)
	}
}

func TestHostColdResumesIdleWorkerWhenThreadCloses(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174444", "idle-thread-closed")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.notifyThreadClosed(started.Worker.CodexThreadID)
	waitForClientRetirement(t, host, firstApplication)

	resumed, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey,
		Message: "cold resume closed idle thread",
	})
	if err != nil || resumed.Worker.Status != store.WorkerRunning {
		t.Fatalf("Followup() = %#v, %v", resumed, err)
	}
	if got := len(secondApplication.snapshot().resumes); got != 1 {
		t.Fatalf("cold resume count = %d, want 1", got)
	}
}

func TestHostStartFailureDoesNotReserveWorkerSlot(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, nil, application)
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174445",
		ParentAgentID: testParentID, TaskName: "start-failure", Prompt: "start failure",
	}); err == nil {
		t.Fatal("first Spawn() unexpectedly succeeded")
	}
	workers, err := state.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 0 {
		t.Fatalf("app-server start failure reserved workers: %#v", workers)
	}
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174446",
		ParentAgentID: testParentID, TaskName: "start-retry", Prompt: "start retry",
	}); err != nil {
		t.Fatalf("second Spawn() error = %v", err)
	}
}

func TestHostFailsClosedWhenWorkerFailureCannotBePersisted(t *testing.T) {
	application := newFakeApplication()
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174447",
		ParentAgentID: testParentID, TaskName: "persistence-failure", Prompt: "persistence failure",
	}
	key := store.WorkerKey{
		ControllerID: testControllerID,
		TreeID:       request.TreeID,
		AgentID:      request.AgentID,
	}
	host, state, paths := newTestHost(t, 1, application)
	paths.allowCloseError.Store(true)
	application.threadStartHook = func() error {
		_, err := state.FailWorker(context.Background(), key, "injected_failure", time.Now())
		return err
	}
	rpcFailure := &appserver.RPCError{Code: -32000, Message: "test thread failure"}
	application.threadStartErr = rpcFailure

	_, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, rpcFailure) || !errors.Is(err, store.ErrWorkerTransition) ||
		!strings.Contains(err.Error(), "record worker failure") {
		t.Fatalf("Spawn() error = %v, want RPC and persistence failures", err)
	}
	select {
	case <-host.Done():
	case <-time.After(time.Second):
		t.Fatal("host did not fail closed after losing authoritative worker state")
	}
	if fatal := host.Err(); !errors.Is(fatal, rpcFailure) || !errors.Is(fatal, store.ErrWorkerTransition) {
		t.Fatalf("host Err() = %v, want combined terminal error", fatal)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, rpcFailure) ||
		!errors.Is(err, store.ErrWorkerTransition) {
		t.Fatalf("second Spawn() error = %v, want terminal host error", err)
	}
	if got := len(application.snapshot().starts); got != 1 {
		t.Fatalf("thread/start calls = %d, want 1", got)
	}
}

func TestHostCloseOverlappingAppServerStartLeavesNoReservation(t *testing.T) {
	application := newFakeApplication()
	application.startGate = make(chan struct{})
	application.startStarted = make(chan struct{})
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174448",
		ParentAgentID: testParentID, TaskName: "closing", Prompt: "closing prompt",
	}
	spawnDone := make(chan error, 1)
	go func() {
		_, err := host.Spawn(context.Background(), request)
		spawnDone <- err
	}()
	select {
	case <-application.startStarted:
	case <-time.After(time.Second):
		t.Fatal("app-server start did not begin")
	}
	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closeDone <- host.Close(ctx)
	}()
	waitHostClosed(t, host)
	close(application.startGate)
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("unclaimed app-server cleanup did not start")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Host Close returned before unclaimed app-server cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case err := <-spawnDone:
		t.Fatalf("Spawn returned before unclaimed app-server cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(application.closeGate)
	select {
	case err := <-spawnDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("overlapping Spawn() error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping Spawn did not return")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	workers, err := state.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 0 {
		t.Fatalf("overlapping Close reserved workers: %#v", workers)
	}
	if got := application.closeCount(); got != 1 {
		t.Fatalf("app-server Close calls = %d, want 1", got)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Spawn() error = %v, want ErrClosed", err)
	}
}

func TestHostCloseOverlappingAppServerStartReportsUnconfirmedExit(t *testing.T) {
	application := newFakeApplication()
	application.startGate = make(chan struct{})
	application.startStarted = make(chan struct{})
	application.closeErr = errors.Join(
		appserver.ErrCloseTimeout,
		appserver.ErrProcessExitUnconfirmed,
	)
	host, _, paths := newTestHost(t, 1, application)
	paths.allowCloseError.Store(true)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174449",
		ParentAgentID: testParentID, TaskName: "unconfirmed-start", Prompt: "unconfirmed start",
	}
	spawnDone := make(chan error, 1)
	go func() {
		_, err := host.Spawn(context.Background(), request)
		spawnDone <- err
	}()
	select {
	case <-application.startStarted:
	case <-time.After(time.Second):
		t.Fatal("app-server start did not begin")
	}
	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closeDone <- host.Close(ctx)
	}()
	waitHostClosed(t, host)
	close(application.startGate)
	select {
	case err := <-spawnDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("overlapping Spawn() error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping Spawn did not return")
	}
	select {
	case err := <-closeDone:
		if !errors.Is(err, appserver.ErrProcessExitUnconfirmed) {
			t.Fatalf("Close() error = %v, want ErrProcessExitUnconfirmed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	if !errors.Is(host.Err(), appserver.ErrProcessExitUnconfirmed) {
		t.Fatalf("host error = %v, want ErrProcessExitUnconfirmed", host.Err())
	}
}

func TestHostRejectsWorkspaceRootAlias(t *testing.T) {
	root := t.TempDir()
	actualRoot := filepath.Join(root, "actual")
	if err := os.Mkdir(actualRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasRoot := filepath.Join(root, "alias")
	if err := os.Symlink(actualRoot, aliasRoot); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}

	if err := config.ValidatePrivateDirectory(aliasRoot); err == nil {
		t.Fatal("private directory validation accepted a symbolic-link workspace root")
	}
}

func TestHostRejectsStoredWorkerAuthorityDrift(t *testing.T) {
	tests := map[string]func(*store.WorkerReservation){
		"workspace root": func(worker *store.WorkerReservation) {
			worker.WorkspacePath = filepath.Join(t.TempDir(), "stale-workspace")
		},
		"profile version": func(worker *store.WorkerReservation) {
			worker.ProfileVersion = workerProfileVersion + 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			workspaceRoot := filepath.Join(root, "workspaces")
			if err := os.Mkdir(workspaceRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			anchored, err := os.OpenRoot(workspaceRoot)
			if err != nil {
				t.Fatal(err)
			}
			defer anchored.Close()
			state, err := store.OpenPeer(ctx, filepath.Join(root, "state", "peer.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			worker := store.WorkerReservation{
				WorkerKey: store.WorkerKey{
					ControllerID: testControllerID,
					TreeID:       testTreeID,
					AgentID:      "123e4567-e89b-42d3-a456-42661417443d",
				},
				ParentAgentID: testParentID,
				DeviceID:      testDeviceID,
				TaskName:      "stored authority",
				PromptDigest:  promptDigest("stored authority"),
				WorkspacePath: filepath.Join(
					workspaceRoot,
					testTreeID+"-123e4567-e89b-42d3-a456-42661417443d",
				),
				ProfileVersion: workerProfileVersion,
			}
			mutate(&worker)
			if _, err := state.ReserveWorker(ctx, worker, 1, time.Unix(1_700_000_000, 0)); err != nil {
				t.Fatal(err)
			}
			host := &Host{
				controllerID:  testControllerID,
				deviceID:      testDeviceID,
				workspaceRoot: anchored,
				state:         state,
			}
			if err := host.validateStoredAuthority(ctx); err == nil {
				t.Fatal("validateStoredAuthority accepted drifted worker state")
			}
		})
	}
}

func TestHostRejectsManagedCodexConfigurationBeforeLaunch(t *testing.T) {
	application := newFakeApplication()
	host, _, paths := newTestHost(t, 1, application)
	configPath := filepath.Join(paths.codexHome, "config.toml")
	if err := os.WriteFile(configPath, []byte("model = \"ambient\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(configPath) })
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174441",
		ParentAgentID: testParentID, TaskName: "blocked config", Prompt: "blocked config",
	})
	if err == nil || !strings.Contains(err.Error(), "config.toml") {
		t.Fatalf("Spawn() error = %v", err)
	}
	if got := application.snapshot(); len(got.starts) != 0 {
		t.Fatalf("app-server started with managed config present: %#v", got)
	}
}

func TestManagedProfileUsesPlatformPermissionBoundary(t *testing.T) {
	host, _, paths := newTestHost(t, 1)
	worker := store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID,
			TreeID:       testTreeID,
			AgentID:      "123e4567-e89b-42d3-a456-426614174443",
		},
		ParentAgentID: testParentID,
		WorkspacePath: filepath.Join(filepath.Dir(paths.codexHome), "managed-worker"),
	}
	config := host.managedConfig(worker)
	if runtime.GOOS == "windows" {
		if config["default_permissions"] != windowsWorkerProfile {
			t.Fatalf("managed Windows permissions = %#v", config["default_permissions"])
		}
		projects, ok := config["projects"].(map[string]any)
		if !ok {
			t.Fatalf("managed Windows projects = %#v", config["projects"])
		}
		project, ok := projects[worker.WorkspacePath].(map[string]any)
		if !ok || project["trust_level"] != "untrusted" {
			t.Fatalf("managed Windows workspace trust = %#v", projects)
		}
		if _, found := config["permissions."+workerPermissionProfile]; found {
			t.Fatalf("managed Windows config retains an unenforceable restricted profile: %#v", config)
		}
		return
	}
	filesystem := managedFilesystemPermissions(t, config)
	if filepath.Dir(paths.configPath) != filepath.Dir(paths.codexBinary) {
		t.Fatal("test fixture does not co-locate the peer config and Codex binary")
	}
	for _, directory := range []string{filepath.Dir(paths.codexBinary), filepath.Dir(host.codexBinary)} {
		if _, found := filesystem[directory]; found {
			t.Fatalf("managed profile grants the Codex binary directory %q: %#v", directory, filesystem)
		}
	}
	if _, found := filesystem[paths.configPath]; found {
		t.Fatalf("managed profile grants the co-located peer config: %#v", filesystem)
	}
	assertCodexRuntimeFilesystemPermission(t, filesystem, host.codexBinary)
}

type testHostPaths struct {
	configPath              string
	delegationBinary        string
	codexBinary             string
	codexHome               string
	providerEnvironmentFile string
	launchOptions           *appserver.Options
	allowCloseError         *atomic.Bool
}

func newTestHost(
	t *testing.T,
	maxSlots int,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostWithWorkspaceRoot(t, maxSlots, "", applications...)
}

func newTestHostWithWorkspaceRoot(
	t *testing.T,
	maxSlots int,
	workspaceRoot string,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostWithStateSetup(t, maxSlots, workspaceRoot, nil, applications...)
}

func newTestHostWithStateSetup(
	t *testing.T,
	maxSlots int,
	workspaceRoot string,
	setup func(*store.PeerStore, string),
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	t.Helper()
	root := t.TempDir()
	paths := testHostPaths{
		configPath: filepath.Join(root, "peer.json"), delegationBinary: filepath.Join(root, "delegation"),
		codexBinary: filepath.Join(root, "codex"), codexHome: filepath.Join(root, "codex-home"),
		providerEnvironmentFile: filepath.Join(root, "peer.env"),
		launchOptions:           &appserver.Options{}, allowCloseError: &atomic.Bool{},
	}
	for _, path := range []string{
		paths.configPath,
		paths.delegationBinary,
		paths.codexBinary,
		paths.providerEnvironmentFile,
	} {
		if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if workspaceRoot == "" {
		workspaceRoot = filepath.Join(root, "workspaces")
	}
	for _, path := range []string{paths.codexHome, workspaceRoot} {
		if err := config.PreparePrivateDirectory(path); err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.OpenPeer(context.Background(), filepath.Join(root, "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(state, workspaceRoot)
	}
	var factoryMu sync.Mutex
	applicationIndex := 0
	host, err := New(context.Background(), Options{
		ControllerID: testControllerID, DeviceID: testDeviceID,
		PeerConfigPath: paths.configPath, DelegationBinary: paths.delegationBinary,
		CodexBinary: paths.codexBinary, CodexHome: paths.codexHome,
		CodexEnvironment: map[string]string{
			"CODEX_ACCESS_TOKEN":  "host-auth",
			"CODEX_API_KEY":       "ambient-codex-auth",
			"OPENAI_API_KEY":      "ambient-openai-auth",
			"CODEX_SQLITE_HOME":   filepath.Join(root, "ambient-sqlite"),
			"TEST_PROVIDER_VALUE": "provider-auth",
		},
		CodexUnsetEnvironment:   []string{"CODEX_MANAGED_BY_NPM"},
		ProviderEnvironmentFile: paths.providerEnvironmentFile,
		WorkspaceRoot:           workspaceRoot, MaxWorkerSlots: maxSlots,
		CodexConfig: map[string]any{
			"model":          "test-model",
			"model_provider": "test",
			"model_providers.test": map[string]any{
				"name": "Test provider", "base_url": "https://example.test/v1",
				"env_key": "TEST_PROVIDER_VALUE", "requires_openai_auth": false,
			},
		},
		Store: state,
		startApplication: func(ctx context.Context, options appserver.Options) (application, error) {
			factoryMu.Lock()
			defer factoryMu.Unlock()
			*paths.launchOptions = options
			if applicationIndex >= len(applications) {
				return nil, errors.New("unexpected app-server restart")
			}
			application := applications[applicationIndex]
			applicationIndex++
			if application == nil {
				return nil, errors.New("test app-server start failure")
			}
			if err := application.awaitStart(ctx); err != nil {
				return nil, err
			}
			return application, nil
		},
	})
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := host.Close(ctx); err != nil && !paths.allowCloseError.Load() {
			t.Errorf("close host: %v", err)
		}
		cancel()
		if err := state.Close(); err != nil {
			t.Errorf("close peer state: %v", err)
		}
	})
	return host, state, paths
}

func spawnTestWorker(t *testing.T, host *Host, agentID, name string) StartedTurn {
	t.Helper()
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: name, Prompt: name + " prompt",
	})
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func assertManagedProfile(
	t *testing.T,
	config map[string]any,
	paths testHostPaths,
	worker store.WorkerReservation,
) {
	t.Helper()
	for _, key := range []string{
		"features.plugins", "features.multi_agent", "features.multi_agent_v2", "features.enable_fanout",
		rootPluginEnabledConfig,
	} {
		if config[key] != false {
			t.Fatalf("managed config %s = %#v", key, config[key])
		}
	}
	if config["model"] != "test-model" {
		t.Fatalf("provider config = %#v", config)
	}
	filesystem := map[string]any{
		":minimal": "read", ":workspace_roots": map[string]any{".": "read"},
	}
	resolvedProviderEnvironmentFile, err := filepath.EvalSymlinks(paths.providerEnvironmentFile)
	if err != nil {
		t.Fatal(err)
	}
	filesystem[resolvedProviderEnvironmentFile] = "deny"
	resolvedCodexBinary, err := filepath.EvalSymlinks(paths.codexBinary)
	if err != nil {
		t.Fatal(err)
	}
	addCodexRuntimeFilesystemPermission(filesystem, resolvedCodexBinary)
	if runtime.GOOS == "windows" {
		if config["default_permissions"] != windowsWorkerProfile {
			t.Fatalf("default permissions = %#v", config["default_permissions"])
		}
		if _, found := config["permissions."+workerPermissionProfile]; found {
			t.Fatalf("managed Windows config retains a restricted profile: %#v", config)
		}
	} else {
		if config["default_permissions"] != workerPermissionProfile {
			t.Fatalf("default permissions = %#v", config["default_permissions"])
		}
		wantPermissions := map[string]any{
			"filesystem": filesystem,
		}
		if !reflect.DeepEqual(config["permissions."+workerPermissionProfile], wantPermissions) {
			t.Fatalf("worker permissions = %#v, want %#v", config["permissions."+workerPermissionProfile], wantPermissions)
		}
	}
	wantShellEnvironment := map[string]any{
		"inherit": "core", "ignore_default_excludes": false,
		"exclude": []string{
			"CODEX_ACCESS_TOKEN", "CODEX_API_KEY", "DELEGATION_CODEX_CONFIG_JSON",
			"OPENAI_API_KEY", "TEST_PROVIDER_VALUE",
		},
	}
	if !reflect.DeepEqual(config["shell_environment_policy"], wantShellEnvironment) {
		t.Fatalf("shell environment policy = %#v, want %#v", config["shell_environment_policy"], wantShellEnvironment)
	}
	wantMCP := map[string]any{
		"command": paths.delegationBinary,
		"args": []string{
			"mcp", "worker", "--config", paths.configPath,
			"--tree-id", worker.TreeID, "--agent-id", worker.AgentID,
			"--parent-agent-id", worker.ParentAgentID,
		},
		"required": true, "startup_timeout_sec": workerMCPTimeout,
	}
	if !reflect.DeepEqual(config["mcp_servers."+workerServerName], wantMCP) {
		t.Fatalf("worker MCP config = %#v, want %#v", config["mcp_servers."+workerServerName], wantMCP)
	}
	if paths.launchOptions.Environment["CODEX_ACCESS_TOKEN"] != "host-auth" ||
		paths.launchOptions.Environment["CODEX_API_KEY"] != "ambient-codex-auth" ||
		paths.launchOptions.Environment["OPENAI_API_KEY"] != "ambient-openai-auth" ||
		paths.launchOptions.Environment["TEST_PROVIDER_VALUE"] != "provider-auth" {
		t.Fatalf("managed app-server environment = %#v", paths.launchOptions.Environment)
	}
	if paths.launchOptions.SupervisorBinary != paths.delegationBinary {
		t.Fatalf("managed app-server supervisor = %q, want %q", paths.launchOptions.SupervisorBinary, paths.delegationBinary)
	}
	if _, found := paths.launchOptions.Environment["CODEX_SQLITE_HOME"]; found {
		t.Fatalf("managed app-server inherited CODEX_SQLITE_HOME: %#v", paths.launchOptions.Environment)
	}
	if !slices.Contains(paths.launchOptions.UnsetEnvironment, codexconfig.EnvironmentVariable) ||
		!slices.Contains(paths.launchOptions.UnsetEnvironment, "CODEX_SQLITE_HOME") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "CODEX_ACCESS_TOKEN") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "CODEX_API_KEY") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "OPENAI_API_KEY") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "TEST_PROVIDER_VALUE") {
		t.Fatalf("managed app-server unset environment = %#v", paths.launchOptions.UnsetEnvironment)
	}
}

func managedFilesystemPermissions(t *testing.T, config map[string]any) map[string]any {
	t.Helper()
	permissions, ok := config["permissions."+workerPermissionProfile].(map[string]any)
	if !ok {
		t.Fatalf("managed permissions = %#v", config["permissions."+workerPermissionProfile])
	}
	filesystem, ok := permissions["filesystem"].(map[string]any)
	if !ok {
		t.Fatalf("managed filesystem permissions = %#v", permissions["filesystem"])
	}
	return filesystem
}

func waitWorkerStatus(
	t *testing.T,
	state *store.PeerStore,
	key store.WorkerKey,
	status store.WorkerStatus,
) store.WorkerReservation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		worker, err := state.GetWorker(context.Background(), key)
		if err == nil && worker.Status == status {
			return worker
		}
		time.Sleep(5 * time.Millisecond)
	}
	worker, err := state.GetWorker(context.Background(), key)
	t.Fatalf("worker status = %#v, %v; want %s", worker, err, status)
	return store.WorkerReservation{}
}

func waitForClientRetirement(t *testing.T, host *Host, retired application) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		host.clientMu.Lock()
		current := host.client
		recovering := host.recovering
		host.clientMu.Unlock()
		if current != retired && recovering == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for app-server retirement")
}

func waitHostClosed(t *testing.T, host *Host) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		host.clientMu.Lock()
		closed := host.closed
		host.clientMu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("host did not enter closed state")
}

type fakeRecord struct {
	starts     []threadStartParams
	resumes    []threadResumeParams
	turns      []turnStartParams
	steers     []turnSteerParams
	interrupts []turnInterruptParams
	preflights int
}

type fakeApplication struct {
	mu                   sync.Mutex
	record               fakeRecord
	tools                []string
	resources            []json.RawMessage
	resourceTemplates    []json.RawMessage
	extraServers         []mcpServerStatus
	authStatus           string
	threadStartHook      func() error
	threadStartErr       error
	resumeErr            error
	mcpStatusErr         error
	turnStartErr         error
	turnSteerErr         error
	turnInterruptErr     error
	turnSteerHook        func(turnSteerParams)
	steerResponseTurnID  string
	completeBeforeReturn bool
	completionStatus     string
	crashAfterComplete   bool
	notifications        chan appserver.Notification
	done                 chan struct{}
	closeOnce            sync.Once
	closeStartedOnce     sync.Once
	startStartedOnce     sync.Once
	turnStartStartedOnce sync.Once
	startGate            chan struct{}
	startStarted         chan struct{}
	turnStartGate        chan struct{}
	turnStartStarted     chan struct{}
	closeGate            chan struct{}
	closeStarted         chan struct{}
	closeCalls           int
	closeErr             error
	err                  error
}

func newFakeApplication() *fakeApplication {
	return &fakeApplication{
		tools:         []string{"send_message", "wait_agent"},
		authStatus:    "unsupported",
		notifications: make(chan appserver.Notification, 16), done: make(chan struct{}),
	}
}

func (a *fakeApplication) ThreadStart(_ context.Context, params, result any) error {
	a.mu.Lock()
	a.record.starts = append(a.record.starts, params.(threadStartParams))
	hook := a.threadStartHook
	threadStartErr := a.threadStartErr
	a.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return err
		}
	}
	if threadStartErr != nil {
		return threadStartErr
	}
	result.(*threadResult).Thread.ID = newTestID()
	return nil
}

func (a *fakeApplication) awaitStart(ctx context.Context) error {
	if a.startStarted != nil {
		a.startStartedOnce.Do(func() { close(a.startStarted) })
	}
	if a.startGate == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.startGate:
		return nil
	}
}

func (a *fakeApplication) ThreadResume(_ context.Context, params, result any) error {
	resume := params.(threadResumeParams)
	a.mu.Lock()
	a.record.resumes = append(a.record.resumes, resume)
	resumeErr := a.resumeErr
	a.mu.Unlock()
	if resumeErr != nil {
		return resumeErr
	}
	result.(*threadResult).Thread.ID = resume.ThreadID
	return nil
}

func (a *fakeApplication) MCPServerStatusList(_ context.Context, params, result any) error {
	a.mu.Lock()
	a.record.preflights++
	tools := append([]string(nil), a.tools...)
	resources := append([]json.RawMessage(nil), a.resources...)
	resourceTemplates := append([]json.RawMessage(nil), a.resourceTemplates...)
	extraServers := append([]mcpServerStatus(nil), a.extraServers...)
	authStatus := a.authStatus
	mcpStatusErr := a.mcpStatusErr
	a.mu.Unlock()
	if mcpStatusErr != nil {
		return mcpStatusErr
	}
	if params.(mcpStatusParams).Detail != "full" {
		return errors.New("managed MCP preflight did not request full inventory")
	}
	toolMap := make(map[string]json.RawMessage, len(tools))
	for _, tool := range tools {
		toolMap[tool] = json.RawMessage(`{}`)
	}
	result.(*mcpStatusPage).Data = append([]mcpServerStatus{{
		Name: workerServerName, Tools: toolMap, Resources: resources,
		ResourceTemplates: resourceTemplates, AuthStatus: authStatus,
	}}, extraServers...)
	return nil
}

func (a *fakeApplication) TurnStart(ctx context.Context, params, result any) error {
	turnParams := params.(turnStartParams)
	turnID := newTestID()
	a.mu.Lock()
	a.record.turns = append(a.record.turns, turnParams)
	complete := a.completeBeforeReturn
	completionStatus := a.completionStatus
	crashAfterComplete := a.crashAfterComplete
	turnStartErr := a.turnStartErr
	turnStartGate := a.turnStartGate
	turnStartStarted := a.turnStartStarted
	a.mu.Unlock()
	if turnStartStarted != nil {
		a.turnStartStartedOnce.Do(func() { close(turnStartStarted) })
	}
	if turnStartGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-turnStartGate:
		}
	}
	if turnStartErr != nil {
		return turnStartErr
	}
	result.(*turnStartResult).Turn = turn{ID: turnID, Status: "inProgress"}
	if complete {
		if completionStatus == "" {
			completionStatus = "completed"
		}
		payload, _ := json.Marshal(turnCompletedNotification{
			ThreadID: turnParams.ThreadID,
			Turn:     turn{ID: turnID, Status: completionStatus, Error: json.RawMessage("null")},
		})
		a.notifications <- appserver.Notification{Method: "turn/completed", Params: payload}
		if crashAfterComplete {
			a.crash(errors.New("test crash after buffered completion"))
		}
	}
	return nil
}

func (a *fakeApplication) TurnSteer(_ context.Context, params, result any) error {
	steer := params.(turnSteerParams)
	a.mu.Lock()
	a.record.steers = append(a.record.steers, steer)
	hook := a.turnSteerHook
	turnSteerErr := a.turnSteerErr
	responseTurnID := a.steerResponseTurnID
	a.mu.Unlock()
	if hook != nil {
		hook(steer)
	}
	if turnSteerErr != nil {
		return turnSteerErr
	}
	if responseTurnID == "" {
		responseTurnID = steer.ExpectedTurnID
	}
	result.(*turnSteerResult).TurnID = responseTurnID
	return nil
}

func (a *fakeApplication) TurnInterrupt(_ context.Context, params, _ any) error {
	interrupt := params.(turnInterruptParams)
	a.mu.Lock()
	a.record.interrupts = append(a.record.interrupts, interrupt)
	turnInterruptErr := a.turnInterruptErr
	a.mu.Unlock()
	return turnInterruptErr
}

func (a *fakeApplication) Notifications() <-chan appserver.Notification {
	return a.notifications
}

func (a *fakeApplication) Done() <-chan struct{} {
	return a.done
}

func (a *fakeApplication) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

func (a *fakeApplication) Close(ctx context.Context) error {
	a.mu.Lock()
	a.closeCalls++
	closeErr := a.closeErr
	a.mu.Unlock()
	if a.closeStarted != nil {
		a.closeStartedOnce.Do(func() { close(a.closeStarted) })
	}
	if a.closeGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.closeGate:
		}
	}
	a.finish(nil)
	return closeErr
}

func (a *fakeApplication) closeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closeCalls
}

func (a *fakeApplication) crash(err error) {
	a.finish(err)
}

func (a *fakeApplication) finish(err error) {
	a.mu.Lock()
	if err != nil {
		a.err = err
	}
	a.mu.Unlock()
	a.closeOnce.Do(func() {
		close(a.notifications)
		close(a.done)
	})
}

func (a *fakeApplication) notifyThreadClosed(threadID string) {
	payload, _ := json.Marshal(map[string]string{"threadId": threadID})
	a.notifications <- appserver.Notification{Method: "thread/closed", Params: payload}
}

func (a *fakeApplication) notifyCompletion(threadID, turnID, status string) {
	payload, _ := json.Marshal(turnCompletedNotification{
		ThreadID: threadID,
		Turn:     turn{ID: turnID, Status: status, Error: json.RawMessage("null")},
	})
	a.notifications <- appserver.Notification{Method: "turn/completed", Params: payload}
}

func (a *fakeApplication) snapshot() fakeRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return fakeRecord{
		starts:     append([]threadStartParams(nil), a.record.starts...),
		resumes:    append([]threadResumeParams(nil), a.record.resumes...),
		turns:      append([]turnStartParams(nil), a.record.turns...),
		steers:     append([]turnSteerParams(nil), a.record.steers...),
		interrupts: append([]turnInterruptParams(nil), a.record.interrupts...),
		preflights: a.record.preflights,
	}
}

func newTestID() string {
	id, err := identity.NewID()
	if err != nil {
		panic(err)
	}
	return id
}
