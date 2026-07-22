package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const (
	workerControllerID = "123e4567-e89b-42d3-a456-426614174000"
	workerTreeID       = "123e4567-e89b-42d3-a456-426614174100"
	workerParentID     = "123e4567-e89b-42d3-a456-426614174200"
	workerDeviceID     = "123e4567-e89b-42d3-a456-426614174300"
	workerPromptDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func TestWorkerReservationsPersistAndEnforceSlots(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	firstInput := workerReservation(t, "123e4567-e89b-42d3-a456-426614174401", "first")
	secondInput := workerReservation(t, "123e4567-e89b-42d3-a456-426614174402", "second")
	thirdInput := workerReservation(t, "123e4567-e89b-42d3-a456-426614174403", "third")
	createdAt := time.Unix(100, 0)
	first, err := state.ReserveWorker(ctx, firstInput, 2, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := state.ReserveWorker(ctx, secondInput, 2, createdAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ReserveWorker(ctx, thirdInput, 2, createdAt.Add(2*time.Second)); !errors.Is(err, ErrWorkerBusy) {
		t.Fatalf("third reservation error = %v, want ErrWorkerBusy", err)
	}
	idempotent, err := state.ReserveWorker(ctx, firstInput, 2, createdAt.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(idempotent, first) {
		t.Fatalf("idempotent reservation = %#v, want %#v", idempotent, first)
	}
	conflict := firstInput
	conflict.TaskName = "different"
	if _, err := state.ReserveWorker(ctx, conflict, 2, createdAt); !errors.Is(err, ErrWorkerReservationConflict) {
		t.Fatalf("conflicting reservation error = %v", err)
	}
	conflict = firstInput
	conflict.PromptDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := state.ReserveWorker(ctx, conflict, 2, createdAt); !errors.Is(err, ErrWorkerReservationConflict) {
		t.Fatalf("prompt conflict reservation error = %v, want ErrWorkerReservationConflict", err)
	}

	first, err = state.BeginWorkerStart(ctx, first.WorkerKey, 2, createdAt.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	threadID := "123e4567-e89b-42d3-a456-426614174501"
	first, err = state.AttachWorkerThread(ctx, first.WorkerKey, threadID, createdAt.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	first, err = state.MarkWorkerReady(ctx, first.WorkerKey, createdAt.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	turnID := "123e4567-e89b-42d3-a456-426614174601"
	first, err = state.MarkWorkerRunning(ctx, first.WorkerKey, turnID, createdAt.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != WorkerRunning || first.ActiveTurnID != turnID {
		t.Fatalf("running worker = %#v", first)
	}
	first, err = state.MarkWorkerIdle(ctx, first.WorkerKey, createdAt.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != WorkerIdle || first.ActiveTurnID != "" || first.CodexThreadID != threadID {
		t.Fatalf("idle worker = %#v", first)
	}
	third, err := state.ReserveWorker(ctx, thirdInput, 2, createdAt.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if third.Status != WorkerReserved {
		t.Fatalf("third worker = %#v", third)
	}
	owner, err := state.WorkerForThread(ctx, workerControllerID, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(owner, first) {
		t.Fatalf("thread owner = %#v, want %#v", owner, first)
	}

	wantWorkers := []WorkerReservation{first, second, third}
	workers, err := state.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(workers, wantWorkers) {
		t.Fatalf("workers = %#v, want %#v", workers, wantWorkers)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	workers, err = reopened.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(workers, wantWorkers) {
		t.Fatalf("reopened workers = %#v, want %#v", workers, wantWorkers)
	}
}

func TestWorkerReservationRejectsInvalidTransitionsAndThreadReuse(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	start := time.Unix(200, 0)
	first, err := state.ReserveWorker(
		ctx,
		workerReservation(t, "123e4567-e89b-42d3-a456-426614174411", "first"),
		2,
		start,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.MarkWorkerRunning(
		ctx,
		first.WorkerKey,
		"123e4567-e89b-42d3-a456-426614174611",
		start,
	); !errors.Is(err, ErrWorkerTransition) {
		t.Fatalf("reserved to running error = %v, want ErrWorkerTransition", err)
	}
	first, err = state.BeginWorkerStart(ctx, first.WorkerKey, 2, start.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	threadID := "123e4567-e89b-42d3-a456-426614174511"
	first, err = state.AttachWorkerThread(ctx, first.WorkerKey, threadID, start.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	invalidReserved := first
	invalidReserved.Status = WorkerReserved
	if err := invalidReserved.Validate(); err == nil {
		t.Fatal("reserved worker accepted an attached Codex thread")
	}
	second, err := state.ReserveWorker(
		ctx,
		workerReservation(t, "123e4567-e89b-42d3-a456-426614174412", "second"),
		2,
		start.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err = state.BeginWorkerStart(ctx, second.WorkerKey, 2, start.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.AttachWorkerThread(
		ctx,
		second.WorkerKey,
		threadID,
		start.Add(5*time.Second),
	); !errors.Is(err, ErrWorkerReservationConflict) {
		t.Fatalf("reused thread error = %v, want ErrWorkerReservationConflict", err)
	}
	if _, err := state.FailWorker(ctx, second.WorkerKey, "", start.Add(6*time.Second)); err == nil {
		t.Fatal("FailWorker accepted an empty failure code")
	}
	if _, err := state.FailWorker(ctx, second.WorkerKey, "mcp_injection_blocked", start.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := state.BeginWorkerStart(ctx, first.WorkerKey, 2, start); err == nil {
		t.Fatal("BeginWorkerStart accepted a timestamp older than stored state")
	}
}

func TestReserveWorkerStartAtomicallyAcquiresSlotAndLifecycle(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	input := workerReservation(t, "123e4567-e89b-42d3-a456-426614174421", "atomic")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := state.ReserveWorkerStart(canceled, input, 1, time.Unix(300, 0)); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("canceled ReserveWorkerStart error = %v", err)
	}
	if _, err := state.GetWorker(ctx, input.WorkerKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled ReserveWorkerStart stored a row: %v", err)
	}
	started, err := state.ReserveWorkerStart(ctx, input, 1, time.Unix(301, 0))
	if err != nil {
		t.Fatal(err)
	}
	if started.Status != WorkerStarting {
		t.Fatalf("atomic reservation = %#v", started)
	}
	idempotent, err := state.ReserveWorkerStart(ctx, input, 1, time.Unix(302, 0))
	if err != nil || !reflect.DeepEqual(idempotent, started) {
		t.Fatalf("idempotent atomic reservation = %#v, %v", idempotent, err)
	}
	second := workerReservation(t, "123e4567-e89b-42d3-a456-426614174422", "busy")
	if _, err := state.ReserveWorkerStart(ctx, second, 1, time.Unix(303, 0)); !errors.Is(
		err,
		ErrWorkerBusy,
	) {
		t.Fatalf("second atomic reservation error = %v", err)
	}
	if _, err := state.GetWorker(ctx, second.WorkerKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("busy atomic reservation stored a row: %v", err)
	}
}

func TestRestoreWorkerAfterUnsentRequest(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	now := time.Unix(400, 0)

	initial := workerReservation(t, "123e4567-e89b-42d3-a456-426614174423", "initial")
	initial, err = state.ReserveWorkerStart(ctx, initial, 2, now)
	if err != nil {
		t.Fatal(err)
	}
	initial, err = state.RestoreWorkerPendingAfterUnsent(ctx, initial.WorkerKey, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if initial.Status != WorkerPending || initial.CodexThreadID != "" {
		t.Fatalf("restored initial worker = %#v", initial)
	}
	replacement := workerReservation(t, "123e4567-e89b-42d3-a456-426614174425", "replacement")
	replacement, err = state.ReserveWorkerStart(ctx, replacement, 1, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("pending worker retained its slot: %v", err)
	}
	if _, err := state.BeginWorkerStart(ctx, initial.WorkerKey, 1, now.Add(3*time.Second)); !errors.Is(
		err,
		ErrWorkerBusy,
	) {
		t.Fatalf("pending worker bypassed a full slot: %v", err)
	}
	if _, err := state.FailWorker(ctx, replacement.WorkerKey, "test_complete", now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	initial, err = state.BeginWorkerStart(ctx, initial.WorkerKey, 1, now.Add(5*time.Second))
	if err != nil {
		t.Fatalf("pending worker did not reacquire a released slot: %v", err)
	}
	initial, err = state.RestoreWorkerPendingAfterUnsent(ctx, initial.WorkerKey, now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	followup := workerReservation(t, "123e4567-e89b-42d3-a456-426614174424", "followup")
	followup, err = state.ReserveWorkerStart(ctx, followup, 1, now.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	followup, err = state.AttachWorkerThread(
		ctx,
		followup.WorkerKey,
		"123e4567-e89b-42d3-a456-426614174524",
		now.Add(8*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	followup, err = state.RestoreWorkerPendingAfterUnsent(ctx, followup.WorkerKey, now.Add(9*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if followup.Status != WorkerPending || followup.CodexThreadID == "" {
		t.Fatalf("restored threaded initial worker = %#v", followup)
	}
	other := workerReservation(t, "123e4567-e89b-42d3-a456-426614174426", "other")
	if _, err := state.ReserveWorkerStart(ctx, other, 1, now.Add(10*time.Second)); err != nil {
		t.Fatalf("threaded pending worker retained its slot: %v", err)
	}
}

func workerReservation(t *testing.T, agentID, taskName string) WorkerReservation {
	t.Helper()
	return WorkerReservation{
		WorkerKey: WorkerKey{
			ControllerID: workerControllerID,
			TreeID:       workerTreeID,
			AgentID:      agentID,
		},
		ParentAgentID:  workerParentID,
		DeviceID:       workerDeviceID,
		TaskName:       taskName,
		PromptDigest:   workerPromptDigest,
		WorkspacePath:  filepath.Join(t.TempDir(), "workspace"),
		ProfileVersion: 1,
	}
}
