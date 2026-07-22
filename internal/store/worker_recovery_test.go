package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoverWorkersPreservesRetryIntentAndAmbiguousTurns(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	start := time.Unix(100, 0)
	workers := map[string]WorkerReservation{
		"reserved":           workerReservation(t, "123e4567-e89b-42d3-a456-426614174310", "reserved"),
		"starting initial":   workerReservation(t, "123e4567-e89b-42d3-a456-426614174311", "starting initial"),
		"preflight initial":  workerReservation(t, "123e4567-e89b-42d3-a456-426614174312", "preflight initial"),
		"starting followup":  workerReservation(t, "123e4567-e89b-42d3-a456-426614174313", "starting followup"),
		"preflight followup": workerReservation(t, "123e4567-e89b-42d3-a456-426614174314", "preflight followup"),
		"ready":              workerReservation(t, "123e4567-e89b-42d3-a456-426614174315", "ready"),
		"running":            workerReservation(t, "123e4567-e89b-42d3-a456-426614174316", "running"),
		"pending":            workerReservation(t, "123e4567-e89b-42d3-a456-426614174317", "pending"),
	}
	for _, worker := range workers {
		if _, err := state.ReserveWorker(ctx, worker, len(workers), start); err != nil {
			t.Fatal(err)
		}
	}

	beginInitial := func(name string) WorkerReservation {
		t.Helper()
		worker, err := state.BeginWorkerStart(ctx, workers[name].WorkerKey, len(workers), start.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}
	attach := func(worker WorkerReservation, suffix string, at time.Time) WorkerReservation {
		t.Helper()
		threadID := "123e4567-e89b-42d3-a456-4266141743" + suffix
		worker, err := state.AttachWorkerThread(ctx, worker.WorkerKey, threadID, at)
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}
	makeIdle := func(name, suffix string) WorkerReservation {
		t.Helper()
		worker := attach(beginInitial(name), suffix, start.Add(2*time.Second))
		worker, err := state.MarkWorkerReady(ctx, worker.WorkerKey, start.Add(3*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		worker, err = state.MarkWorkerRunning(
			ctx,
			worker.WorkerKey,
			"123e4567-e89b-42d3-a456-4266141744"+suffix,
			start.Add(4*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		worker, err = state.MarkWorkerIdle(ctx, worker.WorkerKey, start.Add(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return worker
	}

	beginInitial("starting initial")
	attach(beginInitial("preflight initial"), "20", start.Add(2*time.Second))

	startingFollowup := makeIdle("starting followup", "21")
	if _, err := state.BeginWorkerStart(
		ctx, startingFollowup.WorkerKey, len(workers), start.Add(6*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	preflightFollowup := makeIdle("preflight followup", "22")
	preflightFollowup, err = state.BeginWorkerStart(
		ctx, preflightFollowup.WorkerKey, len(workers), start.Add(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	attach(preflightFollowup, "22", start.Add(7*time.Second))

	ready := attach(beginInitial("ready"), "23", start.Add(2*time.Second))
	if _, err := state.MarkWorkerReady(ctx, ready.WorkerKey, start.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	running := attach(beginInitial("running"), "24", start.Add(2*time.Second))
	running, err = state.MarkWorkerReady(ctx, running.WorkerKey, start.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.MarkWorkerRunning(
		ctx,
		running.WorkerKey,
		"123e4567-e89b-42d3-a456-426614174424",
		start.Add(4*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	pending, err := state.BeginWorkerStart(
		ctx, workers["pending"].WorkerKey, len(workers), start.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	pending, err = state.RestoreWorkerPendingAfterUnsent(ctx, pending.WorkerKey, start.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := state.RecoverWorkers(
		ctx,
		workers["reserved"].ControllerID,
		workers["reserved"].DeviceID,
		start.Add(8*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 7 {
		t.Fatalf("recovered workers = %#v", recovered)
	}
	if stored, err := state.GetWorker(ctx, pending.WorkerKey); err != nil || stored != pending {
		t.Fatalf("pending worker changed during recovery: %#v, %v", stored, err)
	}

	for name, expected := range map[string]struct {
		status      WorkerStatus
		failureCode string
		activeTurn  bool
	}{
		"reserved":           {status: WorkerFailed, failureCode: workerStartupInterruptedFailure},
		"starting initial":   {status: WorkerPending},
		"preflight initial":  {status: WorkerPending},
		"starting followup":  {status: WorkerIdle},
		"preflight followup": {status: WorkerIdle},
		"ready":              {status: WorkerInterrupted, failureCode: workerTurnStartInterruptedFailure},
		"running":            {status: WorkerInterrupted, failureCode: workerRunningTurnInterruptedFailure, activeTurn: true},
	} {
		stored, err := state.GetWorker(ctx, workers[name].WorkerKey)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status != expected.status || stored.FailureCode != expected.failureCode ||
			(stored.ActiveTurnID != "") != expected.activeTurn || stored.RetryTarget != "" {
			t.Fatalf("recovered %s worker = %#v", name, stored)
		}
	}
	if again, err := state.RecoverWorkers(
		ctx,
		workers["reserved"].ControllerID,
		workers["reserved"].DeviceID,
		start.Add(9*time.Second),
	); err != nil || len(again) != 0 {
		t.Fatalf("second recovery = %#v, %v", again, err)
	}
}
