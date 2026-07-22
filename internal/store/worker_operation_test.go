package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkerOperationReceiptsAreDurableAndIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	worker := workerReservation(t, "123e4567-e89b-42d3-a456-426614174450", "operation")
	worker, err = state.ReserveWorker(ctx, worker, 1, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	operationID := "123e4567-e89b-42d3-a456-426614174451"
	message := []byte("durable message that must not be stored")
	digest := sha256.Sum256(message)

	pending, replay, err := state.BeginWorkerOperation(
		ctx,
		operationID,
		WorkerOperationSend,
		worker.WorkerKey,
		message,
		time.Unix(101, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if replay {
		t.Fatal("new operation was reported as a replay")
	}
	wantPending := WorkerOperationReceipt{
		WorkerKey: worker.WorkerKey, OperationID: operationID, Action: WorkerOperationSend,
		PayloadDigest: hex.EncodeToString(digest[:]),
		Status:        WorkerOperationPending, Outcome: WorkerOutcomePending,
		CreatedAt: 101, UpdatedAt: 101,
	}
	if pending != wantPending {
		t.Fatalf("pending receipt = %#v, want %#v", pending, wantPending)
	}

	replayed, replay, err := state.BeginWorkerOperation(
		ctx,
		operationID,
		WorkerOperationSend,
		worker.WorkerKey,
		message,
		time.Unix(102, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replay || replayed != pending {
		t.Fatalf("pending replay = %#v, %t; want %#v, true", replayed, replay, pending)
	}

	completed, err := state.CompleteWorkerOperation(
		ctx,
		worker.WorkerKey,
		operationID,
		WorkerOutcomeSteered,
		"",
		time.Unix(103, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != WorkerOperationSucceeded || completed.Outcome != WorkerOutcomeSteered ||
		completed.UpdatedAt != 103 {
		t.Fatalf("completed receipt = %#v", completed)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	state, err = OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	persisted, replay, err := state.BeginWorkerOperation(
		ctx,
		operationID,
		WorkerOperationSend,
		worker.WorkerKey,
		message,
		time.Unix(104, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replay || persisted != completed {
		t.Fatalf("persisted replay = %#v, %t; want %#v, true", persisted, replay, completed)
	}
}

func TestWorkerOperationReceiptsRejectChangedReplay(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	first := workerReservation(t, "123e4567-e89b-42d3-a456-426614174452", "first")
	first, err = state.ReserveWorker(ctx, first, 2, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	second := workerReservation(t, "123e4567-e89b-42d3-a456-426614174453", "second")
	second, err = state.ReserveWorker(ctx, second, 2, time.Unix(201, 0))
	if err != nil {
		t.Fatal(err)
	}
	operationID := "123e4567-e89b-42d3-a456-426614174454"
	if _, _, err := state.BeginWorkerOperation(
		ctx, operationID, WorkerOperationFollowup, first.WorkerKey, []byte("same"), time.Unix(202, 0),
	); err != nil {
		t.Fatal(err)
	}

	for name, test := range map[string]struct {
		action  WorkerOperationAction
		key     WorkerKey
		payload []byte
	}{
		"action":  {action: WorkerOperationInterrupt, key: first.WorkerKey, payload: []byte("same")},
		"worker":  {action: WorkerOperationFollowup, key: second.WorkerKey, payload: []byte("same")},
		"payload": {action: WorkerOperationFollowup, key: first.WorkerKey, payload: []byte("changed")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := state.BeginWorkerOperation(
				ctx, operationID, test.action, test.key, test.payload, time.Unix(203, 0),
			); !errors.Is(err, ErrWorkerOperationConflict) {
				t.Fatalf("changed replay error = %v, want ErrWorkerOperationConflict", err)
			}
		})
	}
}

func TestWorkerOperationFailureIsStable(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	worker := workerReservation(t, "123e4567-e89b-42d3-a456-426614174455", "failure")
	worker, err = state.ReserveWorker(ctx, worker, 1, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	operationID := "123e4567-e89b-42d3-a456-426614174456"
	if _, _, err := state.BeginWorkerOperation(
		ctx, operationID, WorkerOperationInterrupt, worker.WorkerKey, nil, time.Unix(301, 0),
	); err != nil {
		t.Fatal(err)
	}
	failed, err := state.CompleteWorkerOperation(
		ctx,
		worker.WorkerKey,
		operationID,
		WorkerOutcomeFailed,
		"request_not_written",
		time.Unix(302, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := state.CompleteWorkerOperation(
		ctx,
		worker.WorkerKey,
		operationID,
		WorkerOutcomeFailed,
		"request_not_written",
		time.Unix(303, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != failed {
		t.Fatalf("failed receipt replay = %#v, want %#v", replayed, failed)
	}
	if _, err := state.CompleteWorkerOperation(
		ctx,
		worker.WorkerKey,
		operationID,
		WorkerOutcomeInterrupted,
		"",
		time.Unix(304, 0),
	); !errors.Is(err, ErrWorkerOperationConflict) {
		t.Fatalf("changed completion error = %v, want ErrWorkerOperationConflict", err)
	}
}
