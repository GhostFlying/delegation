package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestWorkerTurnStartIntentPrepareBindAndRecoveryAreIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000, 0)
	threadID := "123e4567-e89b-42d3-a456-426614175101"
	worker := readyInitialTurnWorker(
		t, state, "123e4567-e89b-42d3-a456-426614175102", threadID, now,
	)
	request := availableTurnIntentRequest(
		t,
		worker,
		"123e4567-e89b-42d3-a456-426614175103",
		"123e4567-e89b-42d3-a456-426614175104",
		"",
	)

	for name, mutate := range map[string]func(*PrepareWorkerTurnStartIntentRequest){
		"device": func(value *PrepareWorkerTurnStartIntentRequest) {
			value.IntentID = "123e4567-e89b-42d3-a456-426614175105"
			value.PackageID = "123e4567-e89b-42d3-a456-426614175106"
			value.DeviceID = "123e4567-e89b-42d3-a456-426614175107"
		},
		"thread": func(value *PrepareWorkerTurnStartIntentRequest) {
			value.IntentID = "123e4567-e89b-42d3-a456-426614175108"
			value.PackageID = "123e4567-e89b-42d3-a456-426614175109"
			value.ManagedThreadID = "123e4567-e89b-42d3-a456-426614175110"
			value.Rollout = WorkerRolloutLocator{
				Status: WorkerRolloutUnavailable, FailureCode: "rollout_unavailable",
			}
		},
		"previous turn": func(value *PrepareWorkerTurnStartIntentRequest) {
			value.IntentID = "123e4567-e89b-42d3-a456-426614175111"
			value.PackageID = "123e4567-e89b-42d3-a456-426614175112"
			value.PreviousTurnID = "123e4567-e89b-42d3-a456-426614175113"
		},
	} {
		t.Run("authority "+name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if _, _, err := state.PrepareWorkerTurnStartIntent(
				ctx, changed, now.Add(4*time.Second),
			); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
				t.Fatalf("authority mismatch error = %v", err)
			}
			if _, err := state.GetResultOutbox(ctx, ResultOutboxKey{
				WorkerKey: changed.WorkerKey, SourceDeviceID: changed.DeviceID, PackageID: changed.PackageID,
			}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("authority mismatch reserved a result package: %v", err)
			}
		})
	}

	prepared, replay, err := state.PrepareWorkerTurnStartIntent(ctx, request, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if replay || prepared.State != WorkerTurnStartPrepared || prepared.RetryTarget != WorkerPending ||
		prepared.PreparedRevision != worker.Revision || prepared.Rollout != request.Rollout {
		t.Fatalf("prepared intent = %#v, replay %t", prepared, replay)
	}
	replayed, replay, err := state.PrepareWorkerTurnStartIntent(ctx, request, now.Add(5*time.Second))
	if err != nil || !replay || replayed != prepared {
		t.Fatalf("prepare replay = %#v, %t, %v", replayed, replay, err)
	}
	changed := request
	changed.ReservationLimitBytes--
	if _, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, changed, now.Add(5*time.Second),
	); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
		t.Fatalf("changed prepare replay error = %v", err)
	}
	competing := request
	competing.IntentID = "123e4567-e89b-42d3-a456-426614175116"
	competing.PackageID = "123e4567-e89b-42d3-a456-426614175117"
	if _, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, competing, now.Add(5*time.Second),
	); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
		t.Fatalf("competing prepare error = %v", err)
	}
	if _, err := state.GetResultOutbox(ctx, ResultOutboxKey{
		WorkerKey: competing.WorkerKey, SourceDeviceID: competing.DeviceID, PackageID: competing.PackageID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("competing prepare reserved a result package: %v", err)
	}
	for name, transition := range map[string]func() error{
		"restore pending": func() error {
			_, err := state.RestoreWorkerPendingAfterUnsent(ctx, worker.WorkerKey, now.Add(5*time.Second))
			return err
		},
		"restore idle": func() error {
			_, err := state.RestoreWorkerIdleAfterUnsent(ctx, worker.WorkerKey, now.Add(5*time.Second))
			return err
		},
		"mark running": func() error {
			_, err := state.MarkWorkerRunning(
				ctx,
				worker.WorkerKey,
				"123e4567-e89b-42d3-a456-426614175118",
				now.Add(5*time.Second),
			)
			return err
		},
		"mark idle": func() error {
			_, err := state.MarkWorkerIdle(ctx, worker.WorkerKey, now.Add(5*time.Second))
			return err
		},
		"fail": func() error {
			_, err := state.FailWorker(ctx, worker.WorkerKey, "ordinary_transition", now.Add(5*time.Second))
			return err
		},
	} {
		t.Run("prepared intent fences "+name, func(t *testing.T) {
			if err := transition(); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
				t.Fatalf("transition error = %v, want ErrWorkerTurnStartIntentConflict", err)
			}
			storedWorker, err := state.GetWorker(ctx, worker.WorkerKey)
			if err != nil || storedWorker != worker {
				t.Fatalf("worker changed after fenced transition = %#v, %v; want %#v", storedWorker, err, worker)
			}
			storedIntent, err := state.GetWorkerTurnStartIntent(ctx, worker.WorkerKey, request.IntentID)
			if err != nil || storedIntent != prepared {
				t.Fatalf("intent changed after fenced transition = %#v, %v; want %#v", storedIntent, err, prepared)
			}
		})
	}

	turnID := "123e4567-e89b-42d3-a456-426614175114"
	bound, err := state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, request.IntentID, turnID, now.Add(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Intent.State != WorkerTurnStartBound || bound.Intent.TurnID != turnID ||
		bound.Worker.Status != WorkerRunning || bound.Worker.ActiveTurnID != turnID ||
		bound.Worker.LastBoundTurnID != turnID || bound.Worker.Revision != bound.Intent.ResolutionRevision ||
		bound.Operation != nil {
		t.Fatalf("bound resolution = %#v", bound)
	}
	doubleBound, err := state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, request.IntentID, turnID, now.Add(7*time.Second),
	)
	if err != nil || !reflect.DeepEqual(doubleBound, bound) {
		t.Fatalf("double bind = %#v, %v; want %#v", doubleBound, err, bound)
	}
	if _, err := state.BindWorkerTurnStartIntent(
		ctx,
		worker.WorkerKey,
		request.IntentID,
		"123e4567-e89b-42d3-a456-426614175115",
		now.Add(7*time.Second),
	); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
		t.Fatalf("contradictory bind error = %v", err)
	}
	if replayed, replay, err := state.PrepareWorkerTurnStartIntent(
		ctx, request, now.Add(8*time.Second),
	); err != nil || !replay || replayed != bound.Intent {
		t.Fatalf("terminal prepare replay = %#v, %t, %v", replayed, replay, err)
	}
	for name, transition := range map[string]func() error{
		"mark idle": func() error {
			_, err := state.MarkWorkerIdle(ctx, worker.WorkerKey, now.Add(8*time.Second))
			return err
		},
		"fail": func() error {
			_, err := state.FailWorker(ctx, worker.WorkerKey, "ordinary_transition", now.Add(8*time.Second))
			return err
		},
	} {
		t.Run("bound intent fences "+name, func(t *testing.T) {
			if err := transition(); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
				t.Fatalf("transition error = %v, want ErrWorkerTurnStartIntentConflict", err)
			}
			storedWorker, err := state.GetWorker(ctx, worker.WorkerKey)
			if err != nil || storedWorker != bound.Worker {
				t.Fatalf("worker changed after fenced transition = %#v, %v; want %#v", storedWorker, err, bound.Worker)
			}
		})
	}

	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	unresolved, err := state.ListUnresolvedWorkerTurnStartIntents(
		ctx, worker.ControllerID, worker.DeviceID, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(unresolved, []WorkerTurnStartIntent{bound.Intent}) {
		t.Fatalf("reopened unresolved intents = %#v, want %#v", unresolved, bound.Intent)
	}
	if recovered, err := state.RecoverWorkers(
		ctx, worker.ControllerID, worker.DeviceID, now.Add(9*time.Second),
	); err != nil || len(recovered) != 0 {
		t.Fatalf("recovery changed bound turn = %#v, %v", recovered, err)
	}
	stored, err := state.GetWorker(ctx, worker.WorkerKey)
	if err != nil || stored != bound.Worker {
		t.Fatalf("bound worker after recovery = %#v, %v; want %#v", stored, err, bound.Worker)
	}
}

func TestWorkerTurnStartIntentFollowupBindCompletesReceiptAtomically(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	now := time.Unix(2_000, 0)
	worker := idleTurnIntentWorker(
		t,
		state,
		"123e4567-e89b-42d3-a456-426614175201",
		"123e4567-e89b-42d3-a456-426614175202",
		"123e4567-e89b-42d3-a456-426614175203",
		now,
	)
	operationID := "123e4567-e89b-42d3-a456-426614175204"
	pending, _, err := state.BeginWorkerOperation(
		ctx,
		operationID,
		WorkerOperationFollowup,
		worker.WorkerKey,
		[]byte("follow up"),
		now.Add(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker = readyFollowupTurnWorker(t, state, worker, now.Add(7*time.Second))
	request := unavailableTurnIntentRequest(
		worker,
		"123e4567-e89b-42d3-a456-426614175205",
		"123e4567-e89b-42d3-a456-426614175206",
		operationID,
	)
	prepared, replay, err := state.PrepareWorkerTurnStartIntent(ctx, request, now.Add(10*time.Second))
	if err != nil || replay || prepared.RetryTarget != WorkerIdle {
		t.Fatalf("followup prepare = %#v, %t, %v", prepared, replay, err)
	}
	if _, err := state.CompleteWorkerOperation(
		ctx,
		worker.WorkerKey,
		operationID,
		WorkerOutcomeStarted,
		"",
		now.Add(10*time.Second),
	); !errors.Is(err, ErrWorkerOperationConflict) {
		t.Fatalf("external operation completion error = %v, want ErrWorkerOperationConflict", err)
	}
	if stored, err := state.GetWorkerOperation(
		ctx, worker.ControllerID, operationID,
	); err != nil || stored != pending {
		t.Fatalf("externally completed prepared receipt = %#v, %v; want %#v", stored, err, pending)
	}
	if _, err := state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, request.IntentID, request.PreviousTurnID, now.Add(10*time.Second),
	); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
		t.Fatalf("previous turn bind error = %v, want ErrWorkerTurnStartIntentConflict", err)
	}
	if stored, err := state.GetWorkerTurnStartIntent(
		ctx, worker.WorkerKey, request.IntentID,
	); err != nil || stored != prepared {
		t.Fatalf("previous turn bind changed intent = %#v, %v; want %#v", stored, err, prepared)
	}
	turnID := "123e4567-e89b-42d3-a456-426614175207"
	if _, err := state.db.ExecContext(ctx, `
CREATE TRIGGER fail_test_turn_intent_bind
BEFORE UPDATE OF state ON worker_turn_start_intents
WHEN NEW.state = 'bound'
BEGIN
	SELECT RAISE(ABORT, 'test bind rollback');
END
`); err != nil {
		t.Fatal(err)
	}
	if _, err := state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, request.IntentID, turnID, now.Add(11*time.Second),
	); err == nil {
		t.Fatal("bind succeeded despite the injected terminal receipt failure")
	}
	rolledBackWorker, err := state.GetWorker(ctx, worker.WorkerKey)
	if err != nil || rolledBackWorker != worker {
		t.Fatalf("worker escaped failed bind transaction = %#v, %v; want %#v", rolledBackWorker, err, worker)
	}
	rolledBackOperation, err := state.GetWorkerOperation(ctx, worker.ControllerID, operationID)
	if err != nil || rolledBackOperation != pending {
		t.Fatalf("receipt escaped failed bind transaction = %#v, %v; want %#v", rolledBackOperation, err, pending)
	}
	rolledBackIntent, err := state.GetWorkerTurnStartIntent(ctx, worker.WorkerKey, request.IntentID)
	if err != nil || rolledBackIntent != prepared {
		t.Fatalf("intent escaped failed bind transaction = %#v, %v; want %#v", rolledBackIntent, err, prepared)
	}
	if _, err := state.db.ExecContext(ctx, `DROP TRIGGER fail_test_turn_intent_bind`); err != nil {
		t.Fatal(err)
	}
	bound, err := state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, request.IntentID, turnID, now.Add(12*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Operation == nil || bound.Operation.Status != WorkerOperationSucceeded ||
		bound.Operation.Outcome != WorkerOutcomeStarted || bound.Operation.FailureCode != "" {
		t.Fatalf("bound followup receipt = %#v", bound.Operation)
	}
	storedReceipt, err := state.GetWorkerOperation(ctx, worker.ControllerID, operationID)
	if err != nil || storedReceipt != *bound.Operation || storedReceipt == pending {
		t.Fatalf("stored followup receipt = %#v, %v", storedReceipt, err)
	}
	storedWorker, err := state.GetWorker(ctx, worker.WorkerKey)
	if err != nil || storedWorker != bound.Worker {
		t.Fatalf("stored bound worker = %#v, %v", storedWorker, err)
	}
}

func TestWorkerTurnStartIntentRejectReleasesReservationAndRestoresRetryState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	now := time.Unix(3_000, 0)

	initial := readyInitialTurnWorker(
		t,
		state,
		"123e4567-e89b-42d3-a456-426614175301",
		"123e4567-e89b-42d3-a456-426614175302",
		now,
	)
	initialRequest := unavailableTurnIntentRequest(
		initial,
		"123e4567-e89b-42d3-a456-426614175303",
		"123e4567-e89b-42d3-a456-426614175304",
		"",
	)
	if _, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, initialRequest, now.Add(4*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	initialRejected, err := state.RejectWorkerTurnStartIntent(
		ctx, initial.WorkerKey, initialRequest.IntentID, "turn_start_rejected", now.Add(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if initialRejected.Worker.Status != WorkerPending || initialRejected.Operation != nil {
		t.Fatalf("rejected initial turn = %#v", initialRejected)
	}

	followup := idleTurnIntentWorker(
		t,
		state,
		"123e4567-e89b-42d3-a456-426614175305",
		"123e4567-e89b-42d3-a456-426614175306",
		"123e4567-e89b-42d3-a456-426614175307",
		now.Add(10*time.Second),
	)
	operationID := "123e4567-e89b-42d3-a456-426614175308"
	if _, _, err := state.BeginWorkerOperation(
		ctx,
		operationID,
		WorkerOperationFollowup,
		followup.WorkerKey,
		[]byte("retry"),
		now.Add(16*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	followup = readyFollowupTurnWorker(t, state, followup, now.Add(17*time.Second))
	followupRequest := unavailableTurnIntentRequest(
		followup,
		"123e4567-e89b-42d3-a456-426614175309",
		"123e4567-e89b-42d3-a456-426614175310",
		operationID,
	)
	if _, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, followupRequest, now.Add(20*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	rejected, err := state.RejectWorkerTurnStartIntent(
		ctx, followup.WorkerKey, followupRequest.IntentID, "turn_start_rejected", now.Add(21*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Intent.State != WorkerTurnStartRejected || rejected.Worker.Status != WorkerIdle ||
		rejected.Worker.LastBoundTurnID != followup.LastBoundTurnID || rejected.Operation == nil ||
		rejected.Operation.Status != WorkerOperationFailed ||
		rejected.Operation.Outcome != WorkerOutcomeFailed ||
		rejected.Operation.FailureCode != "turn_start_rejected" {
		t.Fatalf("rejected followup = %#v", rejected)
	}
	if _, err := state.GetResultOutbox(ctx, ResultOutboxKey{
		WorkerKey: followup.WorkerKey, SourceDeviceID: followup.DeviceID,
		PackageID: followupRequest.PackageID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected followup retained outbox reservation: %v", err)
	}
	replayed, err := state.RejectWorkerTurnStartIntent(
		ctx, followup.WorkerKey, followupRequest.IntentID, "turn_start_rejected", now.Add(22*time.Second),
	)
	if err != nil || !reflect.DeepEqual(replayed, rejected) {
		t.Fatalf("reject replay = %#v, %v; want %#v", replayed, err, rejected)
	}
	if _, err := state.RejectWorkerTurnStartIntent(
		ctx, followup.WorkerKey, followupRequest.IntentID, "different_rejection", now.Add(23*time.Second),
	); !errors.Is(err, ErrWorkerTurnStartIntentConflict) {
		t.Fatalf("changed reject replay error = %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]WorkerTurnStartResolution{
		"initial":  initialRejected,
		"followup": rejected,
	} {
		t.Run("reopened "+name, func(t *testing.T) {
			storedIntent, err := state.GetWorkerTurnStartIntent(
				ctx, want.Worker.WorkerKey, want.Intent.IntentID,
			)
			if err != nil || storedIntent != want.Intent {
				t.Fatalf("stored rejected intent = %#v, %v; want %#v", storedIntent, err, want.Intent)
			}
			storedWorker, err := state.GetWorker(ctx, want.Worker.WorkerKey)
			if err != nil || storedWorker != want.Worker {
				t.Fatalf("stored retryable worker = %#v, %v; want %#v", storedWorker, err, want.Worker)
			}
			if _, err := state.GetResultOutbox(ctx, ResultOutboxKey{
				WorkerKey: want.Intent.WorkerKey, SourceDeviceID: want.Intent.DeviceID,
				PackageID: want.Intent.PackageID,
			}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("reopened rejection retained outbox reservation: %v", err)
			}
		})
	}
	storedReceipt, err := state.GetWorkerOperation(ctx, followup.ControllerID, operationID)
	if err != nil || rejected.Operation == nil || storedReceipt != *rejected.Operation {
		t.Fatalf("reopened rejected receipt = %#v, %v; want %#v", storedReceipt, err, rejected.Operation)
	}
	unresolved, err := state.ListUnresolvedWorkerTurnStartIntents(
		ctx, initial.ControllerID, initial.DeviceID, 10,
	)
	if err != nil || len(unresolved) != 0 {
		t.Fatalf("reopened rejected intents are unresolved = %#v, %v", unresolved, err)
	}
	replayedAfterRestart, err := state.RejectWorkerTurnStartIntent(
		ctx, followup.WorkerKey, followupRequest.IntentID, "turn_start_rejected", now.Add(24*time.Second),
	)
	if err != nil || !reflect.DeepEqual(replayedAfterRestart, rejected) {
		t.Fatalf("restarted reject replay = %#v, %v; want %#v", replayedAfterRestart, err, rejected)
	}
}

func TestWorkerTurnStartIntentQuotaFailureIsAtomic(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	now := time.Unix(4_000, 0)
	worker := idleTurnIntentWorker(
		t,
		state,
		"123e4567-e89b-42d3-a456-426614175401",
		"123e4567-e89b-42d3-a456-426614175402",
		"123e4567-e89b-42d3-a456-426614175403",
		now,
	)
	operationID := "123e4567-e89b-42d3-a456-426614175404"
	if _, _, err := state.BeginWorkerOperation(
		ctx,
		operationID,
		WorkerOperationFollowup,
		worker.WorkerKey,
		[]byte("quota"),
		now.Add(6*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	worker = readyFollowupTurnWorker(t, state, worker, now.Add(7*time.Second))
	for index := range MaximumPeerResultPackages {
		if _, err := state.ReserveResultOutbox(
			ctx,
			ResultOutboxKey{
				WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID,
				PackageID: turnIntentTestID(0x5100 + index),
			},
			1,
			now.Add(time.Duration(10+index)*time.Second),
		); err != nil {
			t.Fatalf("fill result outbox %d: %v", index, err)
		}
	}
	beforeWorker, err := state.GetWorker(ctx, worker.WorkerKey)
	if err != nil {
		t.Fatal(err)
	}
	beforeOperation, err := state.GetWorkerOperation(ctx, worker.ControllerID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	request := unavailableTurnIntentRequest(
		worker,
		"123e4567-e89b-42d3-a456-426614175405",
		"123e4567-e89b-42d3-a456-426614175406",
		operationID,
	)
	if _, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, request, now.Add(100*time.Second),
	); !errors.Is(err, ErrResultPackageQuota) {
		t.Fatalf("quota prepare error = %v, want ErrResultPackageQuota", err)
	}
	afterWorker, err := state.GetWorker(ctx, worker.WorkerKey)
	if err != nil || afterWorker != beforeWorker {
		t.Fatalf("worker changed after quota failure = %#v, %v; want %#v", afterWorker, err, beforeWorker)
	}
	afterOperation, err := state.GetWorkerOperation(ctx, worker.ControllerID, operationID)
	if err != nil || afterOperation != beforeOperation {
		t.Fatalf("operation changed after quota failure = %#v, %v; want %#v", afterOperation, err, beforeOperation)
	}
	if _, err := state.GetWorkerTurnStartIntent(ctx, worker.WorkerKey, request.IntentID); !errors.Is(
		err,
		ErrNotFound,
	) {
		t.Fatalf("quota failure stored an intent: %v", err)
	}
}

func TestPreparedWorkerTurnStartIntentSurvivesRestartRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(5_000, 0)
	worker := readyInitialTurnWorker(
		t,
		state,
		"123e4567-e89b-42d3-a456-426614175501",
		"123e4567-e89b-42d3-a456-426614175502",
		now,
	)
	request := unavailableTurnIntentRequest(
		worker,
		"123e4567-e89b-42d3-a456-426614175503",
		"123e4567-e89b-42d3-a456-426614175504",
		"",
	)
	prepared, _, err := state.PrepareWorkerTurnStartIntent(ctx, request, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if recovered, err := state.RecoverWorkers(
		ctx, worker.ControllerID, worker.DeviceID, now.Add(5*time.Second),
	); err != nil || len(recovered) != 0 {
		t.Fatalf("recovery changed prepared turn = %#v, %v", recovered, err)
	}
	unresolved, err := state.ListUnresolvedWorkerTurnStartIntents(
		ctx, worker.ControllerID, worker.DeviceID, 10,
	)
	if err != nil || !reflect.DeepEqual(unresolved, []WorkerTurnStartIntent{prepared}) {
		t.Fatalf("reopened prepared intents = %#v, %v; want %#v", unresolved, err, prepared)
	}
	storedWorker, err := state.GetWorker(ctx, worker.WorkerKey)
	if err != nil || storedWorker != worker {
		t.Fatalf("prepared worker after restart = %#v, %v; want %#v", storedWorker, err, worker)
	}
}

func TestWorkerRolloutLocatorEnforcesManagedHomeAndBounds(t *testing.T) {
	threadID := "123e4567-e89b-42d3-a456-426614175601"
	home := filepath.Clean(t.TempDir())
	valid := WorkerRolloutLocator{
		Status:    WorkerRolloutAvailable,
		CodexHome: home,
		Path: filepath.Join(
			home, "sessions", "2026", "07", "26", "rollout-2026-07-26T00-00-00-"+threadID+".jsonl",
		),
		Offset: maximumWorkerRolloutOffset,
	}
	if err := valid.Validate(threadID); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*WorkerRolloutLocator){
		"outside": func(value *WorkerRolloutLocator) {
			value.Path = filepath.Join(filepath.Dir(home), "rollout-"+threadID+".jsonl")
		},
		"archived": func(value *WorkerRolloutLocator) {
			value.Path = filepath.Join(home, "archived_sessions", "rollout-"+threadID+".jsonl")
		},
		"thread": func(value *WorkerRolloutLocator) {
			value.Path = filepath.Join(
				home,
				"sessions",
				"rollout-2026-07-26T00-00-00-123e4567-e89b-42d3-a456-426614175602.jsonl",
			)
		},
		"offset": func(value *WorkerRolloutLocator) { value.Offset++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if err := changed.Validate(threadID); err == nil {
				t.Fatalf("locator accepted %#v", changed)
			}
		})
	}
}

func TestWorkerTurnStartIntentReceiptsStayBounded(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	now := time.Unix(6_000, 0)
	worker := readyInitialTurnWorker(
		t,
		state,
		"123e4567-e89b-42d3-a456-426614175701",
		"123e4567-e89b-42d3-a456-426614175702",
		now,
	)
	for index := range MaximumWorkerTurnStartIntentReceipts {
		_, err := state.db.ExecContext(ctx, `
INSERT INTO worker_turn_start_intents(
	controller_id, tree_id, agent_id, intent_id, device_id, managed_thread_id,
	previous_turn_id, package_id, operation_id, retry_target,
	locator_status, codex_home, rollout_path, rollout_offset, locator_failure_code,
	reservation_limit_bytes, state, turn_id, rejection_failure_code,
	prepared_revision, resolution_revision, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, '', ?, '', 'pending',
	'unavailable', '', '', 0, 'rollout_unavailable', 1, 'rejected', '', 'turn_start_rejected',
	?, ?, ?, ?)
`,
			worker.ControllerID,
			worker.TreeID,
			worker.AgentID,
			turnIntentTestID(0x7100+index),
			worker.DeviceID,
			worker.CodexThreadID,
			turnIntentTestID(0x7200+index),
			worker.Revision,
			worker.Revision+1,
			now.Unix()+int64(index),
			now.Unix()+int64(index),
		)
		if err != nil {
			t.Fatalf("insert terminal intent %d: %v", index, err)
		}
	}
	request := unavailableTurnIntentRequest(
		worker,
		"123e4567-e89b-42d3-a456-426614175703",
		"123e4567-e89b-42d3-a456-426614175704",
		"",
	)
	if _, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, request, now.Add(300*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := state.db.QueryRowContext(ctx, `SELECT count(*) FROM worker_turn_start_intents`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != MaximumWorkerTurnStartIntentReceipts {
		t.Fatalf("intent receipt count = %d, want %d", count, MaximumWorkerTurnStartIntentReceipts)
	}
	if _, err := state.GetWorkerTurnStartIntent(
		ctx, worker.WorkerKey, turnIntentTestID(0x7100),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest terminal receipt was not pruned: %v", err)
	}
	if _, err := state.GetWorkerTurnStartIntent(ctx, worker.WorkerKey, request.IntentID); err != nil {
		t.Fatalf("new prepared intent missing after pruning: %v", err)
	}
}

func readyInitialTurnWorker(
	t *testing.T,
	state *PeerStore,
	agentID, threadID string,
	now time.Time,
) WorkerReservation {
	t.Helper()
	worker := workerReservation(t, agentID, "turn intent")
	worker, err := state.ReserveWorkerStart(context.Background(), worker, 32, now)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.AttachWorkerThread(
		context.Background(), worker.WorkerKey, threadID, now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerReady(context.Background(), worker.WorkerKey, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func idleTurnIntentWorker(
	t *testing.T,
	state *PeerStore,
	agentID, threadID, turnID string,
	now time.Time,
) WorkerReservation {
	t.Helper()
	worker := readyInitialTurnWorker(t, state, agentID, threadID, now)
	worker, err := state.MarkWorkerRunning(
		context.Background(), worker.WorkerKey, turnID, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerIdle(context.Background(), worker.WorkerKey, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func readyFollowupTurnWorker(
	t *testing.T,
	state *PeerStore,
	worker WorkerReservation,
	now time.Time,
) WorkerReservation {
	t.Helper()
	worker, err := state.BeginWorkerStart(context.Background(), worker.WorkerKey, 32, now)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.AttachWorkerThread(
		context.Background(), worker.WorkerKey, worker.CodexThreadID, now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerReady(context.Background(), worker.WorkerKey, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func availableTurnIntentRequest(
	t *testing.T,
	worker WorkerReservation,
	intentID, packageID, operationID string,
) PrepareWorkerTurnStartIntentRequest {
	t.Helper()
	home := filepath.Clean(t.TempDir())
	return PrepareWorkerTurnStartIntentRequest{
		WorkerKey:             worker.WorkerKey,
		IntentID:              intentID,
		DeviceID:              worker.DeviceID,
		ManagedThreadID:       worker.CodexThreadID,
		PreviousTurnID:        worker.LastBoundTurnID,
		PackageID:             packageID,
		OperationID:           operationID,
		ReservationLimitBytes: protocol.MaximumResultPackageBytes,
		Rollout: WorkerRolloutLocator{
			Status:    WorkerRolloutAvailable,
			CodexHome: home,
			Path: filepath.Join(
				home,
				"sessions",
				"2026",
				"07",
				"26",
				"rollout-2026-07-26T00-00-00-"+worker.CodexThreadID+".jsonl",
			),
			Offset: 123,
		},
	}
}

func unavailableTurnIntentRequest(
	worker WorkerReservation,
	intentID, packageID, operationID string,
) PrepareWorkerTurnStartIntentRequest {
	return PrepareWorkerTurnStartIntentRequest{
		WorkerKey:             worker.WorkerKey,
		IntentID:              intentID,
		DeviceID:              worker.DeviceID,
		ManagedThreadID:       worker.CodexThreadID,
		PreviousTurnID:        worker.LastBoundTurnID,
		PackageID:             packageID,
		OperationID:           operationID,
		ReservationLimitBytes: protocol.MaximumResultPackageBytes,
		Rollout: WorkerRolloutLocator{
			Status: WorkerRolloutUnavailable, FailureCode: "rollout_unavailable",
		},
	}
}

func turnIntentTestID(value int) string {
	return fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", value)
}
