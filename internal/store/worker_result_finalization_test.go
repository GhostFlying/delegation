package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestResultMetadataAcknowledgementReleasesEveryTerminalTarget(t *testing.T) {
	tests := []struct {
		name        string
		target      WorkerStatus
		failureCode string
		terminal    protocol.ResultTerminal
	}{
		{name: "completed", target: WorkerIdle, terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted}},
		{name: "failed", target: WorkerFailed, failureCode: "turn_failed", terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalFailed, FailureCode: "turn_failed"}},
		{name: "interrupted", target: WorkerInterrupted, failureCode: "turn_interrupted", terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalInterrupted, FailureCode: "turn_interrupted"}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, finalization, key, metadata := prepareTerminalResult(
				t, changesTestID(5600+index), test.target, test.failureCode, test.terminal,
			)
			defer state.Close()
			if _, err := state.CommitResultOutboxCapture(
				context.Background(), key, metadata, time.Unix(1_700_200_010, 0),
			); err != nil {
				t.Fatal(err)
			}
			before, err := state.GetWorker(context.Background(), key.WorkerKey)
			if err != nil || !reflect.DeepEqual(before, finalization.Worker) {
				t.Fatalf("worker before metadata ACK = %#v, %v", before, err)
			}
			acknowledged, err := state.AcknowledgeResultOutboxMetadata(
				context.Background(), key, metadata, time.Unix(1_700_200_011, 0),
			)
			if err != nil || acknowledged.Worker.Status != test.target ||
				acknowledged.Worker.FailureCode != test.failureCode ||
				acknowledged.Worker.ActiveTurnID != "" ||
				acknowledged.Worker.FinalTarget != "" || acknowledged.Worker.FinalFailureCode != "" {
				t.Fatalf("metadata acknowledgement = %#v, %v", acknowledged, err)
			}
		})
	}
}

func TestConcurrentExactMetadataAcknowledgementsAdvanceWorkerOnce(t *testing.T) {
	state, _, key, metadata := prepareTerminalResult(
		t, changesTestID(5610), WorkerIdle, "",
		protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
	)
	defer state.Close()
	ctx := context.Background()
	if _, err := state.CommitResultOutboxCapture(ctx, key, metadata, time.Unix(1_700_201_010, 0)); err != nil {
		t.Fatal(err)
	}
	results := make([]WorkerResultFinalization, 2)
	errorsSeen := make([]error, 2)
	var group sync.WaitGroup
	for index := range results {
		group.Add(1)
		go func() {
			defer group.Done()
			results[index], errorsSeen[index] = state.AcknowledgeResultOutboxMetadata(
				ctx, key, metadata, time.Unix(1_700_201_011, 0),
			)
		}()
	}
	group.Wait()
	if errorsSeen[0] != nil || errorsSeen[1] != nil ||
		!reflect.DeepEqual(results[0], results[1]) {
		t.Fatalf("concurrent metadata ACKs = %#v / %#v, errors %#v", results[0], results[1], errorsSeen)
	}
}

func TestMetadataAcknowledgementReplayDoesNotRegressNewFollowupTurn(t *testing.T) {
	state, _, key, metadata := prepareTerminalResult(
		t, changesTestID(5611), WorkerIdle, "",
		protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
	)
	defer state.Close()
	ctx := context.Background()
	if _, err := state.CommitResultOutboxCapture(
		ctx, key, metadata, time.Unix(1_700_201_020, 0),
	); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := state.AcknowledgeResultOutboxMetadata(
		ctx, key, metadata, time.Unix(1_700_201_021, 0),
	)
	if err != nil || acknowledged.Worker.Status != WorkerIdle {
		t.Fatalf("initial metadata ACK = %#v, %v", acknowledged, err)
	}
	operationID := changesTestID(5612)
	if _, _, err := state.BeginWorkerOperation(
		ctx, operationID, WorkerOperationFollowup, key.WorkerKey,
		[]byte("follow-up after metadata ACK"), time.Unix(1_700_201_022, 0),
	); err != nil {
		t.Fatal(err)
	}
	worker, err := state.BeginWorkerStart(
		ctx, key.WorkerKey, 1, time.Unix(1_700_201_023, 0),
	)
	if err == nil {
		worker, err = state.AttachWorkerThread(
			ctx, key.WorkerKey, worker.CodexThreadID, time.Unix(1_700_201_024, 0),
		)
	}
	if err == nil {
		worker, err = state.MarkWorkerReady(ctx, key.WorkerKey, time.Unix(1_700_201_025, 0))
	}
	if err != nil {
		t.Fatal(err)
	}
	request := unavailableTurnIntentRequest(
		worker, changesTestID(5613), changesTestID(5614), operationID,
	)
	request.ReservationLimitBytes = protocol.MaximumResultManifestBytes + protocol.MaximumResultRolloutBytes
	intent, _, err := state.PrepareWorkerTurnStartIntent(
		ctx, request, time.Unix(1_700_201_026, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	running, err := state.BindWorkerTurnStartIntent(
		ctx, key.WorkerKey, intent.IntentID, changesTestID(5615), time.Unix(1_700_201_027, 0),
	)
	if err != nil || running.Worker.Status != WorkerRunning {
		t.Fatalf("follow-up turn = %#v, %v", running, err)
	}
	replayed, err := state.AcknowledgeResultOutboxMetadata(
		ctx, key, metadata, time.Unix(1_700_201_028, 0),
	)
	if err != nil || replayed.Worker != running.Worker ||
		replayed.Outbox.State != ResultOutboxDeliveryPending {
		t.Fatalf("metadata ACK replay = %#v, %v; want running %#v", replayed, err, running.Worker)
	}
	stored, err := state.GetWorker(ctx, key.WorkerKey)
	if err != nil || stored != running.Worker {
		t.Fatalf("worker after metadata ACK replay = %#v, %v; want %#v", stored, err, running.Worker)
	}
}

func TestMetadataAcknowledgementReplaySurvivesTurnIntentReceiptPruning(t *testing.T) {
	state, _, key, metadata := prepareTerminalResult(
		t, changesTestID(5616), WorkerIdle, "",
		protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
	)
	defer state.Close()
	ctx := context.Background()
	if _, err := state.CommitResultOutboxCapture(
		ctx, key, metadata, time.Unix(1_700_201_030, 0),
	); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := state.AcknowledgeResultOutboxMetadata(
		ctx, key, metadata, time.Unix(1_700_201_031, 0),
	)
	if err != nil || acknowledged.Worker.Status != WorkerIdle {
		t.Fatalf("initial metadata ACK = %#v, %v", acknowledged, err)
	}
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < MaximumWorkerTurnStartIntentReceipts-1; index++ {
		_, err := state.db.ExecContext(ctx, `
INSERT INTO worker_turn_start_intents(
	controller_id, tree_id, agent_id, intent_id, device_id, managed_thread_id,
	previous_turn_id, package_id, operation_id, retry_target,
	locator_status, codex_home, rollout_path, rollout_offset, locator_failure_code,
	reservation_limit_bytes, state, turn_id, rejection_failure_code,
	prepared_revision, resolution_revision, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, '', ?, '', 'pending',
	'unavailable', '', '', 0, 'test_unavailable', 1, 'rejected', '', 'test_rejected',
	1, 2, ?, ?)
`, key.ControllerID, key.TreeID, key.AgentID, changesTestID(7000+index),
			key.SourceDeviceID, manifest.ManagedThreadID, changesTestID(8000+index),
			1_800_000_000+index, 1_800_000_000+index)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := withImmediateTransaction(ctx, state.db, "peer", func(connection *sql.Conn) error {
		return requireWorkerTurnStartIntentCapacity(ctx, connection)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.GetWorkerTurnStartIntentByTurn(
		ctx, key.WorkerKey, manifest.TurnID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pruned turn intent error = %v, want ErrNotFound", err)
	}
	replayed, err := state.AcknowledgeResultOutboxMetadata(
		ctx, key, metadata, time.Unix(1_700_201_032, 0),
	)
	if err != nil || replayed.Worker != acknowledged.Worker ||
		replayed.Outbox.State != ResultOutboxDeliveryPending ||
		replayed.Intent != (WorkerTurnStartIntent{}) {
		t.Fatalf("metadata ACK after receipt pruning = %#v, %v", replayed, err)
	}
}

func prepareTerminalResult(
	t *testing.T,
	packageID string,
	target WorkerStatus,
	failureCode string,
	terminal protocol.ResultTerminal,
) (*PeerStore, WorkerResultFinalization, ResultOutboxKey, protocol.ResultPackageMetadata) {
	t.Helper()
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	worker := workerReservation(t, changesTestID(5620), "result terminal")
	now := time.Unix(1_700_200_000, 0)
	worker, err = state.ReserveWorkerStart(ctx, worker, 1, now)
	threadID := changesTestID(5621)
	if err == nil {
		worker, err = state.AttachWorkerThread(ctx, worker.WorkerKey, threadID, now.Add(time.Second))
	}
	if err == nil {
		worker, err = state.MarkWorkerReady(ctx, worker.WorkerKey, now.Add(2*time.Second))
	}
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	request := unavailableTurnIntentRequest(worker, changesTestID(5622), packageID, "")
	intent, _, err := state.PrepareWorkerTurnStartIntent(ctx, request, now.Add(3*time.Second))
	turnID := changesTestID(5623)
	if err == nil {
		_, err = state.BindWorkerTurnStartIntent(
			ctx, worker.WorkerKey, intent.IntentID, turnID, now.Add(4*time.Second),
		)
	}
	var finalization WorkerResultFinalization
	if err == nil {
		finalization, err = state.BeginWorkerResultFinalization(
			ctx, worker.WorkerKey, turnID, target, failureCode, now.Add(5*time.Second),
		)
	}
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	worker = finalization.Worker
	key := ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID, PackageID: packageID,
	}
	metadata := resultMetadataForRevision(t, key, threadID, turnID, worker.Revision, 0)
	metadata = rewriteResultMetadata(t, metadata, func(manifest *protocol.ResultManifest) {
		manifest.Terminal = terminal
	})
	return state, finalization, key, metadata
}
