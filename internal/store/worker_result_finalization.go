package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

// WorkerResultFinalization is the durable worker/result-outbox checkpoint for
// one managed turn. The worker remains finalizing until metadata acknowledgement.
type WorkerResultFinalization struct {
	Intent WorkerTurnStartIntent
	Worker WorkerReservation
	Outbox ResultOutbox
}

// BeginWorkerResultFinalization fences a terminal managed turn before rollout
// or workspace capture starts. The turn's bound intent identifies the one
// pre-reserved result package that may complete the worker lifecycle.
func (s *PeerStore) BeginWorkerResultFinalization(
	ctx context.Context,
	key WorkerKey,
	turnID string,
	target WorkerStatus,
	failureCode string,
	observedAt time.Time,
) (WorkerResultFinalization, error) {
	if err := validateFinalizationRequest(key, turnID, target, failureCode); err != nil {
		return WorkerResultFinalization{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerResultFinalization{}, err
	}

	var finalization WorkerResultFinalization
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		intent, queryErr := queryWorkerTurnStartIntentByTurn(ctx, connection, key, turnID)
		if errors.Is(queryErr, ErrNotFound) {
			return ErrResultPackageAuthority
		}
		if queryErr != nil {
			return queryErr
		}
		if intent.State != WorkerTurnStartBound || intent.WorkerKey != key || intent.TurnID != turnID {
			return ErrResultPackageAuthority
		}
		outbox, queryErr := queryResultOutboxByPackage(ctx, connection, intent.PackageID)
		if queryErr != nil {
			return queryErr
		}
		if outbox.WorkerKey != key || outbox.SourceDeviceID != intent.DeviceID ||
			outbox.PackageID != intent.PackageID ||
			outbox.ReservationLimitBytes != intent.ReservationLimitBytes {
			return ErrResultPackageAuthority
		}
		worker, queryErr := queryWorker(ctx, connection, key)
		if queryErr != nil {
			return queryErr
		}

		if worker.Status == WorkerFinalizing {
			if worker.ActiveTurnID != turnID || worker.FinalTarget != target ||
				worker.FinalFailureCode != failureCode ||
				(outbox.State != ResultOutboxCapturePending && outbox.State != ResultOutboxPublishPending) {
				return ErrResultPackageConflict
			}
			finalization = WorkerResultFinalization{Intent: intent, Worker: worker, Outbox: outbox}
			return nil
		}
		if worker.Status == target && workerTerminalMatches(worker, turnID, failureCode) &&
			(outbox.State == ResultOutboxDeliveryPending || outbox.State == ResultOutboxDelivered) {
			finalization = WorkerResultFinalization{Intent: intent, Worker: worker, Outbox: outbox}
			return nil
		}
		if (worker.Status != WorkerRunning && worker.Status != WorkerInterrupted) ||
			worker.ActiveTurnID != turnID || worker.LastBoundTurnID != turnID ||
			outbox.State != ResultOutboxCapturePending {
			return fmt.Errorf("%w: cannot finalize worker/result package in %s/%s state",
				ErrWorkerTransition, worker.Status, outbox.State)
		}
		if worker.CodexThreadID != intent.ManagedThreadID || worker.DeviceID != intent.DeviceID {
			return ErrResultPackageAuthority
		}

		worker.Status = WorkerFinalizing
		worker.RetryTarget = ""
		worker.FailureCode = ""
		worker.FinalTarget = target
		worker.FinalFailureCode = failureCode
		worker.UpdatedAt = max(timestamp, worker.UpdatedAt, intent.UpdatedAt, outbox.UpdatedAt)
		worker.Revision, queryErr = nextWorkerRevision(ctx, connection)
		if queryErr != nil {
			return queryErr
		}
		if err := worker.Validate(); err != nil {
			return fmt.Errorf("result-finalizing worker is invalid: %w", err)
		}
		result, execErr := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = 'finalizing', retry_target = '', failure_code = '',
	final_target_status = ?, final_failure_code = ?, revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
	AND status IN ('running', 'interrupted') AND active_turn_id = ?
`, worker.FinalTarget, worker.FinalFailureCode, worker.Revision, worker.UpdatedAt,
			worker.ControllerID, worker.TreeID, worker.AgentID, turnID)
		if execErr != nil {
			return fmt.Errorf("begin worker result finalization: %w", execErr)
		}
		if err := requireWorkerTurnStartUpdate(result); err != nil {
			return err
		}
		finalization = WorkerResultFinalization{Intent: intent, Worker: worker, Outbox: outbox}
		return nil
	})
	return finalization, err
}

func workerTerminalMatches(worker WorkerReservation, turnID, failureCode string) bool {
	if worker.FailureCode != failureCode || worker.FinalTarget != "" || worker.FinalFailureCode != "" {
		return false
	}
	return worker.ActiveTurnID == "" && worker.LastBoundTurnID == turnID
}

func completeWorkerResultFinalization(
	ctx context.Context,
	connection *sql.Conn,
	worker WorkerReservation,
	manifest protocol.ResultManifest,
	timestamp int64,
) (WorkerReservation, error) {
	if err := validateResultOutboxWorkerAuthority(ctx, connection, worker, manifest); err != nil {
		return WorkerReservation{}, err
	}
	revision, err := nextWorkerRevision(ctx, connection)
	if err != nil {
		return WorkerReservation{}, err
	}
	worker.Status = worker.FinalTarget
	worker.FailureCode = worker.FinalFailureCode
	worker.FinalTarget = ""
	worker.FinalFailureCode = ""
	worker.RetryTarget = ""
	worker.ActiveTurnID = ""
	worker.Revision = revision
	worker.UpdatedAt = max(timestamp, worker.UpdatedAt)
	if err := worker.Validate(); err != nil {
		return WorkerReservation{}, fmt.Errorf("terminal result worker is invalid: %w", err)
	}
	result, err := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = ?, retry_target = '', active_turn_id = ?, failure_code = ?,
	final_target_status = '', final_failure_code = '', revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
	AND status = 'finalizing' AND active_turn_id = ? AND revision = ?
`, worker.Status, worker.ActiveTurnID, worker.FailureCode, worker.Revision, worker.UpdatedAt,
		worker.ControllerID, worker.TreeID, worker.AgentID, manifest.TurnID, manifest.LifecycleRevision)
	if err != nil {
		return WorkerReservation{}, fmt.Errorf("complete worker result finalization: %w", err)
	}
	if err := requireWorkerTurnStartUpdate(result); err != nil {
		return WorkerReservation{}, err
	}
	return worker, nil
}
