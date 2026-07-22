package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	workerStartupInterruptedFailure     = "app_server_interrupted"
	workerThreadStartAmbiguousFailure   = "thread_start_ambiguous"
	workerTurnStartInterruptedFailure   = "turn_start_interrupted"
	workerRunningTurnInterruptedFailure = "turn_interrupted"
)

// RecoverWorkers records that the connector-owned app-server no longer owns
// any active turns. Threads that reached Codex can be cold-resumed; workers
// interrupted before a thread was attached cannot be recovered.
func (s *PeerStore) RecoverWorkers(
	ctx context.Context,
	controllerID, deviceID string,
	observedAt time.Time,
) ([]WorkerReservation, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return nil, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return nil, fmt.Errorf("deviceId %w", err)
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return nil, err
	}

	var recovered []WorkerReservation
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		rows, err := connection.QueryContext(ctx, workerSelect+`
WHERE controller_id = ? AND device_id = ?
  AND status IN ('reserved', 'starting', 'preflight', 'ready', 'running')
ORDER BY created_at, tree_id, agent_id
`, controllerID, deviceID)
		if err != nil {
			return fmt.Errorf("list interrupted workers: %w", err)
		}
		for rows.Next() {
			worker, err := scanWorker(rows)
			if err != nil {
				_ = rows.Close()
				return err
			}
			recovered = append(recovered, worker)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("list interrupted workers: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close interrupted worker rows: %w", err)
		}

		for index := range recovered {
			worker := &recovered[index]
			worker.UpdatedAt = max(timestamp, worker.UpdatedAt)
			switch worker.Status {
			case WorkerRunning:
				worker.Status = WorkerInterrupted
				worker.RetryTarget = ""
				worker.FailureCode = workerRunningTurnInterruptedFailure
			case WorkerReady:
				worker.Status = WorkerInterrupted
				worker.RetryTarget = ""
				worker.ActiveTurnID = ""
				worker.FailureCode = workerTurnStartInterruptedFailure
			case WorkerStarting:
				worker.ActiveTurnID = ""
				if worker.CodexThreadID == "" {
					// An initial thread/start may have reached Codex before the
					// connector stopped. Without the returned thread ID, retrying
					// would create a second managed thread and leave the first one
					// outside the permanent worker-thread guard.
					worker.Status = WorkerFailed
					worker.RetryTarget = ""
					worker.FailureCode = workerThreadStartAmbiguousFailure
					break
				}
				switch worker.RetryTarget {
				case WorkerPending, WorkerIdle:
					worker.Status = worker.RetryTarget
					worker.RetryTarget = ""
					worker.FailureCode = ""
				default:
					return fmt.Errorf("unexpected retry target %q", worker.RetryTarget)
				}
			case WorkerPreflight:
				worker.ActiveTurnID = ""
				switch worker.RetryTarget {
				case WorkerPending, WorkerIdle:
					worker.Status = worker.RetryTarget
					worker.RetryTarget = ""
					worker.FailureCode = ""
				default:
					return fmt.Errorf("unexpected retry target %q", worker.RetryTarget)
				}
			case WorkerReserved:
				worker.Status = WorkerFailed
				worker.RetryTarget = ""
				worker.ActiveTurnID = ""
				worker.FailureCode = workerStartupInterruptedFailure
			case WorkerPending, WorkerIdle, WorkerInterrupted, WorkerFailed:
				return fmt.Errorf("unexpected recovery status %q", worker.Status)
			}
			if _, err := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = ?, retry_target = ?, active_turn_id = ?, failure_code = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`,
				worker.Status,
				worker.RetryTarget,
				worker.ActiveTurnID,
				worker.FailureCode,
				worker.UpdatedAt,
				worker.ControllerID,
				worker.TreeID,
				worker.AgentID,
			); err != nil {
				return fmt.Errorf("recover interrupted worker: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recovered, nil
}
