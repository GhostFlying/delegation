package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

var ErrWorkerReadinessStale = errors.New("worker readiness update is stale")

func (s *Store) PutWorkerReadiness(
	ctx context.Context,
	controllerID, deviceID string,
	readiness protocol.WorkerReadiness,
) (protocol.WorkerReadiness, error) {
	if err := validateDeviceIdentity(controllerID, deviceID); err != nil {
		return protocol.WorkerReadiness{}, err
	}
	if err := readiness.Validate(); err != nil {
		return protocol.WorkerReadiness{}, err
	}
	var result protocol.WorkerReadiness
	err := s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if _, err := queryDevice(ctx, connection, controllerID, deviceID); err != nil {
			return err
		}
		current, err := queryWorkerReadiness(ctx, connection, controllerID, deviceID)
		switch {
		case err == nil:
			if readiness.Epoch < current.Epoch ||
				(readiness.Epoch == current.Epoch && readiness.UpdatedAt < current.UpdatedAt) {
				return ErrWorkerReadinessStale
			}
			if readiness.Epoch == current.Epoch && readiness.UpdatedAt == current.UpdatedAt {
				if readiness != current {
					return ErrWorkerReadinessStale
				}
				result = current
				return nil
			}
		case errors.Is(err, ErrNotFound):
		default:
			return err
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO device_worker_readiness(
 controller_id, device_id, epoch, state, attempt_count, runtime_digest, config_digest,
 epoch_started_at, next_attempt_at, last_attempt_at, failure_code, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(controller_id, device_id) DO UPDATE SET
 epoch = excluded.epoch, state = excluded.state, attempt_count = excluded.attempt_count,
 runtime_digest = excluded.runtime_digest, config_digest = excluded.config_digest,
 epoch_started_at = excluded.epoch_started_at,
 next_attempt_at = excluded.next_attempt_at, last_attempt_at = excluded.last_attempt_at,
 failure_code = excluded.failure_code, updated_at = excluded.updated_at
`, controllerID, deviceID, readiness.Epoch, readiness.State, readiness.AttemptCount,
			readiness.RuntimeDigest, readiness.ConfigDigest, readiness.EpochStartedAt, readiness.NextAttemptAt,
			readiness.LastAttemptAt, readiness.FailureCode, readiness.UpdatedAt); err != nil {
			return fmt.Errorf("persist worker readiness: %w", err)
		}
		result = readiness
		return nil
	})
	return result, err
}

func (s *Store) WorkerReadiness(
	ctx context.Context, controllerID, deviceID string,
) (protocol.WorkerReadiness, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return protocol.WorkerReadiness{}, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return protocol.WorkerReadiness{}, fmt.Errorf("deviceId %w", err)
	}
	return queryWorkerReadiness(ctx, s.db, controllerID, deviceID)
}

func queryWorkerReadiness(
	ctx context.Context, queryer rowQueryer, controllerID, deviceID string,
) (protocol.WorkerReadiness, error) {
	var readiness protocol.WorkerReadiness
	err := queryer.QueryRowContext(ctx, `
SELECT epoch, state, attempt_count, runtime_digest, config_digest, epoch_started_at, next_attempt_at,
       last_attempt_at, failure_code, updated_at
FROM device_worker_readiness WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID).Scan(
		&readiness.Epoch, &readiness.State, &readiness.AttemptCount,
		&readiness.RuntimeDigest, &readiness.ConfigDigest, &readiness.EpochStartedAt,
		&readiness.NextAttemptAt,
		&readiness.LastAttemptAt, &readiness.FailureCode, &readiness.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.WorkerReadiness{}, ErrNotFound
	}
	if err != nil {
		return protocol.WorkerReadiness{}, fmt.Errorf("read worker readiness: %w", err)
	}
	if err := readiness.Validate(); err != nil {
		return protocol.WorkerReadiness{}, fmt.Errorf("invalid stored worker readiness: %w", err)
	}
	return readiness, nil
}
