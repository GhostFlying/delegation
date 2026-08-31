package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

// EnsureWorkerReadinessEpoch returns the current epoch unless either digest
// changed. A digest change is the only automatic event that creates an epoch.
func (s *PeerStore) EnsureWorkerReadinessEpoch(
	ctx context.Context, runtimeDigest, configDigest string, nowMillis int64,
) (protocol.WorkerReadiness, error) {
	return s.ensureWorkerReadinessEpoch(ctx, runtimeDigest, configDigest, nowMillis, false)
}

// RecheckWorkerReadiness explicitly creates a new qualification epoch.
func (s *PeerStore) RecheckWorkerReadiness(
	ctx context.Context, runtimeDigest, configDigest string, nowMillis int64,
) (protocol.WorkerReadiness, error) {
	return s.ensureWorkerReadinessEpoch(ctx, runtimeDigest, configDigest, nowMillis, true)
}

func (s *PeerStore) ensureWorkerReadinessEpoch(
	ctx context.Context, runtimeDigest, configDigest string, nowMillis int64, force bool,
) (protocol.WorkerReadiness, error) {
	seed := protocol.WorkerReadiness{Epoch: 1, State: protocol.WorkerReadinessPending,
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest, EpochStartedAt: nowMillis,
		NextAttemptAt: nowMillis, UpdatedAt: nowMillis}
	if err := seed.Validate(); err != nil {
		return protocol.WorkerReadiness{}, err
	}
	var result protocol.WorkerReadiness
	err := withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		current, err := queryPeerWorkerReadiness(ctx, connection)
		if err == nil && !force && current.RuntimeDigest == runtimeDigest && current.ConfigDigest == configDigest {
			result = current
			return nil
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if err == nil {
			if current.Epoch == ^uint64(0)>>1 {
				return errors.New("worker readiness epoch exhausted")
			}
			seed.Epoch = current.Epoch + 1
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO worker_readiness(singleton, epoch, state, attempt_count, runtime_digest,
 config_digest, epoch_started_at, next_attempt_at, last_attempt_at, failure_code, updated_at)
VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(singleton) DO UPDATE SET epoch=excluded.epoch, state=excluded.state,
 attempt_count=excluded.attempt_count, runtime_digest=excluded.runtime_digest,
 config_digest=excluded.config_digest, epoch_started_at=excluded.epoch_started_at,
 next_attempt_at=excluded.next_attempt_at,
 last_attempt_at=excluded.last_attempt_at, failure_code=excluded.failure_code,
 updated_at=excluded.updated_at
`, seed.Epoch, seed.State, seed.AttemptCount, seed.RuntimeDigest, seed.ConfigDigest,
			seed.EpochStartedAt, seed.NextAttemptAt, seed.LastAttemptAt, seed.FailureCode,
			seed.UpdatedAt); err != nil {
			return fmt.Errorf("create worker readiness epoch: %w", err)
		}
		result = seed
		return nil
	})
	return result, err
}

func (s *PeerStore) WorkerReadiness(ctx context.Context) (protocol.WorkerReadiness, error) {
	return queryPeerWorkerReadiness(ctx, s.db)
}

// FailWorkerReadiness marks the current epoch intervention-required without
// creating a new epoch. Repair uses this when exact restoration is impossible.
func (s *PeerStore) FailWorkerReadiness(
	ctx context.Context, failureCode string, nowMillis int64,
) (protocol.WorkerReadiness, error) {
	var result protocol.WorkerReadiness
	err := withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		current, err := queryPeerWorkerReadiness(ctx, connection)
		if err != nil {
			return err
		}
		current.State = protocol.WorkerReadinessInterventionRequired
		current.NextAttemptAt = 0
		current.FailureCode = failureCode
		if nowMillis <= current.UpdatedAt {
			nowMillis = current.UpdatedAt + 1
		}
		current.UpdatedAt = nowMillis
		if err := current.Validate(); err != nil {
			return err
		}
		updated, err := connection.ExecContext(ctx, `
UPDATE worker_readiness SET state=?, next_attempt_at=0, failure_code=?, updated_at=?
WHERE singleton=1 AND epoch=? AND updated_at < ?
`, current.State, current.FailureCode, current.UpdatedAt, current.Epoch, current.UpdatedAt)
		if err != nil {
			return fmt.Errorf("fail worker readiness: %w", err)
		}
		changed, err := updated.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect failed worker readiness: %w", err)
		}
		if changed != 1 {
			return ErrWorkerReadinessStale
		}
		result = current
		return nil
	})
	return result, err
}

func (s *PeerStore) UpdateWorkerReadiness(
	ctx context.Context, expectedEpoch uint64, readiness protocol.WorkerReadiness,
) (protocol.WorkerReadiness, error) {
	if err := readiness.Validate(); err != nil {
		return protocol.WorkerReadiness{}, err
	}
	if readiness.Epoch != expectedEpoch {
		return protocol.WorkerReadiness{}, ErrWorkerReadinessStale
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE worker_readiness SET state=?, attempt_count=?, next_attempt_at=?, last_attempt_at=?,
 failure_code=?, updated_at=? WHERE singleton=1 AND epoch=? AND runtime_digest=? AND config_digest=?
 AND epoch_started_at=? AND updated_at < ?
`, readiness.State, readiness.AttemptCount, readiness.NextAttemptAt, readiness.LastAttemptAt,
		readiness.FailureCode, readiness.UpdatedAt, expectedEpoch, readiness.RuntimeDigest,
		readiness.ConfigDigest, readiness.EpochStartedAt, readiness.UpdatedAt)
	if err != nil {
		return protocol.WorkerReadiness{}, fmt.Errorf("update worker readiness: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return protocol.WorkerReadiness{}, fmt.Errorf("inspect worker readiness update: %w", err)
	}
	if changed != 1 {
		return protocol.WorkerReadiness{}, ErrWorkerReadinessStale
	}
	return readiness, nil
}

func queryPeerWorkerReadiness(ctx context.Context, queryer rowQueryer) (protocol.WorkerReadiness, error) {
	var readiness protocol.WorkerReadiness
	err := queryer.QueryRowContext(ctx, `
SELECT epoch, state, attempt_count, runtime_digest, config_digest, epoch_started_at, next_attempt_at,
 last_attempt_at, failure_code, updated_at FROM worker_readiness WHERE singleton=1
`).Scan(&readiness.Epoch, &readiness.State, &readiness.AttemptCount,
		&readiness.RuntimeDigest, &readiness.ConfigDigest, &readiness.EpochStartedAt,
		&readiness.NextAttemptAt,
		&readiness.LastAttemptAt, &readiness.FailureCode, &readiness.UpdatedAt)
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
