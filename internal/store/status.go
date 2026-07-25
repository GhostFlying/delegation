package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

// StatusSnapshot is a bounded, controller-scoped view of durable broker state.
type StatusSnapshot struct {
	Devices    StatusDeviceCounts
	Trees      int
	Dispatches StatusDispatchCounts
	Workers    StatusWorkerCounts
	Artifacts  StatusArtifactCounts
	Lifetime   StatusLifetimeCounters
}

type StatusDeviceCounts struct {
	Total  int
	Online int
}

type StatusDispatchCounts struct {
	Pending int
	Started int
	Failed  int
}

type StatusWorkerCounts struct {
	Running  int
	Occupied int
}

type StatusArtifactCounts struct {
	Available     int
	Unchanged     int
	CaptureFailed int
}

type StatusLifetimeCounters struct {
	DispatchesStarted uint64
	TurnsStarted      uint64
}

// PeerStatusSnapshot is a bounded, device-scoped view of durable peer state.
type PeerStatusSnapshot struct {
	WorkerRevision uint64
	Workers        PeerStatusWorkerCounts
	Artifacts      PeerStatusArtifactCounts
}

type PeerStatusWorkerCounts struct {
	Total       int
	Reserved    int
	Pending     int
	Starting    int
	Preflight   int
	Ready       int
	Running     int
	Finalizing  int
	Idle        int
	Interrupted int
	Failed      int
	Occupied    int
}

type PeerStatusArtifactCounts struct {
	CaptureBacklog int
	PublishBacklog int
	Retained       int
	RetainedBytes  int64
}

type statusLifetimeCounterIncrement struct {
	DispatchesStarted int
	TurnsStarted      int
}

func incrementStatusLifetimeCounters(
	ctx context.Context,
	connection *sql.Conn,
	controllerID string,
	increment statusLifetimeCounterIncrement,
) error {
	_, err := connection.ExecContext(ctx, `
INSERT INTO controller_lifetime_counters(controller_id, dispatches_started, turns_started)
VALUES (?, ?, ?)
ON CONFLICT(controller_id) DO UPDATE SET
	dispatches_started = dispatches_started + excluded.dispatches_started,
	turns_started = turns_started + excluded.turns_started
`, controllerID, increment.DispatchesStarted, increment.TurnsStarted)
	if err != nil {
		return fmt.Errorf("increment status lifetime counters: %w", err)
	}
	return nil
}

// ReadStatusSnapshot returns fixed-size aggregates from one read transaction.
func (s *Store) ReadStatusSnapshot(ctx context.Context, controllerID string) (StatusSnapshot, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return StatusSnapshot{}, fmt.Errorf("controllerId %w", err)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return StatusSnapshot{}, fmt.Errorf("begin broker status snapshot: %w", err)
	}
	defer transaction.Rollback()

	var snapshot StatusSnapshot
	if err := transaction.QueryRowContext(ctx, `
SELECT count(*), COALESCE(sum(CASE WHEN online = 1 THEN 1 ELSE 0 END), 0)
FROM devices
WHERE controller_id = ?
`, controllerID).Scan(&snapshot.Devices.Total, &snapshot.Devices.Online); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read status device counts: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT count(*) FROM trees WHERE controller_id = ?
`, controllerID).Scan(&snapshot.Trees); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read status tree count: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT
	COALESCE(sum(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'started' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0)
FROM agent_spawn_receipts
WHERE controller_id = ?
`, controllerID).Scan(
		&snapshot.Dispatches.Pending,
		&snapshot.Dispatches.Started,
		&snapshot.Dispatches.Failed,
	); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read status dispatch counts: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT
	COALESCE(sum(CASE WHEN phase = 'running' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN phase IN (`+occupiedWorkerStatesSQL+`) THEN 1 ELSE 0 END), 0)
FROM agent_lifecycle_states
WHERE controller_id = ?
`, controllerID).Scan(&snapshot.Workers.Running, &snapshot.Workers.Occupied); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read status worker counts: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT
	COALESCE(sum(CASE WHEN status = 'available' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'unchanged' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'captureFailed' THEN 1 ELSE 0 END), 0)
FROM changes_artifacts
WHERE controller_id = ?
`, controllerID).Scan(
		&snapshot.Artifacts.Available,
		&snapshot.Artifacts.Unchanged,
		&snapshot.Artifacts.CaptureFailed,
	); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read status artifact counts: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT
	COALESCE(max(dispatches_started), 0),
	COALESCE(max(turns_started), 0)
FROM controller_lifetime_counters
WHERE controller_id = ?
`, controllerID).Scan(
		&snapshot.Lifetime.DispatchesStarted,
		&snapshot.Lifetime.TurnsStarted,
	); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read status lifetime counters: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return StatusSnapshot{}, fmt.Errorf("commit broker status snapshot: %w", err)
	}
	return snapshot, nil
}

// ReadPeerStatusSnapshot returns fixed-size peer aggregates from one read transaction.
func (s *PeerStore) ReadPeerStatusSnapshot(
	ctx context.Context,
	controllerID string,
	deviceID string,
) (PeerStatusSnapshot, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return PeerStatusSnapshot{}, err
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PeerStatusSnapshot{}, fmt.Errorf("begin peer status snapshot: %w", err)
	}
	defer transaction.Rollback()

	var snapshot PeerStatusSnapshot
	if err := transaction.QueryRowContext(ctx, `
SELECT worker_revision FROM peer_metadata WHERE singleton = 1
`).Scan(&snapshot.WorkerRevision); err != nil {
		return PeerStatusSnapshot{}, fmt.Errorf("read peer status worker revision: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT
	count(*),
	COALESCE(sum(CASE WHEN status = 'reserved' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'starting' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'preflight' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'ready' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'finalizing' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'idle' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'interrupted' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN status IN (`+occupiedWorkerStatesSQL+`) THEN 1 ELSE 0 END), 0)
FROM worker_reservations
WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID).Scan(
		&snapshot.Workers.Total,
		&snapshot.Workers.Reserved,
		&snapshot.Workers.Pending,
		&snapshot.Workers.Starting,
		&snapshot.Workers.Preflight,
		&snapshot.Workers.Ready,
		&snapshot.Workers.Running,
		&snapshot.Workers.Finalizing,
		&snapshot.Workers.Idle,
		&snapshot.Workers.Interrupted,
		&snapshot.Workers.Failed,
		&snapshot.Workers.Occupied,
	); err != nil {
		return PeerStatusSnapshot{}, fmt.Errorf("read peer status worker counts: %w", err)
	}
	if err := transaction.QueryRowContext(ctx, `
SELECT
	COALESCE(sum(CASE WHEN artifact.state = 'capturePending' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN artifact.state = 'publishPending' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN artifact.state = 'published' THEN 1 ELSE 0 END), 0),
	COALESCE(sum(CASE WHEN artifact.state = 'published' THEN artifact.reserved_bytes ELSE 0 END), 0)
FROM peer_changes_artifacts AS artifact
JOIN worker_reservations AS worker
  ON worker.controller_id = artifact.controller_id
 AND worker.tree_id = artifact.tree_id
 AND worker.agent_id = artifact.agent_id
WHERE artifact.controller_id = ? AND worker.device_id = ?
`, controllerID, deviceID).Scan(
		&snapshot.Artifacts.CaptureBacklog,
		&snapshot.Artifacts.PublishBacklog,
		&snapshot.Artifacts.Retained,
		&snapshot.Artifacts.RetainedBytes,
	); err != nil {
		return PeerStatusSnapshot{}, fmt.Errorf("read peer status artifact counts: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return PeerStatusSnapshot{}, fmt.Errorf("commit peer status snapshot: %w", err)
	}
	return snapshot, nil
}
