package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

var (
	ErrWorkerLifecyclePeerBehind         = errors.New("peer worker revision is behind broker cursor")
	ErrWorkerLifecycleSessionFenced      = errors.New("worker lifecycle session is fenced")
	ErrWorkerLifecycleStaleBase          = errors.New("worker lifecycle page has a stale base revision")
	ErrWorkerLifecycleRevisionNotForward = errors.New("worker lifecycle revision does not advance stored state")
	ErrAgentLifecycleCursorAhead         = errors.New("agent lifecycle cursor is ahead of the stored sequence")
	ErrAgentLifecycleSequenceExhausted   = errors.New("agent lifecycle sequence is exhausted")
)

type WorkerLifecycleSession struct {
	ControllerID  string
	DeviceID      string
	ConnectionID  string
	LeaseRevision uint64
}

type WorkerLifecycleSessionClaim struct {
	Session        WorkerLifecycleSession
	WorkerRevision uint64
}

type WorkerLifecyclePageApply struct {
	Session    WorkerLifecycleSession
	Page       protocol.SyncWorkerLifecycleParams
	ObservedAt time.Time
}

type AgentLifecycleActivity struct {
	TreeID         string
	AgentID        string
	TargetDeviceID string
	TargetRevision uint64
	Phase          protocol.WorkerLifecyclePhase
	FailureCode    string
	Sequence       uint64
	ObservedAt     int64
}

type AgentLifecyclePageRequest struct {
	AfterSequence uint64
	Limit         int
}

type AgentLifecyclePage struct {
	Activities   []AgentLifecycleActivity
	NextSequence uint64
	Highwater    uint64
}

func (s *Store) ClaimWorkerLifecycleSession(
	ctx context.Context,
	claim WorkerLifecycleSessionClaim,
) (uint64, error) {
	if err := claim.Session.Validate(); err != nil {
		return 0, err
	}
	if claim.WorkerRevision > math.MaxInt64 {
		return 0, errors.New("workerRevision exceeds the supported range")
	}

	var appliedRevision uint64
	err := s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if err := verifyWorkerLifecycleLease(ctx, connection, claim.Session); err != nil {
			return err
		}
		var stored int64
		err := connection.QueryRowContext(ctx, `
SELECT applied_revision
FROM peer_worker_sync_cursors
WHERE controller_id = ? AND device_id = ?
`, claim.Session.ControllerID, claim.Session.DeviceID).Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			stored = 0
		} else if err != nil {
			return fmt.Errorf("load peer worker sync cursor: %w", err)
		}
		appliedRevision = uint64(stored)
		if appliedRevision > claim.WorkerRevision {
			return ErrWorkerLifecyclePeerBehind
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO peer_worker_sync_cursors(
    controller_id, device_id, active_connection_id, active_lease_revision, applied_revision
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(controller_id, device_id) DO UPDATE SET
    active_connection_id = excluded.active_connection_id,
    active_lease_revision = excluded.active_lease_revision
`, claim.Session.ControllerID, claim.Session.DeviceID, claim.Session.ConnectionID,
			claim.Session.LeaseRevision, appliedRevision); err != nil {
			return fmt.Errorf("claim peer worker lifecycle session: %w", err)
		}
		return nil
	})
	return appliedRevision, err
}

func (s *Store) ApplyWorkerLifecyclePage(
	ctx context.Context,
	request WorkerLifecyclePageApply,
) (protocol.SyncWorkerLifecycleResult, error) {
	if err := request.Session.Validate(); err != nil {
		return protocol.SyncWorkerLifecycleResult{}, err
	}
	appliedRevision, err := request.Page.AppliedRevision()
	if err != nil {
		return protocol.SyncWorkerLifecycleResult{}, err
	}
	observedAt, err := unixTime(request.ObservedAt, "observedAt")
	if err != nil {
		return protocol.SyncWorkerLifecycleResult{}, err
	}
	result := protocol.SyncWorkerLifecycleResult{AppliedRevision: appliedRevision}
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if err := verifyWorkerLifecycleLease(ctx, connection, request.Session); err != nil {
			return err
		}
		appliedRevision, err := queryWorkerLifecycleCursor(ctx, connection, request.Session)
		if err != nil {
			return err
		}
		if appliedRevision != request.Page.BaseRevision {
			return ErrWorkerLifecycleStaleBase
		}
		for _, snapshot := range request.Page.Workers {
			if err := applyWorkerLifecycleSnapshot(
				ctx, connection, request.Session, snapshot, observedAt,
			); err != nil {
				return err
			}
		}
		update, err := connection.ExecContext(ctx, `
UPDATE peer_worker_sync_cursors
SET applied_revision = ?
WHERE controller_id = ? AND device_id = ?
  AND active_connection_id = ? AND active_lease_revision = ? AND applied_revision = ?
`, result.AppliedRevision, request.Session.ControllerID, request.Session.DeviceID,
			request.Session.ConnectionID, request.Session.LeaseRevision, request.Page.BaseRevision)
		if err != nil {
			return fmt.Errorf("advance peer worker sync cursor: %w", err)
		}
		affected, err := update.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect peer worker sync cursor update: %w", err)
		}
		if affected != 1 {
			return ErrWorkerLifecycleSessionFenced
		}
		return nil
	})
	return result, err
}

func (s *Store) ListAgentLifecycleActivity(
	ctx context.Context,
	root control.PrincipalIdentity,
	request AgentLifecyclePageRequest,
) (AgentLifecyclePage, error) {
	if err := root.Validate(); err != nil {
		return AgentLifecyclePage{}, fmt.Errorf("root: %w", err)
	}
	if request.AfterSequence > math.MaxInt64 {
		return AgentLifecyclePage{}, ErrAgentLifecycleCursorAhead
	}
	if request.Limit < 1 || request.Limit > protocol.MaximumWorkerLifecyclePage {
		return AgentLifecyclePage{}, fmt.Errorf(
			"agent lifecycle page limit must be from 1 through %d",
			protocol.MaximumWorkerLifecyclePage,
		)
	}

	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentLifecyclePage{}, fmt.Errorf("begin agent lifecycle page: %w", err)
	}
	defer transaction.Rollback()
	principal, err := authorizePrincipal(
		ctx, transaction, root, control.CapabilityAgentManageDescendants,
	)
	if err != nil {
		return AgentLifecyclePage{}, err
	}
	if principal.ParentAgentID != "" {
		return AgentLifecyclePage{}, ErrAuthorizationDenied
	}
	var highwater int64
	if err := transaction.QueryRowContext(ctx, `
SELECT last_lifecycle_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, root.ControllerID, root.TreeID).Scan(&highwater); errors.Is(err, sql.ErrNoRows) {
		return AgentLifecyclePage{}, ErrNotFound
	} else if err != nil {
		return AgentLifecyclePage{}, fmt.Errorf("load tree lifecycle sequence: %w", err)
	}
	if request.AfterSequence > uint64(highwater) {
		return AgentLifecyclePage{}, ErrAgentLifecycleCursorAhead
	}
	page := AgentLifecyclePage{
		Activities:   []AgentLifecycleActivity{},
		NextSequence: request.AfterSequence,
		Highwater:    uint64(highwater),
	}
	rows, err := transaction.QueryContext(ctx, `
SELECT tree_id, agent_id, target_device_id, target_revision, phase,
       failure_code, lifecycle_sequence, observed_at
FROM agent_lifecycle_states
WHERE controller_id = ? AND tree_id = ? AND lifecycle_sequence > ?
ORDER BY lifecycle_sequence
LIMIT ?
`, root.ControllerID, root.TreeID, request.AfterSequence, request.Limit)
	if err != nil {
		return AgentLifecyclePage{}, fmt.Errorf("list agent lifecycle activity: %w", err)
	}
	for rows.Next() {
		activity, err := scanAgentLifecycleActivity(rows)
		if err != nil {
			rows.Close()
			return AgentLifecyclePage{}, err
		}
		page.Activities = append(page.Activities, activity)
		page.NextSequence = activity.Sequence
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AgentLifecyclePage{}, fmt.Errorf("list agent lifecycle activity: %w", err)
	}
	if err := rows.Close(); err != nil {
		return AgentLifecyclePage{}, fmt.Errorf("close agent lifecycle activity: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return AgentLifecyclePage{}, fmt.Errorf("commit agent lifecycle page: %w", err)
	}
	return page, nil
}

func (s WorkerLifecycleSession) Validate() error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: s.ControllerID},
		{name: "deviceId", value: s.DeviceID},
		{name: "connectionId", value: s.ConnectionID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if s.LeaseRevision == 0 || s.LeaseRevision > math.MaxInt64 {
		return errors.New("leaseRevision is outside the supported range")
	}
	return nil
}

func verifyWorkerLifecycleLease(
	ctx context.Context,
	queryer rowQueryer,
	session WorkerLifecycleSession,
) error {
	device, err := queryDevice(ctx, queryer, session.ControllerID, session.DeviceID)
	if errors.Is(err, ErrNotFound) {
		return ErrWorkerLifecycleSessionFenced
	}
	if err != nil {
		return err
	}
	if !device.Online || device.Revision != session.LeaseRevision {
		return ErrWorkerLifecycleSessionFenced
	}
	return nil
}

func queryWorkerLifecycleCursor(
	ctx context.Context,
	queryer rowQueryer,
	session WorkerLifecycleSession,
) (uint64, error) {
	var (
		connectionID    string
		leaseRevision   int64
		appliedRevision int64
	)
	err := queryer.QueryRowContext(ctx, `
SELECT active_connection_id, active_lease_revision, applied_revision
FROM peer_worker_sync_cursors
WHERE controller_id = ? AND device_id = ?
`, session.ControllerID, session.DeviceID).Scan(
		&connectionID, &leaseRevision, &appliedRevision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrWorkerLifecycleSessionFenced
	}
	if err != nil {
		return 0, fmt.Errorf("load active worker lifecycle session: %w", err)
	}
	if connectionID != session.ConnectionID || uint64(leaseRevision) != session.LeaseRevision {
		return 0, ErrWorkerLifecycleSessionFenced
	}
	return uint64(appliedRevision), nil
}

func applyWorkerLifecycleSnapshot(
	ctx context.Context,
	connection *sql.Conn,
	session WorkerLifecycleSession,
	snapshot protocol.WorkerLifecycleSnapshot,
	observedAt int64,
) error {
	if err := authorizeWorkerLifecycleSnapshot(ctx, connection, session, snapshot); err != nil {
		return err
	}
	var (
		targetDeviceID   string
		targetRevision   int64
		phase            protocol.WorkerLifecyclePhase
		failureCode      string
		sequence         int64
		storedObservedAt int64
	)
	err := connection.QueryRowContext(ctx, `
SELECT target_device_id, target_revision, phase, failure_code, lifecycle_sequence, observed_at
FROM agent_lifecycle_states
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, session.ControllerID, snapshot.TreeID, snapshot.AgentID).Scan(
		&targetDeviceID, &targetRevision, &phase, &failureCode, &sequence, &storedObservedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		sequence, err = nextTreeLifecycleSequence(ctx, connection, session.ControllerID, snapshot.TreeID)
		if err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO agent_lifecycle_states(
    controller_id, tree_id, agent_id, target_device_id, target_revision,
    phase, failure_code, lifecycle_sequence, observed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`, session.ControllerID, snapshot.TreeID, snapshot.AgentID, session.DeviceID,
			snapshot.Revision, snapshot.Phase, snapshot.FailureCode, sequence, observedAt); err != nil {
			return fmt.Errorf("create agent lifecycle state: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load agent lifecycle state: %w", err)
	}
	if targetDeviceID != session.DeviceID {
		return ErrAuthorizationDenied
	}
	if snapshot.Revision <= uint64(targetRevision) {
		return ErrWorkerLifecycleRevisionNotForward
	}
	if observedAt < storedObservedAt {
		observedAt = storedObservedAt
	}
	if phase == snapshot.Phase && failureCode == snapshot.FailureCode {
		result, err := connection.ExecContext(ctx, `
UPDATE agent_lifecycle_states
SET target_revision = ?, observed_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND target_revision = ?
`, snapshot.Revision, observedAt, session.ControllerID, snapshot.TreeID,
			snapshot.AgentID, targetRevision)
		if err != nil {
			return fmt.Errorf("advance unchanged agent lifecycle state: %w", err)
		}
		if err := requireSingleLifecycleUpdate(result); err != nil {
			return err
		}
		return nil
	}
	sequence, err = nextTreeLifecycleSequence(ctx, connection, session.ControllerID, snapshot.TreeID)
	if err != nil {
		return err
	}
	result, err := connection.ExecContext(ctx, `
UPDATE agent_lifecycle_states
SET target_revision = ?, phase = ?, failure_code = ?, lifecycle_sequence = ?, observed_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND target_revision = ?
`, snapshot.Revision, snapshot.Phase, snapshot.FailureCode, sequence, observedAt,
		session.ControllerID, snapshot.TreeID, snapshot.AgentID, targetRevision)
	if err != nil {
		return fmt.Errorf("update agent lifecycle state: %w", err)
	}
	if err := requireSingleLifecycleUpdate(result); err != nil {
		return err
	}
	return nil
}

func requireSingleLifecycleUpdate(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect agent lifecycle state update: %w", err)
	}
	if affected != 1 {
		return errors.New("agent lifecycle state changed during update")
	}
	return nil
}

func authorizeWorkerLifecycleSnapshot(
	ctx context.Context,
	queryer rowQueryer,
	session WorkerLifecycleSession,
	snapshot protocol.WorkerLifecycleSnapshot,
) error {
	var (
		parentAgentID       string
		principalDeviceID   string
		rootAgentID         string
		spawnSourceAgentID  string
		spawnTargetDeviceID string
	)
	err := queryer.QueryRowContext(ctx, `
SELECT p.parent_agent_id, p.device_id, t.root_agent_id,
       r.source_agent_id, r.target_device_id
FROM principals AS p
JOIN trees AS t
  ON t.controller_id = p.controller_id AND t.tree_id = p.tree_id
JOIN agent_spawn_receipts AS r
  ON r.controller_id = p.controller_id AND r.tree_id = p.tree_id AND r.agent_id = p.agent_id
WHERE p.controller_id = ? AND p.tree_id = ? AND p.agent_id = ?
`, session.ControllerID, snapshot.TreeID, snapshot.AgentID).Scan(
		&parentAgentID, &principalDeviceID, &rootAgentID,
		&spawnSourceAgentID, &spawnTargetDeviceID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAuthorizationDenied
	}
	if err != nil {
		return fmt.Errorf("authorize agent lifecycle target: %w", err)
	}
	if parentAgentID == "" || parentAgentID != rootAgentID || spawnSourceAgentID != rootAgentID ||
		principalDeviceID != session.DeviceID || spawnTargetDeviceID != session.DeviceID {
		return ErrAuthorizationDenied
	}
	return nil
}

func nextTreeLifecycleSequence(
	ctx context.Context,
	connection *sql.Conn,
	controllerID, treeID string,
) (int64, error) {
	var current int64
	if err := connection.QueryRowContext(ctx, `
SELECT last_lifecycle_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, controllerID, treeID).Scan(&current); errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, fmt.Errorf("load tree lifecycle sequence: %w", err)
	}
	if current == math.MaxInt64 {
		return 0, ErrAgentLifecycleSequenceExhausted
	}
	next := current + 1
	result, err := connection.ExecContext(ctx, `
UPDATE trees
SET last_lifecycle_sequence = ?
WHERE controller_id = ? AND tree_id = ? AND last_lifecycle_sequence = ?
`, next, controllerID, treeID, current)
	if err != nil {
		return 0, fmt.Errorf("advance tree lifecycle sequence: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect tree lifecycle sequence update: %w", err)
	}
	if affected != 1 {
		return 0, errors.New("tree lifecycle sequence changed during update")
	}
	return next, nil
}

func scanAgentLifecycleActivity(scanner rowScanner) (AgentLifecycleActivity, error) {
	var activity AgentLifecycleActivity
	err := scanner.Scan(
		&activity.TreeID,
		&activity.AgentID,
		&activity.TargetDeviceID,
		&activity.TargetRevision,
		&activity.Phase,
		&activity.FailureCode,
		&activity.Sequence,
		&activity.ObservedAt,
	)
	if err != nil {
		return AgentLifecycleActivity{}, fmt.Errorf("scan agent lifecycle activity: %w", err)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "treeId", value: activity.TreeID},
		{name: "agentId", value: activity.AgentID},
		{name: "targetDeviceId", value: activity.TargetDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return AgentLifecycleActivity{}, fmt.Errorf("stored agent lifecycle %s %w", field.name, err)
		}
	}
	if activity.TargetRevision == 0 || activity.TargetRevision > math.MaxInt64 ||
		activity.Sequence == 0 || activity.Sequence > math.MaxInt64 || activity.ObservedAt < 0 {
		return AgentLifecycleActivity{}, errors.New("stored agent lifecycle numeric state is invalid")
	}
	if err := activity.Phase.Validate(activity.FailureCode); err != nil {
		return AgentLifecycleActivity{}, fmt.Errorf("stored agent lifecycle state is invalid: %w", err)
	}
	return activity, nil
}
