package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	MaximumWorkerTurnStartIntentReceipts = 256
	maximumWorkerTurnIntentPage          = 256
	maximumWorkerRolloutOffset           = int64(1 << 40)
)

var (
	ErrWorkerTurnStartIntentConflict = errors.New("worker turn-start intent conflicts with existing state")
	ErrWorkerTurnStartIntentQuota    = errors.New("worker turn-start intent receipt quota exceeded")
)

type WorkerTurnStartIntentState string

const (
	WorkerTurnStartPrepared WorkerTurnStartIntentState = "prepared"
	WorkerTurnStartBound    WorkerTurnStartIntentState = "bound"
	WorkerTurnStartRejected WorkerTurnStartIntentState = "rejected"
)

type WorkerRolloutLocatorStatus string

const (
	WorkerRolloutAvailable   WorkerRolloutLocatorStatus = "available"
	WorkerRolloutUnavailable WorkerRolloutLocatorStatus = "unavailable"
)

// WorkerRolloutLocator is a caller-validated local rollout position. The store
// persists and structurally checks it, but never opens or interprets rollout
// bytes inside a database transaction.
type WorkerRolloutLocator struct {
	Status      WorkerRolloutLocatorStatus
	CodexHome   string
	Path        string
	Offset      int64
	FailureCode string
}

type PrepareWorkerTurnStartIntentRequest struct {
	WorkerKey
	IntentID              string
	DeviceID              string
	ManagedThreadID       string
	PreviousTurnID        string
	PackageID             string
	OperationID           string
	Rollout               WorkerRolloutLocator
	ReservationLimitBytes int64
}

type WorkerTurnStartIntent struct {
	WorkerKey
	IntentID              string
	DeviceID              string
	ManagedThreadID       string
	PreviousTurnID        string
	PackageID             string
	OperationID           string
	RetryTarget           WorkerStatus
	Rollout               WorkerRolloutLocator
	ReservationLimitBytes int64
	State                 WorkerTurnStartIntentState
	TurnID                string
	RejectionFailureCode  string
	PreparedRevision      uint64
	ResolutionRevision    uint64
	CreatedAt             int64
	UpdatedAt             int64
}

type WorkerTurnStartResolution struct {
	Intent    WorkerTurnStartIntent
	Worker    WorkerReservation
	Operation *WorkerOperationReceipt
}

func (s *PeerStore) PrepareWorkerTurnStartIntent(
	ctx context.Context,
	request PrepareWorkerTurnStartIntentRequest,
	observedAt time.Time,
) (intent WorkerTurnStartIntent, replay bool, err error) {
	if err := request.Validate(); err != nil {
		return WorkerTurnStartIntent{}, false, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerTurnStartIntent{}, false, err
	}
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		existing, queryErr := queryWorkerTurnStartIntentByID(ctx, connection, request.IntentID)
		if queryErr == nil {
			if !sameWorkerTurnStartPreparation(existing, request) {
				return ErrWorkerTurnStartIntentConflict
			}
			intent = existing
			replay = true
			return nil
		}
		if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}

		worker, queryErr := queryWorker(ctx, connection, request.WorkerKey)
		if queryErr != nil {
			return queryErr
		}
		if err := validateTurnStartPreparationAuthority(ctx, connection, worker, request); err != nil {
			return err
		}
		if _, queryErr := queryWorkerTurnStartIntentByPackage(ctx, connection, request.PackageID); queryErr == nil {
			return ErrWorkerTurnStartIntentConflict
		} else if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		if _, queryErr := queryPreparedWorkerTurnStartIntent(
			ctx, connection, request.WorkerKey,
		); queryErr == nil {
			return ErrWorkerTurnStartIntentConflict
		} else if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		if _, queryErr := queryResultOutboxByPackage(ctx, connection, request.PackageID); queryErr == nil {
			return ErrWorkerTurnStartIntentConflict
		} else if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		if err := requireWorkerTurnStartIntentCapacity(ctx, connection); err != nil {
			return err
		}

		outboxKey := ResultOutboxKey{
			WorkerKey: request.WorkerKey, SourceDeviceID: request.DeviceID, PackageID: request.PackageID,
		}
		if _, err := reserveResultOutbox(
			ctx, connection, outboxKey, request.ReservationLimitBytes, timestamp,
		); err != nil {
			return err
		}
		intent = WorkerTurnStartIntent{
			WorkerKey:             request.WorkerKey,
			IntentID:              request.IntentID,
			DeviceID:              request.DeviceID,
			ManagedThreadID:       request.ManagedThreadID,
			PreviousTurnID:        request.PreviousTurnID,
			PackageID:             request.PackageID,
			OperationID:           request.OperationID,
			RetryTarget:           worker.RetryTarget,
			Rollout:               request.Rollout,
			ReservationLimitBytes: request.ReservationLimitBytes,
			State:                 WorkerTurnStartPrepared,
			PreparedRevision:      worker.Revision,
			CreatedAt:             timestamp,
			UpdatedAt:             timestamp,
		}
		if _, execErr := connection.ExecContext(ctx, `
INSERT INTO worker_turn_start_intents(
	controller_id, tree_id, agent_id, intent_id, device_id, managed_thread_id,
	previous_turn_id, package_id, operation_id, retry_target,
	locator_status, codex_home, rollout_path, rollout_offset, locator_failure_code,
	reservation_limit_bytes, state, prepared_revision, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			intent.ControllerID,
			intent.TreeID,
			intent.AgentID,
			intent.IntentID,
			intent.DeviceID,
			intent.ManagedThreadID,
			intent.PreviousTurnID,
			intent.PackageID,
			intent.OperationID,
			intent.RetryTarget,
			intent.Rollout.Status,
			intent.Rollout.CodexHome,
			intent.Rollout.Path,
			intent.Rollout.Offset,
			intent.Rollout.FailureCode,
			intent.ReservationLimitBytes,
			intent.State,
			intent.PreparedRevision,
			intent.CreatedAt,
			intent.UpdatedAt,
		); execErr != nil {
			return fmt.Errorf("create worker turn-start intent: %w", execErr)
		}
		return nil
	})
	return intent, replay, err
}

func (s *PeerStore) BindWorkerTurnStartIntent(
	ctx context.Context,
	key WorkerKey,
	intentID, turnID string,
	observedAt time.Time,
) (resolution WorkerTurnStartResolution, err error) {
	if err := validateWorkerTurnStartResolutionIdentity(key, intentID); err != nil {
		return WorkerTurnStartResolution{}, err
	}
	if err := identity.ValidateID(turnID); err != nil {
		return WorkerTurnStartResolution{}, fmt.Errorf("turnId %w", err)
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerTurnStartResolution{}, err
	}
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		intent, queryErr := queryWorkerTurnStartIntent(ctx, connection, key, intentID)
		if queryErr != nil {
			return queryErr
		}
		if intent.State == WorkerTurnStartBound {
			if intent.TurnID != turnID {
				return ErrWorkerTurnStartIntentConflict
			}
			resolution, queryErr = loadWorkerTurnStartResolution(ctx, connection, intent)
			return queryErr
		}
		if intent.State != WorkerTurnStartPrepared {
			return ErrWorkerTurnStartIntentConflict
		}
		worker, queryErr := queryWorker(ctx, connection, key)
		if queryErr != nil {
			return queryErr
		}
		if err := validatePreparedTurnIntentWorker(worker, intent); err != nil {
			return err
		}
		if existing, queryErr := queryWorkerTurnStartIntentByTurn(
			ctx, connection, key, turnID,
		); queryErr == nil && existing.IntentID != intentID {
			return ErrWorkerTurnStartIntentConflict
		} else if queryErr != nil && !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}

		revision, queryErr := nextWorkerRevision(ctx, connection)
		if queryErr != nil {
			return queryErr
		}
		updatedAt := max(timestamp, worker.UpdatedAt, intent.UpdatedAt)
		worker.Status = WorkerRunning
		worker.RetryTarget = ""
		worker.ActiveTurnID = turnID
		worker.LastBoundTurnID = turnID
		worker.FailureCode = ""
		worker.Revision = revision
		worker.UpdatedAt = updatedAt
		if err := worker.Validate(); err != nil {
			return fmt.Errorf("bound worker lifecycle is invalid: %w", err)
		}
		workerUpdate, execErr := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = 'running', retry_target = '', active_turn_id = ?, last_bound_turn_id = ?,
	failure_code = '', revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
	AND status = 'ready' AND revision = ? AND retry_target = ?
`, turnID, turnID, revision, updatedAt, key.ControllerID, key.TreeID, key.AgentID,
			intent.PreparedRevision, intent.RetryTarget)
		if execErr != nil {
			return fmt.Errorf("bind worker turn-start lifecycle: %w", execErr)
		}
		if err := requireWorkerTurnStartUpdate(workerUpdate); err != nil {
			return err
		}

		var operation *WorkerOperationReceipt
		if intent.OperationID != "" {
			receipt, completeErr := completeWorkerOperationReceipt(
				ctx,
				connection,
				key,
				intent.OperationID,
				WorkerOutcomeStarted,
				"",
				updatedAt,
			)
			if completeErr != nil {
				return completeErr
			}
			operation = &receipt
		}

		intent.State = WorkerTurnStartBound
		intent.TurnID = turnID
		intent.ResolutionRevision = revision
		intent.UpdatedAt = updatedAt
		if err := intent.Validate(); err != nil {
			return fmt.Errorf("bound worker turn-start intent is invalid: %w", err)
		}
		intentUpdate, execErr := connection.ExecContext(ctx, `
UPDATE worker_turn_start_intents SET
	state = 'bound', turn_id = ?, resolution_revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND intent_id = ?
	AND state = 'prepared' AND prepared_revision = ?
`, turnID, revision, updatedAt, key.ControllerID, key.TreeID, key.AgentID, intentID,
			intent.PreparedRevision)
		if execErr != nil {
			return fmt.Errorf("bind worker turn-start intent: %w", execErr)
		}
		if err := requireWorkerTurnStartUpdate(intentUpdate); err != nil {
			return err
		}
		resolution = WorkerTurnStartResolution{Intent: intent, Worker: worker, Operation: operation}
		return nil
	})
	return resolution, err
}

func (s *PeerStore) RejectWorkerTurnStartIntent(
	ctx context.Context,
	key WorkerKey,
	intentID, failureCode string,
	observedAt time.Time,
) (resolution WorkerTurnStartResolution, err error) {
	if err := validateWorkerTurnStartResolutionIdentity(key, intentID); err != nil {
		return WorkerTurnStartResolution{}, err
	}
	if err := validateFailureCode(failureCode); err != nil {
		return WorkerTurnStartResolution{}, err
	}
	if failureCode == "" {
		return WorkerTurnStartResolution{}, errors.New("failureCode is required")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerTurnStartResolution{}, err
	}
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		intent, queryErr := queryWorkerTurnStartIntent(ctx, connection, key, intentID)
		if queryErr != nil {
			return queryErr
		}
		if intent.State == WorkerTurnStartRejected {
			if intent.RejectionFailureCode != failureCode {
				return ErrWorkerTurnStartIntentConflict
			}
			resolution, queryErr = loadWorkerTurnStartResolution(ctx, connection, intent)
			return queryErr
		}
		if intent.State != WorkerTurnStartPrepared {
			return ErrWorkerTurnStartIntentConflict
		}
		worker, queryErr := queryWorker(ctx, connection, key)
		if queryErr != nil {
			return queryErr
		}
		if err := validatePreparedTurnIntentWorker(worker, intent); err != nil {
			return err
		}
		if err := releaseTurnStartResultReservation(ctx, connection, intent); err != nil {
			return err
		}

		revision, queryErr := nextWorkerRevision(ctx, connection)
		if queryErr != nil {
			return queryErr
		}
		updatedAt := max(timestamp, worker.UpdatedAt, intent.UpdatedAt)
		worker.Status = intent.RetryTarget
		worker.RetryTarget = ""
		worker.ActiveTurnID = ""
		worker.FailureCode = ""
		worker.Revision = revision
		worker.UpdatedAt = updatedAt
		if err := worker.Validate(); err != nil {
			return fmt.Errorf("rejected worker lifecycle is invalid: %w", err)
		}
		workerUpdate, execErr := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = ?, retry_target = '', active_turn_id = '', failure_code = '',
	revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
	AND status = 'ready' AND revision = ? AND retry_target = ?
`, worker.Status, revision, updatedAt, key.ControllerID, key.TreeID, key.AgentID,
			intent.PreparedRevision, intent.RetryTarget)
		if execErr != nil {
			return fmt.Errorf("reject worker turn-start lifecycle: %w", execErr)
		}
		if err := requireWorkerTurnStartUpdate(workerUpdate); err != nil {
			return err
		}

		var operation *WorkerOperationReceipt
		if intent.OperationID != "" {
			receipt, completeErr := completeWorkerOperationReceipt(
				ctx,
				connection,
				key,
				intent.OperationID,
				WorkerOutcomeFailed,
				failureCode,
				updatedAt,
			)
			if completeErr != nil {
				return completeErr
			}
			operation = &receipt
		}

		intent.State = WorkerTurnStartRejected
		intent.RejectionFailureCode = failureCode
		intent.ResolutionRevision = revision
		intent.UpdatedAt = updatedAt
		if err := intent.Validate(); err != nil {
			return fmt.Errorf("rejected worker turn-start intent is invalid: %w", err)
		}
		intentUpdate, execErr := connection.ExecContext(ctx, `
UPDATE worker_turn_start_intents SET
	state = 'rejected', rejection_failure_code = ?, resolution_revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND intent_id = ?
	AND state = 'prepared' AND prepared_revision = ?
`, failureCode, revision, updatedAt, key.ControllerID, key.TreeID, key.AgentID, intentID,
			intent.PreparedRevision)
		if execErr != nil {
			return fmt.Errorf("reject worker turn-start intent: %w", execErr)
		}
		if err := requireWorkerTurnStartUpdate(intentUpdate); err != nil {
			return err
		}
		resolution = WorkerTurnStartResolution{Intent: intent, Worker: worker, Operation: operation}
		return nil
	})
	return resolution, err
}

func (s *PeerStore) GetWorkerTurnStartIntent(
	ctx context.Context,
	key WorkerKey,
	intentID string,
) (WorkerTurnStartIntent, error) {
	if err := validateWorkerTurnStartResolutionIdentity(key, intentID); err != nil {
		return WorkerTurnStartIntent{}, err
	}
	return queryWorkerTurnStartIntent(ctx, s.db, key, intentID)
}

// ListUnresolvedWorkerTurnStartIntents returns every prepared intent and each
// bound intent whose exact turn is still running or finalizing. These records
// must be reconciled before ordinary worker recovery can change their state.
func (s *PeerStore) ListUnresolvedWorkerTurnStartIntents(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]WorkerTurnStartIntent, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return nil, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return nil, fmt.Errorf("deviceId %w", err)
	}
	if limit < 1 || limit > maximumWorkerTurnIntentPage {
		return nil, fmt.Errorf("limit must be from 1 through %d", maximumWorkerTurnIntentPage)
	}
	rows, err := s.db.QueryContext(ctx, workerTurnStartIntentSelect+`
WHERE controller_id = ? AND device_id = ? AND (
	state = 'prepared' OR
	(state = 'bound' AND EXISTS (
		SELECT 1 FROM worker_reservations AS worker
		WHERE worker.controller_id = worker_turn_start_intents.controller_id
		  AND worker.tree_id = worker_turn_start_intents.tree_id
		  AND worker.agent_id = worker_turn_start_intents.agent_id
		  AND worker.status IN ('running', 'finalizing')
		  AND worker.active_turn_id = worker_turn_start_intents.turn_id
	))
)
ORDER BY updated_at, created_at, tree_id, agent_id, intent_id
LIMIT ?
`, controllerID, deviceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list unresolved worker turn-start intents: %w", err)
	}
	defer rows.Close()
	intents := make([]WorkerTurnStartIntent, 0)
	for rows.Next() {
		intent, scanErr := scanWorkerTurnStartIntent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unresolved worker turn-start intents: %w", err)
	}
	return intents, nil
}

func (r PrepareWorkerTurnStartIntentRequest) Validate() error {
	if err := r.WorkerKey.Validate(); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "intentId", value: r.IntentID},
		{name: "deviceId", value: r.DeviceID},
		{name: "managedThreadId", value: r.ManagedThreadID},
		{name: "packageId", value: r.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if r.PreviousTurnID != "" {
		if err := identity.ValidateID(r.PreviousTurnID); err != nil {
			return fmt.Errorf("previousTurnId %w", err)
		}
	}
	if r.OperationID != "" {
		if err := identity.ValidateID(r.OperationID); err != nil {
			return fmt.Errorf("operationId %w", err)
		}
	}
	if err := r.Rollout.Validate(r.ManagedThreadID); err != nil {
		return err
	}
	if r.ReservationLimitBytes < 1 || r.ReservationLimitBytes > protocol.MaximumResultPackageBytes {
		return fmt.Errorf(
			"reservationLimitBytes must be from 1 through %d", protocol.MaximumResultPackageBytes,
		)
	}
	return nil
}

func (l WorkerRolloutLocator) Validate(threadID string) error {
	switch l.Status {
	case WorkerRolloutAvailable:
		if l.FailureCode != "" {
			return errors.New("available worker rollout locator cannot contain failureCode")
		}
		if !filepath.IsAbs(l.CodexHome) || !filepath.IsAbs(l.Path) ||
			len(l.CodexHome) > maximumWorkspacePath || len(l.Path) > maximumWorkspacePath ||
			filepath.Clean(l.CodexHome) != l.CodexHome || filepath.Clean(l.Path) != l.Path {
			return errors.New("worker rollout locator paths must be bounded clean absolute paths")
		}
		if l.Offset < 0 || l.Offset > maximumWorkerRolloutOffset {
			return fmt.Errorf("worker rollout offset must be from 0 through %d", maximumWorkerRolloutOffset)
		}
		relative, err := filepath.Rel(l.CodexHome, l.Path)
		if err != nil || relative == "." || filepath.IsAbs(relative) || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("worker rollout path is outside managed Codex home")
		}
		components := strings.Split(relative, string(filepath.Separator))
		if len(components) < 2 || !sameManagedPathComponent(components[0], "sessions") {
			return errors.New("worker rollout path is outside the active sessions hierarchy")
		}
		name := components[len(components)-1]
		expectedSuffix := "-" + threadID + ".jsonl"
		if !strings.HasPrefix(name, "rollout-") || !sameManagedPathSuffix(name, expectedSuffix) {
			return errors.New("worker rollout path does not name the managed thread")
		}
	case WorkerRolloutUnavailable:
		if l.CodexHome != "" || l.Path != "" || l.Offset != 0 {
			return errors.New("unavailable worker rollout locator cannot contain a path or offset")
		}
		if err := validateFailureCode(l.FailureCode); err != nil {
			return fmt.Errorf("rollout locator %w", err)
		}
		if l.FailureCode == "" {
			return errors.New("unavailable worker rollout locator requires failureCode")
		}
	default:
		return fmt.Errorf("unsupported worker rollout locator status %q", l.Status)
	}
	return nil
}

func (i WorkerTurnStartIntent) Validate() error {
	request := PrepareWorkerTurnStartIntentRequest{
		WorkerKey:             i.WorkerKey,
		IntentID:              i.IntentID,
		DeviceID:              i.DeviceID,
		ManagedThreadID:       i.ManagedThreadID,
		PreviousTurnID:        i.PreviousTurnID,
		PackageID:             i.PackageID,
		OperationID:           i.OperationID,
		Rollout:               i.Rollout,
		ReservationLimitBytes: i.ReservationLimitBytes,
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if i.RetryTarget != WorkerPending && i.RetryTarget != WorkerIdle {
		return errors.New("worker turn-start retry target is invalid")
	}
	if (i.RetryTarget == WorkerPending && i.OperationID != "") ||
		(i.RetryTarget == WorkerIdle && i.OperationID == "") {
		return errors.New("worker turn-start operation does not match retry target")
	}
	if i.PreparedRevision == 0 || i.PreparedRevision >= uint64(1<<63) ||
		i.ResolutionRevision >= uint64(1<<63) {
		return errors.New("worker turn-start intent revision is invalid")
	}
	if err := validateFailureCode(i.RejectionFailureCode); err != nil {
		return fmt.Errorf("rejection %w", err)
	}
	switch i.State {
	case WorkerTurnStartPrepared:
		if i.TurnID != "" || i.RejectionFailureCode != "" || i.ResolutionRevision != 0 {
			return errors.New("prepared worker turn-start intent has resolution details")
		}
	case WorkerTurnStartBound:
		if err := identity.ValidateID(i.TurnID); err != nil {
			return fmt.Errorf("turnId %w", err)
		}
		if i.RejectionFailureCode != "" || i.ResolutionRevision <= i.PreparedRevision {
			return errors.New("bound worker turn-start intent has invalid resolution details")
		}
	case WorkerTurnStartRejected:
		if i.TurnID != "" || i.RejectionFailureCode == "" ||
			i.ResolutionRevision <= i.PreparedRevision {
			return errors.New("rejected worker turn-start intent has invalid resolution details")
		}
	default:
		return fmt.Errorf("unsupported worker turn-start intent state %q", i.State)
	}
	if i.CreatedAt < 0 || i.UpdatedAt < i.CreatedAt {
		return errors.New("worker turn-start intent timestamps are invalid")
	}
	return nil
}

func validateTurnStartPreparationAuthority(
	ctx context.Context,
	queryer rowQueryer,
	worker WorkerReservation,
	request PrepareWorkerTurnStartIntentRequest,
) error {
	if worker.WorkerKey != request.WorkerKey || worker.DeviceID != request.DeviceID ||
		worker.CodexThreadID != request.ManagedThreadID || worker.LastBoundTurnID != request.PreviousTurnID ||
		worker.Status != WorkerReady ||
		(worker.RetryTarget != WorkerPending && worker.RetryTarget != WorkerIdle) {
		return ErrWorkerTurnStartIntentConflict
	}
	if worker.RetryTarget == WorkerPending {
		if request.OperationID != "" {
			return ErrWorkerTurnStartIntentConflict
		}
		return nil
	}
	if request.OperationID == "" {
		return ErrWorkerTurnStartIntentConflict
	}
	receipt, err := queryWorkerOperation(ctx, queryer, worker.ControllerID, request.OperationID)
	if err != nil {
		return err
	}
	if receipt.WorkerKey != worker.WorkerKey || receipt.Action != WorkerOperationFollowup ||
		receipt.Status != WorkerOperationPending || receipt.Outcome != WorkerOutcomePending {
		return ErrWorkerTurnStartIntentConflict
	}
	return nil
}

func validatePreparedTurnIntentWorker(worker WorkerReservation, intent WorkerTurnStartIntent) error {
	if worker.WorkerKey != intent.WorkerKey || worker.DeviceID != intent.DeviceID ||
		worker.CodexThreadID != intent.ManagedThreadID || worker.LastBoundTurnID != intent.PreviousTurnID ||
		worker.Status != WorkerReady || worker.RetryTarget != intent.RetryTarget ||
		worker.Revision != intent.PreparedRevision {
		return ErrWorkerTurnStartIntentConflict
	}
	return nil
}

func releaseTurnStartResultReservation(
	ctx context.Context,
	connection *sql.Conn,
	intent WorkerTurnStartIntent,
) error {
	key := ResultOutboxKey{
		WorkerKey: intent.WorkerKey, SourceDeviceID: intent.DeviceID, PackageID: intent.PackageID,
	}
	outbox, err := queryResultOutbox(ctx, connection, key)
	if err != nil {
		return err
	}
	if outbox.State != ResultOutboxCapturePending ||
		outbox.ReservationLimitBytes != intent.ReservationLimitBytes {
		return ErrWorkerTurnStartIntentConflict
	}
	result, err := connection.ExecContext(ctx, `
DELETE FROM peer_result_outbox
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND source_device_id = ?
	AND package_id = ? AND state = 'capturePending' AND reservation_limit_bytes = ?
`, key.ControllerID, key.TreeID, key.AgentID, key.SourceDeviceID, key.PackageID,
		intent.ReservationLimitBytes)
	if err != nil {
		return fmt.Errorf("release rejected worker result reservation: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("inspect rejected worker result reservation: %w", err)
	} else if affected != 1 {
		return ErrWorkerTurnStartIntentConflict
	}
	return nil
}

func loadWorkerTurnStartResolution(
	ctx context.Context,
	queryer rowQueryer,
	intent WorkerTurnStartIntent,
) (WorkerTurnStartResolution, error) {
	worker, err := queryWorker(ctx, queryer, intent.WorkerKey)
	if err != nil {
		return WorkerTurnStartResolution{}, err
	}
	var operation *WorkerOperationReceipt
	if intent.OperationID != "" {
		receipt, err := queryWorkerOperation(ctx, queryer, intent.ControllerID, intent.OperationID)
		if err != nil {
			return WorkerTurnStartResolution{}, err
		}
		switch intent.State {
		case WorkerTurnStartBound:
			if receipt.Status != WorkerOperationSucceeded || receipt.Outcome != WorkerOutcomeStarted ||
				receipt.FailureCode != "" {
				return WorkerTurnStartResolution{}, ErrWorkerTurnStartIntentConflict
			}
		case WorkerTurnStartRejected:
			if receipt.Status != WorkerOperationFailed || receipt.Outcome != WorkerOutcomeFailed ||
				receipt.FailureCode != intent.RejectionFailureCode {
				return WorkerTurnStartResolution{}, ErrWorkerTurnStartIntentConflict
			}
		case WorkerTurnStartPrepared:
			return WorkerTurnStartResolution{}, ErrWorkerTurnStartIntentConflict
		default:
			return WorkerTurnStartResolution{}, ErrWorkerTurnStartIntentConflict
		}
		operation = &receipt
	}
	return WorkerTurnStartResolution{Intent: intent, Worker: worker, Operation: operation}, nil
}

func requireWorkerTurnStartIntentCapacity(ctx context.Context, connection *sql.Conn) error {
	var count int
	if err := connection.QueryRowContext(ctx, `SELECT count(*) FROM worker_turn_start_intents`).Scan(&count); err != nil {
		return fmt.Errorf("count worker turn-start intent receipts: %w", err)
	}
	if count < MaximumWorkerTurnStartIntentReceipts {
		return nil
	}
	remove := count - MaximumWorkerTurnStartIntentReceipts + 1
	result, err := connection.ExecContext(ctx, `
DELETE FROM worker_turn_start_intents
WHERE intent_id IN (
	SELECT intent.intent_id
	FROM worker_turn_start_intents AS intent
	WHERE intent.state = 'rejected' OR (
		intent.state = 'bound' AND NOT EXISTS (
			SELECT 1 FROM worker_reservations AS worker
			WHERE worker.controller_id = intent.controller_id
			  AND worker.tree_id = intent.tree_id
			  AND worker.agent_id = intent.agent_id
			  AND worker.status IN ('running', 'finalizing')
			  AND worker.active_turn_id = intent.turn_id
		)
	)
	ORDER BY intent.updated_at, intent.created_at, intent.intent_id
	LIMIT ?
)
`, remove)
	if err != nil {
		return fmt.Errorf("prune worker turn-start intent receipts: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect pruned worker turn-start intent receipts: %w", err)
	}
	if removed != int64(remove) {
		return ErrWorkerTurnStartIntentQuota
	}
	return nil
}

func queryWorkerTurnStartIntent(
	ctx context.Context,
	queryer rowQueryer,
	key WorkerKey,
	intentID string,
) (WorkerTurnStartIntent, error) {
	return scanWorkerTurnStartIntent(queryer.QueryRowContext(ctx, workerTurnStartIntentSelect+`
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND intent_id = ?
`, key.ControllerID, key.TreeID, key.AgentID, intentID))
}

func queryWorkerTurnStartIntentByID(
	ctx context.Context,
	queryer rowQueryer,
	intentID string,
) (WorkerTurnStartIntent, error) {
	return scanWorkerTurnStartIntent(queryer.QueryRowContext(ctx, workerTurnStartIntentSelect+`
WHERE intent_id = ?
`, intentID))
}

func queryWorkerTurnStartIntentByPackage(
	ctx context.Context,
	queryer rowQueryer,
	packageID string,
) (WorkerTurnStartIntent, error) {
	return scanWorkerTurnStartIntent(queryer.QueryRowContext(ctx, workerTurnStartIntentSelect+`
WHERE package_id = ?
`, packageID))
}

func queryPreparedWorkerTurnStartIntent(
	ctx context.Context,
	queryer rowQueryer,
	key WorkerKey,
) (WorkerTurnStartIntent, error) {
	return scanWorkerTurnStartIntent(queryer.QueryRowContext(ctx, workerTurnStartIntentSelect+`
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND state = 'prepared'
`, key.ControllerID, key.TreeID, key.AgentID))
}

func queryWorkerTurnStartIntentByTurn(
	ctx context.Context,
	queryer rowQueryer,
	key WorkerKey,
	turnID string,
) (WorkerTurnStartIntent, error) {
	return scanWorkerTurnStartIntent(queryer.QueryRowContext(ctx, workerTurnStartIntentSelect+`
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND turn_id = ?
`, key.ControllerID, key.TreeID, key.AgentID, turnID))
}

const workerTurnStartIntentSelect = `
SELECT controller_id, tree_id, agent_id, intent_id, device_id, managed_thread_id,
	previous_turn_id, package_id, operation_id, retry_target,
	locator_status, codex_home, rollout_path, rollout_offset, locator_failure_code,
	reservation_limit_bytes, state, turn_id, rejection_failure_code,
	prepared_revision, resolution_revision, created_at, updated_at
FROM worker_turn_start_intents
`

func scanWorkerTurnStartIntent(scanner rowScanner) (WorkerTurnStartIntent, error) {
	var intent WorkerTurnStartIntent
	if err := scanner.Scan(
		&intent.ControllerID,
		&intent.TreeID,
		&intent.AgentID,
		&intent.IntentID,
		&intent.DeviceID,
		&intent.ManagedThreadID,
		&intent.PreviousTurnID,
		&intent.PackageID,
		&intent.OperationID,
		&intent.RetryTarget,
		&intent.Rollout.Status,
		&intent.Rollout.CodexHome,
		&intent.Rollout.Path,
		&intent.Rollout.Offset,
		&intent.Rollout.FailureCode,
		&intent.ReservationLimitBytes,
		&intent.State,
		&intent.TurnID,
		&intent.RejectionFailureCode,
		&intent.PreparedRevision,
		&intent.ResolutionRevision,
		&intent.CreatedAt,
		&intent.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return WorkerTurnStartIntent{}, ErrNotFound
	} else if err != nil {
		return WorkerTurnStartIntent{}, fmt.Errorf("load worker turn-start intent: %w", err)
	}
	if err := intent.Validate(); err != nil {
		return WorkerTurnStartIntent{}, fmt.Errorf("stored worker turn-start intent is invalid: %w", err)
	}
	return intent, nil
}

func sameWorkerTurnStartPreparation(
	stored WorkerTurnStartIntent,
	request PrepareWorkerTurnStartIntentRequest,
) bool {
	return stored.WorkerKey == request.WorkerKey &&
		stored.IntentID == request.IntentID &&
		stored.DeviceID == request.DeviceID &&
		stored.ManagedThreadID == request.ManagedThreadID &&
		stored.PreviousTurnID == request.PreviousTurnID &&
		stored.PackageID == request.PackageID &&
		stored.OperationID == request.OperationID &&
		stored.Rollout == request.Rollout &&
		stored.ReservationLimitBytes == request.ReservationLimitBytes
}

func validateWorkerTurnStartResolutionIdentity(key WorkerKey, intentID string) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(intentID); err != nil {
		return fmt.Errorf("intentId %w", err)
	}
	return nil
}

func requireWorkerTurnStartUpdate(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect worker turn-start update: %w", err)
	}
	if affected != 1 {
		return ErrWorkerTurnStartIntentConflict
	}
	return nil
}

func sameManagedPathComponent(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func sameManagedPathSuffix(path, suffix string) bool {
	if runtime.GOOS == "windows" {
		return strings.HasSuffix(strings.ToLower(path), strings.ToLower(suffix))
	}
	return strings.HasSuffix(path, suffix)
}
