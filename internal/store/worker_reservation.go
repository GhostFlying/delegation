package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	maximumWorkerTaskName    = 128
	maximumWorkerFailureCode = 64
	maximumWorkspacePath     = 32 * 1024
)

var (
	ErrWorkerBusy                = errors.New("peer worker slots are busy")
	ErrWorkerReservationConflict = errors.New("worker reservation conflicts with existing state")
	ErrWorkerTransition          = errors.New("invalid worker lifecycle transition")
)

type WorkerStatus string

const (
	WorkerReserved    WorkerStatus = "reserved"
	WorkerPending     WorkerStatus = "pending"
	WorkerStarting    WorkerStatus = "starting"
	WorkerPreflight   WorkerStatus = "preflight"
	WorkerReady       WorkerStatus = "ready"
	WorkerRunning     WorkerStatus = "running"
	WorkerIdle        WorkerStatus = "idle"
	WorkerInterrupted WorkerStatus = "interrupted"
	WorkerFailed      WorkerStatus = "failed"
)

type WorkerKey struct {
	ControllerID string
	TreeID       string
	AgentID      string
}

type WorkerReservation struct {
	WorkerKey
	ParentAgentID  string
	DeviceID       string
	TaskName       string
	PromptDigest   string
	WorkspacePath  string
	CodexThreadID  string
	ProfileVersion int
	Status         WorkerStatus
	RetryTarget    WorkerStatus
	ActiveTurnID   string
	FailureCode    string
	Revision       uint64
	CreatedAt      int64
	UpdatedAt      int64
}

func (s *PeerStore) ReserveWorker(
	ctx context.Context,
	reservation WorkerReservation,
	maxActive int,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.reserveWorker(ctx, reservation, maxActive, observedAt, WorkerReserved)
}

// ReserveWorkerStart atomically creates a reservation in starting state. It
// prevents cancellation or storage failure between slot acquisition and the
// first lifecycle transition from leaving a reserved slot behind.
func (s *PeerStore) ReserveWorkerStart(
	ctx context.Context,
	reservation WorkerReservation,
	maxActive int,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.reserveWorker(ctx, reservation, maxActive, observedAt, WorkerStarting)
}

func (s *PeerStore) reserveWorker(
	ctx context.Context,
	reservation WorkerReservation,
	maxActive int,
	observedAt time.Time,
	initialStatus WorkerStatus,
) (WorkerReservation, error) {
	if err := validateNewWorkerReservation(reservation); err != nil {
		return WorkerReservation{}, err
	}
	if initialStatus != WorkerReserved && initialStatus != WorkerStarting {
		return WorkerReservation{}, errors.New("initial worker status is invalid")
	}
	if maxActive < 1 {
		return WorkerReservation{}, errors.New("maxActive must be positive")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerReservation{}, err
	}
	reservation.Status = initialStatus
	if initialStatus == WorkerStarting {
		reservation.RetryTarget = WorkerPending
	}
	reservation.CreatedAt = timestamp
	reservation.UpdatedAt = timestamp

	var stored WorkerReservation
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err = queryWorker(ctx, connection, reservation.WorkerKey)
		if err == nil {
			if !sameWorkerReservation(stored, reservation) {
				return ErrWorkerReservationConflict
			}
			if stored.Status == initialStatus {
				return nil
			}
			return fmt.Errorf(
				"%w: existing worker is %s, requested initial state is %s",
				ErrWorkerTransition,
				stored.Status,
				initialStatus,
			)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if err := requireWorkerSlot(ctx, connection, maxActive); err != nil {
			return err
		}
		reservation.Revision, err = nextWorkerRevision(ctx, connection)
		if err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO worker_reservations(
    controller_id, tree_id, agent_id, parent_agent_id, device_id,
	task_name, prompt_digest, workspace_path, profile_version, status, retry_target, revision, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			reservation.ControllerID,
			reservation.TreeID,
			reservation.AgentID,
			reservation.ParentAgentID,
			reservation.DeviceID,
			reservation.TaskName,
			reservation.PromptDigest,
			reservation.WorkspacePath,
			reservation.ProfileVersion,
			reservation.Status,
			reservation.RetryTarget,
			reservation.Revision,
			reservation.CreatedAt,
			reservation.UpdatedAt,
		); err != nil {
			return fmt.Errorf("create worker reservation: %w", err)
		}
		stored = reservation
		return nil
	})
	return stored, err
}

func (s *PeerStore) BeginWorkerStart(
	ctx context.Context,
	key WorkerKey,
	maxActive int,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.transitionWorker(ctx, key, maxActive, observedAt, []WorkerStatus{
		WorkerReserved,
		WorkerPending,
		WorkerIdle,
		WorkerInterrupted,
	}, WorkerStarting, "", "")
}

// RestoreWorkerPendingAfterUnsent preserves an initial worker for an
// idempotent retry without consuming an active worker slot.
func (s *PeerStore) RestoreWorkerPendingAfterUnsent(
	ctx context.Context,
	key WorkerKey,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerStarting, WorkerPreflight, WorkerReady},
		WorkerPending,
		"",
		"",
	)
}

// RestoreWorkerIdleAfterUnsent makes a follow-up retryable after its
// thread/resume, MCP preflight, or turn/start request was never written.
func (s *PeerStore) RestoreWorkerIdleAfterUnsent(
	ctx context.Context,
	key WorkerKey,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerStarting, WorkerPreflight, WorkerReady},
		WorkerIdle,
		"",
		"",
	)
}

func (s *PeerStore) AttachWorkerThread(
	ctx context.Context,
	key WorkerKey,
	threadID string,
	observedAt time.Time,
) (WorkerReservation, error) {
	if err := identity.ValidateID(threadID); err != nil {
		return WorkerReservation{}, fmt.Errorf("codexThreadId %w", err)
	}
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerStarting},
		WorkerPreflight,
		threadID,
		"",
	)
}

func (s *PeerStore) MarkWorkerReady(
	ctx context.Context,
	key WorkerKey,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerPreflight},
		WorkerReady,
		"",
		"",
	)
}

func (s *PeerStore) MarkWorkerRunning(
	ctx context.Context,
	key WorkerKey,
	turnID string,
	observedAt time.Time,
) (WorkerReservation, error) {
	if err := identity.ValidateID(turnID); err != nil {
		return WorkerReservation{}, fmt.Errorf("turnId %w", err)
	}
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerReady},
		WorkerRunning,
		"",
		turnID,
	)
}

func (s *PeerStore) MarkWorkerIdle(
	ctx context.Context,
	key WorkerKey,
	observedAt time.Time,
) (WorkerReservation, error) {
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerRunning, WorkerReady, WorkerInterrupted},
		WorkerIdle,
		"",
		"",
	)
}

func (s *PeerStore) FailWorker(
	ctx context.Context,
	key WorkerKey,
	failureCode string,
	observedAt time.Time,
) (WorkerReservation, error) {
	if err := validateFailureCode(failureCode); err != nil {
		return WorkerReservation{}, err
	}
	if failureCode == "" {
		return WorkerReservation{}, errors.New("failureCode is required")
	}
	return s.transitionWorker(
		ctx,
		key,
		0,
		observedAt,
		[]WorkerStatus{WorkerReserved, WorkerPending, WorkerStarting, WorkerPreflight, WorkerReady, WorkerRunning, WorkerInterrupted},
		WorkerFailed,
		"",
		failureCode,
	)
}

func (s *PeerStore) GetWorker(ctx context.Context, key WorkerKey) (WorkerReservation, error) {
	if err := key.Validate(); err != nil {
		return WorkerReservation{}, err
	}
	return queryWorker(ctx, s.db, key)
}

func (s *PeerStore) ListWorkers(ctx context.Context) ([]WorkerReservation, error) {
	rows, err := s.db.QueryContext(ctx, workerSelect+`
ORDER BY created_at, controller_id, tree_id, agent_id
`)
	if err != nil {
		return nil, fmt.Errorf("list worker reservations: %w", err)
	}
	defer rows.Close()
	workers := make([]WorkerReservation, 0)
	for rows.Next() {
		worker, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list worker reservations: %w", err)
	}
	return workers, nil
}

func (s *PeerStore) WorkerForThread(
	ctx context.Context,
	controllerID, threadID string,
) (WorkerReservation, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return WorkerReservation{}, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(threadID); err != nil {
		return WorkerReservation{}, fmt.Errorf("threadId %w", err)
	}
	return queryWorkerByThread(ctx, s.db, controllerID, threadID)
}

func (s *PeerStore) transitionWorker(
	ctx context.Context,
	key WorkerKey,
	maxActive int,
	observedAt time.Time,
	from []WorkerStatus,
	to WorkerStatus,
	threadID, detail string,
) (WorkerReservation, error) {
	if err := key.Validate(); err != nil {
		return WorkerReservation{}, err
	}
	if to == WorkerInterrupted {
		return WorkerReservation{}, errors.New("interrupted workers require recovery-specific state")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerReservation{}, err
	}
	var worker WorkerReservation
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		worker, err = queryWorker(ctx, connection, key)
		if err != nil {
			return err
		}
		if worker.Status == to && transitionAlreadyApplied(worker, threadID, detail) {
			return nil
		}
		if !slices.Contains(from, worker.Status) {
			return fmt.Errorf("%w: cannot move from %s to %s", ErrWorkerTransition, worker.Status, to)
		}
		if to == WorkerPreflight {
			owner, ownerErr := queryWorkerByThread(ctx, connection, key.ControllerID, threadID)
			if ownerErr == nil && owner.WorkerKey != key {
				return ErrWorkerReservationConflict
			}
			if ownerErr != nil && !errors.Is(ownerErr, ErrNotFound) {
				return ownerErr
			}
		}
		if to == WorkerStarting &&
			(worker.Status == WorkerPending || worker.Status == WorkerIdle || worker.Status == WorkerInterrupted) {
			if maxActive < 1 {
				return errors.New("maxActive must be positive")
			}
			if err := requireWorkerSlot(ctx, connection, maxActive); err != nil {
				return err
			}
		}
		if (to == WorkerPending || to == WorkerIdle) &&
			(worker.Status == WorkerStarting || worker.Status == WorkerPreflight || worker.Status == WorkerReady) &&
			worker.RetryTarget != to {
			return fmt.Errorf(
				"%w: worker retry target is %s, cannot restore to %s",
				ErrWorkerTransition,
				worker.RetryTarget,
				to,
			)
		}
		timestamp = max(timestamp, worker.UpdatedAt)
		fromStatus := worker.Status
		worker.Status = to
		worker.UpdatedAt = timestamp
		worker.Revision, err = nextWorkerRevision(ctx, connection)
		if err != nil {
			return err
		}
		worker.FailureCode = ""
		switch to {
		case WorkerStarting:
			switch fromStatus {
			case WorkerReserved, WorkerPending:
				worker.RetryTarget = WorkerPending
			case WorkerIdle, WorkerInterrupted:
				worker.RetryTarget = WorkerIdle
			}
			worker.ActiveTurnID = ""
		case WorkerPreflight:
			worker.CodexThreadID = threadID
			worker.ActiveTurnID = ""
		case WorkerReady:
			worker.ActiveTurnID = ""
		case WorkerRunning:
			worker.RetryTarget = ""
			worker.ActiveTurnID = detail
		case WorkerIdle:
			worker.RetryTarget = ""
			worker.ActiveTurnID = ""
		case WorkerFailed:
			worker.RetryTarget = ""
			worker.ActiveTurnID = ""
			worker.FailureCode = detail
		case WorkerReserved, WorkerPending:
			worker.RetryTarget = ""
			worker.ActiveTurnID = ""
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
    codex_thread_id = ?, status = ?, retry_target = ?, active_turn_id = ?, failure_code = ?, revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`,
			worker.CodexThreadID,
			worker.Status,
			worker.RetryTarget,
			worker.ActiveTurnID,
			worker.FailureCode,
			worker.Revision,
			worker.UpdatedAt,
			worker.ControllerID,
			worker.TreeID,
			worker.AgentID,
		); err != nil {
			return fmt.Errorf("update worker lifecycle: %w", err)
		}
		return nil
	})
	return worker, err
}

func transitionAlreadyApplied(worker WorkerReservation, threadID, detail string) bool {
	switch worker.Status {
	case WorkerPreflight:
		return worker.CodexThreadID == threadID
	case WorkerReady:
		return true
	case WorkerRunning:
		return worker.ActiveTurnID == detail
	case WorkerFailed:
		return worker.FailureCode == detail
	case WorkerReserved, WorkerPending, WorkerStarting, WorkerIdle:
		return true
	default:
		return false
	}
}

func nextWorkerRevision(ctx context.Context, connection *sql.Conn) (uint64, error) {
	var revision uint64
	if err := connection.QueryRowContext(ctx, `
UPDATE peer_metadata SET worker_revision = worker_revision + 1
WHERE singleton = 1 AND worker_revision < 9223372036854775807
RETURNING worker_revision
`).Scan(&revision); errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("worker revision is exhausted")
	} else if err != nil {
		return 0, fmt.Errorf("allocate worker revision: %w", err)
	}
	return revision, nil
}

func requireWorkerSlot(ctx context.Context, queryer rowQueryer, maxActive int) error {
	var active int
	if err := queryer.QueryRowContext(ctx, `
SELECT count(*) FROM worker_reservations
WHERE status IN ('reserved', 'starting', 'preflight', 'ready', 'running')
`).Scan(&active); err != nil {
		return fmt.Errorf("count active worker reservations: %w", err)
	}
	if active >= maxActive {
		return ErrWorkerBusy
	}
	return nil
}

func queryWorker(
	ctx context.Context,
	queryer rowQueryer,
	key WorkerKey,
) (WorkerReservation, error) {
	return scanWorker(queryer.QueryRowContext(ctx, workerSelect+`
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, key.ControllerID, key.TreeID, key.AgentID))
}

func queryWorkerByThread(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, threadID string,
) (WorkerReservation, error) {
	return scanWorker(queryer.QueryRowContext(ctx, workerSelect+`
WHERE controller_id = ? AND codex_thread_id = ?
`, controllerID, threadID))
}

const workerSelect = `
SELECT controller_id, tree_id, agent_id, parent_agent_id, device_id,
	   task_name, prompt_digest, workspace_path, codex_thread_id, profile_version,
       status, retry_target, active_turn_id, failure_code, revision, created_at, updated_at
FROM worker_reservations
`

func scanWorker(scanner rowScanner) (WorkerReservation, error) {
	var worker WorkerReservation
	if err := scanner.Scan(
		&worker.ControllerID,
		&worker.TreeID,
		&worker.AgentID,
		&worker.ParentAgentID,
		&worker.DeviceID,
		&worker.TaskName,
		&worker.PromptDigest,
		&worker.WorkspacePath,
		&worker.CodexThreadID,
		&worker.ProfileVersion,
		&worker.Status,
		&worker.RetryTarget,
		&worker.ActiveTurnID,
		&worker.FailureCode,
		&worker.Revision,
		&worker.CreatedAt,
		&worker.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return WorkerReservation{}, ErrNotFound
	} else if err != nil {
		return WorkerReservation{}, fmt.Errorf("load worker reservation: %w", err)
	}
	if err := worker.Validate(); err != nil {
		return WorkerReservation{}, fmt.Errorf("stored worker reservation is invalid: %w", err)
	}
	return worker, nil
}

func (k WorkerKey) Validate() error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: k.ControllerID},
		{name: "treeId", value: k.TreeID},
		{name: "agentId", value: k.AgentID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

func (w WorkerReservation) Validate() error {
	if err := w.WorkerKey.Validate(); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "parentAgentId", value: w.ParentAgentID},
		{name: "deviceId", value: w.DeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if strings.TrimSpace(w.TaskName) == "" || len(w.TaskName) > maximumWorkerTaskName {
		return fmt.Errorf("taskName must contain from 1 through %d bytes", maximumWorkerTaskName)
	}
	if decoded, err := hex.DecodeString(w.PromptDigest); err != nil || len(decoded) != 32 {
		return errors.New("promptDigest must be a lowercase SHA-256 digest")
	}
	if w.PromptDigest != strings.ToLower(w.PromptDigest) {
		return errors.New("promptDigest must be a lowercase SHA-256 digest")
	}
	if !filepath.IsAbs(w.WorkspacePath) || len(w.WorkspacePath) > maximumWorkspacePath {
		return errors.New("workspacePath must be a bounded absolute path")
	}
	if w.CodexThreadID != "" {
		if err := identity.ValidateID(w.CodexThreadID); err != nil {
			return fmt.Errorf("codexThreadId %w", err)
		}
	}
	if w.ProfileVersion < 1 {
		return errors.New("profileVersion must be positive")
	}
	if !w.Status.valid() {
		return fmt.Errorf("unsupported worker status %q", w.Status)
	}
	if w.ActiveTurnID != "" {
		if err := identity.ValidateID(w.ActiveTurnID); err != nil {
			return fmt.Errorf("activeTurnId %w", err)
		}
	}
	if err := validateFailureCode(w.FailureCode); err != nil {
		return err
	}
	switch w.Status {
	case WorkerReserved:
		if w.CodexThreadID != "" || w.RetryTarget != "" || w.ActiveTurnID != "" || w.FailureCode != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerPending:
		if w.ActiveTurnID != "" || w.FailureCode != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
		if w.RetryTarget != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerStarting:
		if (w.RetryTarget != WorkerPending && w.RetryTarget != WorkerIdle) ||
			w.ActiveTurnID != "" || w.FailureCode != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerPreflight:
		if w.CodexThreadID == "" ||
			(w.RetryTarget != WorkerPending && w.RetryTarget != WorkerIdle) ||
			w.ActiveTurnID != "" || w.FailureCode != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerReady, WorkerIdle:
		if w.CodexThreadID == "" || w.ActiveTurnID != "" || w.FailureCode != "" ||
			(w.Status == WorkerReady && w.RetryTarget != WorkerPending && w.RetryTarget != WorkerIdle) ||
			(w.Status == WorkerIdle && w.RetryTarget != "") {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerRunning:
		if w.CodexThreadID == "" || w.ActiveTurnID == "" || w.FailureCode != "" || w.RetryTarget != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerInterrupted:
		if w.CodexThreadID == "" || w.FailureCode == "" || w.RetryTarget != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	case WorkerFailed:
		if w.ActiveTurnID != "" || w.FailureCode == "" || w.RetryTarget != "" {
			return errors.New("worker lifecycle details do not match its status")
		}
	}
	if w.CreatedAt < 0 || w.UpdatedAt < w.CreatedAt {
		return errors.New("worker timestamps are invalid")
	}
	if w.Revision == 0 || w.Revision >= uint64(1<<63) {
		return errors.New("worker revision is invalid")
	}
	return nil
}

func validateNewWorkerReservation(reservation WorkerReservation) error {
	if reservation.Status != "" || reservation.RetryTarget != "" || reservation.CodexThreadID != "" ||
		reservation.ActiveTurnID != "" || reservation.FailureCode != "" ||
		reservation.Revision != 0 || reservation.CreatedAt != 0 || reservation.UpdatedAt != 0 {
		return errors.New("new worker reservation must not contain lifecycle state")
	}
	reservation.Status = WorkerReserved
	reservation.Revision = 1
	return reservation.Validate()
}

func validateFailureCode(code string) error {
	if len(code) > maximumWorkerFailureCode || strings.ContainsAny(code, "\r\n\x00") {
		return fmt.Errorf("failureCode must contain at most %d safe bytes", maximumWorkerFailureCode)
	}
	return nil
}

func (s WorkerStatus) valid() bool {
	switch s {
	case WorkerReserved, WorkerPending, WorkerStarting, WorkerPreflight, WorkerReady, WorkerRunning, WorkerIdle, WorkerInterrupted, WorkerFailed:
		return true
	default:
		return false
	}
}

func sameWorkerReservation(stored, requested WorkerReservation) bool {
	return stored.WorkerKey == requested.WorkerKey &&
		stored.ParentAgentID == requested.ParentAgentID &&
		stored.DeviceID == requested.DeviceID &&
		stored.TaskName == requested.TaskName &&
		stored.PromptDigest == requested.PromptDigest &&
		stored.WorkspacePath == requested.WorkspacePath &&
		stored.ProfileVersion == requested.ProfileVersion
}
