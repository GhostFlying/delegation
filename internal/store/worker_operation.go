package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
)

var ErrWorkerOperationConflict = errors.New("worker operation conflicts with existing receipt")

type WorkerOperationAction string

const (
	WorkerOperationSend      WorkerOperationAction = "send"
	WorkerOperationFollowup  WorkerOperationAction = "followup"
	WorkerOperationInterrupt WorkerOperationAction = "interrupt"
)

type WorkerOperationStatus string

const (
	WorkerOperationPending   WorkerOperationStatus = "pending"
	WorkerOperationSucceeded WorkerOperationStatus = "succeeded"
	WorkerOperationFailed    WorkerOperationStatus = "failed"
)

type WorkerOperationOutcome string

const (
	WorkerOutcomePending     WorkerOperationOutcome = "pending"
	WorkerOutcomeQueued      WorkerOperationOutcome = "queued"
	WorkerOutcomeSteered     WorkerOperationOutcome = "steered"
	WorkerOutcomeStarted     WorkerOperationOutcome = "started"
	WorkerOutcomeInterrupted WorkerOperationOutcome = "interrupted"
	WorkerOutcomeFailed      WorkerOperationOutcome = "failed"
)

type WorkerOperationReceipt struct {
	WorkerKey
	OperationID   string
	Action        WorkerOperationAction
	PayloadDigest string
	Status        WorkerOperationStatus
	Outcome       WorkerOperationOutcome
	FailureCode   string
	CreatedAt     int64
	UpdatedAt     int64
}

// BeginWorkerOperation durably claims an operation before the app-server side
// effect begins. An exact replay returns the existing receipt with replay=true;
// callers must never repeat a pending receipt's side effect.
func (s *PeerStore) BeginWorkerOperation(
	ctx context.Context,
	operationID string,
	action WorkerOperationAction,
	key WorkerKey,
	payload []byte,
	observedAt time.Time,
) (receipt WorkerOperationReceipt, replay bool, err error) {
	if err := validateWorkerOperationIdentity(operationID, action, key); err != nil {
		return WorkerOperationReceipt{}, false, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerOperationReceipt{}, false, err
	}
	digest := sha256.Sum256(payload)
	desired := WorkerOperationReceipt{
		WorkerKey: key, OperationID: operationID, Action: action,
		PayloadDigest: hex.EncodeToString(digest[:]),
		Status:        WorkerOperationPending, Outcome: WorkerOutcomePending,
		CreatedAt: timestamp, UpdatedAt: timestamp,
	}
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		existing, queryErr := queryWorkerOperation(ctx, connection, key.ControllerID, operationID)
		if queryErr == nil {
			if !sameWorkerOperation(existing, desired) {
				return ErrWorkerOperationConflict
			}
			receipt = existing
			replay = true
			return nil
		}
		if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		if _, queryErr := queryWorker(ctx, connection, key); queryErr != nil {
			return queryErr
		}
		if _, execErr := connection.ExecContext(ctx, `
INSERT INTO worker_operation_receipts(
	controller_id, operation_id, tree_id, agent_id, action, payload_digest,
	status, outcome, failure_code, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			desired.ControllerID,
			desired.OperationID,
			desired.TreeID,
			desired.AgentID,
			desired.Action,
			desired.PayloadDigest,
			desired.Status,
			desired.Outcome,
			desired.FailureCode,
			desired.CreatedAt,
			desired.UpdatedAt,
		); execErr != nil {
			return fmt.Errorf("create worker operation receipt: %w", execErr)
		}
		receipt = desired
		return nil
	})
	return receipt, replay, err
}

func (s *PeerStore) CompleteWorkerOperation(
	ctx context.Context,
	key WorkerKey,
	operationID string,
	outcome WorkerOperationOutcome,
	failureCode string,
	observedAt time.Time,
) (WorkerOperationReceipt, error) {
	if err := key.Validate(); err != nil {
		return WorkerOperationReceipt{}, err
	}
	if err := identity.ValidateID(operationID); err != nil {
		return WorkerOperationReceipt{}, fmt.Errorf("operationId %w", err)
	}
	status := WorkerOperationSucceeded
	if outcome == WorkerOutcomeFailed {
		status = WorkerOperationFailed
	}
	if !outcome.terminal() {
		return WorkerOperationReceipt{}, fmt.Errorf("unsupported terminal worker operation outcome %q", outcome)
	}
	if err := validateFailureCode(failureCode); err != nil {
		return WorkerOperationReceipt{}, err
	}
	if status == WorkerOperationFailed && failureCode == "" {
		return WorkerOperationReceipt{}, errors.New("failureCode is required for failed worker operations")
	}
	if status == WorkerOperationSucceeded && failureCode != "" {
		return WorkerOperationReceipt{}, errors.New("successful worker operations cannot contain failureCode")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerOperationReceipt{}, err
	}
	var receipt WorkerOperationReceipt
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		receipt, err = queryWorkerOperation(ctx, connection, key.ControllerID, operationID)
		if err != nil {
			return err
		}
		if receipt.WorkerKey != key {
			return ErrWorkerOperationConflict
		}
		if receipt.Status != WorkerOperationPending {
			if receipt.Status == status && receipt.Outcome == outcome && receipt.FailureCode == failureCode {
				return nil
			}
			return ErrWorkerOperationConflict
		}
		receipt.Status = status
		receipt.Outcome = outcome
		receipt.FailureCode = failureCode
		receipt.UpdatedAt = max(timestamp, receipt.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE worker_operation_receipts SET
	status = ?, outcome = ?, failure_code = ?, updated_at = ?
WHERE controller_id = ? AND operation_id = ?
`,
			receipt.Status,
			receipt.Outcome,
			receipt.FailureCode,
			receipt.UpdatedAt,
			receipt.ControllerID,
			receipt.OperationID,
		); execErr != nil {
			return fmt.Errorf("complete worker operation receipt: %w", execErr)
		}
		return nil
	})
	return receipt, err
}

func (s *PeerStore) GetWorkerOperation(
	ctx context.Context,
	controllerID, operationID string,
) (WorkerOperationReceipt, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return WorkerOperationReceipt{}, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(operationID); err != nil {
		return WorkerOperationReceipt{}, fmt.Errorf("operationId %w", err)
	}
	return queryWorkerOperation(ctx, s.db, controllerID, operationID)
}

func queryWorkerOperation(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, operationID string,
) (WorkerOperationReceipt, error) {
	return scanWorkerOperation(queryer.QueryRowContext(ctx, workerOperationSelect+`
WHERE controller_id = ? AND operation_id = ?
`, controllerID, operationID))
}

const workerOperationSelect = `
SELECT controller_id, tree_id, agent_id, operation_id, action, payload_digest,
	status, outcome, failure_code, created_at, updated_at
FROM worker_operation_receipts
`

func scanWorkerOperation(scanner rowScanner) (WorkerOperationReceipt, error) {
	var receipt WorkerOperationReceipt
	if err := scanner.Scan(
		&receipt.ControllerID,
		&receipt.TreeID,
		&receipt.AgentID,
		&receipt.OperationID,
		&receipt.Action,
		&receipt.PayloadDigest,
		&receipt.Status,
		&receipt.Outcome,
		&receipt.FailureCode,
		&receipt.CreatedAt,
		&receipt.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return WorkerOperationReceipt{}, ErrNotFound
	} else if err != nil {
		return WorkerOperationReceipt{}, fmt.Errorf("load worker operation receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return WorkerOperationReceipt{}, fmt.Errorf("stored worker operation receipt is invalid: %w", err)
	}
	return receipt, nil
}

func (r WorkerOperationReceipt) Validate() error {
	if err := validateWorkerOperationIdentity(r.OperationID, r.Action, r.WorkerKey); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(r.PayloadDigest)
	if err != nil || len(decoded) != sha256.Size || r.PayloadDigest != strings.ToLower(r.PayloadDigest) {
		return errors.New("payloadDigest must be a lowercase SHA-256 digest")
	}
	if !r.Status.valid() || !r.Outcome.valid() {
		return errors.New("worker operation status or outcome is invalid")
	}
	if err := validateFailureCode(r.FailureCode); err != nil {
		return err
	}
	switch r.Status {
	case WorkerOperationPending:
		if r.Outcome != WorkerOutcomePending || r.FailureCode != "" {
			return errors.New("pending worker operation details are invalid")
		}
	case WorkerOperationSucceeded:
		if !r.Outcome.successful() || r.FailureCode != "" {
			return errors.New("successful worker operation details are invalid")
		}
	case WorkerOperationFailed:
		if r.Outcome != WorkerOutcomeFailed || r.FailureCode == "" {
			return errors.New("failed worker operation details are invalid")
		}
	}
	if r.CreatedAt < 0 || r.UpdatedAt < r.CreatedAt {
		return errors.New("worker operation timestamps are invalid")
	}
	return nil
}

func validateWorkerOperationIdentity(
	operationID string,
	action WorkerOperationAction,
	key WorkerKey,
) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(operationID); err != nil {
		return fmt.Errorf("operationId %w", err)
	}
	if !action.valid() {
		return fmt.Errorf("unsupported worker operation action %q", action)
	}
	return nil
}

func sameWorkerOperation(stored, requested WorkerOperationReceipt) bool {
	return stored.WorkerKey == requested.WorkerKey &&
		stored.OperationID == requested.OperationID &&
		stored.Action == requested.Action &&
		stored.PayloadDigest == requested.PayloadDigest
}

func (a WorkerOperationAction) valid() bool {
	switch a {
	case WorkerOperationSend, WorkerOperationFollowup, WorkerOperationInterrupt:
		return true
	default:
		return false
	}
}

func (s WorkerOperationStatus) valid() bool {
	switch s {
	case WorkerOperationPending, WorkerOperationSucceeded, WorkerOperationFailed:
		return true
	default:
		return false
	}
}

func (o WorkerOperationOutcome) valid() bool {
	return o == WorkerOutcomePending || o.terminal()
}

func (o WorkerOperationOutcome) terminal() bool {
	return o.successful() || o == WorkerOutcomeFailed
}

func (o WorkerOperationOutcome) successful() bool {
	switch o {
	case WorkerOutcomeQueued, WorkerOutcomeSteered, WorkerOutcomeStarted, WorkerOutcomeInterrupted:
		return true
	case WorkerOutcomePending, WorkerOutcomeFailed:
		return false
	default:
		return false
	}
}
