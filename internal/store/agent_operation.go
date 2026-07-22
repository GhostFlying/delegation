package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

type AgentOperationIntent struct {
	Source        control.PrincipalIdentity
	OperationID   string
	AgentID       string
	Action        protocol.AgentOperationAction
	PayloadDigest [sha256.Size]byte
}

type AgentOperationKey struct {
	ControllerID  string
	TreeID        string
	SourceAgentID string
	OperationID   string
}

type AgentOperationReceipt struct {
	Key            AgentOperationKey
	AgentID        string
	TargetDeviceID string
	Action         protocol.AgentOperationAction
	PayloadDigest  [sha256.Size]byte
	Outcome        protocol.AgentOperationOutcome
	FailureCode    string
	CreatedAt      int64
	UpdatedAt      int64
}

func (s *Store) BeginAgentOperation(
	ctx context.Context,
	intent AgentOperationIntent,
	createdAt time.Time,
) (AgentOperationReceipt, error) {
	if err := validateAgentOperationIntent(intent); err != nil {
		return AgentOperationReceipt{}, err
	}
	timestamp, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return AgentOperationReceipt{}, err
	}
	key := AgentOperationKey{
		ControllerID:  intent.Source.ControllerID,
		TreeID:        intent.Source.TreeID,
		SourceAgentID: intent.Source.AgentID,
		OperationID:   intent.OperationID,
	}
	var receipt AgentOperationReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if err := authorizeAgentOperationSource(ctx, connection, intent.Source); err != nil {
			return err
		}
		receipt, err = queryAgentOperationReceipt(ctx, connection, key)
		if err == nil {
			if !agentOperationMatchesIntent(receipt, intent) {
				return fmt.Errorf("%w: operationId already identifies another request", ErrConflict)
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		target, err := queryStartedDirectDescendant(ctx, connection, intent.Source, intent.AgentID)
		if err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO agent_operation_receipts(
    controller_id, tree_id, source_agent_id, operation_id, action, agent_id,
    payload_digest, outcome, failure_code, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
`, key.ControllerID, key.TreeID, key.SourceAgentID, key.OperationID, intent.Action,
			intent.AgentID, intent.PayloadDigest[:], protocol.AgentOperationOutcomePending,
			timestamp, timestamp); err != nil {
			return fmt.Errorf("create agent operation receipt: %w", err)
		}
		receipt = AgentOperationReceipt{
			Key:            key,
			AgentID:        intent.AgentID,
			TargetDeviceID: target.Agent.Principal.DeviceID,
			Action:         intent.Action,
			PayloadDigest:  intent.PayloadDigest,
			Outcome:        protocol.AgentOperationOutcomePending,
			CreatedAt:      timestamp,
			UpdatedAt:      timestamp,
		}
		return nil
	})
	return receipt, err
}

func (s *Store) GetAgentOperation(
	ctx context.Context,
	source control.PrincipalIdentity,
	operationID string,
) (AgentOperationReceipt, error) {
	if err := source.Validate(); err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("source: %w", err)
	}
	if err := identity.ValidateID(operationID); err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("operationId %w", err)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("begin agent operation lookup: %w", err)
	}
	defer transaction.Rollback()
	if err := authorizeAgentOperationSource(ctx, transaction, source); err != nil {
		return AgentOperationReceipt{}, err
	}
	receipt, err := queryAgentOperationReceipt(ctx, transaction, AgentOperationKey{
		ControllerID:  source.ControllerID,
		TreeID:        source.TreeID,
		SourceAgentID: source.AgentID,
		OperationID:   operationID,
	})
	if err != nil {
		return AgentOperationReceipt{}, err
	}
	if err := transaction.Commit(); err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("commit agent operation lookup: %w", err)
	}
	return receipt, nil
}

func (s *Store) FinishAgentOperation(
	ctx context.Context,
	key AgentOperationKey,
	outcome protocol.AgentOperationOutcome,
	failureCode string,
	observedAt time.Time,
) (AgentOperationReceipt, error) {
	if err := key.Validate(); err != nil {
		return AgentOperationReceipt{}, err
	}
	if outcome == protocol.AgentOperationOutcomePending {
		return AgentOperationReceipt{}, errors.New("agent operation terminal outcome is invalid")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return AgentOperationReceipt{}, err
	}
	var receipt AgentOperationReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		receipt, err = queryAgentOperationReceipt(ctx, connection, key)
		if err != nil {
			return err
		}
		receipt, err = finishAgentOperationTx(
			ctx, connection, receipt, outcome, failureCode, timestamp,
		)
		return err
	})
	return receipt, err
}

// QueueAgentMessageAndFinishOperation commits the worker mailbox delivery and
// the corresponding terminal operation receipt as one durable state change.
func (s *Store) QueueAgentMessageAndFinishOperation(
	ctx context.Context,
	key AgentOperationKey,
	message string,
	observedAt time.Time,
) (AgentOperationReceipt, MailboxDelivery, error) {
	if err := key.Validate(); err != nil {
		return AgentOperationReceipt{}, MailboxDelivery{}, err
	}
	if err := protocol.ValidateMailboxMessage(message); err != nil {
		return AgentOperationReceipt{}, MailboxDelivery{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return AgentOperationReceipt{}, MailboxDelivery{}, err
	}
	digest := sha256.Sum256([]byte(message))
	var (
		receipt  AgentOperationReceipt
		delivery MailboxDelivery
	)
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		receipt, err = queryAgentOperationReceipt(ctx, connection, key)
		if err != nil {
			return err
		}
		if receipt.Action != protocol.AgentOperationSend || receipt.PayloadDigest != digest {
			return fmt.Errorf("%w: queued message does not match agent operation", ErrConflict)
		}
		if receipt.Outcome == protocol.AgentOperationOutcomeQueued {
			return nil
		}
		if receipt.Outcome != protocol.AgentOperationOutcomePending {
			return fmt.Errorf("%w: agent operation already has a terminal result", ErrConflict)
		}
		source, err := queryPrincipal(
			ctx, connection, key.ControllerID, key.TreeID, key.SourceAgentID,
		)
		if err != nil {
			return err
		}
		if err := authorizeAgentOperationSource(ctx, connection, source.Identity()); err != nil {
			return err
		}
		delivery, err = sendMailboxMessageTx(
			ctx,
			connection,
			source,
			protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: receipt.AgentID},
			key.OperationID,
			message,
			timestamp,
		)
		if err != nil {
			return err
		}
		receipt, err = finishAgentOperationTx(
			ctx,
			connection,
			receipt,
			protocol.AgentOperationOutcomeQueued,
			"",
			timestamp,
		)
		return err
	})
	return receipt, delivery, err
}

func finishAgentOperationTx(
	ctx context.Context,
	connection *sql.Conn,
	receipt AgentOperationReceipt,
	outcome protocol.AgentOperationOutcome,
	failureCode string,
	timestamp int64,
) (AgentOperationReceipt, error) {
	if err := outcome.Validate(receipt.Action, failureCode); err != nil {
		return AgentOperationReceipt{}, err
	}
	if receipt.Outcome == outcome {
		if receipt.FailureCode != failureCode {
			return AgentOperationReceipt{}, fmt.Errorf(
				"%w: agent operation terminal result differs", ErrConflict,
			)
		}
		return receipt, nil
	}
	if receipt.Outcome != protocol.AgentOperationOutcomePending {
		return AgentOperationReceipt{}, fmt.Errorf(
			"%w: agent operation already has a terminal result", ErrConflict,
		)
	}
	if timestamp < receipt.CreatedAt {
		return AgentOperationReceipt{}, errors.New("observedAt precedes agent operation creation")
	}
	result, err := connection.ExecContext(ctx, `
UPDATE agent_operation_receipts
SET outcome = ?, failure_code = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND operation_id = ?
  AND outcome = ?
`, outcome, failureCode, timestamp, receipt.Key.ControllerID, receipt.Key.TreeID,
		receipt.Key.SourceAgentID, receipt.Key.OperationID, protocol.AgentOperationOutcomePending)
	if err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("finish agent operation: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("count finished agent operations: %w", err)
	}
	if updated != 1 {
		return AgentOperationReceipt{}, fmt.Errorf(
			"%w: agent operation changed while finishing", ErrConflict,
		)
	}
	receipt.Outcome = outcome
	receipt.FailureCode = failureCode
	receipt.UpdatedAt = timestamp
	return receipt, nil
}

func (k AgentOperationKey) Validate() error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: k.ControllerID},
		{name: "treeId", value: k.TreeID},
		{name: "sourceAgentId", value: k.SourceAgentID},
		{name: "operationId", value: k.OperationID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

func validateAgentOperationIntent(intent AgentOperationIntent) error {
	if err := intent.Source.Validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if err := identity.ValidateID(intent.OperationID); err != nil {
		return fmt.Errorf("operationId %w", err)
	}
	if err := identity.ValidateID(intent.AgentID); err != nil {
		return fmt.Errorf("agentId %w", err)
	}
	return intent.Action.Validate()
}

func authorizeAgentOperationSource(
	ctx context.Context,
	queryer rowQueryer,
	source control.PrincipalIdentity,
) error {
	principal, err := authorizePrincipal(
		ctx, queryer, source, control.CapabilityAgentManageDescendants,
	)
	if err != nil {
		return err
	}
	if principal.ParentAgentID != "" {
		return ErrAuthorizationDenied
	}
	return nil
}

func queryStartedDirectDescendant(
	ctx context.Context,
	queryer rowQueryer,
	source control.PrincipalIdentity,
	agentID string,
) (AgentSpawnReceipt, error) {
	receipt, err := scanAgentSpawnReceipt(queryer.QueryRowContext(ctx, agentSpawnSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.agent_id = ?
`, source.ControllerID, source.TreeID, agentID))
	if err != nil {
		return AgentSpawnReceipt{}, err
	}
	if receipt.Agent.Principal.ParentAgentID != source.AgentID {
		return AgentSpawnReceipt{}, ErrAuthorizationDenied
	}
	if receipt.Agent.Status != protocol.AgentSpawnStarted {
		return AgentSpawnReceipt{}, fmt.Errorf("%w: managed agent has not started", ErrConflict)
	}
	return receipt, nil
}

func agentOperationMatchesIntent(receipt AgentOperationReceipt, intent AgentOperationIntent) bool {
	return receipt.Key.ControllerID == intent.Source.ControllerID &&
		receipt.Key.TreeID == intent.Source.TreeID &&
		receipt.Key.SourceAgentID == intent.Source.AgentID &&
		receipt.Key.OperationID == intent.OperationID &&
		receipt.AgentID == intent.AgentID &&
		receipt.Action == intent.Action &&
		receipt.PayloadDigest == intent.PayloadDigest
}

func queryAgentOperationReceipt(
	ctx context.Context,
	queryer rowQueryer,
	key AgentOperationKey,
) (AgentOperationReceipt, error) {
	return scanAgentOperationReceipt(queryer.QueryRowContext(ctx, agentOperationSelect+`
WHERE o.controller_id = ? AND o.tree_id = ? AND o.source_agent_id = ? AND o.operation_id = ?
`, key.ControllerID, key.TreeID, key.SourceAgentID, key.OperationID))
}

func scanAgentOperationReceipt(scanner rowScanner) (AgentOperationReceipt, error) {
	var receipt AgentOperationReceipt
	var digest []byte
	var targetParentAgentID string
	err := scanner.Scan(
		&receipt.Key.ControllerID,
		&receipt.Key.TreeID,
		&receipt.Key.SourceAgentID,
		&receipt.Key.OperationID,
		&receipt.Action,
		&receipt.AgentID,
		&targetParentAgentID,
		&receipt.TargetDeviceID,
		&digest,
		&receipt.Outcome,
		&receipt.FailureCode,
		&receipt.CreatedAt,
		&receipt.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentOperationReceipt{}, ErrNotFound
	}
	if err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("load agent operation receipt: %w", err)
	}
	if len(digest) != sha256.Size {
		return AgentOperationReceipt{}, errors.New("stored agent operation payload digest is invalid")
	}
	copy(receipt.PayloadDigest[:], digest)
	if err := receipt.Key.Validate(); err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("stored agent operation key is invalid: %w", err)
	}
	if targetParentAgentID != receipt.Key.SourceAgentID {
		return AgentOperationReceipt{}, errors.New("stored agent operation target is not a direct descendant")
	}
	if err := identity.ValidateID(receipt.TargetDeviceID); err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("stored agent operation targetDeviceId %w", err)
	}
	result := protocol.AgentOperationResult{
		OperationID: receipt.Key.OperationID,
		AgentID:     receipt.AgentID,
		Action:      receipt.Action,
		Outcome:     receipt.Outcome,
		FailureCode: receipt.FailureCode,
	}
	if err := result.Validate(); err != nil {
		return AgentOperationReceipt{}, fmt.Errorf("stored agent operation receipt is invalid: %w", err)
	}
	if receipt.CreatedAt < 0 || receipt.UpdatedAt < receipt.CreatedAt {
		return AgentOperationReceipt{}, errors.New("stored agent operation timestamps are invalid")
	}
	return receipt, nil
}

const agentOperationSelect = `
SELECT o.controller_id, o.tree_id, o.source_agent_id, o.operation_id, o.action, o.agent_id,
       p.parent_agent_id, p.device_id, o.payload_digest, o.outcome, o.failure_code,
       o.created_at, o.updated_at
FROM agent_operation_receipts AS o
JOIN principals AS p
  ON p.controller_id = o.controller_id AND p.tree_id = o.tree_id AND p.agent_id = o.agent_id
`
