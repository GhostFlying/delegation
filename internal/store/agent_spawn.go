package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

var ErrAgentLimit = errors.New("tree agent limit reached")

type AgentSpawnIntent struct {
	Source         control.PrincipalIdentity
	SpawnID        string
	AgentID        string
	TargetDeviceID string
	TaskName       string
	PromptDigest   [sha256.Size]byte
}

type AgentSpawnKey struct {
	ControllerID  string
	TreeID        string
	SourceAgentID string
	SpawnID       string
}

type AgentSpawnReceipt struct {
	Agent        protocol.AgentSummary
	PromptDigest [sha256.Size]byte
	CreatedAt    int64
	UpdatedAt    int64
}

type AgentPageRequest struct {
	AfterSequence uint64
	Limit         int
}

type AgentPage struct {
	Agents       []protocol.AgentSummary
	NextSequence uint64
}

func (s *Store) BeginAgentSpawn(
	ctx context.Context,
	intent AgentSpawnIntent,
	createdAt time.Time,
) (AgentSpawnReceipt, error) {
	if err := validateAgentSpawnIntent(intent); err != nil {
		return AgentSpawnReceipt{}, err
	}
	timestamp, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return AgentSpawnReceipt{}, err
	}
	principal := control.NewWorkerPrincipal(
		intent.Source.ControllerID,
		intent.Source.TreeID,
		intent.AgentID,
		intent.Source.AgentID,
		intent.TargetDeviceID,
	)
	capabilitiesJSON, err := json.Marshal(principal.Capabilities)
	if err != nil {
		return AgentSpawnReceipt{}, fmt.Errorf("encode worker capabilities: %w", err)
	}
	key := AgentSpawnKey{
		ControllerID:  intent.Source.ControllerID,
		TreeID:        intent.Source.TreeID,
		SourceAgentID: intent.Source.AgentID,
		SpawnID:       intent.SpawnID,
	}
	var receipt AgentSpawnReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if _, err := authorizePrincipal(ctx, connection, intent.Source, control.CapabilityAgentSpawn); err != nil {
			return err
		}
		receipt, err = queryAgentSpawnReceipt(ctx, connection, key)
		if err == nil {
			if !receiptMatchesIntent(receipt, intent) {
				return fmt.Errorf("%w: spawnId already identifies another request", ErrConflict)
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		device, err := queryDevice(ctx, connection, intent.Source.ControllerID, intent.TargetDeviceID)
		if err != nil {
			return fmt.Errorf("worker device: %w", err)
		}
		if !device.Online {
			return fmt.Errorf("%w: worker device must be online", ErrConflict)
		}
		var conflictingSpawn string
		err = connection.QueryRowContext(ctx, `
SELECT spawn_id
FROM agent_spawn_receipts
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND task_name = ?
`, intent.Source.ControllerID, intent.Source.TreeID, intent.Source.AgentID, intent.TaskName).Scan(&conflictingSpawn)
		if err == nil {
			return fmt.Errorf("%w: taskName already identifies another agent", ErrConflict)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check agent task name: %w", err)
		}
		var lastSequence uint64
		if err := connection.QueryRowContext(ctx, `
SELECT last_agent_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, intent.Source.ControllerID, intent.Source.TreeID).Scan(&lastSequence); errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return fmt.Errorf("load tree agent sequence: %w", err)
		}
		if lastSequence >= protocol.MaximumAgentsPerTree {
			return ErrAgentLimit
		}
		sequence := lastSequence + 1
		if _, err := connection.ExecContext(ctx, `
INSERT INTO principals(
    controller_id, tree_id, agent_id, parent_agent_id, device_id, capabilities_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
`, principal.ControllerID, principal.TreeID, principal.AgentID, principal.ParentAgentID,
			principal.DeviceID, string(capabilitiesJSON), timestamp); err != nil {
			return fmt.Errorf("create spawned worker principal: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO agent_spawn_receipts(
    controller_id, tree_id, sequence, source_agent_id, spawn_id, agent_id,
    target_device_id, task_name, prompt_digest, status, failure_code, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
`, principal.ControllerID, principal.TreeID, sequence, principal.ParentAgentID, intent.SpawnID,
			principal.AgentID, principal.DeviceID, intent.TaskName, intent.PromptDigest[:],
			protocol.AgentSpawnPending, timestamp, timestamp); err != nil {
			return fmt.Errorf("create agent spawn receipt: %w", err)
		}
		result, err := connection.ExecContext(ctx, `
UPDATE trees
SET last_agent_sequence = ?
WHERE controller_id = ? AND tree_id = ? AND last_agent_sequence = ?
`, sequence, principal.ControllerID, principal.TreeID, lastSequence)
		if err != nil {
			return fmt.Errorf("advance tree agent sequence: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("inspect tree agent sequence update: %w", err)
		} else if affected != 1 {
			return errors.New("tree agent sequence changed during spawn")
		}
		receipt = AgentSpawnReceipt{
			Agent: protocol.AgentSummary{
				SpawnID:   intent.SpawnID,
				Principal: principal.Identity(),
				TaskName:  intent.TaskName,
				Status:    protocol.AgentSpawnPending,
				Sequence:  sequence,
			},
			PromptDigest: intent.PromptDigest,
			CreatedAt:    timestamp,
			UpdatedAt:    timestamp,
		}
		return nil
	})
	return receipt, err
}

func (s *Store) MarkAgentSpawnStarted(
	ctx context.Context,
	key AgentSpawnKey,
	observedAt time.Time,
) (AgentSpawnReceipt, error) {
	return s.finishAgentSpawn(ctx, key, protocol.AgentSpawnStarted, "", observedAt)
}

func (s *Store) MarkAgentSpawnFailed(
	ctx context.Context,
	key AgentSpawnKey,
	failureCode string,
	observedAt time.Time,
) (AgentSpawnReceipt, error) {
	return s.finishAgentSpawn(ctx, key, protocol.AgentSpawnFailed, failureCode, observedAt)
}

func (s *Store) finishAgentSpawn(
	ctx context.Context,
	key AgentSpawnKey,
	status protocol.AgentSpawnStatus,
	failureCode string,
	observedAt time.Time,
) (AgentSpawnReceipt, error) {
	if err := key.Validate(); err != nil {
		return AgentSpawnReceipt{}, err
	}
	if status != protocol.AgentSpawnStarted && status != protocol.AgentSpawnFailed {
		return AgentSpawnReceipt{}, errors.New("agent spawn terminal status is invalid")
	}
	if err := status.Validate(failureCode); err != nil {
		return AgentSpawnReceipt{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return AgentSpawnReceipt{}, err
	}
	var receipt AgentSpawnReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		receipt, err = queryAgentSpawnReceipt(ctx, connection, key)
		if err != nil {
			return err
		}
		if receipt.Agent.Status == status {
			if receipt.Agent.FailureCode != failureCode {
				return fmt.Errorf("%w: agent terminal result differs", ErrConflict)
			}
			return nil
		}
		if receipt.Agent.Status != protocol.AgentSpawnPending {
			return fmt.Errorf("%w: agent already has a terminal result", ErrConflict)
		}
		if timestamp < receipt.CreatedAt {
			return errors.New("observedAt precedes agent creation")
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE agent_spawn_receipts
SET status = ?, failure_code = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND spawn_id = ?
`, status, failureCode, timestamp, key.ControllerID, key.TreeID, key.SourceAgentID, key.SpawnID); err != nil {
			return fmt.Errorf("finish agent spawn: %w", err)
		}
		receipt.Agent.Status = status
		receipt.Agent.FailureCode = failureCode
		receipt.UpdatedAt = timestamp
		return nil
	})
	return receipt, err
}

func (s *Store) ListAgents(
	ctx context.Context,
	source control.PrincipalIdentity,
	request AgentPageRequest,
) (AgentPage, error) {
	if err := source.Validate(); err != nil {
		return AgentPage{}, fmt.Errorf("source: %w", err)
	}
	params := protocol.ListAgentsParams{AfterSequence: request.AfterSequence, Limit: request.Limit}
	if err := params.Validate(); err != nil {
		return AgentPage{}, err
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentPage{}, fmt.Errorf("begin agent page: %w", err)
	}
	defer transaction.Rollback()
	if _, err := authorizePrincipal(ctx, transaction, source, control.CapabilityAgentManageDescendants); err != nil {
		return AgentPage{}, err
	}
	rows, err := transaction.QueryContext(ctx, agentSpawnSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.sequence > ?
ORDER BY r.sequence
LIMIT ?
`, source.ControllerID, source.TreeID, request.AfterSequence, request.Limit+1)
	if err != nil {
		return AgentPage{}, fmt.Errorf("list agents: %w", err)
	}
	receipts := make([]AgentSpawnReceipt, 0, request.Limit+1)
	for rows.Next() {
		receipt, err := scanAgentSpawnReceipt(rows)
		if err != nil {
			rows.Close()
			return AgentPage{}, err
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AgentPage{}, fmt.Errorf("list agents: %w", err)
	}
	if err := rows.Close(); err != nil {
		return AgentPage{}, fmt.Errorf("close agent page: %w", err)
	}
	page := AgentPage{Agents: make([]protocol.AgentSummary, 0, min(len(receipts), request.Limit))}
	for _, receipt := range receipts[:min(len(receipts), request.Limit)] {
		page.Agents = append(page.Agents, receipt.Agent)
	}
	if len(receipts) > request.Limit {
		page.NextSequence = page.Agents[len(page.Agents)-1].Sequence
	}
	if err := transaction.Commit(); err != nil {
		return AgentPage{}, fmt.Errorf("commit agent page: %w", err)
	}
	return page, nil
}

func (k AgentSpawnKey) Validate() error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: k.ControllerID},
		{name: "treeId", value: k.TreeID},
		{name: "sourceAgentId", value: k.SourceAgentID},
		{name: "spawnId", value: k.SpawnID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

func validateAgentSpawnIntent(intent AgentSpawnIntent) error {
	if err := intent.Source.Validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "spawnId", value: intent.SpawnID},
		{name: "agentId", value: intent.AgentID},
		{name: "targetDeviceId", value: intent.TargetDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := protocol.ValidateAgentTaskName(intent.TaskName); err != nil {
		return err
	}
	return nil
}

func receiptMatchesIntent(receipt AgentSpawnReceipt, intent AgentSpawnIntent) bool {
	return receipt.Agent.SpawnID == intent.SpawnID &&
		receipt.Agent.Principal.ControllerID == intent.Source.ControllerID &&
		receipt.Agent.Principal.TreeID == intent.Source.TreeID &&
		receipt.Agent.Principal.ParentAgentID == intent.Source.AgentID &&
		receipt.Agent.Principal.DeviceID == intent.TargetDeviceID &&
		receipt.Agent.TaskName == intent.TaskName &&
		receipt.PromptDigest == intent.PromptDigest
}

func queryAgentSpawnReceipt(
	ctx context.Context,
	queryer rowQueryer,
	key AgentSpawnKey,
) (AgentSpawnReceipt, error) {
	return scanAgentSpawnReceipt(queryer.QueryRowContext(ctx, agentSpawnSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.source_agent_id = ? AND r.spawn_id = ?
`, key.ControllerID, key.TreeID, key.SourceAgentID, key.SpawnID))
}

func scanAgentSpawnReceipt(scanner rowScanner) (AgentSpawnReceipt, error) {
	var receipt AgentSpawnReceipt
	var digest []byte
	var targetDeviceID string
	err := scanner.Scan(
		&receipt.Agent.SpawnID,
		&receipt.Agent.Principal.ControllerID,
		&receipt.Agent.Principal.TreeID,
		&receipt.Agent.Principal.AgentID,
		&receipt.Agent.Principal.ParentAgentID,
		&receipt.Agent.Principal.DeviceID,
		&targetDeviceID,
		&receipt.Agent.TaskName,
		&digest,
		&receipt.Agent.Status,
		&receipt.Agent.FailureCode,
		&receipt.Agent.Sequence,
		&receipt.CreatedAt,
		&receipt.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentSpawnReceipt{}, ErrNotFound
	}
	if err != nil {
		return AgentSpawnReceipt{}, fmt.Errorf("load agent spawn receipt: %w", err)
	}
	if len(digest) != sha256.Size {
		return AgentSpawnReceipt{}, errors.New("stored agent prompt digest is invalid")
	}
	if targetDeviceID != receipt.Agent.Principal.DeviceID {
		return AgentSpawnReceipt{}, errors.New("stored agent target device does not match its principal")
	}
	copy(receipt.PromptDigest[:], digest)
	if err := receipt.Agent.Validate(); err != nil {
		return AgentSpawnReceipt{}, fmt.Errorf("stored agent spawn receipt is invalid: %w", err)
	}
	if receipt.CreatedAt < 0 || receipt.UpdatedAt < receipt.CreatedAt {
		return AgentSpawnReceipt{}, errors.New("stored agent spawn timestamps are invalid")
	}
	return receipt, nil
}

const agentSpawnSelect = `
SELECT r.spawn_id, p.controller_id, p.tree_id, p.agent_id, p.parent_agent_id, p.device_id,
	   r.target_device_id,
       r.task_name, r.prompt_digest, r.status, r.failure_code, r.sequence, r.created_at, r.updated_at
FROM agent_spawn_receipts AS r
JOIN principals AS p
  ON p.controller_id = r.controller_id AND p.tree_id = r.tree_id AND p.agent_id = r.agent_id
`
