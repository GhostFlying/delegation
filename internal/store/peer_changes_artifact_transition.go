package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
)

// BeginWorkerFinalization durably fences a completed turn before any artifact
// capture begins. Replays for the same worker turn return the stable artifact
// ID, including after the broker acknowledgement made the worker terminal.
func (s *PeerStore) BeginWorkerFinalization(
	ctx context.Context,
	key WorkerKey,
	turnID string,
	target WorkerStatus,
	failureCode string,
	observedAt time.Time,
) (WorkerFinalization, error) {
	if err := validateFinalizationRequest(key, turnID, target, failureCode); err != nil {
		return WorkerFinalization{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerFinalization{}, err
	}

	var finalization WorkerFinalization
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		existing, queryErr := queryPeerChangesArtifactByTurn(ctx, connection, key, turnID)
		if queryErr == nil {
			if existing.CompletionTarget != target || existing.CompletionFailureCode != failureCode {
				return ErrChangesArtifactConflict
			}
			worker, workerErr := queryWorker(ctx, connection, key)
			if workerErr != nil {
				return workerErr
			}
			finalization = WorkerFinalization{Worker: worker, Artifact: existing}
			return nil
		}
		if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		worker, queryErr := queryWorker(ctx, connection, key)
		if queryErr != nil {
			return queryErr
		}
		if worker.Status != WorkerRunning && worker.Status != WorkerInterrupted {
			return fmt.Errorf(
				"%w: cannot finalize worker in %s state",
				ErrWorkerTransition,
				worker.Status,
			)
		}
		if worker.ActiveTurnID != turnID {
			return ErrChangesArtifactConflict
		}
		workspace, authorityErr := requireFinalizationWorkspace(ctx, connection, worker)
		if authorityErr != nil {
			return authorityErr
		}
		artifactID, identityErr := identity.NewID()
		if identityErr != nil {
			return fmt.Errorf("create changes artifact ID: %w", identityErr)
		}
		finalization, queryErr = beginWorkerFinalization(
			ctx,
			connection,
			worker,
			workspace,
			artifactID,
			target,
			failureCode,
			timestamp,
		)
		return queryErr
	})
	return finalization, err
}

// ReserveChangesArtifactPayload atomically reserves one retained artifact and
// its maximum prospective payload before the filesystem capture starts.
func (s *PeerStore) ReserveChangesArtifactPayload(
	ctx context.Context,
	key WorkerKey,
	artifactID string,
	bytes int64,
	observedAt time.Time,
) (ChangesArtifact, error) {
	if err := validateChangesArtifactIdentity(key, artifactID); err != nil {
		return ChangesArtifact{}, err
	}
	if bytes < 1 || bytes > MaximumChangesArtifactPayloadBytes {
		return ChangesArtifact{}, fmt.Errorf(
			"reserved payload bytes must be from 1 through %d",
			MaximumChangesArtifactPayloadBytes,
		)
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return ChangesArtifact{}, err
	}
	var artifact ChangesArtifact
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		artifact, err = queryPeerChangesArtifact(ctx, connection, key, artifactID)
		if err != nil {
			return err
		}
		if artifact.State != ChangesCapturePending {
			return ErrChangesArtifactTransition
		}
		if artifact.RetentionReserved {
			if artifact.ReservedBytes != bytes {
				return ErrChangesArtifactConflict
			}
			return nil
		}
		var retainedBytes int64
		if queryErr := connection.QueryRowContext(ctx, `
SELECT COALESCE(sum(reserved_bytes), 0)
FROM peer_changes_artifacts
WHERE retention_reserved = 1
`).Scan(&retainedBytes); queryErr != nil {
			return fmt.Errorf("inspect changes artifact quota: %w", queryErr)
		}
		if retainedBytes > MaximumRetainedChangesPayloadBytes-bytes {
			return ErrChangesArtifactQuota
		}
		artifact.RetentionReserved = true
		artifact.ReservedBytes = bytes
		artifact.UpdatedAt = max(timestamp, artifact.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_changes_artifacts SET
	retention_reserved = 1, reserved_bytes = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND artifact_id = ?
`, artifact.ReservedBytes, artifact.UpdatedAt,
			artifact.ControllerID, artifact.TreeID, artifact.AgentID, artifact.ArtifactID); execErr != nil {
			return fmt.Errorf("reserve changes artifact payload: %w", execErr)
		}
		return nil
	})
	return artifact, err
}

// CompleteChangesArtifactCapture records immutable publication metadata. The
// filesystem payload must already be durable under the controlled part names.
func (s *PeerStore) CompleteChangesArtifactCapture(
	ctx context.Context,
	key WorkerKey,
	artifactID string,
	result ChangesCaptureResult,
	observedAt time.Time,
) (ChangesArtifact, error) {
	if err := validateChangesArtifactIdentity(key, artifactID); err != nil {
		return ChangesArtifact{}, err
	}
	result.Parts = canonicalChangesParts(result.Parts)
	if err := validateChangesCaptureResult(result); err != nil {
		return ChangesArtifact{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return ChangesArtifact{}, err
	}
	resultWarningsJSON, err := json.Marshal(append([]string{}, result.ResultWarnings...))
	if err != nil {
		return ChangesArtifact{}, err
	}

	var artifact ChangesArtifact
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		artifact, err = queryPeerChangesArtifact(ctx, connection, key, artifactID)
		if err != nil {
			return err
		}
		if artifact.State != ChangesCapturePending {
			if sameChangesCapture(artifact, result) {
				return nil
			}
			return ErrChangesArtifactConflict
		}
		if validationErr := validateChangesCaptureForArtifact(artifact, result); validationErr != nil {
			return validationErr
		}

		worker, workerErr := queryWorker(ctx, connection, key)
		if workerErr != nil {
			return workerErr
		}
		if worker.Status != WorkerFinalizing || worker.ActiveTurnID != artifact.TurnID ||
			worker.WorkspaceID != artifact.WorkspaceID {
			return ErrChangesArtifactAuthority
		}

		artifact.State = ChangesPublishPending
		artifact.Status = result.Status
		artifact.ResultHeadOID = result.ResultHeadOID
		artifact.ResultSnapshotHash = result.ResultSnapshotHash
		artifact.ResultClean = result.ResultClean
		artifact.Parts = slices.Clone(result.Parts)
		artifact.ResultWarnings = slices.Clone(result.ResultWarnings)
		artifact.FailureCode = result.FailureCode
		artifact.PayloadBytes = changesPayloadBytes(result.Parts)
		artifact.UpdatedAt = max(timestamp, artifact.UpdatedAt)
		if result.Status == ChangesAvailable && artifact.PayloadBytes > 0 {
			artifact.ReservedBytes = artifact.PayloadBytes
		} else {
			artifact.RetentionReserved = false
			artifact.ReservedBytes = 0
		}
		if result.Status == ChangesCaptureFailed {
			worker.FinalTarget = WorkerFailed
			worker.FinalFailureCode = changesCaptureFailureCode
		}
		bundle, overlay := splitChangesParts(artifact.Parts)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_changes_artifacts SET
	state = ?, capture_status = ?, result_head_oid = ?, result_snapshot_hash = ?,
	result_clean = ?, bundle_part_name = ?, bundle_size_bytes = ?, bundle_sha256 = ?,
	overlay_part_name = ?, overlay_size_bytes = ?, overlay_sha256 = ?, result_warnings_json = ?,
	failure_code = ?, retention_reserved = ?, reserved_bytes = ?, payload_bytes = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND artifact_id = ?
`, artifact.State, artifact.Status, artifact.ResultHeadOID, artifact.ResultSnapshotHash,
			artifact.ResultClean, bundle.Name, bundle.SizeBytes, bundle.SHA256,
			overlay.Name, overlay.SizeBytes, overlay.SHA256, string(resultWarningsJSON),
			artifact.FailureCode, artifact.RetentionReserved, artifact.ReservedBytes,
			artifact.PayloadBytes, artifact.UpdatedAt,
			artifact.ControllerID, artifact.TreeID, artifact.AgentID, artifact.ArtifactID); execErr != nil {
			return fmt.Errorf("complete changes artifact capture: %w", execErr)
		}
		if result.Status == ChangesCaptureFailed {
			if _, execErr := connection.ExecContext(ctx, `
UPDATE worker_reservations SET final_target_status = ?, final_failure_code = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND
	status = 'finalizing' AND active_turn_id = ?
`, worker.FinalTarget, worker.FinalFailureCode,
				worker.ControllerID, worker.TreeID, worker.AgentID, worker.ActiveTurnID); execErr != nil {
				return fmt.Errorf("record failed changes capture target: %w", execErr)
			}
		}
		return nil
	})
	return artifact, err
}

// AcknowledgeChangesArtifact atomically records the broker sequence and makes
// the worker terminal. Exact ACK replay is safe; a different sequence fails.
func (s *PeerStore) AcknowledgeChangesArtifact(
	ctx context.Context,
	key WorkerKey,
	artifactID string,
	brokerSequence uint64,
	observedAt time.Time,
) (WorkerFinalization, error) {
	if err := validateChangesArtifactIdentity(key, artifactID); err != nil {
		return WorkerFinalization{}, err
	}
	if brokerSequence == 0 || brokerSequence > math.MaxInt64 {
		return WorkerFinalization{}, errors.New("brokerSequence must be a positive signed 64-bit integer")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkerFinalization{}, err
	}

	var finalization WorkerFinalization
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		artifact, queryErr := queryPeerChangesArtifact(ctx, connection, key, artifactID)
		if queryErr != nil {
			return queryErr
		}
		worker, queryErr := queryWorker(ctx, connection, key)
		if queryErr != nil {
			return queryErr
		}
		if artifact.State == ChangesPublished {
			if artifact.BrokerSequence != brokerSequence {
				return ErrChangesArtifactConflict
			}
			finalization = WorkerFinalization{Worker: worker, Artifact: artifact}
			return nil
		}
		if artifact.State != ChangesPublishPending {
			return ErrChangesArtifactTransition
		}
		if worker.Status != WorkerFinalizing || worker.ActiveTurnID != artifact.TurnID ||
			worker.WorkspaceID != artifact.WorkspaceID {
			return ErrChangesArtifactAuthority
		}
		worker.UpdatedAt = max(timestamp, worker.UpdatedAt)
		worker.Revision, queryErr = nextWorkerRevision(ctx, connection)
		if queryErr != nil {
			return queryErr
		}
		worker.Status = worker.FinalTarget
		worker.FailureCode = worker.FinalFailureCode
		worker.FinalTarget = ""
		worker.FinalFailureCode = ""
		worker.RetryTarget = ""
		if worker.Status != WorkerInterrupted {
			worker.ActiveTurnID = ""
		}
		if validationErr := worker.Validate(); validationErr != nil {
			return fmt.Errorf("terminal worker is invalid: %w", validationErr)
		}
		artifact.State = ChangesPublished
		artifact.BrokerSequence = brokerSequence
		artifact.UpdatedAt = max(timestamp, artifact.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_changes_artifacts SET state = ?, broker_sequence = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND artifact_id = ?
`, artifact.State, artifact.BrokerSequence, artifact.UpdatedAt,
			artifact.ControllerID, artifact.TreeID, artifact.AgentID, artifact.ArtifactID); execErr != nil {
			return fmt.Errorf("acknowledge changes artifact: %w", execErr)
		}
		if _, execErr := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = ?, retry_target = ?, active_turn_id = ?, failure_code = ?,
	final_target_status = '', final_failure_code = '', revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND
	status = 'finalizing' AND active_turn_id = ?
`, worker.Status, worker.RetryTarget, worker.ActiveTurnID, worker.FailureCode,
			worker.Revision, worker.UpdatedAt,
			worker.ControllerID, worker.TreeID, worker.AgentID, artifact.TurnID); execErr != nil {
			return fmt.Errorf("complete finalized worker: %w", execErr)
		}
		finalization = WorkerFinalization{Worker: worker, Artifact: artifact}
		return nil
	})
	return finalization, err
}

func beginWorkerFinalization(
	ctx context.Context,
	connection *sql.Conn,
	worker WorkerReservation,
	workspace PreparedWorkspace,
	artifactID string,
	target WorkerStatus,
	failureCode string,
	timestamp int64,
) (WorkerFinalization, error) {
	worker.Status = WorkerFinalizing
	worker.RetryTarget = ""
	worker.FailureCode = ""
	worker.FinalTarget = target
	worker.FinalFailureCode = failureCode
	worker.UpdatedAt = max(timestamp, worker.UpdatedAt)
	var err error
	worker.Revision, err = nextWorkerRevision(ctx, connection)
	if err != nil {
		return WorkerFinalization{}, err
	}
	artifact := ChangesArtifact{
		WorkerKey: worker.WorkerKey, ArtifactID: artifactID, TurnID: worker.ActiveTurnID,
		WorkspaceID: worker.WorkspaceID, CompletionTarget: target,
		WorkspaceSourceDeviceID: workspace.SourceDeviceID,
		WorkspaceTargetDeviceID: workspace.TargetDeviceID,
		CompletionFailureCode:   failureCode, State: ChangesCapturePending,
		ObjectFormat: workspace.ObjectFormat, BaseHeadOID: workspace.HeadOID,
		BaseClean:        workspace.Clean,
		BaseManifestHash: workspace.ManifestHash, BaseSnapshotHash: workspace.SourceSnapshotHash,
		BaseWarnings: slices.Clone(workspace.Warnings),
		CreatedAt:    timestamp, UpdatedAt: timestamp,
	}
	if err := artifact.Validate(); err != nil {
		return WorkerFinalization{}, err
	}
	baseWarningsJSON, err := json.Marshal(append([]string{}, artifact.BaseWarnings...))
	if err != nil {
		return WorkerFinalization{}, err
	}
	if _, err := connection.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = 'finalizing', retry_target = '', failure_code = '',
	final_target_status = ?, final_failure_code = ?, revision = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND active_turn_id = ?
`, worker.FinalTarget, worker.FinalFailureCode, worker.Revision, worker.UpdatedAt,
		worker.ControllerID, worker.TreeID, worker.AgentID, worker.ActiveTurnID); err != nil {
		return WorkerFinalization{}, fmt.Errorf("begin worker finalization: %w", err)
	}
	if _, err := connection.ExecContext(ctx, `
INSERT INTO peer_changes_artifacts(
	controller_id, tree_id, agent_id, turn_id, artifact_id, workspace_id,
	workspace_source_device_id, workspace_target_device_id,
	completion_target_status, completion_failure_code, state, object_format,
	base_head_oid, base_clean, base_manifest_hash, base_snapshot_hash, base_warnings_json,
	created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, artifact.ControllerID, artifact.TreeID, artifact.AgentID, artifact.TurnID,
		artifact.ArtifactID, artifact.WorkspaceID,
		artifact.WorkspaceSourceDeviceID, artifact.WorkspaceTargetDeviceID,
		artifact.CompletionTarget,
		artifact.CompletionFailureCode, artifact.State, artifact.ObjectFormat,
		artifact.BaseHeadOID, artifact.BaseClean, artifact.BaseManifestHash, artifact.BaseSnapshotHash,
		string(baseWarningsJSON),
		artifact.CreatedAt, artifact.UpdatedAt); err != nil {
		return WorkerFinalization{}, fmt.Errorf("create changes artifact: %w", err)
	}
	return WorkerFinalization{Worker: worker, Artifact: artifact}, nil
}

func requireFinalizationWorkspace(
	ctx context.Context,
	queryer rowQueryer,
	worker WorkerReservation,
) (PreparedWorkspace, error) {
	if worker.WorkspaceID == "" {
		return PreparedWorkspace{}, ErrChangesArtifactAuthority
	}
	workspace, err := queryPreparedWorkspace(ctx, queryer, PreparedWorkspaceKey{
		ControllerID: worker.ControllerID,
		TreeID:       worker.TreeID,
		WorkspaceID:  worker.WorkspaceID,
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return PreparedWorkspace{}, ErrChangesArtifactAuthority
		}
		return PreparedWorkspace{}, err
	}
	if workspace.Status != PreparedWorkspaceClaimed || workspace.ClaimedAgentID != worker.AgentID ||
		workspace.SourceAgentID != worker.ParentAgentID || workspace.TargetDeviceID != worker.DeviceID ||
		workspace.WorkspacePath != worker.WorkspacePath ||
		workspace.WorkingDirectory != worker.WorkingDirectory {
		return PreparedWorkspace{}, ErrChangesArtifactAuthority
	}
	return workspace, nil
}
