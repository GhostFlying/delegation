package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	// MaximumRetainedChangesArtifacts is the published-artifact target for GC callers.
	// Pending artifact records are governed by worker-slot admission instead.
	MaximumRetainedChangesArtifacts    = 64
	MaximumRetainedChangesPayloadBytes = int64(2 * 1024 * 1024 * 1024)
	MaximumChangesArtifactPayloadBytes = protocol.MaximumWorkspaceTransferBytes
	maximumChangesArtifactPage         = 256
	changesCaptureFailureCode          = "changes_capture_failed"
)

const (
	ChangesBundlePartName  = "changes.bundle"
	ChangesOverlayPartName = "changes-overlay.tar.zst"
)

var (
	ErrChangesArtifactConflict   = errors.New("changes artifact conflicts with existing state")
	ErrChangesArtifactTransition = errors.New("invalid changes artifact transition")
	ErrChangesArtifactQuota      = errors.New("changes artifact retention quota exceeded")
	ErrChangesArtifactAuthority  = errors.New("changes artifact is outside the worker workspace authority")
)

type ChangesArtifactState string

const (
	ChangesCapturePending ChangesArtifactState = "capturePending"
	ChangesPublishPending ChangesArtifactState = "publishPending"
	ChangesPublished      ChangesArtifactState = "published"
)

type ChangesCaptureStatus string

const (
	ChangesAvailable     ChangesCaptureStatus = "available"
	ChangesUnchanged     ChangesCaptureStatus = "unchanged"
	ChangesCaptureFailed ChangesCaptureStatus = "captureFailed"
)

type ChangesArtifactPartKind string

const (
	ChangesArtifactBundle  ChangesArtifactPartKind = "bundle"
	ChangesArtifactOverlay ChangesArtifactPartKind = "overlay"
)

type ChangesArtifactPart struct {
	Kind      ChangesArtifactPartKind
	Name      string
	SizeBytes int64
	SHA256    string
}

// ChangesArtifact is the peer-local durable outbox record. Part names are
// fixed relative names under an artifact-owned directory; arbitrary paths are
// intentionally not representable here or in the database.
type ChangesArtifact struct {
	WorkerKey
	ArtifactID            string
	TurnID                string
	WorkspaceID           string
	CompletionTarget      WorkerStatus
	CompletionFailureCode string
	State                 ChangesArtifactState
	Status                ChangesCaptureStatus
	ObjectFormat          string
	BaseHeadOID           string
	BaseClean             bool
	BaseManifestHash      string
	BaseSnapshotHash      string
	BaseWarnings          []string
	ResultHeadOID         string
	ResultSnapshotHash    string
	ResultClean           bool
	Parts                 []ChangesArtifactPart
	ResultWarnings        []string
	FailureCode           string
	RetentionReserved     bool
	ReservedBytes         int64
	PayloadBytes          int64
	BrokerSequence        uint64
	CreatedAt             int64
	UpdatedAt             int64
}

type ChangesArtifactRetention struct {
	Count         int
	ReservedBytes int64
}

type WorkerFinalization struct {
	Worker   WorkerReservation
	Artifact ChangesArtifact
}

type ChangesCaptureResult struct {
	Status             ChangesCaptureStatus
	ResultHeadOID      string
	ResultSnapshotHash string
	ResultClean        bool
	Parts              []ChangesArtifactPart
	ResultWarnings     []string
	FailureCode        string
}

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

func (s *PeerStore) GetChangesArtifact(
	ctx context.Context,
	key WorkerKey,
	artifactID string,
) (ChangesArtifact, error) {
	if err := validateChangesArtifactIdentity(key, artifactID); err != nil {
		return ChangesArtifact{}, err
	}
	return queryPeerChangesArtifact(ctx, s.db, key, artifactID)
}

func (s *PeerStore) ListPendingChangesCaptures(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ChangesArtifact, error) {
	return s.listChangesArtifacts(ctx, controllerID, deviceID, ChangesCapturePending, limit)
}

func (s *PeerStore) ListPendingChangesPublications(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ChangesArtifact, error) {
	return s.listChangesArtifacts(ctx, controllerID, deviceID, ChangesPublishPending, limit)
}

// ListPublishedChangesArtifacts returns retained metadata oldest publication
// first, with stable identity tie-breakers for equal timestamps.
func (s *PeerStore) ListPublishedChangesArtifacts(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ChangesArtifact, error) {
	if err := validateChangesArtifactListRequest(controllerID, deviceID, limit); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, peerChangesArtifactSelect+`
JOIN worker_reservations AS worker
  ON worker.controller_id = artifact.controller_id
 AND worker.tree_id = artifact.tree_id
 AND worker.agent_id = artifact.agent_id
WHERE artifact.controller_id = ? AND worker.device_id = ? AND artifact.state = 'published'
ORDER BY artifact.updated_at, artifact.created_at,
         artifact.tree_id, artifact.agent_id, artifact.turn_id
LIMIT ?
`, controllerID, deviceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list published changes artifacts: %w", err)
	}
	return scanChangesArtifactRows(rows, "list published changes artifacts")
}

func (s *PeerStore) GetPublishedChangesArtifactRetention(
	ctx context.Context,
	controllerID, deviceID string,
) (ChangesArtifactRetention, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return ChangesArtifactRetention{}, err
	}
	var retention ChangesArtifactRetention
	err := s.db.QueryRowContext(ctx, `
SELECT count(*), COALESCE(sum(artifact.reserved_bytes), 0)
FROM peer_changes_artifacts AS artifact
JOIN worker_reservations AS worker
  ON worker.controller_id = artifact.controller_id
 AND worker.tree_id = artifact.tree_id
 AND worker.agent_id = artifact.agent_id
WHERE artifact.controller_id = ? AND worker.device_id = ? AND artifact.state = 'published'
`, controllerID, deviceID).Scan(&retention.Count, &retention.ReservedBytes)
	if err != nil {
		return ChangesArtifactRetention{}, fmt.Errorf("inspect published changes artifact retention: %w", err)
	}
	return retention, nil
}

// DeletePublishedChangesArtifact removes retained metadata after the caller has
// removed any filesystem payload. Exact replay after deletion is a no-op.
func (s *PeerStore) DeletePublishedChangesArtifact(
	ctx context.Context,
	key WorkerKey,
	artifactID string,
) error {
	if err := validateChangesArtifactIdentity(key, artifactID); err != nil {
		return err
	}
	return withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		artifact, err := queryPeerChangesArtifact(ctx, connection, key, artifactID)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if artifact.State != ChangesPublished {
			return ErrChangesArtifactTransition
		}
		result, err := connection.ExecContext(ctx, `
DELETE FROM peer_changes_artifacts
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND artifact_id = ?
  AND state = 'published'
`, key.ControllerID, key.TreeID, key.AgentID, artifactID)
		if err != nil {
			return fmt.Errorf("delete published changes artifact: %w", err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect deleted changes artifact: %w", err)
		}
		if deleted != 1 {
			return ErrChangesArtifactTransition
		}
		return nil
	})
}

func (s *PeerStore) listChangesArtifacts(
	ctx context.Context,
	controllerID, deviceID string,
	state ChangesArtifactState,
	limit int,
) ([]ChangesArtifact, error) {
	if err := validateChangesArtifactListRequest(controllerID, deviceID, limit); err != nil {
		return nil, err
	}
	if state != ChangesCapturePending && state != ChangesPublishPending {
		return nil, fmt.Errorf("unsupported pending changes artifact state %q", state)
	}
	rows, err := s.db.QueryContext(ctx, peerChangesArtifactSelect+`
JOIN worker_reservations AS worker
  ON worker.controller_id = artifact.controller_id
 AND worker.tree_id = artifact.tree_id
 AND worker.agent_id = artifact.agent_id
WHERE artifact.controller_id = ? AND worker.device_id = ? AND artifact.state = ?
ORDER BY artifact.created_at, artifact.tree_id, artifact.agent_id, artifact.turn_id
LIMIT ?
`, controllerID, deviceID, state, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending changes artifacts: %w", err)
	}
	return scanChangesArtifactRows(rows, "list pending changes artifacts")
}

func validateChangesArtifactListRequest(controllerID, deviceID string, limit int) error {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return err
	}
	if limit < 1 || limit > maximumChangesArtifactPage {
		return fmt.Errorf("limit must be from 1 through %d", maximumChangesArtifactPage)
	}
	return nil
}

func validateChangesArtifactDevice(controllerID, deviceID string) error {
	if err := identity.ValidateID(controllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	return nil
}

func scanChangesArtifactRows(rows *sql.Rows, operation string) ([]ChangesArtifact, error) {
	defer rows.Close()
	artifacts := make([]ChangesArtifact, 0)
	for rows.Next() {
		artifact, scanErr := scanPeerChangesArtifact(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", operation, scanErr)
		}
		artifacts = append(artifacts, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return artifacts, nil
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
		CompletionFailureCode: failureCode, State: ChangesCapturePending,
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
	completion_target_status, completion_failure_code, state, object_format,
	base_head_oid, base_clean, base_manifest_hash, base_snapshot_hash, base_warnings_json,
	created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, artifact.ControllerID, artifact.TreeID, artifact.AgentID, artifact.TurnID,
		artifact.ArtifactID, artifact.WorkspaceID, artifact.CompletionTarget,
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

func queryPeerChangesArtifact(
	ctx context.Context,
	queryer rowQueryer,
	key WorkerKey,
	artifactID string,
) (ChangesArtifact, error) {
	return scanPeerChangesArtifact(queryer.QueryRowContext(ctx, peerChangesArtifactSelect+`
WHERE artifact.controller_id = ? AND artifact.tree_id = ? AND artifact.agent_id = ?
  AND artifact.artifact_id = ?
`, key.ControllerID, key.TreeID, key.AgentID, artifactID))
}

func queryPeerChangesArtifactByTurn(
	ctx context.Context,
	queryer rowQueryer,
	key WorkerKey,
	turnID string,
) (ChangesArtifact, error) {
	return scanPeerChangesArtifact(queryer.QueryRowContext(ctx, peerChangesArtifactSelect+`
WHERE artifact.controller_id = ? AND artifact.tree_id = ? AND artifact.agent_id = ?
  AND artifact.turn_id = ?
`, key.ControllerID, key.TreeID, key.AgentID, turnID))
}

const peerChangesArtifactSelect = `
SELECT artifact.controller_id, artifact.tree_id, artifact.agent_id,
	artifact.turn_id, artifact.artifact_id, artifact.workspace_id,
	artifact.completion_target_status, artifact.completion_failure_code,
	artifact.state, artifact.capture_status, artifact.object_format,
	artifact.base_head_oid, artifact.base_clean, artifact.base_manifest_hash, artifact.base_snapshot_hash,
	artifact.base_warnings_json,
	artifact.result_head_oid, artifact.result_snapshot_hash, artifact.result_clean,
	artifact.bundle_part_name, artifact.bundle_size_bytes, artifact.bundle_sha256,
	artifact.overlay_part_name, artifact.overlay_size_bytes, artifact.overlay_sha256,
	artifact.result_warnings_json, artifact.failure_code, artifact.retention_reserved,
	artifact.reserved_bytes, artifact.payload_bytes, artifact.broker_sequence,
	artifact.created_at, artifact.updated_at
FROM peer_changes_artifacts AS artifact
`

func scanPeerChangesArtifact(scanner rowScanner) (ChangesArtifact, error) {
	var artifact ChangesArtifact
	var bundle, overlay ChangesArtifactPart
	var baseWarningsJSON, resultWarningsJSON string
	if err := scanner.Scan(
		&artifact.ControllerID, &artifact.TreeID, &artifact.AgentID,
		&artifact.TurnID, &artifact.ArtifactID, &artifact.WorkspaceID,
		&artifact.CompletionTarget, &artifact.CompletionFailureCode,
		&artifact.State, &artifact.Status, &artifact.ObjectFormat,
		&artifact.BaseHeadOID, &artifact.BaseClean, &artifact.BaseManifestHash, &artifact.BaseSnapshotHash,
		&baseWarningsJSON,
		&artifact.ResultHeadOID, &artifact.ResultSnapshotHash, &artifact.ResultClean,
		&bundle.Name, &bundle.SizeBytes, &bundle.SHA256,
		&overlay.Name, &overlay.SizeBytes, &overlay.SHA256,
		&resultWarningsJSON, &artifact.FailureCode, &artifact.RetentionReserved,
		&artifact.ReservedBytes, &artifact.PayloadBytes, &artifact.BrokerSequence,
		&artifact.CreatedAt, &artifact.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return ChangesArtifact{}, ErrNotFound
	} else if err != nil {
		return ChangesArtifact{}, fmt.Errorf("load changes artifact: %w", err)
	}
	if bundle.Name != "" {
		bundle.Kind = ChangesArtifactBundle
		artifact.Parts = append(artifact.Parts, bundle)
	}
	if overlay.Name != "" {
		overlay.Kind = ChangesArtifactOverlay
		artifact.Parts = append(artifact.Parts, overlay)
	}
	if err := json.Unmarshal([]byte(baseWarningsJSON), &artifact.BaseWarnings); err != nil {
		return ChangesArtifact{}, errors.New("stored changes artifact base warnings are invalid")
	}
	if err := json.Unmarshal([]byte(resultWarningsJSON), &artifact.ResultWarnings); err != nil {
		return ChangesArtifact{}, errors.New("stored changes artifact result warnings are invalid")
	}
	if err := artifact.Validate(); err != nil {
		return ChangesArtifact{}, fmt.Errorf("stored changes artifact is invalid: %w", err)
	}
	return artifact, nil
}

func (a ChangesArtifact) Validate() error {
	if err := validateChangesArtifactIdentity(a.WorkerKey, a.ArtifactID); err != nil {
		return err
	}
	if err := identity.ValidateID(a.TurnID); err != nil {
		return fmt.Errorf("turnId %w", err)
	}
	if err := identity.ValidateID(a.WorkspaceID); err != nil {
		return fmt.Errorf("workspaceId %w", err)
	}
	if err := validateFinalTarget(a.CompletionTarget, a.CompletionFailureCode); err != nil {
		return err
	}
	if !a.State.valid() {
		return fmt.Errorf("unsupported changes artifact state %q", a.State)
	}
	if err := validateObjectID(a.ObjectFormat, a.BaseHeadOID); err != nil {
		return fmt.Errorf("baseHeadOid: %w", err)
	}
	if err := validateDigest("baseManifestHash", a.BaseManifestHash); err != nil {
		return err
	}
	if err := validateDigest("baseSnapshotHash", a.BaseSnapshotHash); err != nil {
		return err
	}
	if err := protocol.ValidateWorkspaceWarnings(a.BaseWarnings); err != nil {
		return fmt.Errorf("base warnings: %w", err)
	}
	if a.CreatedAt < 0 || a.UpdatedAt < a.CreatedAt {
		return errors.New("changes artifact timestamps are invalid")
	}
	if a.ReservedBytes < 0 || a.ReservedBytes > MaximumChangesArtifactPayloadBytes ||
		a.PayloadBytes < 0 || a.PayloadBytes > MaximumChangesArtifactPayloadBytes {
		return errors.New("changes artifact byte count is invalid")
	}
	if a.RetentionReserved != (a.ReservedBytes > 0) {
		return errors.New("changes artifact retention reservation is invalid")
	}
	switch a.State {
	case ChangesCapturePending:
		if a.Status != "" || a.ResultHeadOID != "" || a.ResultSnapshotHash != "" ||
			a.ResultClean || len(a.Parts) != 0 || len(a.ResultWarnings) != 0 || a.FailureCode != "" ||
			a.PayloadBytes != 0 || a.BrokerSequence != 0 {
			return errors.New("capture-pending changes artifact contains capture output")
		}
	case ChangesPublishPending, ChangesPublished:
		result := ChangesCaptureResult{
			Status: a.Status, ResultHeadOID: a.ResultHeadOID,
			ResultSnapshotHash: a.ResultSnapshotHash, ResultClean: a.ResultClean,
			Parts: a.Parts, ResultWarnings: a.ResultWarnings, FailureCode: a.FailureCode,
		}
		if err := validateChangesCaptureForArtifact(a, result); err != nil {
			return err
		}
		if a.State == ChangesPublishPending && a.BrokerSequence != 0 {
			return errors.New("publish-pending changes artifact contains broker sequence")
		}
		if a.State == ChangesPublished && (a.BrokerSequence == 0 || a.BrokerSequence > math.MaxInt64) {
			return errors.New("published changes artifact has invalid broker sequence")
		}
	}
	return nil
}

func validateFinalizationRequest(
	key WorkerKey,
	turnID string,
	target WorkerStatus,
	failureCode string,
) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(turnID); err != nil {
		return fmt.Errorf("turnId %w", err)
	}
	return validateFinalTarget(target, failureCode)
}

func validateFinalTarget(target WorkerStatus, failureCode string) error {
	if !target.finalTarget() {
		return fmt.Errorf("unsupported final worker target %q", target)
	}
	if err := validateFailureCode(failureCode); err != nil {
		return err
	}
	if target == WorkerIdle && failureCode != "" {
		return errors.New("idle final target cannot contain failureCode")
	}
	if target != WorkerIdle && failureCode == "" {
		return errors.New("non-idle final target requires failureCode")
	}
	return nil
}

func validateChangesArtifactIdentity(key WorkerKey, artifactID string) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(artifactID); err != nil {
		return fmt.Errorf("artifactId %w", err)
	}
	return nil
}

func validateChangesCaptureResult(result ChangesCaptureResult) error {
	if !result.Status.valid() {
		return fmt.Errorf("unsupported changes capture status %q", result.Status)
	}
	if err := protocol.ValidateWorkspaceSourceWarnings(result.ResultWarnings); err != nil {
		return fmt.Errorf("result warnings: %w", err)
	}
	if err := validateFailureCode(result.FailureCode); err != nil {
		return err
	}
	seen := make(map[ChangesArtifactPartKind]struct{}, len(result.Parts))
	for _, part := range result.Parts {
		if err := part.Validate(); err != nil {
			return err
		}
		if _, exists := seen[part.Kind]; exists {
			return fmt.Errorf("duplicate changes artifact part %q", part.Kind)
		}
		seen[part.Kind] = struct{}{}
	}
	if changesPayloadBytes(result.Parts) > MaximumChangesArtifactPayloadBytes {
		return errors.New("changes artifact payload exceeds transfer limit")
	}
	switch result.Status {
	case ChangesAvailable:
		if result.ResultHeadOID == "" || result.ResultSnapshotHash == "" || result.FailureCode != "" {
			return errors.New("available changes capture details are incomplete")
		}
	case ChangesUnchanged:
		if result.ResultHeadOID == "" || result.ResultSnapshotHash == "" ||
			len(result.Parts) != 0 || result.FailureCode != "" {
			return errors.New("unchanged changes capture details are invalid")
		}
	case ChangesCaptureFailed:
		if result.ResultHeadOID != "" || result.ResultSnapshotHash != "" || result.ResultClean ||
			len(result.Parts) != 0 || len(result.ResultWarnings) != 0 || result.FailureCode == "" {
			return errors.New("failed changes capture details are invalid")
		}
	}
	return nil
}

func validateChangesCaptureForArtifact(
	artifact ChangesArtifact,
	result ChangesCaptureResult,
) error {
	if err := validateChangesCaptureResult(result); err != nil {
		return err
	}
	if result.Status == ChangesCaptureFailed {
		return nil
	}
	if err := validateObjectID(artifact.ObjectFormat, result.ResultHeadOID); err != nil {
		return fmt.Errorf("resultHeadOid: %w", err)
	}
	if err := validateDigest("resultSnapshotHash", result.ResultSnapshotHash); err != nil {
		return err
	}
	if result.Status == ChangesUnchanged {
		if result.ResultHeadOID != artifact.BaseHeadOID ||
			result.ResultSnapshotHash != artifact.BaseSnapshotHash ||
			result.ResultClean != artifact.BaseClean {
			return errors.New("unchanged capture does not match the prepared workspace base")
		}
		return nil
	}
	payloadBytes := changesPayloadBytes(result.Parts)
	if payloadBytes == 0 {
		if artifact.BaseClean || result.ResultHeadOID != artifact.BaseHeadOID || !result.ResultClean {
			return errors.New("zero-payload available capture must clean an unchanged dirty base")
		}
		return nil
	}
	if !artifact.RetentionReserved || payloadBytes > artifact.ReservedBytes {
		return ErrChangesArtifactQuota
	}
	return nil
}

func (p ChangesArtifactPart) Validate() error {
	switch p.Kind {
	case ChangesArtifactBundle:
		if p.Name != ChangesBundlePartName {
			return errors.New("bundle part must use its controlled relative name")
		}
	case ChangesArtifactOverlay:
		if p.Name != ChangesOverlayPartName {
			return errors.New("overlay part must use its controlled relative name")
		}
	default:
		return fmt.Errorf("unsupported changes artifact part kind %q", p.Kind)
	}
	if p.SizeBytes < 1 || p.SizeBytes > protocol.MaximumWorkspaceArtifactBytes {
		return fmt.Errorf(
			"changes artifact part size must be from 1 through %d",
			protocol.MaximumWorkspaceArtifactBytes,
		)
	}
	return validateDigest("changes artifact part sha256", p.SHA256)
}

func validateObjectID(objectFormat, oid string) error {
	want := 0
	switch objectFormat {
	case "sha1":
		want = 40
	case "sha256":
		want = 64
	default:
		return fmt.Errorf("unsupported Git object format %q", objectFormat)
	}
	if len(oid) != want || strings.ToLower(oid) != oid {
		return fmt.Errorf("must be a lowercase %s object ID", objectFormat)
	}
	if _, err := hex.DecodeString(oid); err != nil {
		return fmt.Errorf("must be a lowercase %s object ID", objectFormat)
	}
	return nil
}

func validateDigest(name, value string) error {
	if len(value) != 64 || strings.ToLower(value) != value {
		return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
	}
	return nil
}

func canonicalChangesParts(parts []ChangesArtifactPart) []ChangesArtifactPart {
	result := slices.Clone(parts)
	slices.SortFunc(result, func(left, right ChangesArtifactPart) int {
		return strings.Compare(string(left.Kind), string(right.Kind))
	})
	return result
}

func splitChangesParts(parts []ChangesArtifactPart) (ChangesArtifactPart, ChangesArtifactPart) {
	var bundle, overlay ChangesArtifactPart
	for _, part := range parts {
		switch part.Kind {
		case ChangesArtifactBundle:
			bundle = part
		case ChangesArtifactOverlay:
			overlay = part
		}
	}
	return bundle, overlay
}

func changesPayloadBytes(parts []ChangesArtifactPart) int64 {
	var total int64
	for _, part := range parts {
		total += part.SizeBytes
	}
	return total
}

func sameChangesCapture(artifact ChangesArtifact, result ChangesCaptureResult) bool {
	return artifact.Status == result.Status &&
		artifact.ResultHeadOID == result.ResultHeadOID &&
		artifact.ResultSnapshotHash == result.ResultSnapshotHash &&
		artifact.ResultClean == result.ResultClean &&
		slices.Equal(artifact.Parts, result.Parts) &&
		slices.Equal(artifact.ResultWarnings, result.ResultWarnings) &&
		artifact.FailureCode == result.FailureCode
}

func (s ChangesArtifactState) valid() bool {
	switch s {
	case ChangesCapturePending, ChangesPublishPending, ChangesPublished:
		return true
	default:
		return false
	}
}

func (s ChangesCaptureStatus) valid() bool {
	switch s {
	case ChangesAvailable, ChangesUnchanged, ChangesCaptureFailed:
		return true
	default:
		return false
	}
}
