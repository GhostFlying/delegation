package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

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
	artifact.workspace_source_device_id, artifact.workspace_target_device_id,
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
