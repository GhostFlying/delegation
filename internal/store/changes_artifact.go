package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

var (
	ErrChangesArtifactCursorAhead       = errors.New("changes artifact cursor is ahead of the stored sequence")
	ErrChangesArtifactSequenceExhausted = errors.New("changes artifact sequence is exhausted")
)

const maximumChangesArtifactStorePage = protocol.MaximumAgentWaitArtifacts + 1

type ChangesArtifactPageRequest struct {
	AfterSequence uint64
	Limit         int
}

type ChangesArtifactPage struct {
	Artifacts    []protocol.ChangesArtifactMetadata
	NextSequence uint64
	Highwater    uint64
}

func (s *Store) PublishChangesArtifact(
	ctx context.Context,
	connectedDeviceID string,
	source control.PrincipalIdentity,
	params protocol.PublishChangesArtifactParams,
	observedAt time.Time,
) (protocol.PublishChangesArtifactResult, error) {
	if err := identity.ValidateID(connectedDeviceID); err != nil {
		return protocol.PublishChangesArtifactResult{}, fmt.Errorf("connectedDeviceId %w", err)
	}
	if err := source.Validate(); err != nil {
		return protocol.PublishChangesArtifactResult{}, fmt.Errorf("source: %w", err)
	}
	if err := params.Validate(); err != nil {
		return protocol.PublishChangesArtifactResult{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return protocol.PublishChangesArtifactResult{}, err
	}
	var result protocol.PublishChangesArtifactResult
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		principal, err := authorizePrincipal(
			ctx, connection, source, control.CapabilityArtifactPublishSelf,
		)
		if err != nil {
			return err
		}
		if principal.ParentAgentID == "" || principal.DeviceID != connectedDeviceID {
			return ErrAuthorizationDenied
		}
		authority, err := queryChangesArtifactAuthority(ctx, connection, principal.Identity())
		if err != nil {
			return err
		}
		if authority.ParentAgentID != principal.ParentAgentID ||
			authority.TargetDeviceID != connectedDeviceID ||
			authority.WorkspaceTargetDeviceID != connectedDeviceID ||
			authority.WorkspaceID != params.WorkspaceID ||
			authority.WorkspaceStatus != WorkspaceSyncPrepared ||
			authority.SpawnStatus != protocol.AgentSpawnStarted ||
			authority.ConsumedSpawnID != authority.SpawnID {
			return ErrAuthorizationDenied
		}
		if authority.HeadOID != params.BaseHeadOID ||
			authority.ManifestHash != params.BaseManifestHash ||
			authority.SourceSnapshotHash != params.BaseSnapshotHash {
			return fmt.Errorf("%w: changes artifact base differs from the prepared workspace", ErrConflict)
		}
		metadata := protocol.ChangesArtifactMetadata{
			TreeID: source.TreeID, ArtifactID: params.ArtifactID, TurnID: params.TurnID,
			WorkspaceID: params.WorkspaceID, Status: params.Status,
			SourceAgentID: source.AgentID, SourceDeviceID: connectedDeviceID,
			ObjectFormat: authority.ObjectFormat, BaseHeadOID: params.BaseHeadOID,
			BaseManifestHash: params.BaseManifestHash, BaseSnapshotHash: params.BaseSnapshotHash,
			BaseClean: authority.SourceClean, ResultHeadOID: params.ResultHeadOID,
			ResultSnapshotHash: params.ResultSnapshotHash, ResultClean: params.ResultClean,
			Parts:    append([]protocol.WorkspaceArtifactDescriptor{}, params.Parts...),
			Warnings: append([]string{}, params.Warnings...), FailureCode: params.FailureCode,
			Sequence: 1, ObservedAt: timestamp,
		}
		if err := metadata.Validate(); err != nil {
			return fmt.Errorf("%w: changes artifact does not match its prepared base: %v", ErrConflict, err)
		}

		stored, err := queryChangesArtifactByID(
			ctx, connection, source.ControllerID, source.TreeID, params.ArtifactID,
		)
		if err == nil {
			return replayChangesArtifact(stored, metadata, &result)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		stored, err = queryChangesArtifactByTurn(
			ctx, connection, source.ControllerID, source.TreeID, source.AgentID, params.TurnID,
		)
		if err == nil {
			return replayChangesArtifact(stored, metadata, &result)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}

		sequence, err := nextTreeChangesArtifactSequence(
			ctx, connection, source.ControllerID, source.TreeID,
		)
		if err != nil {
			return err
		}
		metadata.Sequence = sequence
		partsJSON, err := json.Marshal(metadata.Parts)
		if err != nil {
			return fmt.Errorf("encode changes artifact parts: %w", err)
		}
		warningsJSON, err := json.Marshal(metadata.Warnings)
		if err != nil {
			return fmt.Errorf("encode changes artifact warnings: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO changes_artifacts(
    controller_id, tree_id, artifact_id, turn_id, workspace_id, status,
    source_agent_id, source_device_id, object_format, base_head_oid,
    base_manifest_hash, base_snapshot_hash, base_clean, result_head_oid,
    result_snapshot_hash, result_clean, parts_json, warnings_json, failure_code,
    artifact_sequence, observed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, source.ControllerID, source.TreeID, metadata.ArtifactID, metadata.TurnID,
			metadata.WorkspaceID, metadata.Status, metadata.SourceAgentID, metadata.SourceDeviceID,
			metadata.ObjectFormat, metadata.BaseHeadOID, metadata.BaseManifestHash,
			metadata.BaseSnapshotHash, metadata.BaseClean, metadata.ResultHeadOID,
			metadata.ResultSnapshotHash, metadata.ResultClean, string(partsJSON),
			string(warningsJSON), metadata.FailureCode, metadata.Sequence, metadata.ObservedAt); err != nil {
			return fmt.Errorf("create changes artifact metadata: %w", err)
		}
		result = protocol.PublishChangesArtifactResult{
			ArtifactID: metadata.ArtifactID, Sequence: metadata.Sequence,
		}
		return nil
	})
	return result, err
}

func (s *Store) ListChangesArtifacts(
	ctx context.Context,
	root control.PrincipalIdentity,
	request ChangesArtifactPageRequest,
) (ChangesArtifactPage, error) {
	if err := root.Validate(); err != nil {
		return ChangesArtifactPage{}, fmt.Errorf("root: %w", err)
	}
	if request.AfterSequence > math.MaxInt64 {
		return ChangesArtifactPage{}, ErrChangesArtifactCursorAhead
	}
	if request.Limit < 1 || request.Limit > maximumChangesArtifactStorePage {
		return ChangesArtifactPage{}, fmt.Errorf(
			"changes artifact page limit must be from 1 through %d",
			maximumChangesArtifactStorePage,
		)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChangesArtifactPage{}, fmt.Errorf("begin changes artifact page: %w", err)
	}
	defer transaction.Rollback()
	principal, err := authorizePrincipal(
		ctx, transaction, root, control.CapabilityAgentManageDescendants,
	)
	if err != nil {
		return ChangesArtifactPage{}, err
	}
	if principal.ParentAgentID != "" {
		return ChangesArtifactPage{}, ErrAuthorizationDenied
	}
	var highwater int64
	if err := transaction.QueryRowContext(ctx, `
SELECT last_artifact_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, root.ControllerID, root.TreeID).Scan(&highwater); errors.Is(err, sql.ErrNoRows) {
		return ChangesArtifactPage{}, ErrNotFound
	} else if err != nil {
		return ChangesArtifactPage{}, fmt.Errorf("load tree changes artifact sequence: %w", err)
	}
	if request.AfterSequence > uint64(highwater) {
		return ChangesArtifactPage{}, ErrChangesArtifactCursorAhead
	}
	page := ChangesArtifactPage{
		Artifacts:    []protocol.ChangesArtifactMetadata{},
		NextSequence: request.AfterSequence,
		Highwater:    uint64(highwater),
	}
	rows, err := transaction.QueryContext(ctx, changesArtifactSelect+`
WHERE controller_id = ? AND tree_id = ? AND artifact_sequence > ?
ORDER BY artifact_sequence
LIMIT ?
`, root.ControllerID, root.TreeID, request.AfterSequence, request.Limit)
	if err != nil {
		return ChangesArtifactPage{}, fmt.Errorf("list changes artifacts: %w", err)
	}
	for rows.Next() {
		metadata, err := scanChangesArtifact(rows)
		if err != nil {
			rows.Close()
			return ChangesArtifactPage{}, err
		}
		page.Artifacts = append(page.Artifacts, metadata)
		page.NextSequence = metadata.Sequence
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ChangesArtifactPage{}, fmt.Errorf("list changes artifacts: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ChangesArtifactPage{}, fmt.Errorf("close changes artifact page: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return ChangesArtifactPage{}, fmt.Errorf("commit changes artifact page: %w", err)
	}
	return page, nil
}

type changesArtifactAuthority struct {
	ParentAgentID           string
	SpawnID                 string
	TargetDeviceID          string
	WorkspaceID             string
	SpawnStatus             protocol.AgentSpawnStatus
	WorkspaceTargetDeviceID string
	WorkspaceStatus         WorkspaceSyncStatus
	ConsumedSpawnID         string
	HeadOID                 string
	ObjectFormat            string
	SourceClean             bool
	SourceSnapshotHash      string
	ManifestHash            string
}

func queryChangesArtifactAuthority(
	ctx context.Context,
	queryer rowQueryer,
	source control.PrincipalIdentity,
) (changesArtifactAuthority, error) {
	var authority changesArtifactAuthority
	err := queryer.QueryRowContext(ctx, `
SELECT r.source_agent_id, r.spawn_id, r.target_device_id, r.workspace_id, r.status,
       w.target_device_id, w.status, w.consumed_spawn_id, w.head_oid,
       w.object_format, w.source_clean, w.source_snapshot_hash, w.manifest_hash
FROM agent_spawn_receipts AS r
JOIN workspace_sync_receipts AS w
  ON w.controller_id = r.controller_id AND w.tree_id = r.tree_id
 AND w.source_agent_id = r.source_agent_id AND w.sync_id = r.workspace_id
WHERE r.controller_id = ? AND r.tree_id = ? AND r.agent_id = ?
`, source.ControllerID, source.TreeID, source.AgentID).Scan(
		&authority.ParentAgentID, &authority.SpawnID, &authority.TargetDeviceID,
		&authority.WorkspaceID, &authority.SpawnStatus,
		&authority.WorkspaceTargetDeviceID, &authority.WorkspaceStatus,
		&authority.ConsumedSpawnID, &authority.HeadOID, &authority.ObjectFormat,
		&authority.SourceClean, &authority.SourceSnapshotHash, &authority.ManifestHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return changesArtifactAuthority{}, ErrAuthorizationDenied
	}
	if err != nil {
		return changesArtifactAuthority{}, fmt.Errorf("load changes artifact authority: %w", err)
	}
	return authority, nil
}

func replayChangesArtifact(
	stored, requested protocol.ChangesArtifactMetadata,
	result *protocol.PublishChangesArtifactResult,
) error {
	if stored.TreeID != requested.TreeID || stored.SourceAgentID != requested.SourceAgentID ||
		stored.SourceDeviceID != requested.SourceDeviceID || stored.ObjectFormat != requested.ObjectFormat ||
		stored.BaseClean != requested.BaseClean ||
		!protocol.SameChangesArtifactParams(changesArtifactParams(stored), changesArtifactParams(requested)) {
		return fmt.Errorf("%w: artifactId or turnId already identifies different changes", ErrConflict)
	}
	*result = protocol.PublishChangesArtifactResult{
		ArtifactID: stored.ArtifactID, Sequence: stored.Sequence,
	}
	return nil
}

func changesArtifactParams(metadata protocol.ChangesArtifactMetadata) protocol.PublishChangesArtifactParams {
	return protocol.PublishChangesArtifactParams{
		ArtifactID: metadata.ArtifactID, TurnID: metadata.TurnID, WorkspaceID: metadata.WorkspaceID,
		Status: metadata.Status, BaseHeadOID: metadata.BaseHeadOID,
		BaseManifestHash: metadata.BaseManifestHash, BaseSnapshotHash: metadata.BaseSnapshotHash,
		ResultHeadOID: metadata.ResultHeadOID, ResultSnapshotHash: metadata.ResultSnapshotHash,
		ResultClean: metadata.ResultClean, Parts: metadata.Parts, Warnings: metadata.Warnings,
		FailureCode: metadata.FailureCode,
	}
}

func queryChangesArtifactByID(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID, artifactID string,
) (protocol.ChangesArtifactMetadata, error) {
	return scanChangesArtifact(queryer.QueryRowContext(ctx, changesArtifactSelect+`
WHERE controller_id = ? AND tree_id = ? AND artifact_id = ?
`, controllerID, treeID, artifactID))
}

func queryChangesArtifactByTurn(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID, sourceAgentID, turnID string,
) (protocol.ChangesArtifactMetadata, error) {
	return scanChangesArtifact(queryer.QueryRowContext(ctx, changesArtifactSelect+`
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND turn_id = ?
`, controllerID, treeID, sourceAgentID, turnID))
}

func scanChangesArtifact(scanner rowScanner) (protocol.ChangesArtifactMetadata, error) {
	var metadata protocol.ChangesArtifactMetadata
	var partsJSON, warningsJSON string
	err := scanner.Scan(
		&metadata.TreeID, &metadata.ArtifactID, &metadata.TurnID, &metadata.WorkspaceID,
		&metadata.Status, &metadata.SourceAgentID, &metadata.SourceDeviceID,
		&metadata.ObjectFormat, &metadata.BaseHeadOID, &metadata.BaseManifestHash,
		&metadata.BaseSnapshotHash, &metadata.BaseClean, &metadata.ResultHeadOID,
		&metadata.ResultSnapshotHash, &metadata.ResultClean, &partsJSON, &warningsJSON,
		&metadata.FailureCode, &metadata.Sequence, &metadata.ObservedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.ChangesArtifactMetadata{}, ErrNotFound
	}
	if err != nil {
		return protocol.ChangesArtifactMetadata{}, fmt.Errorf("load changes artifact metadata: %w", err)
	}
	if err := json.Unmarshal([]byte(partsJSON), &metadata.Parts); err != nil {
		return protocol.ChangesArtifactMetadata{}, fmt.Errorf("decode changes artifact parts: %w", err)
	}
	if err := json.Unmarshal([]byte(warningsJSON), &metadata.Warnings); err != nil {
		return protocol.ChangesArtifactMetadata{}, fmt.Errorf("decode changes artifact warnings: %w", err)
	}
	if err := metadata.Validate(); err != nil {
		return protocol.ChangesArtifactMetadata{}, fmt.Errorf("stored changes artifact is invalid: %w", err)
	}
	return metadata, nil
}

func nextTreeChangesArtifactSequence(
	ctx context.Context,
	connection *sql.Conn,
	controllerID, treeID string,
) (uint64, error) {
	var current int64
	if err := connection.QueryRowContext(ctx, `
SELECT last_artifact_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, controllerID, treeID).Scan(&current); errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, fmt.Errorf("load tree changes artifact sequence: %w", err)
	}
	if current == math.MaxInt64 {
		return 0, ErrChangesArtifactSequenceExhausted
	}
	next := current + 1
	result, err := connection.ExecContext(ctx, `
UPDATE trees
SET last_artifact_sequence = ?
WHERE controller_id = ? AND tree_id = ? AND last_artifact_sequence = ?
`, next, controllerID, treeID, current)
	if err != nil {
		return 0, fmt.Errorf("advance tree changes artifact sequence: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect tree changes artifact sequence update: %w", err)
	}
	if affected != 1 {
		return 0, errors.New("tree changes artifact sequence changed during update")
	}
	return uint64(next), nil
}

const changesArtifactSelect = `
SELECT tree_id, artifact_id, turn_id, workspace_id, status, source_agent_id,
       source_device_id, object_format, base_head_oid, base_manifest_hash,
       base_snapshot_hash, base_clean, result_head_oid, result_snapshot_hash,
       result_clean, parts_json, warnings_json, failure_code, artifact_sequence,
       observed_at
FROM changes_artifacts
`
