package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

type PreparedWorkspaceStatus string

const (
	PreparedWorkspaceReady   PreparedWorkspaceStatus = "prepared"
	PreparedWorkspaceClaimed PreparedWorkspaceStatus = "claimed"
)

type PreparedWorkspaceKey struct {
	ControllerID string
	TreeID       string
	WorkspaceID  string
}

type PreparedWorkspace struct {
	PreparedWorkspaceKey
	SourceAgentID    string
	SourceDeviceID   string
	TargetDeviceID   string
	GitURL           string
	HeadOID          string
	ObjectFormat     string
	WorkingDirectory string
	WorkspacePath    string
	Strategy         protocol.WorkspaceStrategy
	ManifestHash     string
	Warnings         []string
	Status           PreparedWorkspaceStatus
	ClaimedAgentID   string
	CreatedAt        int64
	UpdatedAt        int64
}

func (s *PeerStore) RecordPreparedWorkspace(
	ctx context.Context,
	workspace PreparedWorkspace,
	createdAt time.Time,
) (PreparedWorkspace, error) {
	if err := validateNewPreparedWorkspace(workspace); err != nil {
		return PreparedWorkspace{}, err
	}
	timestamp, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return PreparedWorkspace{}, err
	}
	warningsJSON, err := json.Marshal(workspace.Warnings)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	workspace.Status = PreparedWorkspaceReady
	workspace.CreatedAt = timestamp
	workspace.UpdatedAt = timestamp
	var stored PreparedWorkspace
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err = queryPreparedWorkspace(ctx, connection, workspace.PreparedWorkspaceKey)
		if err == nil {
			if !samePreparedWorkspace(stored, workspace) {
				return ErrWorkerReservationConflict
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO prepared_workspaces(
	controller_id, tree_id, workspace_id, source_agent_id, source_device_id,
	target_device_id, git_url, head_oid, object_format, working_directory,
	workspace_path, strategy, manifest_hash, warnings_json, status, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?)
`, workspace.ControllerID, workspace.TreeID, workspace.WorkspaceID,
			workspace.SourceAgentID, workspace.SourceDeviceID, workspace.TargetDeviceID,
			workspace.GitURL, workspace.HeadOID, workspace.ObjectFormat,
			workspace.WorkingDirectory, workspace.WorkspacePath, workspace.Strategy,
			workspace.ManifestHash, string(warningsJSON), timestamp, timestamp); err != nil {
			return fmt.Errorf("record prepared workspace: %w", err)
		}
		stored = workspace
		return nil
	})
	return stored, err
}

func (s *PeerStore) GetPreparedWorkspace(
	ctx context.Context,
	key PreparedWorkspaceKey,
) (PreparedWorkspace, error) {
	if err := key.Validate(); err != nil {
		return PreparedWorkspace{}, err
	}
	return queryPreparedWorkspace(ctx, s.db, key)
}

func (s *PeerStore) ListPreparedWorkspaces(ctx context.Context) ([]PreparedWorkspace, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT controller_id, tree_id, workspace_id, source_agent_id, source_device_id,
	target_device_id, git_url, head_oid, object_format, working_directory,
	workspace_path, strategy, manifest_hash, warnings_json, status,
	claimed_agent_id, created_at, updated_at
FROM prepared_workspaces
ORDER BY created_at, controller_id, tree_id, workspace_id
`)
	if err != nil {
		return nil, fmt.Errorf("list prepared workspaces: %w", err)
	}
	defer rows.Close()
	workspaces := make([]PreparedWorkspace, 0)
	for rows.Next() {
		var workspace PreparedWorkspace
		var warningsJSON string
		if err := rows.Scan(
			&workspace.ControllerID, &workspace.TreeID, &workspace.WorkspaceID,
			&workspace.SourceAgentID, &workspace.SourceDeviceID,
			&workspace.TargetDeviceID, &workspace.GitURL, &workspace.HeadOID,
			&workspace.ObjectFormat, &workspace.WorkingDirectory,
			&workspace.WorkspacePath, &workspace.Strategy, &workspace.ManifestHash,
			&warningsJSON, &workspace.Status, &workspace.ClaimedAgentID,
			&workspace.CreatedAt, &workspace.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan prepared workspace: %w", err)
		}
		if err := json.Unmarshal([]byte(warningsJSON), &workspace.Warnings); err != nil {
			return nil, errors.New("stored prepared workspace warnings are invalid")
		}
		if err := workspace.Validate(); err != nil {
			return nil, fmt.Errorf("stored prepared workspace is invalid: %w", err)
		}
		workspaces = append(workspaces, workspace)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list prepared workspaces: %w", err)
	}
	return workspaces, nil
}

func (k PreparedWorkspaceKey) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "controllerId", value: k.ControllerID},
		{name: "treeId", value: k.TreeID},
		{name: "workspaceId", value: k.WorkspaceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

func (w PreparedWorkspace) Validate() error {
	if err := w.PreparedWorkspaceKey.Validate(); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "sourceAgentId", value: w.SourceAgentID},
		{name: "sourceDeviceId", value: w.SourceDeviceID},
		{name: "targetDeviceId", value: w.TargetDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	summary := protocol.WorkspaceSummary{
		WorkspaceID: w.WorkspaceID, SourceDeviceID: w.SourceDeviceID,
		TargetDeviceID: w.TargetDeviceID, HeadOID: w.HeadOID,
		ObjectFormat: w.ObjectFormat, WorkingDirectory: w.WorkingDirectory,
		Strategy: w.Strategy, ManifestHash: w.ManifestHash, Warnings: w.Warnings,
	}
	if err := summary.Validate(); err != nil {
		return err
	}
	if !filepath.IsAbs(w.WorkspacePath) || len(w.WorkspacePath) > maximumWorkspacePath {
		return errors.New("prepared workspacePath must be a bounded absolute path")
	}
	switch w.Status {
	case PreparedWorkspaceReady:
		if w.ClaimedAgentID != "" {
			return errors.New("unclaimed workspace contains claimedAgentId")
		}
	case PreparedWorkspaceClaimed:
		if err := identity.ValidateID(w.ClaimedAgentID); err != nil {
			return fmt.Errorf("claimedAgentId %w", err)
		}
	default:
		return fmt.Errorf("unsupported prepared workspace status %q", w.Status)
	}
	if w.CreatedAt < 0 || w.UpdatedAt < w.CreatedAt {
		return errors.New("prepared workspace timestamps are invalid")
	}
	return nil
}

func validateNewPreparedWorkspace(workspace PreparedWorkspace) error {
	if workspace.Status != "" || workspace.ClaimedAgentID != "" ||
		workspace.CreatedAt != 0 || workspace.UpdatedAt != 0 {
		return errors.New("new prepared workspace must not contain lifecycle state")
	}
	workspace.Status = PreparedWorkspaceReady
	return workspace.Validate()
}

func samePreparedWorkspace(stored, requested PreparedWorkspace) bool {
	return stored.PreparedWorkspaceKey == requested.PreparedWorkspaceKey &&
		stored.SourceAgentID == requested.SourceAgentID &&
		stored.SourceDeviceID == requested.SourceDeviceID &&
		stored.TargetDeviceID == requested.TargetDeviceID &&
		stored.GitURL == requested.GitURL && stored.HeadOID == requested.HeadOID &&
		stored.ObjectFormat == requested.ObjectFormat &&
		stored.WorkingDirectory == requested.WorkingDirectory &&
		stored.WorkspacePath == requested.WorkspacePath &&
		stored.Strategy == requested.Strategy && stored.ManifestHash == requested.ManifestHash &&
		slices.Equal(stored.Warnings, requested.Warnings)
}

func queryPreparedWorkspace(
	ctx context.Context,
	queryer rowQueryer,
	key PreparedWorkspaceKey,
) (PreparedWorkspace, error) {
	if err := key.Validate(); err != nil {
		return PreparedWorkspace{}, err
	}
	workspace := PreparedWorkspace{PreparedWorkspaceKey: key}
	var warningsJSON string
	err := queryer.QueryRowContext(ctx, `
SELECT source_agent_id, source_device_id, target_device_id, git_url, head_oid,
	object_format, working_directory, workspace_path, strategy, manifest_hash,
	warnings_json, status, claimed_agent_id, created_at, updated_at
FROM prepared_workspaces
WHERE controller_id = ? AND tree_id = ? AND workspace_id = ?
`, key.ControllerID, key.TreeID, key.WorkspaceID).Scan(
		&workspace.SourceAgentID, &workspace.SourceDeviceID, &workspace.TargetDeviceID,
		&workspace.GitURL, &workspace.HeadOID, &workspace.ObjectFormat,
		&workspace.WorkingDirectory, &workspace.WorkspacePath, &workspace.Strategy,
		&workspace.ManifestHash, &warningsJSON, &workspace.Status,
		&workspace.ClaimedAgentID, &workspace.CreatedAt, &workspace.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return PreparedWorkspace{}, ErrNotFound
	}
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("load prepared workspace: %w", err)
	}
	if err := json.Unmarshal([]byte(warningsJSON), &workspace.Warnings); err != nil {
		return PreparedWorkspace{}, errors.New("stored prepared workspace warnings are invalid")
	}
	if err := workspace.Validate(); err != nil {
		return PreparedWorkspace{}, fmt.Errorf("stored prepared workspace is invalid: %w", err)
	}
	return workspace, nil
}
