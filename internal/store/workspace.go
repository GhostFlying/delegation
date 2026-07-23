package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

type WorkspaceSyncStatus string

const (
	WorkspaceSyncPending   WorkspaceSyncStatus = "pending"
	WorkspaceSyncInspected WorkspaceSyncStatus = "inspected"
	WorkspaceSyncPrepared  WorkspaceSyncStatus = "prepared"
)

type WorkspaceSyncIntent struct {
	Source         control.PrincipalIdentity
	SyncID         string
	TargetDeviceID string
	GitURL         string
	SourcePathHash [sha256.Size]byte
}

type WorkspaceSyncKey struct {
	ControllerID  string
	TreeID        string
	SourceAgentID string
	SyncID        string
}

type WorkspaceSyncReceipt struct {
	Key                WorkspaceSyncKey
	SourceDeviceID     string
	TargetDeviceID     string
	GitURL             string
	SourcePathHash     [sha256.Size]byte
	Status             WorkspaceSyncStatus
	HeadOID            string
	ObjectFormat       string
	WorkingDirectory   string
	SourceClean        bool
	SourceSnapshotHash string
	Strategy           protocol.WorkspaceStrategy
	ManifestHash       string
	Warnings           []string
	ConsumedSpawnID    string
	CreatedAt          int64
	UpdatedAt          int64
}

func (r WorkspaceSyncReceipt) Manifest() protocol.WorkspaceManifest {
	return protocol.WorkspaceManifest{
		GitURL: r.GitURL, HeadOID: r.HeadOID, ObjectFormat: r.ObjectFormat,
		WorkingDirectory: r.WorkingDirectory, Clean: r.SourceClean,
		SourceSnapshotHash: r.SourceSnapshotHash,
		Warnings:           append([]string{}, r.Warnings...),
	}
}

func (r WorkspaceSyncReceipt) Summary() protocol.WorkspaceSummary {
	return protocol.WorkspaceSummary{
		WorkspaceID: r.Key.SyncID, SourceDeviceID: r.SourceDeviceID,
		TargetDeviceID: r.TargetDeviceID, HeadOID: r.HeadOID,
		ObjectFormat: r.ObjectFormat, WorkingDirectory: r.WorkingDirectory,
		Strategy: r.Strategy, ManifestHash: r.ManifestHash,
		Warnings: append([]string(nil), r.Warnings...),
	}
}

func (s *Store) BeginWorkspaceSync(
	ctx context.Context,
	intent WorkspaceSyncIntent,
	createdAt time.Time,
) (WorkspaceSyncReceipt, error) {
	if err := validateWorkspaceSyncIntent(intent); err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	timestamp, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	key := WorkspaceSyncKey{
		ControllerID: intent.Source.ControllerID, TreeID: intent.Source.TreeID,
		SourceAgentID: intent.Source.AgentID, SyncID: intent.SyncID,
	}
	var receipt WorkspaceSyncReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if _, err := authorizePrincipal(ctx, connection, intent.Source, control.CapabilityWorkspaceSync); err != nil {
			return err
		}
		receipt, err = queryWorkspaceSyncReceipt(ctx, connection, key)
		if err == nil {
			if !workspaceReceiptMatchesIntent(receipt, intent) {
				return fmt.Errorf("%w: syncId already identifies another request", ErrConflict)
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		target, err := queryDevice(ctx, connection, intent.Source.ControllerID, intent.TargetDeviceID)
		if err != nil {
			return fmt.Errorf("workspace target: %w", err)
		}
		if !target.Online {
			return fmt.Errorf("%w: workspace target must be online", ErrConflict)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO workspace_sync_receipts(
	controller_id, tree_id, source_agent_id, sync_id, source_device_id,
	target_device_id, git_url, source_path_digest, status, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
`, key.ControllerID, key.TreeID, key.SourceAgentID, key.SyncID,
			intent.Source.DeviceID, intent.TargetDeviceID, intent.GitURL,
			intent.SourcePathHash[:], timestamp, timestamp); err != nil {
			return fmt.Errorf("create workspace sync receipt: %w", err)
		}
		receipt = WorkspaceSyncReceipt{
			Key: key, SourceDeviceID: intent.Source.DeviceID,
			TargetDeviceID: intent.TargetDeviceID, GitURL: intent.GitURL,
			SourcePathHash: intent.SourcePathHash, Status: WorkspaceSyncPending,
			Warnings: []string{}, CreatedAt: timestamp, UpdatedAt: timestamp,
		}
		return nil
	})
	return receipt, err
}

func (s *Store) FinishWorkspaceSync(
	ctx context.Context,
	key WorkspaceSyncKey,
	summary protocol.WorkspaceSummary,
	observedAt time.Time,
) (WorkspaceSyncReceipt, error) {
	if err := key.Validate(); err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	if err := summary.Validate(); err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	if summary.WorkspaceID != key.SyncID {
		return WorkspaceSyncReceipt{}, errors.New("workspace summary does not match its sync key")
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	warningsJSON, err := json.Marshal(summary.Warnings)
	if err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	var receipt WorkspaceSyncReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		receipt, err = queryWorkspaceSyncReceipt(ctx, connection, key)
		if err != nil {
			return err
		}
		if receipt.Status == WorkspaceSyncPrepared {
			if !sameWorkspaceSummary(receipt.Summary(), summary) {
				return fmt.Errorf("%w: workspace result differs", ErrConflict)
			}
			return nil
		}
		expectedWarnings, warningErr := protocol.WorkspaceWarningsForStrategy(receipt.Warnings, summary.Strategy)
		if warningErr != nil || receipt.Status != WorkspaceSyncInspected ||
			receipt.ManifestHash != summary.ManifestHash ||
			receipt.HeadOID != summary.HeadOID || receipt.ObjectFormat != summary.ObjectFormat ||
			receipt.WorkingDirectory != summary.WorkingDirectory ||
			!slices.Equal(expectedWarnings, summary.Warnings) {
			return fmt.Errorf("%w: workspace result differs from inspected source", ErrConflict)
		}
		if receipt.SourceDeviceID != summary.SourceDeviceID ||
			receipt.TargetDeviceID != summary.TargetDeviceID {
			return fmt.Errorf("%w: workspace result authority differs", ErrConflict)
		}
		if timestamp < receipt.UpdatedAt {
			return errors.New("observedAt precedes workspace sync state")
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE workspace_sync_receipts
SET status = 'prepared', head_oid = ?, object_format = ?, working_directory = ?,
	strategy = ?, manifest_hash = ?, warnings_json = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND sync_id = ?
`, summary.HeadOID, summary.ObjectFormat, summary.WorkingDirectory, summary.Strategy,
			summary.ManifestHash, string(warningsJSON), timestamp, key.ControllerID,
			key.TreeID, key.SourceAgentID, key.SyncID); err != nil {
			return fmt.Errorf("finish workspace sync: %w", err)
		}
		receipt.Status = WorkspaceSyncPrepared
		receipt.HeadOID = summary.HeadOID
		receipt.ObjectFormat = summary.ObjectFormat
		receipt.WorkingDirectory = summary.WorkingDirectory
		receipt.Strategy = summary.Strategy
		receipt.ManifestHash = summary.ManifestHash
		receipt.Warnings = append([]string(nil), summary.Warnings...)
		receipt.UpdatedAt = timestamp
		return nil
	})
	return receipt, err
}

func (s *Store) PinWorkspaceSyncManifest(
	ctx context.Context,
	key WorkspaceSyncKey,
	manifest protocol.WorkspaceManifest,
	observedAt time.Time,
) (WorkspaceSyncReceipt, error) {
	if err := key.Validate(); err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	timestamp, err := unixTime(observedAt, "observedAt")
	if err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	warningsJSON, err := json.Marshal(manifest.Warnings)
	if err != nil {
		return WorkspaceSyncReceipt{}, err
	}
	var receipt WorkspaceSyncReceipt
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		receipt, err = queryWorkspaceSyncReceipt(ctx, connection, key)
		if err != nil {
			return err
		}
		if receipt.GitURL != manifest.GitURL {
			return fmt.Errorf("%w: inspected Git URL differs", ErrConflict)
		}
		if receipt.Status != WorkspaceSyncPending {
			if receipt.ManifestHash != manifestHash || !sameWorkspaceManifest(receipt.Manifest(), manifest) {
				return fmt.Errorf("%w: source workspace changed for this syncId", ErrConflict)
			}
			return nil
		}
		if timestamp < receipt.CreatedAt {
			return errors.New("observedAt precedes workspace sync creation")
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE workspace_sync_receipts
SET status = 'inspected', head_oid = ?, object_format = ?, working_directory = ?,
	source_clean = ?, source_snapshot_hash = ?, manifest_hash = ?, warnings_json = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND sync_id = ?
`, manifest.HeadOID, manifest.ObjectFormat, manifest.WorkingDirectory,
			manifest.Clean, manifest.SourceSnapshotHash, manifestHash, string(warningsJSON), timestamp,
			key.ControllerID, key.TreeID, key.SourceAgentID, key.SyncID); err != nil {
			return fmt.Errorf("pin workspace manifest: %w", err)
		}
		receipt.Status = WorkspaceSyncInspected
		receipt.HeadOID = manifest.HeadOID
		receipt.ObjectFormat = manifest.ObjectFormat
		receipt.WorkingDirectory = manifest.WorkingDirectory
		receipt.SourceClean = manifest.Clean
		receipt.SourceSnapshotHash = manifest.SourceSnapshotHash
		receipt.ManifestHash = manifestHash
		receipt.Warnings = append([]string{}, manifest.Warnings...)
		receipt.UpdatedAt = timestamp
		return nil
	})
	return receipt, err
}

func sameWorkspaceManifest(left, right protocol.WorkspaceManifest) bool {
	return left.GitURL == right.GitURL && left.HeadOID == right.HeadOID &&
		left.ObjectFormat == right.ObjectFormat && left.WorkingDirectory == right.WorkingDirectory &&
		left.Clean == right.Clean && left.SourceSnapshotHash == right.SourceSnapshotHash &&
		slices.Equal(left.Warnings, right.Warnings)
}

func sameWorkspaceSummary(left, right protocol.WorkspaceSummary) bool {
	return left.WorkspaceID == right.WorkspaceID &&
		left.SourceDeviceID == right.SourceDeviceID &&
		left.TargetDeviceID == right.TargetDeviceID &&
		left.HeadOID == right.HeadOID &&
		left.ObjectFormat == right.ObjectFormat &&
		left.WorkingDirectory == right.WorkingDirectory &&
		left.Strategy == right.Strategy && left.ManifestHash == right.ManifestHash &&
		slices.Equal(left.Warnings, right.Warnings)
}

func (k WorkspaceSyncKey) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "controllerId", value: k.ControllerID},
		{name: "treeId", value: k.TreeID},
		{name: "sourceAgentId", value: k.SourceAgentID},
		{name: "syncId", value: k.SyncID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

func validateWorkspaceSyncIntent(intent WorkspaceSyncIntent) error {
	if err := intent.Source.Validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if intent.Source.ParentAgentID != "" {
		return errors.New("workspace source must be a tree root")
	}
	if err := identity.ValidateID(intent.SyncID); err != nil {
		return fmt.Errorf("syncId %w", err)
	}
	if err := identity.ValidateID(intent.TargetDeviceID); err != nil {
		return fmt.Errorf("targetDeviceId %w", err)
	}
	if intent.GitURL == "" || len(intent.GitURL) > protocol.MaximumGitURLBytes {
		return errors.New("gitUrl is invalid")
	}
	return nil
}

func workspaceReceiptMatchesIntent(receipt WorkspaceSyncReceipt, intent WorkspaceSyncIntent) bool {
	return receipt.Key.ControllerID == intent.Source.ControllerID &&
		receipt.Key.TreeID == intent.Source.TreeID &&
		receipt.Key.SourceAgentID == intent.Source.AgentID &&
		receipt.Key.SyncID == intent.SyncID &&
		receipt.SourceDeviceID == intent.Source.DeviceID &&
		receipt.TargetDeviceID == intent.TargetDeviceID &&
		receipt.GitURL == intent.GitURL && receipt.SourcePathHash == intent.SourcePathHash
}

func queryWorkspaceSyncReceipt(
	ctx context.Context,
	queryer rowQueryer,
	key WorkspaceSyncKey,
) (WorkspaceSyncReceipt, error) {
	var receipt WorkspaceSyncReceipt
	receipt.Key = key
	var digest []byte
	var warningsJSON string
	err := queryer.QueryRowContext(ctx, `
SELECT source_device_id, target_device_id, git_url, source_path_digest, status,
	head_oid, object_format, working_directory, source_clean, source_snapshot_hash,
	strategy, manifest_hash,
	warnings_json, consumed_spawn_id, created_at, updated_at
FROM workspace_sync_receipts
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND sync_id = ?
`, key.ControllerID, key.TreeID, key.SourceAgentID, key.SyncID).Scan(
		&receipt.SourceDeviceID, &receipt.TargetDeviceID, &receipt.GitURL, &digest,
		&receipt.Status, &receipt.HeadOID, &receipt.ObjectFormat,
		&receipt.WorkingDirectory, &receipt.SourceClean, &receipt.SourceSnapshotHash,
		&receipt.Strategy, &receipt.ManifestHash,
		&warningsJSON, &receipt.ConsumedSpawnID, &receipt.CreatedAt, &receipt.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceSyncReceipt{}, ErrNotFound
	}
	if err != nil {
		return WorkspaceSyncReceipt{}, fmt.Errorf("load workspace sync receipt: %w", err)
	}
	if len(digest) != sha256.Size {
		return WorkspaceSyncReceipt{}, errors.New("stored source path digest is invalid")
	}
	copy(receipt.SourcePathHash[:], digest)
	if err := json.Unmarshal([]byte(warningsJSON), &receipt.Warnings); err != nil {
		return WorkspaceSyncReceipt{}, errors.New("stored workspace warnings are invalid")
	}
	if receipt.Status == WorkspaceSyncPrepared {
		if err := receipt.Summary().Validate(); err != nil {
			return WorkspaceSyncReceipt{}, fmt.Errorf("stored workspace sync receipt is invalid: %w", err)
		}
		if err := receipt.Manifest().Validate(); err != nil {
			return WorkspaceSyncReceipt{}, fmt.Errorf("stored workspace source manifest is invalid: %w", err)
		}
	} else if receipt.Status == WorkspaceSyncInspected {
		if err := receipt.Manifest().Validate(); err != nil {
			return WorkspaceSyncReceipt{}, fmt.Errorf("stored workspace source manifest is invalid: %w", err)
		}
		expectedHash, err := protocol.WorkspaceManifestHash(receipt.Manifest())
		if err != nil || expectedHash != receipt.ManifestHash {
			return WorkspaceSyncReceipt{}, errors.New("stored workspace manifest hash is invalid")
		}
	} else if receipt.Status != WorkspaceSyncPending {
		return WorkspaceSyncReceipt{}, errors.New("stored workspace sync status is invalid")
	}
	return receipt, nil
}
