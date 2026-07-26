package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

type resultApplyAuthorization struct {
	Params protocol.AuthorizeResultApplyParams
	Result protocol.AuthorizeResultApplyResult
}

// AuthorizeResultApply records a durable, metadata-only authorization for one
// root-local apply attempt. It neither checks local inbox bytes nor claims that
// the root worktree was modified.
func (s *Store) AuthorizeResultApply(
	ctx context.Context,
	connectedDeviceID string,
	root control.PrincipalIdentity,
	params protocol.AuthorizeResultApplyParams,
	authorizedAt time.Time,
) (protocol.AuthorizeResultApplyResult, error) {
	if err := identity.ValidateID(connectedDeviceID); err != nil {
		return protocol.AuthorizeResultApplyResult{}, fmt.Errorf("connectedDeviceId %w", err)
	}
	if err := root.Validate(); err != nil {
		return protocol.AuthorizeResultApplyResult{}, fmt.Errorf("root: %w", err)
	}
	if err := params.Validate(); err != nil {
		return protocol.AuthorizeResultApplyResult{}, err
	}
	pathDigest, err := hex.DecodeString(params.SourcePathSHA256)
	if err != nil || len(pathDigest) != sha256.Size {
		return protocol.AuthorizeResultApplyResult{}, errors.New("sourcePathSha256 is invalid")
	}
	timestamp, err := unixTime(authorizedAt, "authorizedAt")
	if err != nil {
		return protocol.AuthorizeResultApplyResult{}, err
	}

	var result protocol.AuthorizeResultApplyResult
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		principal, err := authorizePrincipal(ctx, connection, root, control.CapabilityArtifactApply)
		if err != nil {
			return err
		}
		if principal.ParentAgentID != "" || principal.DeviceID != connectedDeviceID {
			return ErrAuthorizationDenied
		}

		stored, err := queryResultApplyAuthorization(ctx, connection, root, params.ApplyID)
		if err == nil {
			if !protocol.SameAuthorizeResultApplyParams(stored.Params, params) {
				return fmt.Errorf("%w: applyId already identifies another request", ErrConflict)
			}
			result = stored.Result
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}

		resultPackage, err := queryResultPackageByID(
			ctx, connection, root.ControllerID, root.TreeID, params.PackageID,
		)
		if err != nil {
			return err
		}
		if resultPackage.State != ResultPackageDelivered || resultPackage.RootPrincipal != root ||
			resultPackage.RootDeviceID != connectedDeviceID {
			return ErrAuthorizationDenied
		}
		workspace := resultPackage.Manifest.Workspace
		if workspace.Status != protocol.ResultWorkspaceChanged &&
			workspace.Status != protocol.ResultWorkspaceUnchanged {
			return fmt.Errorf("%w: result package has no applicable workspace result", ErrConflict)
		}
		receipt, err := queryWorkspaceSyncReceipt(ctx, connection, WorkspaceSyncKey{
			ControllerID: root.ControllerID, TreeID: root.TreeID,
			SourceAgentID: root.AgentID, SyncID: workspace.WorkspaceID,
		})
		if errors.Is(err, ErrNotFound) {
			return ErrAuthorizationDenied
		}
		if err != nil {
			return err
		}
		if !resultApplyWorkspaceMatchesReceipt(receipt, workspace, resultPackage, root) {
			return ErrAuthorizationDenied
		}
		if receipt.GitURL != params.GitURL ||
			!slices.Equal(receipt.SourcePathHash[:], pathDigest) {
			return fmt.Errorf("%w: apply source differs from the synchronized root workspace", ErrConflict)
		}

		result = protocol.AuthorizeResultApplyResult{
			ApplyID: params.ApplyID, PackageID: params.PackageID,
			ManifestSHA256: resultPackage.Metadata.ManifestDescriptor.SHA256,
			WorkspaceID:    workspace.WorkspaceID, BaseManifestHash: receipt.ManifestHash,
		}
		if err := result.Validate(); err != nil {
			return fmt.Errorf("build result apply authorization: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO result_apply_authorizations(
	controller_id, tree_id, root_agent_id, apply_id, package_id,
	git_url, source_path_digest, manifest_sha256, workspace_id,
	base_manifest_hash, authorized_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, root.ControllerID, root.TreeID, root.AgentID, params.ApplyID, params.PackageID,
			params.GitURL, pathDigest, result.ManifestSHA256, result.WorkspaceID,
			result.BaseManifestHash, timestamp); err != nil {
			return fmt.Errorf("record result apply authorization: %w", err)
		}
		return nil
	})
	return result, err
}

func resultApplyWorkspaceMatchesReceipt(
	receipt WorkspaceSyncReceipt,
	workspace protocol.ResultWorkspaceComponent,
	resultPackage ResultPackageRecord,
	root control.PrincipalIdentity,
) bool {
	return receipt.Status == WorkspaceSyncPrepared && receipt.ConsumedSpawnID != "" &&
		receipt.SourceDeviceID == root.DeviceID && receipt.SourceDeviceID == workspace.SourceDeviceID &&
		receipt.TargetDeviceID == resultPackage.SourcePrincipal.DeviceID &&
		receipt.TargetDeviceID == workspace.TargetDeviceID &&
		receipt.ObjectFormat == workspace.ObjectFormat && receipt.HeadOID == workspace.BaseHeadOID &&
		receipt.ManifestHash == workspace.BaseManifestHash &&
		receipt.SourceSnapshotHash == workspace.BaseSnapshotHash &&
		receipt.SourceClean == workspace.BaseClean && slices.Equal(receipt.Warnings, workspace.BaseWarnings)
}

func queryResultApplyAuthorization(
	ctx context.Context,
	queryer rowQueryer,
	root control.PrincipalIdentity,
	applyID string,
) (resultApplyAuthorization, error) {
	var stored resultApplyAuthorization
	var pathDigest []byte
	err := queryer.QueryRowContext(ctx, `
SELECT apply_id, package_id, git_url, source_path_digest,
	manifest_sha256, workspace_id, base_manifest_hash
FROM result_apply_authorizations
WHERE controller_id = ? AND tree_id = ? AND root_agent_id = ? AND apply_id = ?
`, root.ControllerID, root.TreeID, root.AgentID, applyID).Scan(
		&stored.Params.ApplyID, &stored.Params.PackageID, &stored.Params.GitURL, &pathDigest,
		&stored.Result.ManifestSHA256, &stored.Result.WorkspaceID, &stored.Result.BaseManifestHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return resultApplyAuthorization{}, ErrNotFound
	}
	if err != nil {
		return resultApplyAuthorization{}, fmt.Errorf("load result apply authorization: %w", err)
	}
	if len(pathDigest) != sha256.Size {
		return resultApplyAuthorization{}, errors.New("stored result apply source path digest is invalid")
	}
	stored.Params.SourcePathSHA256 = hex.EncodeToString(pathDigest)
	stored.Result.ApplyID = stored.Params.ApplyID
	stored.Result.PackageID = stored.Params.PackageID
	if err := stored.Params.Validate(); err != nil {
		return resultApplyAuthorization{}, fmt.Errorf("stored result apply request is invalid: %w", err)
	}
	if err := stored.Result.Validate(); err != nil {
		return resultApplyAuthorization{}, fmt.Errorf("stored result apply authorization is invalid: %w", err)
	}
	return stored, nil
}
