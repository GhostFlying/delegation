package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

var (
	ErrResultPackageCursorAhead       = errors.New("result package cursor is ahead of the stored sequence")
	ErrResultPackageLifecycleNotReady = errors.New("result package lifecycle authority is not ready")
	ErrResultPackageSequenceExhausted = errors.New("result package sequence is exhausted")
)

// Root wait exposes one package and overfetches one record to report more work.
const (
	maximumResultPackageStorePage = 2
	maximumPendingResultRelays    = 128
)

type ResultPackageDeliveryState string

const (
	ResultPackageDeliveryPending ResultPackageDeliveryState = "deliveryPending"
	ResultPackageDelivered       ResultPackageDeliveryState = "delivered"
)

type ResultPackageRecord struct {
	Metadata             protocol.ResultPackageMetadata
	Manifest             protocol.ResultManifest
	SourcePrincipal      control.PrincipalIdentity
	RootPrincipal        control.PrincipalIdentity
	RootDeviceID         string
	State                ResultPackageDeliveryState
	Sequence             uint64
	PublishedAt          int64
	DeliveredAt          int64
	SourceAcknowledgedAt int64
}

type ResultPackagePageRequest struct {
	AfterSequence uint64
	Limit         int
}

type ResultPackagePage struct {
	Packages     []ResultPackageRecord
	NextSequence uint64
	Highwater    uint64
}

type ResultPackageRelayCursor struct {
	PublishedAt int64
	PackageID   string
}

type ResultPackageRelayPageRequest struct {
	After *ResultPackageRelayCursor
	Limit int
}

type ResultPackageRelayPage struct {
	Packages  []ResultPackageRecord
	NextAfter *ResultPackageRelayCursor
}

func (s *Store) PublishResultPackage(
	ctx context.Context,
	connectedDeviceID string,
	source control.PrincipalIdentity,
	params protocol.PublishResultPackageParams,
	publishedAt time.Time,
) (protocol.PublishResultPackageResult, error) {
	if err := identity.ValidateID(connectedDeviceID); err != nil {
		return protocol.PublishResultPackageResult{}, fmt.Errorf("connectedDeviceId %w", err)
	}
	if err := source.Validate(); err != nil {
		return protocol.PublishResultPackageResult{}, fmt.Errorf("source: %w", err)
	}
	manifest, err := params.Metadata.DecodeManifest()
	if err != nil {
		return protocol.PublishResultPackageResult{}, err
	}
	if manifest.ControllerID != source.ControllerID || manifest.TreeID != source.TreeID ||
		manifest.SourceAgentID != source.AgentID || manifest.SourceDeviceID != source.DeviceID {
		return protocol.PublishResultPackageResult{}, ErrAuthorizationDenied
	}
	timestamp, err := unixTime(publishedAt, "publishedAt")
	if err != nil {
		return protocol.PublishResultPackageResult{}, err
	}

	result := protocol.PublishResultPackageResult{PackageID: manifest.PackageID}
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

		stored, err := queryResultPackageByID(
			ctx, connection, source.ControllerID, source.TreeID, manifest.PackageID,
		)
		if err == nil {
			return replayResultPackage(stored, params.Metadata, manifest)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		stored, err = queryResultPackageByTurn(
			ctx, connection, source.ControllerID, source.TreeID, source.AgentID, manifest.TurnID,
		)
		if err == nil {
			return replayResultPackage(stored, params.Metadata, manifest)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}

		rootDeviceID, err := authorizeNewResultPackage(
			ctx, connection, connectedDeviceID, principal, manifest,
		)
		if err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO result_packages(
	controller_id, tree_id, package_id, source_agent_id, source_device_id,
	managed_thread_id, turn_id, lifecycle_revision, root_device_id,
	manifest_bytes, manifest_size, manifest_sha256, state, published_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'deliveryPending', ?)
`, manifest.ControllerID, manifest.TreeID, manifest.PackageID, manifest.SourceAgentID,
			manifest.SourceDeviceID, manifest.ManagedThreadID, manifest.TurnID,
			manifest.LifecycleRevision, rootDeviceID, params.Metadata.Manifest,
			params.Metadata.ManifestDescriptor.Size, params.Metadata.ManifestDescriptor.SHA256,
			timestamp); err != nil {
			return fmt.Errorf("create result package metadata: %w", err)
		}
		return nil
	})
	return result, err
}

// GetResultPackageForDelivery returns immutable relay metadata to the package's
// authenticated source. Delivered records retain their source acknowledgement
// state so a lost final acknowledgement can be replayed without transferring
// bytes again.
func (s *Store) GetResultPackageForDelivery(
	ctx context.Context,
	connectedDeviceID string,
	source control.PrincipalIdentity,
	packageID string,
) (ResultPackageRecord, error) {
	if err := identity.ValidateID(connectedDeviceID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("connectedDeviceId %w", err)
	}
	if err := source.Validate(); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("source: %w", err)
	}
	if err := identity.ValidateID(packageID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("packageId %w", err)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ResultPackageRecord{}, fmt.Errorf("begin result package delivery lookup: %w", err)
	}
	defer transaction.Rollback()
	principal, err := authorizePrincipal(
		ctx, transaction, source, control.CapabilityArtifactPublishSelf,
	)
	if err != nil {
		return ResultPackageRecord{}, err
	}
	if principal.ParentAgentID == "" || principal.DeviceID != connectedDeviceID {
		return ResultPackageRecord{}, ErrAuthorizationDenied
	}
	record, err := queryResultPackageByID(
		ctx, transaction, source.ControllerID, source.TreeID, packageID,
	)
	if err != nil {
		return ResultPackageRecord{}, err
	}
	if record.SourcePrincipal != source || record.Manifest.SourceDeviceID != connectedDeviceID {
		return ResultPackageRecord{}, ErrAuthorizationDenied
	}
	if err := transaction.Commit(); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("commit result package delivery lookup: %w", err)
	}
	return record, nil
}

// MarkResultPackageDelivered records that the original root peer made the
// complete package durable. The tree-visible sequence is allocated here, never
// during metadata publication.
func (s *Store) MarkResultPackageDelivered(
	ctx context.Context,
	connectedDeviceID string,
	root control.PrincipalIdentity,
	packageID string,
	deliveredAt time.Time,
) (ResultPackageRecord, error) {
	if err := identity.ValidateID(connectedDeviceID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("connectedDeviceId %w", err)
	}
	if err := root.Validate(); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("root: %w", err)
	}
	if err := identity.ValidateID(packageID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("packageId %w", err)
	}
	timestamp, err := unixTime(deliveredAt, "deliveredAt")
	if err != nil {
		return ResultPackageRecord{}, err
	}

	var record ResultPackageRecord
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		principal, err := authorizePrincipal(
			ctx, connection, root, control.CapabilityAgentManageDescendants,
		)
		if err != nil {
			return err
		}
		if principal.ParentAgentID != "" || principal.DeviceID != connectedDeviceID {
			return ErrAuthorizationDenied
		}
		record, err = queryResultPackageByID(
			ctx, connection, root.ControllerID, root.TreeID, packageID,
		)
		if err != nil {
			return err
		}
		if record.RootPrincipal != root || record.RootDeviceID != connectedDeviceID {
			return ErrAuthorizationDenied
		}
		if record.State == ResultPackageDelivered {
			return nil
		}
		if timestamp < record.PublishedAt {
			return errors.New("deliveredAt precedes result package publication")
		}
		sequence, err := nextTreeResultPackageSequence(
			ctx, connection, root.ControllerID, root.TreeID,
		)
		if err != nil {
			return err
		}
		result, err := connection.ExecContext(ctx, `
UPDATE result_packages
SET state = 'delivered', result_sequence = ?, delivered_at = ?
WHERE controller_id = ? AND tree_id = ? AND package_id = ?
  AND state = 'deliveryPending' AND result_sequence = 0 AND delivered_at = 0
`, sequence, timestamp, root.ControllerID, root.TreeID, packageID)
		if err != nil {
			return fmt.Errorf("mark result package delivered: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect result package delivery update: %w", err)
		}
		if affected != 1 {
			return errors.New("result package state changed during delivery")
		}
		record.State = ResultPackageDelivered
		record.Sequence = sequence
		record.DeliveredAt = timestamp
		return nil
	})
	return record, err
}

// MarkResultPackageSourceAcknowledged records that the source peer accepted
// the final delivery sequence. Replays preserve the first acknowledgement
// timestamp so a broker crash between the peer RPC and this update remains
// recoverable by sending the idempotent acknowledgement again.
func (s *Store) MarkResultPackageSourceAcknowledged(
	ctx context.Context,
	connectedDeviceID string,
	source control.PrincipalIdentity,
	packageID string,
	acknowledgedAt time.Time,
) (ResultPackageRecord, error) {
	if err := identity.ValidateID(connectedDeviceID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("connectedDeviceId %w", err)
	}
	if err := source.Validate(); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("source: %w", err)
	}
	if err := identity.ValidateID(packageID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("packageId %w", err)
	}
	timestamp, err := unixTime(acknowledgedAt, "acknowledgedAt")
	if err != nil {
		return ResultPackageRecord{}, err
	}

	var record ResultPackageRecord
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
		record, err = queryResultPackageByID(
			ctx, connection, source.ControllerID, source.TreeID, packageID,
		)
		if err != nil {
			return err
		}
		if record.SourcePrincipal != source || record.Manifest.SourceDeviceID != connectedDeviceID {
			return ErrAuthorizationDenied
		}
		if record.State != ResultPackageDelivered {
			return ErrConflict
		}
		if record.SourceAcknowledgedAt != 0 {
			return nil
		}
		timestamp = max(timestamp, record.DeliveredAt, int64(1))
		result, err := connection.ExecContext(ctx, `
UPDATE result_packages
SET source_acknowledged_at = ?
WHERE controller_id = ? AND tree_id = ? AND package_id = ?
  AND state = 'delivered' AND source_acknowledged_at = 0
`, timestamp, source.ControllerID, source.TreeID, packageID)
		if err != nil {
			return fmt.Errorf("mark result package source acknowledged: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect result package source acknowledgement update: %w", err)
		}
		if affected != 1 {
			return errors.New("result package state changed during source acknowledgement")
		}
		record.SourceAcknowledgedAt = timestamp
		return nil
	})
	return record, err
}

func (s *Store) ListDeliveredResultPackages(
	ctx context.Context,
	root control.PrincipalIdentity,
	request ResultPackagePageRequest,
) (ResultPackagePage, error) {
	if err := root.Validate(); err != nil {
		return ResultPackagePage{}, fmt.Errorf("root: %w", err)
	}
	if request.AfterSequence > math.MaxInt64 {
		return ResultPackagePage{}, ErrResultPackageCursorAhead
	}
	if request.Limit < 1 || request.Limit > maximumResultPackageStorePage {
		return ResultPackagePage{}, fmt.Errorf(
			"result package page limit must be from 1 through %d", maximumResultPackageStorePage,
		)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ResultPackagePage{}, fmt.Errorf("begin result package page: %w", err)
	}
	defer transaction.Rollback()
	principal, err := authorizePrincipal(
		ctx, transaction, root, control.CapabilityAgentManageDescendants,
	)
	if err != nil {
		return ResultPackagePage{}, err
	}
	if principal.ParentAgentID != "" {
		return ResultPackagePage{}, ErrAuthorizationDenied
	}
	var highwater int64
	if err := transaction.QueryRowContext(ctx, `
SELECT last_result_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, root.ControllerID, root.TreeID).Scan(&highwater); errors.Is(err, sql.ErrNoRows) {
		return ResultPackagePage{}, ErrNotFound
	} else if err != nil {
		return ResultPackagePage{}, fmt.Errorf("load tree result package sequence: %w", err)
	}
	if request.AfterSequence > uint64(highwater) {
		return ResultPackagePage{}, ErrResultPackageCursorAhead
	}
	page := ResultPackagePage{
		Packages: []ResultPackageRecord{}, NextSequence: request.AfterSequence,
		Highwater: uint64(highwater),
	}
	rows, err := transaction.QueryContext(ctx, resultPackageSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.state = 'delivered' AND r.result_sequence > ?
ORDER BY r.result_sequence
LIMIT ?
`, root.ControllerID, root.TreeID, request.AfterSequence, request.Limit)
	if err != nil {
		return ResultPackagePage{}, fmt.Errorf("list delivered result packages: %w", err)
	}
	for rows.Next() {
		record, err := scanResultPackage(rows)
		if err != nil {
			rows.Close()
			return ResultPackagePage{}, err
		}
		if record.RootPrincipal != principal.Identity() || record.RootDeviceID != principal.DeviceID {
			rows.Close()
			return ResultPackagePage{}, errors.New("stored result package root differs from its tree")
		}
		page.Packages = append(page.Packages, record)
		page.NextSequence = record.Sequence
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ResultPackagePage{}, fmt.Errorf("list delivered result packages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ResultPackagePage{}, fmt.Errorf("close delivered result packages: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return ResultPackagePage{}, fmt.Errorf("commit result package page: %w", err)
	}
	return page, nil
}

// ListPendingResultPackageRelaysForPeer returns bounded durable relay or source
// acknowledgement work involving a peer. Delivery-pending packages match
// either endpoint; delivered packages match only their source while the final
// acknowledgement remains unconfirmed. Principal identities are joined from
// principals and trees rather than reconstructed from manifest fields.
func (s *Store) ListPendingResultPackageRelaysForPeer(
	ctx context.Context,
	controllerID, deviceID string,
	request ResultPackageRelayPageRequest,
) (ResultPackageRelayPage, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return ResultPackageRelayPage{}, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return ResultPackageRelayPage{}, fmt.Errorf("deviceId %w", err)
	}
	if request.Limit < 1 || request.Limit > maximumPendingResultRelays {
		return ResultPackageRelayPage{}, fmt.Errorf(
			"pending result package relay limit must be from 1 through %d",
			maximumPendingResultRelays,
		)
	}
	afterPublishedAt := int64(0)
	afterPackageID := ""
	hasAfter := 0
	if request.After != nil {
		if request.After.PublishedAt < 0 {
			return ResultPackageRelayPage{}, errors.New(
				"pending result package relay cursor publishedAt must be non-negative",
			)
		}
		if err := identity.ValidateID(request.After.PackageID); err != nil {
			return ResultPackageRelayPage{}, fmt.Errorf("pending result package relay cursor packageId %w", err)
		}
		afterPublishedAt = request.After.PublishedAt
		afterPackageID = request.After.PackageID
		hasAfter = 1
	}
	rows, err := s.db.QueryContext(ctx, resultPackageSelect+`
WHERE r.controller_id = ? AND (
  (r.state = 'deliveryPending' AND (r.source_device_id = ? OR r.root_device_id = ?)) OR
  (r.state = 'delivered' AND r.source_device_id = ? AND r.source_acknowledged_at = 0)
)
AND (? = 0 OR r.published_at > ? OR (r.published_at = ? AND r.package_id > ?))
ORDER BY r.published_at, r.package_id
LIMIT ?
`, controllerID, deviceID, deviceID, deviceID, hasAfter, afterPublishedAt, afterPublishedAt,
		afterPackageID, request.Limit)
	if err != nil {
		return ResultPackageRelayPage{}, fmt.Errorf("list pending result package relays for peer: %w", err)
	}
	defer rows.Close()
	page := ResultPackageRelayPage{
		Packages: make([]ResultPackageRecord, 0, min(request.Limit, 16)),
	}
	for rows.Next() {
		record, err := scanResultPackage(rows)
		if err != nil {
			return ResultPackageRelayPage{}, err
		}
		page.Packages = append(page.Packages, record)
	}
	if err := rows.Err(); err != nil {
		return ResultPackageRelayPage{}, fmt.Errorf("list pending result package relays for peer: %w", err)
	}
	if len(page.Packages) == request.Limit {
		last := page.Packages[len(page.Packages)-1]
		page.NextAfter = &ResultPackageRelayCursor{
			PublishedAt: last.PublishedAt,
			PackageID:   last.Manifest.PackageID,
		}
	}
	return page, nil
}

func authorizeNewResultPackage(
	ctx context.Context,
	queryer rowQueryer,
	connectedDeviceID string,
	principal control.Principal,
	manifest protocol.ResultManifest,
) (string, error) {
	spawn, err := queryAgentSpawnReceiptByAgent(
		ctx, queryer, principal.ControllerID, principal.TreeID, principal.AgentID,
	)
	if errors.Is(err, ErrNotFound) {
		return "", ErrAuthorizationDenied
	}
	if err != nil {
		return "", err
	}
	tree, err := queryTreeByID(ctx, queryer, principal.ControllerID, principal.TreeID)
	if err != nil {
		return "", err
	}
	if spawn.Agent.Status != protocol.AgentSpawnStarted ||
		spawn.Agent.Principal != principal.Identity() ||
		spawn.Agent.Principal.ParentAgentID != tree.RootAgentID ||
		spawn.Agent.Principal.DeviceID != connectedDeviceID {
		return "", ErrAuthorizationDenied
	}

	authority, err := queryWorkerLifecycleAuthority(
		ctx, queryer, principal.ControllerID, principal.TreeID, principal.AgentID,
	)
	if errors.Is(err, ErrNotFound) {
		return "", resultPackageLifecycleMissing(
			ctx, queryer, principal.ControllerID, connectedDeviceID, manifest.LifecycleRevision,
		)
	}
	if err != nil {
		return "", err
	}
	if authority.Snapshot.Revision < manifest.LifecycleRevision {
		return "", resultPackageLifecycleMissing(
			ctx, queryer, principal.ControllerID, connectedDeviceID, manifest.LifecycleRevision,
		)
	}
	if authority.TargetDeviceID != connectedDeviceID ||
		authority.Snapshot.Phase != protocol.WorkerLifecycleFinalizing ||
		authority.Snapshot.CodexThreadID != manifest.ManagedThreadID ||
		authority.Snapshot.ActiveTurnID != manifest.TurnID ||
		authority.AuthorityRevision != manifest.LifecycleRevision {
		return "", fmt.Errorf("%w: result package differs from finalizing worker authority", ErrConflict)
	}

	if err := authorizeResultPackageWorkspace(ctx, queryer, spawn, manifest.Workspace); err != nil {
		return "", err
	}
	return tree.RootDeviceID, nil
}

func resultPackageLifecycleMissing(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, deviceID string,
	requiredRevision uint64,
) error {
	var appliedRevision uint64
	err := queryer.QueryRowContext(ctx, `
SELECT applied_revision
FROM peer_worker_sync_cursors
WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID).Scan(&appliedRevision)
	if errors.Is(err, sql.ErrNoRows) || err == nil && appliedRevision < requiredRevision {
		return ErrResultPackageLifecycleNotReady
	}
	if err != nil {
		return fmt.Errorf("load result package lifecycle cursor: %w", err)
	}
	return fmt.Errorf("%w: result package lifecycle revision has no matching authority", ErrConflict)
}

func authorizeResultPackageWorkspace(
	ctx context.Context,
	queryer rowQueryer,
	spawn AgentSpawnReceipt,
	workspace protocol.ResultWorkspaceComponent,
) error {
	if workspace.Status == protocol.ResultWorkspaceNotManaged {
		if spawn.Agent.WorkspaceID != "" {
			return ErrAuthorizationDenied
		}
		return nil
	}
	if spawn.Agent.WorkspaceID == "" || spawn.Agent.WorkspaceID != workspace.WorkspaceID {
		return ErrAuthorizationDenied
	}
	receipt, err := queryWorkspaceSyncReceipt(ctx, queryer, WorkspaceSyncKey{
		ControllerID:  spawn.Agent.Principal.ControllerID,
		TreeID:        spawn.Agent.Principal.TreeID,
		SourceAgentID: spawn.Agent.Principal.ParentAgentID,
		SyncID:        spawn.Agent.WorkspaceID,
	})
	if errors.Is(err, ErrNotFound) {
		return ErrAuthorizationDenied
	}
	if err != nil {
		return err
	}
	if receipt.Status != WorkspaceSyncPrepared || receipt.ConsumedSpawnID != spawn.Agent.SpawnID ||
		receipt.SourceDeviceID != workspace.SourceDeviceID ||
		receipt.TargetDeviceID != workspace.TargetDeviceID {
		return ErrAuthorizationDenied
	}
	if receipt.ObjectFormat != workspace.ObjectFormat || receipt.HeadOID != workspace.BaseHeadOID ||
		receipt.ManifestHash != workspace.BaseManifestHash ||
		receipt.SourceSnapshotHash != workspace.BaseSnapshotHash ||
		receipt.SourceClean != workspace.BaseClean ||
		!slices.Equal(receipt.Warnings, workspace.BaseWarnings) {
		return fmt.Errorf("%w: result package workspace differs from the consumed receipt", ErrConflict)
	}
	return nil
}

func replayResultPackage(
	stored ResultPackageRecord,
	requested protocol.ResultPackageMetadata,
	manifest protocol.ResultManifest,
) error {
	if stored.Manifest.PackageID != manifest.PackageID ||
		stored.Manifest.SourceAgentID != manifest.SourceAgentID ||
		stored.Manifest.TurnID != manifest.TurnID ||
		!protocol.SameResultPackageMetadata(stored.Metadata, requested) {
		return fmt.Errorf("%w: packageId or source turn already identifies a different result", ErrConflict)
	}
	return nil
}

func queryAgentSpawnReceiptByAgent(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID, agentID string,
) (AgentSpawnReceipt, error) {
	return scanAgentSpawnReceipt(queryer.QueryRowContext(ctx, agentSpawnSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.agent_id = ?
`, controllerID, treeID, agentID))
}

func queryResultPackageByID(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID, packageID string,
) (ResultPackageRecord, error) {
	return scanResultPackage(queryer.QueryRowContext(ctx, resultPackageSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.package_id = ?
`, controllerID, treeID, packageID))
}

func queryResultPackageByTurn(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID, sourceAgentID, turnID string,
) (ResultPackageRecord, error) {
	return scanResultPackage(queryer.QueryRowContext(ctx, resultPackageSelect+`
WHERE r.controller_id = ? AND r.tree_id = ? AND r.source_agent_id = ? AND r.turn_id = ?
`, controllerID, treeID, sourceAgentID, turnID))
}

func scanResultPackage(scanner rowScanner) (ResultPackageRecord, error) {
	var record ResultPackageRecord
	var (
		controllerID, treeID, packageID, sourceAgentID, sourceDeviceID string
		managedThreadID, turnID, sourceParentAgentID, rootAgentID      string
		principalSourceDeviceID, treeRootDeviceID                      string
		lifecycleRevision, manifestSize                                uint64
		manifestSHA256                                                 string
	)
	err := scanner.Scan(
		&controllerID, &treeID, &packageID, &sourceAgentID, &sourceDeviceID,
		&managedThreadID, &turnID, &lifecycleRevision, &record.RootDeviceID,
		&record.Metadata.Manifest, &manifestSize, &manifestSHA256, &record.State,
		&record.Sequence, &record.PublishedAt, &record.DeliveredAt,
		&record.SourceAcknowledgedAt,
		&sourceParentAgentID, &principalSourceDeviceID, &rootAgentID, &treeRootDeviceID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ResultPackageRecord{}, ErrNotFound
	}
	if err != nil {
		return ResultPackageRecord{}, fmt.Errorf("load result package metadata: %w", err)
	}
	record.Metadata.ManifestDescriptor = protocol.ResultPackagePartDescriptor{
		Kind: protocol.ResultPackagePartManifest, Size: int64(manifestSize), SHA256: manifestSHA256,
	}
	manifest, err := record.Metadata.DecodeManifest()
	if err != nil {
		return ResultPackageRecord{}, fmt.Errorf("stored result package manifest is invalid: %w", err)
	}
	record.Manifest = manifest
	record.SourcePrincipal = control.PrincipalIdentity{
		ControllerID:  controllerID,
		TreeID:        treeID,
		AgentID:       sourceAgentID,
		ParentAgentID: sourceParentAgentID,
		DeviceID:      sourceDeviceID,
	}
	record.RootPrincipal = control.PrincipalIdentity{
		ControllerID: controllerID,
		TreeID:       treeID,
		AgentID:      rootAgentID,
		DeviceID:     treeRootDeviceID,
	}
	if manifest.ControllerID != controllerID || manifest.TreeID != treeID ||
		manifest.PackageID != packageID || manifest.SourceAgentID != sourceAgentID ||
		manifest.SourceDeviceID != sourceDeviceID || manifest.ManagedThreadID != managedThreadID ||
		manifest.TurnID != turnID || manifest.LifecycleRevision != lifecycleRevision {
		return ResultPackageRecord{}, errors.New("stored result package authority differs from its manifest")
	}
	if err := identity.ValidateID(record.RootDeviceID); err != nil {
		return ResultPackageRecord{}, fmt.Errorf("stored result package rootDeviceId %w", err)
	}
	if principalSourceDeviceID != sourceDeviceID || treeRootDeviceID != record.RootDeviceID {
		return ResultPackageRecord{}, errors.New("stored result package principals differ from its authority")
	}
	if err := record.SourcePrincipal.Validate(); err != nil || record.SourcePrincipal.ParentAgentID == "" {
		return ResultPackageRecord{}, errors.New("stored result package source principal is invalid")
	}
	if err := record.RootPrincipal.Validate(); err != nil || record.RootPrincipal.ParentAgentID != "" {
		return ResultPackageRecord{}, errors.New("stored result package root principal is invalid")
	}
	if record.PublishedAt < 0 {
		return ResultPackageRecord{}, errors.New("stored result package publication time is invalid")
	}
	switch record.State {
	case ResultPackageDeliveryPending:
		if record.Sequence != 0 || record.DeliveredAt != 0 || record.SourceAcknowledgedAt != 0 {
			return ResultPackageRecord{}, errors.New("stored pending result package contains delivery state")
		}
	case ResultPackageDelivered:
		if record.Sequence == 0 || record.Sequence > math.MaxInt64 ||
			record.DeliveredAt < record.PublishedAt ||
			(record.SourceAcknowledgedAt != 0 && record.SourceAcknowledgedAt < record.DeliveredAt) {
			return ResultPackageRecord{}, errors.New("stored delivered result package state is invalid")
		}
	default:
		return ResultPackageRecord{}, fmt.Errorf("stored result package has unsupported state %q", record.State)
	}
	return record, nil
}

func nextTreeResultPackageSequence(
	ctx context.Context,
	connection *sql.Conn,
	controllerID, treeID string,
) (uint64, error) {
	var current int64
	if err := connection.QueryRowContext(ctx, `
SELECT last_result_sequence
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, controllerID, treeID).Scan(&current); errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, fmt.Errorf("load tree result package sequence: %w", err)
	}
	if current == math.MaxInt64 {
		return 0, ErrResultPackageSequenceExhausted
	}
	next := current + 1
	result, err := connection.ExecContext(ctx, `
UPDATE trees
SET last_result_sequence = ?
WHERE controller_id = ? AND tree_id = ? AND last_result_sequence = ?
`, next, controllerID, treeID, current)
	if err != nil {
		return 0, fmt.Errorf("advance tree result package sequence: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect tree result package sequence update: %w", err)
	}
	if affected != 1 {
		return 0, errors.New("tree result package sequence changed during update")
	}
	return uint64(next), nil
}

const resultPackageSelect = `
SELECT r.controller_id, r.tree_id, r.package_id, r.source_agent_id, r.source_device_id,
	r.managed_thread_id, r.turn_id, r.lifecycle_revision, r.root_device_id,
	r.manifest_bytes, r.manifest_size, r.manifest_sha256, r.state, r.result_sequence,
	r.published_at, r.delivered_at, r.source_acknowledged_at,
	source.parent_agent_id, source.device_id,
	tree.root_agent_id, tree.root_device_id
FROM result_packages AS r
JOIN principals AS source
  ON source.controller_id = r.controller_id
 AND source.tree_id = r.tree_id
 AND source.agent_id = r.source_agent_id
JOIN trees AS tree
  ON tree.controller_id = r.controller_id
 AND tree.tree_id = r.tree_id
`
