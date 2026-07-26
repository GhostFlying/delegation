package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

func (s *PeerStore) ReserveResultOutbox(
	ctx context.Context,
	key ResultOutboxKey,
	reservedBytes int64,
	observedAt time.Time,
) (ResultOutbox, error) {
	if err := key.Validate(); err != nil {
		return ResultOutbox{}, err
	}
	if reservedBytes < 1 || reservedBytes > protocol.MaximumResultPackageBytes {
		return ResultOutbox{}, fmt.Errorf(
			"reservedBytes must be from 1 through %d", protocol.MaximumResultPackageBytes,
		)
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultOutbox{}, err
	}
	var stored ResultOutbox
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err = reserveResultOutbox(ctx, connection, key, reservedBytes, timestamp)
		return err
	})
	return stored, err
}

// reserveResultOutbox runs inside an existing immediate peer transaction. The
// worker-start checkpoint composes this primitive with worker reservation so
// result capacity is durable before turn/start can be written.
func reserveResultOutbox(
	ctx context.Context,
	connection *sql.Conn,
	key ResultOutboxKey,
	reservedBytes, timestamp int64,
) (ResultOutbox, error) {
	if err := key.Validate(); err != nil {
		return ResultOutbox{}, err
	}
	if reservedBytes < 1 || reservedBytes > protocol.MaximumResultPackageBytes {
		return ResultOutbox{}, fmt.Errorf(
			"reservedBytes must be from 1 through %d", protocol.MaximumResultPackageBytes,
		)
	}
	if timestamp < 0 {
		return ResultOutbox{}, errors.New("result outbox timestamp must not be negative")
	}
	want := ResultOutbox{
		ResultOutboxKey:       key,
		State:                 ResultOutboxCapturePending,
		ReservationLimitBytes: reservedBytes,
		ReservedBytes:         reservedBytes,
		CreatedAt:             timestamp,
		UpdatedAt:             timestamp,
	}
	stored, err := queryResultOutboxByPackage(ctx, connection, key.PackageID)
	if err == nil {
		if stored.ResultOutboxKey != key || stored.ReservationLimitBytes != reservedBytes {
			return ResultOutbox{}, ErrResultPackageConflict
		}
		return stored, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return ResultOutbox{}, err
	}
	worker, err := queryWorker(ctx, connection, key.WorkerKey)
	if err != nil {
		return ResultOutbox{}, err
	}
	if worker.DeviceID != key.SourceDeviceID {
		return ResultOutbox{}, ErrResultPackageAuthority
	}
	if err := requireResultStoreCapacity(ctx, connection, "peer_result_outbox", reservedBytes); err != nil {
		return ResultOutbox{}, err
	}
	if _, err := connection.ExecContext(ctx, `
INSERT INTO peer_result_outbox(
	controller_id, tree_id, source_agent_id, source_device_id, package_id,
	state, reservation_limit_bytes, reserved_bytes, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, key.ControllerID, key.TreeID, key.AgentID, key.SourceDeviceID, key.PackageID,
		want.State, want.ReservationLimitBytes, want.ReservedBytes,
		want.CreatedAt, want.UpdatedAt); err != nil {
		return ResultOutbox{}, fmt.Errorf("reserve result outbox: %w", err)
	}
	return want, nil
}

// CommitResultOutboxCapture is the database commit boundary after the caller
// has durably published the fixed package files and containing directory. It
// never writes payload bytes itself.
func (s *PeerStore) CommitResultOutboxCapture(
	ctx context.Context,
	key ResultOutboxKey,
	metadata protocol.ResultPackageMetadata,
	observedAt time.Time,
) (ResultOutbox, error) {
	if err := key.Validate(); err != nil {
		return ResultOutbox{}, err
	}
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		return ResultOutbox{}, err
	}
	if err := validateStoredResultMetadata(key, metadata, manifest); err != nil {
		return ResultOutbox{}, err
	}
	packageBytes, err := resultPackageBytes(metadata)
	if err != nil {
		return ResultOutbox{}, err
	}
	partsJSON, err := encodeResultParts(manifest.Parts)
	if err != nil {
		return ResultOutbox{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultOutbox{}, err
	}
	var stored ResultOutbox
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err = queryResultOutbox(ctx, connection, key)
		if err != nil {
			return err
		}
		if stored.State != ResultOutboxCapturePending {
			if protocol.SameResultPackageMetadata(stored.Metadata, metadata) {
				return nil
			}
			return ErrResultPackageConflict
		}
		if packageBytes > stored.ReservationLimitBytes {
			return ErrResultPackageQuota
		}
		worker, queryErr := queryWorker(ctx, connection, key.WorkerKey)
		if queryErr != nil {
			return queryErr
		}
		if err := validateResultOutboxWorkerAuthority(ctx, connection, worker, manifest); err != nil {
			return err
		}
		stored.State = ResultOutboxPublishPending
		stored.Metadata = cloneResultMetadata(metadata)
		stored.Manifest = manifest
		stored.PackageBytes = packageBytes
		stored.ReservedBytes = packageBytes
		stored.UpdatedAt = max(timestamp, stored.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_result_outbox SET
	state = ?, managed_thread_id = ?, turn_id = ?, lifecycle_revision = ?,
	manifest_bytes = ?, manifest_size_bytes = ?, manifest_sha256 = ?, parts_json = ?,
	reserved_bytes = ?, package_bytes = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND package_id = ?
	AND state = 'capturePending'
`, stored.State, manifest.ManagedThreadID, manifest.TurnID, manifest.LifecycleRevision,
			metadata.Manifest, metadata.ManifestDescriptor.Size, metadata.ManifestDescriptor.SHA256, partsJSON,
			stored.ReservedBytes, stored.PackageBytes, stored.UpdatedAt,
			key.ControllerID, key.TreeID, key.AgentID, key.PackageID); execErr != nil {
			return fmt.Errorf("commit result outbox capture: %w", execErr)
		}
		return nil
	})
	return stored, err
}

// AcknowledgeResultOutboxMetadata records the broker's authoritative metadata
// acknowledgement. The caller may release the worker slot only after this
// transaction succeeds.
func (s *PeerStore) AcknowledgeResultOutboxMetadata(
	ctx context.Context,
	key ResultOutboxKey,
	observedAt time.Time,
) (ResultOutbox, error) {
	return s.transitionResultOutbox(ctx, key, observedAt, ResultOutboxPublishPending, ResultOutboxDeliveryPending, 0)
}

func (s *PeerStore) AcknowledgeResultOutboxDelivery(
	ctx context.Context,
	key ResultOutboxKey,
	sequence uint64,
	observedAt time.Time,
) (ResultOutbox, error) {
	if sequence == 0 || sequence > math.MaxInt64 {
		return ResultOutbox{}, errors.New("delivery sequence must be a positive signed 64-bit integer")
	}
	return s.transitionResultOutbox(
		ctx, key, observedAt, ResultOutboxDeliveryPending, ResultOutboxDelivered, sequence,
	)
}

func (s *PeerStore) transitionResultOutbox(
	ctx context.Context,
	key ResultOutboxKey,
	observedAt time.Time,
	from, to ResultOutboxState,
	sequence uint64,
) (ResultOutbox, error) {
	if err := key.Validate(); err != nil {
		return ResultOutbox{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultOutbox{}, err
	}
	var stored ResultOutbox
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err = queryResultOutbox(ctx, connection, key)
		if err != nil {
			return err
		}
		if stored.State == to {
			if stored.DeliverySequence == sequence {
				return nil
			}
			return ErrResultPackageConflict
		}
		if to == ResultOutboxDeliveryPending && stored.State == ResultOutboxDelivered {
			return nil
		}
		if stored.State != from {
			return ErrResultPackageTransition
		}
		stored.State = to
		stored.DeliverySequence = sequence
		stored.UpdatedAt = max(timestamp, stored.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_result_outbox
SET state = ?, delivery_sequence = ?, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND package_id = ? AND state = ?
`, stored.State, stored.DeliverySequence, stored.UpdatedAt,
			key.ControllerID, key.TreeID, key.AgentID, key.PackageID, from); execErr != nil {
			return fmt.Errorf("transition result outbox: %w", execErr)
		}
		return nil
	})
	return stored, err
}

func (s *PeerStore) GetResultOutbox(
	ctx context.Context,
	key ResultOutboxKey,
) (ResultOutbox, error) {
	if err := key.Validate(); err != nil {
		return ResultOutbox{}, err
	}
	return queryResultOutbox(ctx, s.db, key)
}

func (s *PeerStore) ListPendingResultPublications(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultOutbox, error) {
	return s.listResultOutboxes(ctx, controllerID, deviceID, ResultOutboxPublishPending, limit)
}

func (s *PeerStore) ListPendingResultDeliveries(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultOutbox, error) {
	return s.listResultOutboxes(ctx, controllerID, deviceID, ResultOutboxDeliveryPending, limit)
}

// ListDeliveredResultOutboxes returns only ordinary-GC-eligible records,
// oldest first. Pending capture, publication, and delivery never appear.
func (s *PeerStore) ListDeliveredResultOutboxes(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultOutbox, error) {
	return s.listResultOutboxes(ctx, controllerID, deviceID, ResultOutboxDelivered, limit)
}

func (s *PeerStore) GetResultOutboxRetention(ctx context.Context) (ResultPackageRetention, error) {
	return inspectResultStoreCapacity(ctx, s.db, "peer_result_outbox")
}

// DeleteDeliveredResultOutbox is the database commit boundary after the caller
// has removed the package directory and fsynced its parent. Exact replay is a
// no-op; non-delivered records remain protected.
func (s *PeerStore) DeleteDeliveredResultOutbox(ctx context.Context, key ResultOutboxKey) error {
	if err := key.Validate(); err != nil {
		return err
	}
	return withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err := queryResultOutbox(ctx, connection, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if stored.State != ResultOutboxDelivered {
			return ErrResultPackageTransition
		}
		if _, err := connection.ExecContext(ctx, `
DELETE FROM peer_result_outbox
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND package_id = ?
	AND state = 'delivered'
`, key.ControllerID, key.TreeID, key.AgentID, key.PackageID); err != nil {
			return fmt.Errorf("delete delivered result outbox: %w", err)
		}
		return nil
	})
}

func (s *PeerStore) listResultOutboxes(
	ctx context.Context,
	controllerID, deviceID string,
	state ResultOutboxState,
	limit int,
) ([]ResultOutbox, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	switch state {
	case ResultOutboxPublishPending, ResultOutboxDeliveryPending, ResultOutboxDelivered:
	default:
		return nil, fmt.Errorf("unsupported result outbox list state %q", state)
	}
	rows, err := s.db.QueryContext(ctx, resultOutboxSelect+`
WHERE controller_id = ? AND source_device_id = ? AND state = ?
ORDER BY updated_at, created_at, tree_id, source_agent_id, package_id
LIMIT ?
`, controllerID, deviceID, state, limit)
	if err != nil {
		return nil, fmt.Errorf("list result outboxes: %w", err)
	}
	return scanResultOutboxRows(rows)
}

func scanResultOutboxRows(rows *sql.Rows) ([]ResultOutbox, error) {
	defer rows.Close()
	results := make([]ResultOutbox, 0)
	for rows.Next() {
		result, err := scanResultOutbox(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list result outboxes: %w", err)
	}
	return results, nil
}

func queryResultOutbox(
	ctx context.Context,
	queryer rowQueryer,
	key ResultOutboxKey,
) (ResultOutbox, error) {
	return scanResultOutbox(queryer.QueryRowContext(ctx, resultOutboxSelect+`
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND source_device_id = ?
	AND package_id = ?
`, key.ControllerID, key.TreeID, key.AgentID, key.SourceDeviceID, key.PackageID))
}

func queryResultOutboxByPackage(
	ctx context.Context,
	queryer rowQueryer,
	packageID string,
) (ResultOutbox, error) {
	if err := identity.ValidateID(packageID); err != nil {
		return ResultOutbox{}, fmt.Errorf("packageId %w", err)
	}
	return scanResultOutbox(queryer.QueryRowContext(ctx, resultOutboxSelect+`
WHERE package_id = ?
`, packageID))
}

func validateResultOutboxWorkerAuthority(
	ctx context.Context,
	queryer rowQueryer,
	worker WorkerReservation,
	manifest protocol.ResultManifest,
) error {
	if worker.ControllerID != manifest.ControllerID || worker.TreeID != manifest.TreeID ||
		worker.AgentID != manifest.SourceAgentID || worker.DeviceID != manifest.SourceDeviceID ||
		worker.CodexThreadID != manifest.ManagedThreadID || worker.ActiveTurnID != manifest.TurnID ||
		worker.Revision != manifest.LifecycleRevision || worker.Status != WorkerFinalizing {
		return ErrResultPackageAuthority
	}
	switch manifest.Terminal.Outcome {
	case protocol.ResultTerminalCompleted:
		if worker.FinalTarget != WorkerIdle || worker.FinalFailureCode != "" {
			return ErrResultPackageAuthority
		}
	case protocol.ResultTerminalFailed:
		if worker.FinalTarget != WorkerFailed || worker.FinalFailureCode != manifest.Terminal.FailureCode {
			return ErrResultPackageAuthority
		}
	case protocol.ResultTerminalInterrupted:
		if worker.FinalTarget != WorkerInterrupted || worker.FinalFailureCode != manifest.Terminal.FailureCode {
			return ErrResultPackageAuthority
		}
	default:
		return ErrResultPackageAuthority
	}
	if manifest.Workspace.Status == protocol.ResultWorkspaceNotManaged {
		if worker.WorkspaceID != "" {
			return ErrResultPackageAuthority
		}
		return nil
	}
	if worker.WorkspaceID == "" || worker.WorkspaceID != manifest.Workspace.WorkspaceID {
		return ErrResultPackageAuthority
	}
	workspace, err := queryPreparedWorkspace(ctx, queryer, PreparedWorkspaceKey{
		ControllerID: worker.ControllerID,
		TreeID:       worker.TreeID,
		WorkspaceID:  worker.WorkspaceID,
	})
	if err != nil {
		return err
	}
	result := manifest.Workspace
	if workspace.SourceDeviceID != result.SourceDeviceID || workspace.TargetDeviceID != result.TargetDeviceID ||
		workspace.ObjectFormat != result.ObjectFormat || workspace.HeadOID != result.BaseHeadOID ||
		workspace.ManifestHash != result.BaseManifestHash || workspace.SourceSnapshotHash != result.BaseSnapshotHash ||
		workspace.Clean != result.BaseClean || !slices.Equal(workspace.Warnings, result.BaseWarnings) {
		return ErrResultPackageAuthority
	}
	return nil
}

func requireResultStoreCapacity(
	ctx context.Context,
	queryer rowQueryer,
	table string,
	additionalBytes int64,
) error {
	retention, err := inspectResultStoreCapacity(ctx, queryer, table)
	if err != nil {
		return err
	}
	if retention.Count >= MaximumPeerResultPackages ||
		additionalBytes > MaximumPeerResultStoreBytes-retention.Bytes {
		return ErrResultPackageQuota
	}
	return nil
}

func inspectResultStoreCapacity(
	ctx context.Context,
	queryer rowQueryer,
	table string,
) (ResultPackageRetention, error) {
	var query string
	switch table {
	case "peer_result_outbox":
		query = `
SELECT count(*), COALESCE(sum(
	reserved_bytes
), 0)
FROM peer_result_outbox
`
	case "peer_result_inbox":
		query = `SELECT count(*), COALESCE(sum(package_bytes), 0) FROM peer_result_inbox`
	default:
		return ResultPackageRetention{}, errors.New("unsupported result package store")
	}
	var retention ResultPackageRetention
	if err := queryer.QueryRowContext(ctx, query).Scan(&retention.Count, &retention.Bytes); err != nil {
		return ResultPackageRetention{}, fmt.Errorf("inspect result package retention: %w", err)
	}
	return retention, nil
}
