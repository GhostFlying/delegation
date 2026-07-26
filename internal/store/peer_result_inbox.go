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

func (s *PeerStore) BeginResultInbox(
	ctx context.Context,
	authority ResultInboxAuthority,
	params protocol.BeginResultPackageParams,
	observedAt time.Time,
) (protocol.BeginResultPackageResult, error) {
	if err := authority.Validate(); err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	if err := params.Validate(); err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	localLeaseExpiresAt, err := resultInboxLeaseExpiresAt(timestamp)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	manifest, err := params.Metadata.DecodeManifest()
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	if err := validateInboxMetadata(authority, params.PackageID, params.Metadata, manifest); err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	packageBytes, err := resultPackageBytes(params.Metadata)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	partsJSON, err := encodeResultParts(manifest.Parts)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}

	var result protocol.BeginResultPackageResult
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		removal, removalErr := queryResultInboxRemoval(ctx, connection, params.PackageID, params.AttemptID)
		if removalErr == nil {
			if removal.Authority != authority {
				return ErrResultPackageAuthority
			}
			return ErrResultPackageTransition
		}
		if !errors.Is(removalErr, ErrNotFound) {
			return removalErr
		}
		existing, queryErr := queryResultInboxByPackage(ctx, connection, params.PackageID)
		if queryErr == nil {
			if existing.Authority != authority {
				return ErrResultPackageAuthority
			}
			if !protocol.SameResultPackageMetadata(existing.Metadata, params.Metadata) {
				return ErrResultPackageConflict
			}
			switch existing.State {
			case ResultInboxAvailable:
				result = protocol.BeginResultPackageResult{
					AttemptID: params.AttemptID, PackageID: params.PackageID,
					Outcome: protocol.ResultPackageAlreadyAvailable, Offsets: []protocol.ResultPackagePartOffset{},
				}
				return nil
			case ResultInboxReceiving:
				if existing.AttemptID != params.AttemptID || params.LeaseExpiresAt > existing.LeaseExpiresAt {
					return ErrResultPackageConflict
				}
				result = protocol.BeginResultPackageResult{
					AttemptID: params.AttemptID, PackageID: params.PackageID,
					Outcome: protocol.ResultPackageReceiving, Offsets: slices.Clone(existing.Offsets),
				}
				return nil
			case ResultInboxEvictionTombstone:
				return ErrResultPackageTransition
			default:
				return ErrResultPackageTransition
			}
		}
		if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		prior, queryErr := queryResultInboxRemovalByPackage(ctx, connection, params.PackageID)
		if queryErr == nil {
			if prior.Authority != authority {
				return ErrResultPackageAuthority
			}
		} else if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		if params.LeaseExpiresAt <= timestamp || params.LeaseExpiresAt > localLeaseExpiresAt {
			return fmt.Errorf(
				"leaseExpiresAt must be after observedAt and at most %s in the future",
				MaximumResultInboxLease,
			)
		}
		if err := requireResultStoreCapacity(ctx, connection, "peer_result_inbox", packageBytes); err != nil {
			return err
		}
		if _, execErr := connection.ExecContext(ctx, `
INSERT INTO peer_result_inbox(
	controller_id, tree_id, root_agent_id, root_device_id,
	source_agent_id, source_device_id, managed_thread_id, turn_id,
	package_id, state, attempt_id, lease_expires_at, lifecycle_revision,
	manifest_bytes, manifest_size_bytes, manifest_sha256, parts_json,
	package_bytes, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, authority.ControllerID, authority.TreeID, authority.RootAgentID, authority.RootDeviceID,
			manifest.SourceAgentID, manifest.SourceDeviceID, manifest.ManagedThreadID, manifest.TurnID,
			params.PackageID, ResultInboxReceiving, params.AttemptID, localLeaseExpiresAt,
			manifest.LifecycleRevision, params.Metadata.Manifest, params.Metadata.ManifestDescriptor.Size,
			params.Metadata.ManifestDescriptor.SHA256, partsJSON, packageBytes, timestamp, timestamp); execErr != nil {
			return fmt.Errorf("begin result inbox: %w", execErr)
		}
		offsets := make([]protocol.ResultPackagePartOffset, 0, len(manifest.Parts))
		for _, part := range manifest.Parts {
			if _, execErr := connection.ExecContext(ctx, `
INSERT INTO peer_result_inbox_parts(package_id, kind, size_bytes, sha256, next_offset)
VALUES (?, ?, ?, ?, 0)
`, params.PackageID, part.Kind, part.Size, part.SHA256); execErr != nil {
				return fmt.Errorf("create result inbox part: %w", execErr)
			}
			offsets = append(offsets, protocol.ResultPackagePartOffset{Kind: part.Kind})
		}
		result = protocol.BeginResultPackageResult{
			AttemptID: params.AttemptID, PackageID: params.PackageID,
			Outcome: protocol.ResultPackageReceiving, Offsets: offsets,
		}
		return nil
	})
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	return result, result.Validate()
}

// CommitResultInboxChunk advances the durable offset only after the caller has
// written and fsynced the exact chunk. For a replay below the offset returned
// by GetResultInbox or BeginResultInbox, the caller must instead read back and
// verify the durable file bytes before calling; the stored chunk receipt then
// proves the replay identity. A crash before a new-chunk commit is recovered by
// truncating the file back to the still-recorded offset.
func (s *PeerStore) CommitResultInboxChunk(
	ctx context.Context,
	authority ResultInboxAuthority,
	attemptID, packageID string,
	chunk ResultInboxChunkCommit,
	observedAt time.Time,
) (ResultInboxChunkCommitResult, error) {
	if err := authority.Validate(); err != nil {
		return ResultInboxChunkCommitResult{}, err
	}
	if err := validateResultPackageAttempt(attemptID, packageID); err != nil {
		return ResultInboxChunkCommitResult{}, err
	}
	if err := chunk.Validate(); err != nil {
		return ResultInboxChunkCommitResult{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultInboxChunkCommitResult{}, err
	}
	if chunk.Offset > protocol.MaximumResultPackageBytes-chunk.Size {
		return ResultInboxChunkCommitResult{}, ErrResultPackageConflict
	}
	var committed ResultInboxChunkCommitResult
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		if _, removalErr := queryResultInboxRemoval(ctx, connection, packageID, attemptID); removalErr == nil {
			return ErrResultPackageTransition
		} else if !errors.Is(removalErr, ErrNotFound) {
			return removalErr
		}
		inbox, queryErr := queryResultInboxByPackage(ctx, connection, packageID)
		if queryErr != nil {
			return queryErr
		}
		if inbox.Authority != authority {
			return ErrResultPackageAuthority
		}
		if inbox.State != ResultInboxReceiving || inbox.AttemptID != attemptID {
			return ErrResultPackageTransition
		}
		activityAt := max(timestamp, inbox.UpdatedAt)
		leaseExpiresAt, leaseErr := resultInboxLeaseExpiresAt(activityAt)
		if leaseErr != nil {
			return leaseErr
		}
		leaseExpiresAt = max(leaseExpiresAt, inbox.LeaseExpiresAt)
		var sizeBytes, nextOffset int64
		var partSHA256 string
		if queryErr := connection.QueryRowContext(ctx, `
SELECT size_bytes, sha256, next_offset
FROM peer_result_inbox_parts
WHERE package_id = ? AND kind = ?
`, packageID, chunk.Kind).Scan(&sizeBytes, &partSHA256, &nextOffset); errors.Is(queryErr, sql.ErrNoRows) {
			return ErrResultPackageConflict
		} else if queryErr != nil {
			return fmt.Errorf("load result inbox part offset: %w", queryErr)
		}
		end := chunk.Offset + chunk.Size
		if end > sizeBytes {
			return ErrResultPackageConflict
		}
		expectedSize := min(int64(protocol.ResultPackageChunkBytes), sizeBytes-chunk.Offset)
		if chunk.Size != expectedSize {
			return ErrResultPackageConflict
		}
		if chunk.Offset < nextOffset {
			var storedSize int64
			var storedSHA256 string
			queryErr := connection.QueryRowContext(ctx, `
SELECT size_bytes, sha256
FROM peer_result_inbox_chunks
WHERE package_id = ? AND attempt_id = ? AND kind = ? AND offset_bytes = ?
`, packageID, attemptID, chunk.Kind, chunk.Offset).Scan(&storedSize, &storedSHA256)
			if errors.Is(queryErr, sql.ErrNoRows) {
				return ErrResultPackageConflict
			}
			if queryErr != nil {
				return fmt.Errorf("load result inbox chunk receipt: %w", queryErr)
			}
			if storedSize != chunk.Size || storedSHA256 != chunk.SHA256 || end > nextOffset {
				return ErrResultPackageConflict
			}
			committed = ResultInboxChunkCommitResult{NextOffset: nextOffset, Replay: true}
			return updateResultInboxActivity(
				ctx, connection, packageID, attemptID, activityAt, leaseExpiresAt,
			)
		}
		if chunk.Offset != nextOffset {
			return ErrResultPackageConflict
		}
		if _, execErr := connection.ExecContext(ctx, `
INSERT INTO peer_result_inbox_chunks(
	package_id, attempt_id, kind, offset_bytes, size_bytes, sha256
) VALUES (?, ?, ?, ?, ?, ?)
`, packageID, attemptID, chunk.Kind, chunk.Offset, chunk.Size, chunk.SHA256); execErr != nil {
			return fmt.Errorf("record result inbox chunk: %w", execErr)
		}
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_result_inbox_parts SET next_offset = ?
WHERE package_id = ? AND kind = ? AND next_offset = ? AND size_bytes = ? AND sha256 = ?
`, end, packageID, chunk.Kind, chunk.Offset, sizeBytes, partSHA256); execErr != nil {
			return fmt.Errorf("advance result inbox part offset: %w", execErr)
		}
		if execErr := updateResultInboxActivity(
			ctx, connection, packageID, attemptID, activityAt, leaseExpiresAt,
		); execErr != nil {
			return execErr
		}
		committed = ResultInboxChunkCommitResult{NextOffset: end}
		return nil
	})
	return committed, err
}

func updateResultInboxActivity(
	ctx context.Context,
	connection *sql.Conn,
	packageID, attemptID string,
	updatedAt, leaseExpiresAt int64,
) error {
	if _, err := connection.ExecContext(ctx, `
UPDATE peer_result_inbox SET updated_at = ?, lease_expires_at = ?
WHERE package_id = ? AND state = 'receiving' AND attempt_id = ?
`, updatedAt, leaseExpiresAt, packageID, attemptID); err != nil {
		return fmt.Errorf("update result inbox chunk activity: %w", err)
	}
	return nil
}

func resultInboxLeaseExpiresAt(observedAt int64) (int64, error) {
	maximumLeaseSeconds := int64(MaximumResultInboxLease / time.Second)
	if observedAt > math.MaxInt64-maximumLeaseSeconds {
		return 0, errors.New("result inbox observedAt is too large")
	}
	return observedAt + maximumLeaseSeconds, nil
}

// CommitResultInboxAvailable may be called only after the caller has verified
// every descriptor, fsynced every file and temporary directory, atomically
// renamed the package directory, and fsynced its parent. The database never
// claims availability before that filesystem publication boundary.
func (s *PeerStore) CommitResultInboxAvailable(
	ctx context.Context,
	authority ResultInboxAuthority,
	attemptID, packageID string,
	observedAt time.Time,
) (ResultInbox, error) {
	if err := authority.Validate(); err != nil {
		return ResultInbox{}, err
	}
	if err := validateResultPackageAttempt(attemptID, packageID); err != nil {
		return ResultInbox{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultInbox{}, err
	}
	var result ResultInbox
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		if _, removalErr := queryResultInboxRemoval(ctx, connection, packageID, attemptID); removalErr == nil {
			return ErrResultPackageTransition
		} else if !errors.Is(removalErr, ErrNotFound) {
			return removalErr
		}
		result, err = queryResultInboxByPackage(ctx, connection, packageID)
		if err != nil {
			return err
		}
		if result.Authority != authority {
			return ErrResultPackageAuthority
		}
		if result.AttemptID != attemptID {
			return ErrResultPackageConflict
		}
		if result.State == ResultInboxAvailable {
			return nil
		}
		if result.State != ResultInboxReceiving {
			return ErrResultPackageTransition
		}
		for index, offset := range result.Offsets {
			if offset.NextOffset != result.Manifest.Parts[index].Size {
				return ErrResultPackageTransition
			}
		}
		result.State = ResultInboxAvailable
		result.UpdatedAt = max(timestamp, result.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_result_inbox SET state = 'available', updated_at = ?
WHERE package_id = ? AND state = 'receiving' AND attempt_id = ?
`, result.UpdatedAt, packageID, attemptID); execErr != nil {
			return fmt.Errorf("commit available result inbox: %w", execErr)
		}
		return nil
	})
	return result, err
}

// PrepareResultInboxCancel durably records exact-attempt removal intent before
// any partial files are removed. Startup lists prepared removals and resumes
// the filesystem deletion before committing completion.
func (s *PeerStore) PrepareResultInboxCancel(
	ctx context.Context,
	authority ResultInboxAuthority,
	attemptID, packageID string,
	observedAt time.Time,
) (ResultInboxRemoval, error) {
	return s.prepareResultInboxRemoval(
		ctx, authority, attemptID, packageID, observedAt, ResultInboxRemovalCancelled, false,
	)
}

// PrepareExpiredResultInboxReclaim requires the bounded receive lease to have
// expired. The caller must hold its in-process writer lock while checking that
// this exact attempt has no active writer.
func (s *PeerStore) PrepareExpiredResultInboxReclaim(
	ctx context.Context,
	authority ResultInboxAuthority,
	attemptID, packageID string,
	observedAt time.Time,
) (ResultInboxRemoval, error) {
	return s.prepareResultInboxRemoval(
		ctx, authority, attemptID, packageID, observedAt, ResultInboxRemovalReclaimed, true,
	)
}

func (s *PeerStore) prepareResultInboxRemoval(
	ctx context.Context,
	authority ResultInboxAuthority,
	attemptID, packageID string,
	observedAt time.Time,
	outcome ResultInboxRemovalOutcome,
	requireExpired bool,
) (ResultInboxRemoval, error) {
	if err := authority.Validate(); err != nil {
		return ResultInboxRemoval{}, err
	}
	if err := validateResultPackageAttempt(attemptID, packageID); err != nil {
		return ResultInboxRemoval{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultInboxRemoval{}, err
	}
	var removal ResultInboxRemoval
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		removal, err = queryResultInboxRemoval(ctx, connection, packageID, attemptID)
		if err == nil {
			if removal.Authority != authority || removal.Outcome != outcome {
				return ErrResultPackageConflict
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		inbox, queryErr := queryResultInboxByPackage(ctx, connection, packageID)
		if queryErr != nil {
			return queryErr
		}
		if inbox.Authority != authority {
			return ErrResultPackageAuthority
		}
		if inbox.State != ResultInboxReceiving || inbox.AttemptID != attemptID {
			return ErrResultPackageConflict
		}
		if requireExpired && inbox.LeaseExpiresAt > timestamp {
			return ErrResultPackageTransition
		}
		removal = ResultInboxRemoval{
			Authority: authority, PackageID: packageID, AttemptID: attemptID,
			Outcome: outcome, Phase: ResultInboxRemovalPrepared,
			CreatedAt: timestamp, UpdatedAt: timestamp,
		}
		if err := ensureResultInboxRemovalCapacity(ctx, connection); err != nil {
			return err
		}
		if _, execErr := connection.ExecContext(ctx, `
INSERT INTO peer_result_inbox_attempt_receipts(
	controller_id, tree_id, root_agent_id, root_device_id,
	package_id, attempt_id, outcome, phase, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?)
`, authority.ControllerID, authority.TreeID, authority.RootAgentID, authority.RootDeviceID,
			packageID, attemptID, outcome, timestamp, timestamp); execErr != nil {
			return fmt.Errorf("prepare result inbox removal: %w", execErr)
		}
		return nil
	})
	return removal, err
}

// CommitResultInboxRemoval is the database commit boundary after the caller
// removed this prepared attempt's partial files and fsynced the parent. Exact
// replay returns the completed receipt.
func (s *PeerStore) CommitResultInboxRemoval(
	ctx context.Context,
	removal ResultInboxRemoval,
	observedAt time.Time,
) (ResultInboxRemoval, error) {
	if err := removal.Validate(); err != nil {
		return ResultInboxRemoval{}, err
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultInboxRemoval{}, err
	}
	var stored ResultInboxRemoval
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err = queryResultInboxRemoval(ctx, connection, removal.PackageID, removal.AttemptID)
		if err != nil {
			return err
		}
		if stored.Authority != removal.Authority || stored.Outcome != removal.Outcome {
			return ErrResultPackageConflict
		}
		if stored.Phase == ResultInboxRemovalCompleted {
			return nil
		}
		if stored.Phase != ResultInboxRemovalPrepared {
			return ErrResultPackageTransition
		}
		inbox, queryErr := queryResultInboxByPackage(ctx, connection, removal.PackageID)
		if queryErr != nil {
			return queryErr
		}
		if inbox.Authority != removal.Authority || inbox.AttemptID != removal.AttemptID ||
			inbox.State != ResultInboxReceiving {
			return ErrResultPackageConflict
		}
		if _, execErr := connection.ExecContext(ctx, `
DELETE FROM peer_result_inbox
WHERE package_id = ? AND state = 'receiving' AND attempt_id = ?
`, removal.PackageID, removal.AttemptID); execErr != nil {
			return fmt.Errorf("commit receiving result inbox removal: %w", execErr)
		}
		stored.Phase = ResultInboxRemovalCompleted
		stored.UpdatedAt = max(timestamp, stored.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_result_inbox_attempt_receipts SET phase = 'completed', updated_at = ?
WHERE package_id = ? AND attempt_id = ? AND phase = 'prepared'
`, stored.UpdatedAt, stored.PackageID, stored.AttemptID); execErr != nil {
			return fmt.Errorf("complete result inbox removal receipt: %w", execErr)
		}
		return nil
	})
	return stored, err
}

func (s *PeerStore) GetResultInbox(
	ctx context.Context,
	authority ResultInboxAuthority,
	packageID string,
) (ResultInbox, error) {
	if err := authority.Validate(); err != nil {
		return ResultInbox{}, err
	}
	if err := identity.ValidateID(packageID); err != nil {
		return ResultInbox{}, fmt.Errorf("packageId %w", err)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ResultInbox{}, fmt.Errorf("begin result inbox snapshot: %w", err)
	}
	defer transaction.Rollback()
	result, err := queryResultInboxByPackage(ctx, transaction, packageID)
	if err != nil {
		return ResultInbox{}, err
	}
	if result.Authority != authority {
		return ResultInbox{}, ErrResultPackageAuthority
	}
	if err := transaction.Commit(); err != nil {
		return ResultInbox{}, fmt.Errorf("commit result inbox snapshot: %w", err)
	}
	return result, nil
}

func (s *PeerStore) LookupResultInboxAvailability(
	ctx context.Context,
	authority ResultInboxAuthority,
	packageID string,
) (ResultInboxAvailability, error) {
	result, err := s.GetResultInbox(ctx, authority, packageID)
	if errors.Is(err, ErrNotFound) {
		return ResultInboxAvailabilityEvicted, nil
	}
	if err != nil {
		return "", err
	}
	switch result.State {
	case ResultInboxReceiving:
		return ResultInboxAvailabilityReceiving, nil
	case ResultInboxAvailable:
		return ResultInboxAvailabilityAvailable, nil
	case ResultInboxEvictionTombstone:
		return ResultInboxAvailabilityEvicted, nil
	default:
		return "", ErrResultPackageTransition
	}
}

func (s *PeerStore) ListExpiredResultInboxes(
	ctx context.Context,
	controllerID, deviceID string,
	expiredAt time.Time,
	limit int,
) ([]ResultInbox, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	timestamp, err := unixTime(expiredAt, "expiredAt")
	if err != nil {
		return nil, err
	}
	return s.listResultInboxes(ctx, `
WHERE controller_id = ? AND root_device_id = ? AND state = 'receiving' AND lease_expires_at <= ?
ORDER BY lease_expires_at, created_at, tree_id, root_agent_id, package_id
LIMIT ?
`, controllerID, deviceID, timestamp, limit)
}

// ListReceivingResultInboxes returns every non-published inbox owned by the
// peer. The filesystem manager uses this bounded list during startup to repair
// bytes to their committed offsets before accepting new transfer requests.
func (s *PeerStore) ListReceivingResultInboxes(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultInbox, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	return s.listResultInboxes(ctx, `
WHERE controller_id = ? AND root_device_id = ? AND state = 'receiving'
ORDER BY updated_at, created_at, tree_id, root_agent_id, package_id
LIMIT ?
`, controllerID, deviceID, limit)
}

func (s *PeerStore) ListPreparedResultInboxRemovals(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultInboxRemoval, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT controller_id, tree_id, root_agent_id, root_device_id,
	package_id, attempt_id, outcome, phase, created_at, updated_at
FROM peer_result_inbox_attempt_receipts
WHERE controller_id = ? AND root_device_id = ? AND phase = 'prepared'
ORDER BY updated_at, created_at, tree_id, root_agent_id, package_id, attempt_id
LIMIT ?
`, controllerID, deviceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list prepared result inbox removals: %w", err)
	}
	defer rows.Close()
	results := make([]ResultInboxRemoval, 0)
	for rows.Next() {
		result, err := scanResultInboxRemoval(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list prepared result inbox removals: %w", err)
	}
	return results, nil
}

func (s *PeerStore) ListCompletedResultInboxRemovals(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultInboxRemoval, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT controller_id, tree_id, root_agent_id, root_device_id,
	package_id, attempt_id, outcome, phase, created_at, updated_at
FROM peer_result_inbox_attempt_receipts
WHERE controller_id = ? AND root_device_id = ? AND phase = 'completed'
ORDER BY updated_at, created_at, tree_id, root_agent_id, package_id, attempt_id
LIMIT ?
`, controllerID, deviceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list completed result inbox removals: %w", err)
	}
	defer rows.Close()
	results := make([]ResultInboxRemoval, 0)
	for rows.Next() {
		result, err := scanResultInboxRemoval(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list completed result inbox removals: %w", err)
	}
	return results, nil
}

func (s *PeerStore) DeleteCompletedResultInboxRemoval(
	ctx context.Context,
	removal ResultInboxRemoval,
) error {
	if err := removal.Validate(); err != nil {
		return err
	}
	return withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		stored, err := queryResultInboxRemoval(ctx, connection, removal.PackageID, removal.AttemptID)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if stored.Authority != removal.Authority || stored.Outcome != removal.Outcome {
			return ErrResultPackageConflict
		}
		if stored.Phase != ResultInboxRemovalCompleted {
			return ErrResultPackageTransition
		}
		if _, err := connection.ExecContext(ctx, `
DELETE FROM peer_result_inbox_attempt_receipts
WHERE package_id = ? AND attempt_id = ? AND phase = 'completed'
`, stored.PackageID, stored.AttemptID); err != nil {
			return fmt.Errorf("delete completed result inbox removal: %w", err)
		}
		return nil
	})
}

// ListAvailableResultInboxes returns only ordinary-GC-eligible records oldest
// first. Receiving rows and eviction tombstones never appear.
func (s *PeerStore) ListAvailableResultInboxes(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultInbox, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	return s.listResultInboxes(ctx, `
WHERE controller_id = ? AND root_device_id = ? AND state = 'available'
ORDER BY updated_at, created_at, tree_id, root_agent_id, package_id
LIMIT ?
`, controllerID, deviceID, limit)
}

func (s *PeerStore) ListResultInboxEvictionTombstones(
	ctx context.Context,
	controllerID, deviceID string,
	limit int,
) ([]ResultInbox, error) {
	if err := validateChangesArtifactDevice(controllerID, deviceID); err != nil {
		return nil, err
	}
	if err := validateResultPage(limit); err != nil {
		return nil, err
	}
	return s.listResultInboxes(ctx, `
WHERE controller_id = ? AND root_device_id = ? AND state = 'evictionTombstone'
ORDER BY updated_at, created_at, tree_id, root_agent_id, package_id
LIMIT ?
`, controllerID, deviceID, limit)
}

func (s *PeerStore) GetResultInboxRetention(ctx context.Context) (ResultPackageRetention, error) {
	return inspectResultStoreCapacity(ctx, s.db, "peer_result_inbox")
}

// PrepareResultInboxEviction commits the durable tombstone before the caller
// removes bytes. Startup can resume deletion from this state.
func (s *PeerStore) PrepareResultInboxEviction(
	ctx context.Context,
	authority ResultInboxAuthority,
	packageID string,
	observedAt time.Time,
) (ResultInbox, error) {
	if err := authority.Validate(); err != nil {
		return ResultInbox{}, err
	}
	if err := identity.ValidateID(packageID); err != nil {
		return ResultInbox{}, fmt.Errorf("packageId %w", err)
	}
	timestamp, err := resultObservedAt(observedAt)
	if err != nil {
		return ResultInbox{}, err
	}
	var result ResultInbox
	err = withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		result, err = queryResultInboxByPackage(ctx, connection, packageID)
		if err != nil {
			return err
		}
		if result.Authority != authority {
			return ErrResultPackageAuthority
		}
		if result.State == ResultInboxEvictionTombstone {
			return nil
		}
		if result.State != ResultInboxAvailable {
			return ErrResultPackageTransition
		}
		result.State = ResultInboxEvictionTombstone
		result.UpdatedAt = max(timestamp, result.UpdatedAt)
		if _, execErr := connection.ExecContext(ctx, `
UPDATE peer_result_inbox SET state = 'evictionTombstone', updated_at = ?
WHERE package_id = ? AND state = 'available'
`, result.UpdatedAt, packageID); execErr != nil {
			return fmt.Errorf("prepare result inbox eviction: %w", execErr)
		}
		return nil
	})
	return result, err
}

// CompactResultInboxEviction is the commit boundary after the caller removed
// package bytes and fsynced the parent directory. Exact replay is a no-op.
func (s *PeerStore) CompactResultInboxEviction(
	ctx context.Context,
	authority ResultInboxAuthority,
	packageID string,
) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(packageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	return withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		result, err := queryResultInboxByPackage(ctx, connection, packageID)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if result.Authority != authority {
			return ErrResultPackageAuthority
		}
		if result.State != ResultInboxEvictionTombstone {
			return ErrResultPackageTransition
		}
		updated, err := connection.ExecContext(ctx, `
UPDATE peer_metadata
SET result_inbox_evicted = result_inbox_evicted + 1
WHERE singleton = 1 AND result_inbox_evicted < 9223372036854775807
`)
		if err != nil {
			return fmt.Errorf("increment result inbox eviction counter: %w", err)
		}
		affected, err := updated.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect result inbox eviction counter: %w", err)
		}
		if affected != 1 {
			return errors.New("result inbox eviction counter is exhausted")
		}
		if _, err := connection.ExecContext(ctx, `
DELETE FROM peer_result_inbox WHERE package_id = ? AND state = 'evictionTombstone'
`, packageID); err != nil {
			return fmt.Errorf("compact result inbox eviction: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
DELETE FROM peer_result_inbox_attempt_receipts WHERE package_id = ?
`, packageID); err != nil {
			return fmt.Errorf("compact result inbox attempt receipts: %w", err)
		}
		return nil
	})
}

func (s *PeerStore) listResultInboxes(
	ctx context.Context,
	clause string,
	args ...any,
) ([]ResultInbox, error) {
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin result inbox list snapshot: %w", err)
	}
	defer transaction.Rollback()
	rows, err := transaction.QueryContext(ctx, resultInboxSelect+clause, args...)
	if err != nil {
		return nil, fmt.Errorf("list result inboxes: %w", err)
	}
	results := make([]ResultInbox, 0)
	for rows.Next() {
		result, err := scanResultInbox(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("list result inboxes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close result inbox rows: %w", err)
	}
	for index := range results {
		if err := loadResultInboxOffsets(ctx, transaction, &results[index]); err != nil {
			return nil, err
		}
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("commit result inbox list snapshot: %w", err)
	}
	return results, nil
}

func queryResultInboxByPackage(
	ctx context.Context,
	queryer resultRowsQueryer,
	packageID string,
) (ResultInbox, error) {
	if err := identity.ValidateID(packageID); err != nil {
		return ResultInbox{}, fmt.Errorf("packageId %w", err)
	}
	result, err := scanResultInbox(queryer.QueryRowContext(ctx, resultInboxSelect+`
WHERE package_id = ?
`, packageID))
	if err != nil {
		return ResultInbox{}, err
	}
	if err := loadResultInboxOffsets(ctx, queryer, &result); err != nil {
		return ResultInbox{}, err
	}
	return result, nil
}

func validateResultPackageAttempt(attemptID, packageID string) error {
	if err := identity.ValidateID(attemptID); err != nil {
		return fmt.Errorf("attemptId %w", err)
	}
	if err := identity.ValidateID(packageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	return nil
}

func queryResultInboxRemoval(
	ctx context.Context,
	queryer rowQueryer,
	packageID, attemptID string,
) (ResultInboxRemoval, error) {
	return scanResultInboxRemoval(queryer.QueryRowContext(ctx, `
SELECT controller_id, tree_id, root_agent_id, root_device_id,
	package_id, attempt_id, outcome, phase, created_at, updated_at
FROM peer_result_inbox_attempt_receipts
WHERE package_id = ? AND attempt_id = ?
`, packageID, attemptID))
}

func ensureResultInboxRemovalCapacity(ctx context.Context, connection *sql.Conn) error {
	for {
		var count int
		if err := connection.QueryRowContext(ctx, `
SELECT count(*) FROM peer_result_inbox_attempt_receipts
`).Scan(&count); err != nil {
			return fmt.Errorf("inspect result inbox removal capacity: %w", err)
		}
		if count < MaximumResultInboxRemovalReceipts {
			return nil
		}
		result, err := connection.ExecContext(ctx, `
DELETE FROM peer_result_inbox_attempt_receipts
WHERE (package_id, attempt_id) IN (
	SELECT package_id, attempt_id
	FROM peer_result_inbox_attempt_receipts
	WHERE phase = 'completed'
	ORDER BY updated_at, created_at, package_id, attempt_id
	LIMIT 1
)
`)
		if err != nil {
			return fmt.Errorf("prune completed result inbox removal: %w", err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect pruned result inbox removal: %w", err)
		}
		if deleted != 1 {
			return ErrResultPackageQuota
		}
	}
}

func queryResultInboxRemovalByPackage(
	ctx context.Context,
	queryer rowQueryer,
	packageID string,
) (ResultInboxRemoval, error) {
	return scanResultInboxRemoval(queryer.QueryRowContext(ctx, `
SELECT controller_id, tree_id, root_agent_id, root_device_id,
	package_id, attempt_id, outcome, phase, created_at, updated_at
FROM peer_result_inbox_attempt_receipts
WHERE package_id = ?
ORDER BY updated_at DESC, attempt_id DESC
LIMIT 1
`, packageID))
}

func scanResultInboxRemoval(scanner rowScanner) (ResultInboxRemoval, error) {
	var result ResultInboxRemoval
	if err := scanner.Scan(
		&result.Authority.ControllerID, &result.Authority.TreeID,
		&result.Authority.RootAgentID, &result.Authority.RootDeviceID,
		&result.PackageID, &result.AttemptID, &result.Outcome, &result.Phase,
		&result.CreatedAt, &result.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return ResultInboxRemoval{}, ErrNotFound
	} else if err != nil {
		return ResultInboxRemoval{}, fmt.Errorf("load result inbox removal: %w", err)
	}
	if err := result.Validate(); err != nil {
		return ResultInboxRemoval{}, fmt.Errorf("stored result inbox removal is invalid: %w", err)
	}
	return result, nil
}
