package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/securefs"
)

// DatabaseKind identifies one Delegation state database without exposing
// implementation-specific schema constants to upgrade callers.
type DatabaseKind string

const (
	DatabaseBroker DatabaseKind = "broker"
	DatabasePeer   DatabaseKind = "peer"
)

// DatabaseIdentity is the compatibility identity stored in SQLite headers.
type DatabaseIdentity struct {
	ApplicationID int `json:"applicationId"`
	SchemaVersion int `json:"schemaVersion"`
}

func CurrentDatabaseIdentity(kind DatabaseKind) (DatabaseIdentity, error) {
	switch kind {
	case DatabaseBroker:
		return DatabaseIdentity{ApplicationID: storeApplicationID, SchemaVersion: schemaVersion}, nil
	case DatabasePeer:
		return DatabaseIdentity{ApplicationID: peerStoreApplicationID, SchemaVersion: peerSchemaVersion}, nil
	default:
		return DatabaseIdentity{}, fmt.Errorf("unsupported database kind %q", kind)
	}
}

// SupportedUpgradeSource reports whether source can be migrated directly by
// this runtime. Pre-release upgrade support is deliberately an exact one-step
// allow-list rather than a general schema converter.
func SupportedUpgradeSource(kind DatabaseKind, source, target DatabaseIdentity) bool {
	current, err := CurrentDatabaseIdentity(kind)
	if err != nil || target != current || source.ApplicationID != target.ApplicationID {
		return false
	}
	return source.SchemaVersion == target.SchemaVersion ||
		kind == DatabaseBroker && source.SchemaVersion == 19 && target.SchemaVersion == 20 ||
		kind == DatabasePeer && source.SchemaVersion == 15 && target.SchemaVersion == 16
}

// MigrateUpgradeDatabase applies the only supported stopped-shadow schema
// transitions. The canonical database is never passed to this function.
func MigrateUpgradeDatabase(
	ctx context.Context, path string, kind DatabaseKind, target DatabaseIdentity,
) error {
	source, err := InspectUpgradeDatabase(ctx, path, kind)
	if err != nil {
		return err
	}
	if !SupportedUpgradeSource(kind, source, target) {
		return fmt.Errorf(
			"unsupported %s database migration from application ID %d schema %d to %d/%d",
			kind, source.ApplicationID, source.SchemaVersion,
			target.ApplicationID, target.SchemaVersion,
		)
	}
	if source == target {
		return nil
	}
	database, err := sql.Open("sqlite", dataSourceName(filepath.Clean(path)))
	if err != nil {
		return fmt.Errorf("open upgrade shadow database: %w", err)
	}
	defer database.Close()
	description := string(kind) + " upgrade"
	return withImmediateTransaction(ctx, database, description, func(connection *sql.Conn) error {
		var statement string
		switch kind {
		case DatabaseBroker:
			statement = `
CREATE TABLE device_worker_readiness (
 controller_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 epoch INTEGER NOT NULL CHECK (epoch BETWEEN 1 AND 9223372036854775807),
 state TEXT NOT NULL CHECK (state IN ('pending', 'ready', 'intervention_required')),
 attempt_count INTEGER NOT NULL CHECK (attempt_count BETWEEN 0 AND 5),
 runtime_digest TEXT NOT NULL CHECK (length(runtime_digest) = 64 AND runtime_digest NOT GLOB '*[^0-9a-f]*'),
 config_digest TEXT NOT NULL CHECK (length(config_digest) = 64 AND config_digest NOT GLOB '*[^0-9a-f]*'),
 epoch_started_at INTEGER NOT NULL CHECK (epoch_started_at > 0),
 next_attempt_at INTEGER NOT NULL CHECK (next_attempt_at >= 0),
 last_attempt_at INTEGER NOT NULL CHECK (last_attempt_at >= 0),
 failure_code TEXT NOT NULL CHECK (length(CAST(failure_code AS BLOB)) <= 64),
 updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
 PRIMARY KEY (controller_id, device_id),
 FOREIGN KEY (controller_id, device_id)
  REFERENCES devices(controller_id, device_id) ON DELETE CASCADE
) STRICT;
PRAGMA user_version = 20;
`
		case DatabasePeer:
			statement = `
CREATE TABLE worker_readiness (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 epoch INTEGER NOT NULL CHECK (epoch BETWEEN 1 AND 9223372036854775807),
 state TEXT NOT NULL CHECK (state IN ('pending', 'ready', 'intervention_required')),
 attempt_count INTEGER NOT NULL CHECK (attempt_count BETWEEN 0 AND 5),
 runtime_digest TEXT NOT NULL CHECK (length(runtime_digest) = 64 AND runtime_digest NOT GLOB '*[^0-9a-f]*'),
 config_digest TEXT NOT NULL CHECK (length(config_digest) = 64 AND config_digest NOT GLOB '*[^0-9a-f]*'),
 epoch_started_at INTEGER NOT NULL CHECK (epoch_started_at > 0),
 next_attempt_at INTEGER NOT NULL CHECK (next_attempt_at >= 0),
 last_attempt_at INTEGER NOT NULL CHECK (last_attempt_at >= 0),
 failure_code TEXT NOT NULL CHECK (length(CAST(failure_code AS BLOB)) <= 64),
 updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
) STRICT;
PRAGMA user_version = 16;
`
		default:
			return fmt.Errorf("unsupported database kind %q", kind)
		}
		if _, err := connection.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply %s database migration: %w", kind, err)
		}
		identity, err := readSchemaIdentity(ctx, connection)
		if err != nil {
			return err
		}
		if identity.applicationID != target.ApplicationID || identity.version != target.SchemaVersion {
			return errors.New("upgrade database migration did not reach its target identity")
		}
		return nil
	})
}

// ReadUpgradeBlockers reads the common pre-upgrade work tables from either
// the current schema or its exact supported predecessor without initializing
// or migrating the live database.
func ReadUpgradeBlockers(
	ctx context.Context, path string, kind DatabaseKind, controllerID, deviceID string,
) (UpgradeBlockers, error) {
	identity, err := InspectUpgradeDatabase(ctx, path, kind)
	if err != nil {
		return UpgradeBlockers{}, err
	}
	target, err := CurrentDatabaseIdentity(kind)
	if err != nil {
		return UpgradeBlockers{}, err
	}
	if !SupportedUpgradeSource(kind, identity, target) {
		return UpgradeBlockers{}, errors.New("upgrade blocker schema is unsupported")
	}
	database, err := sql.Open("sqlite", dataSourceName(filepath.Clean(path)))
	if err != nil {
		return UpgradeBlockers{}, err
	}
	defer database.Close()
	transaction, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return UpgradeBlockers{}, err
	}
	defer transaction.Rollback()
	var blockers UpgradeBlockers
	switch kind {
	case DatabaseBroker:
		queries := []struct {
			destination *int
			query       string
		}{
			{&blockers.OccupiedWorkers, "SELECT count(*) FROM agent_lifecycle_states WHERE controller_id = ? AND phase IN (" + occupiedWorkerStatesSQL + ")"},
			{&blockers.PendingSpawns, "SELECT count(*) FROM agent_spawn_receipts WHERE controller_id = ? AND status = 'pending'"},
			{&blockers.PendingOperations, "SELECT count(*) FROM agent_operation_receipts WHERE controller_id = ? AND outcome = 'pending'"},
			{&blockers.WorkspaceTransfers, "SELECT count(*) FROM workspace_sync_receipts WHERE controller_id = ? AND status <> 'prepared'"},
			{&blockers.ResultFinalizations, "SELECT count(*) FROM result_packages WHERE controller_id = ? AND (state = 'deliveryPending' OR source_released_at = 0)"},
		}
		for _, query := range queries {
			if err := transaction.QueryRowContext(ctx, query.query, controllerID).Scan(query.destination); err != nil {
				return UpgradeBlockers{}, fmt.Errorf("read broker upgrade blockers: %w", err)
			}
		}
	case DatabasePeer:
		queries := []struct {
			destination *int
			query       string
			arguments   []any
		}{
			{&blockers.OccupiedWorkers, "SELECT count(*) FROM worker_reservations WHERE controller_id = ? AND device_id = ? AND status IN (" + occupiedWorkerStatesSQL + ")", []any{controllerID, deviceID}},
			{&blockers.PendingOperations, `
SELECT count(*) FROM worker_operation_receipts AS operation
JOIN worker_reservations AS worker
 ON worker.controller_id = operation.controller_id
 AND worker.tree_id = operation.tree_id AND worker.agent_id = operation.agent_id
WHERE operation.controller_id = ? AND worker.device_id = ? AND operation.status = 'pending'`, []any{controllerID, deviceID}},
			{&blockers.ResultFinalizations, `
SELECT
 (SELECT count(*) FROM peer_changes_artifacts AS artifact
  JOIN worker_reservations AS worker
   ON worker.controller_id = artifact.controller_id
   AND worker.tree_id = artifact.tree_id AND worker.agent_id = artifact.agent_id
  WHERE artifact.controller_id = ? AND worker.device_id = ? AND artifact.state <> 'published') +
 (SELECT count(*) FROM peer_result_outbox
  WHERE controller_id = ? AND source_device_id = ? AND state <> 'releasePending') +
 (SELECT count(*) FROM peer_result_inbox
  WHERE controller_id = ? AND root_device_id = ? AND state = 'receiving')`,
				[]any{controllerID, deviceID, controllerID, deviceID, controllerID, deviceID}},
		}
		for _, query := range queries {
			if err := transaction.QueryRowContext(ctx, query.query, query.arguments...).Scan(query.destination); err != nil {
				return UpgradeBlockers{}, fmt.Errorf("read peer upgrade blockers: %w", err)
			}
		}
	default:
		return UpgradeBlockers{}, fmt.Errorf("unsupported database kind %q", kind)
	}
	if err := transaction.Commit(); err != nil {
		return UpgradeBlockers{}, err
	}
	return blockers, nil
}

// OpenUpgradeDatabaseRoot pins and validates the protected directory that
// owns a configured database and all same-directory upgrade material.
func OpenUpgradeDatabaseRoot(path string) (*securefs.Root, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	root, err := openStateDirectoryGuard(filepath.Dir(filepath.Clean(path)))
	if err != nil {
		return nil, err
	}
	return root, nil
}

// CheckpointUpgradeDatabase incorporates committed WAL data into the database
// and verifies its current application/schema identity. The caller must hold
// the role lease after stopping the service.
func CheckpointUpgradeDatabase(
	ctx context.Context, path string, kind DatabaseKind, expected DatabaseIdentity,
) (DatabaseIdentity, error) {
	root, err := OpenUpgradeDatabaseRoot(path)
	if err != nil {
		return DatabaseIdentity{}, err
	}
	defer root.Close()
	before, err := root.Lstat(filepath.Base(path))
	if err != nil {
		return DatabaseIdentity{}, fmt.Errorf("inspect upgrade database: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return DatabaseIdentity{}, errors.New("upgrade database must be a regular file, not a symbolic link")
	}
	database, err := sql.Open("sqlite", dataSourceName(filepath.Clean(path)))
	if err != nil {
		return DatabaseIdentity{}, fmt.Errorf("open upgrade database: %w", err)
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return DatabaseIdentity{}, fmt.Errorf("open upgrade database: %w", err)
	}
	identity, err := inspectUpgradeDatabase(ctx, database, kind)
	if err != nil {
		return DatabaseIdentity{}, err
	}
	if identity != expected {
		return DatabaseIdentity{}, fmt.Errorf(
			"upgrade source database is application ID %d schema %d, expected application ID %d schema %d",
			identity.ApplicationID, identity.SchemaVersion, expected.ApplicationID, expected.SchemaVersion,
		)
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(
		&busy, &logFrames, &checkpointed,
	); err != nil {
		return DatabaseIdentity{}, fmt.Errorf("checkpoint upgrade database WAL: %w", err)
	}
	if busy != 0 || logFrames != 0 {
		return DatabaseIdentity{}, fmt.Errorf(
			"checkpoint upgrade database WAL remained busy (busy=%d log=%d checkpointed=%d)",
			busy, logFrames, checkpointed,
		)
	}
	if err := validateUpgradeDatabaseConnection(ctx, database); err != nil {
		return DatabaseIdentity{}, err
	}
	after, err := root.Lstat(filepath.Base(path))
	if err != nil || !os.SameFile(before, after) {
		return DatabaseIdentity{}, errors.Join(err, errors.New("upgrade database changed during checkpoint"))
	}
	if err := root.VerifyPath(); err != nil {
		return DatabaseIdentity{}, err
	}
	return identity, nil
}

// InspectUpgradeDatabase reads and validates a role database identity and
// integrity without requiring the schema version compiled into this runtime.
func InspectUpgradeDatabase(ctx context.Context, path string, kind DatabaseKind) (DatabaseIdentity, error) {
	root, err := OpenUpgradeDatabaseRoot(path)
	if err != nil {
		return DatabaseIdentity{}, err
	}
	defer root.Close()
	before, err := root.Lstat(filepath.Base(path))
	if err != nil {
		return DatabaseIdentity{}, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return DatabaseIdentity{}, errors.New("upgrade database must be a regular file, not a symbolic link")
	}
	database, err := sql.Open("sqlite", dataSourceName(filepath.Clean(path)))
	if err != nil {
		return DatabaseIdentity{}, err
	}
	defer database.Close()
	identity, err := inspectUpgradeDatabase(ctx, database, kind)
	if err != nil {
		return DatabaseIdentity{}, err
	}
	if err := validateUpgradeDatabaseConnection(ctx, database); err != nil {
		return DatabaseIdentity{}, err
	}
	after, err := root.Lstat(filepath.Base(path))
	if err != nil || !os.SameFile(before, after) {
		return DatabaseIdentity{}, errors.Join(err, errors.New("upgrade database changed during inspection"))
	}
	if err := root.VerifyPath(); err != nil {
		return DatabaseIdentity{}, err
	}
	return identity, nil
}

// ValidateUpgradeDatabase verifies a stopped shadow or rollback database
// against an explicit target identity without initializing or migrating it.
func ValidateUpgradeDatabase(
	ctx context.Context, path string, kind DatabaseKind, expected DatabaseIdentity,
) error {
	root, err := OpenUpgradeDatabaseRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	before, err := root.Lstat(filepath.Base(path))
	if err != nil {
		return fmt.Errorf("inspect upgrade database: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return errors.New("upgrade database must be a regular file, not a symbolic link")
	}
	database, err := sql.Open("sqlite", dataSourceName(filepath.Clean(path)))
	if err != nil {
		return fmt.Errorf("open upgrade database: %w", err)
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("open upgrade database: %w", err)
	}
	identity, err := readSchemaIdentity(ctx, database)
	if err != nil {
		return err
	}
	if identity.applicationID != expected.ApplicationID || identity.version != expected.SchemaVersion {
		return fmt.Errorf(
			"database identity is application ID %d schema %d, expected application ID %d schema %d",
			identity.applicationID, identity.version, expected.ApplicationID, expected.SchemaVersion,
		)
	}
	current, err := CurrentDatabaseIdentity(kind)
	if err != nil {
		return err
	}
	if expected.ApplicationID != current.ApplicationID {
		return errors.New("target database application ID differs from the role database")
	}
	if err := validateUpgradeDatabaseConnection(ctx, database); err != nil {
		return err
	}
	after, err := root.Lstat(filepath.Base(path))
	if err != nil || !os.SameFile(before, after) {
		return errors.Join(err, errors.New("upgrade database changed during validation"))
	}
	return root.VerifyPath()
}

func inspectUpgradeDatabase(
	ctx context.Context, database *sql.DB, kind DatabaseKind,
) (DatabaseIdentity, error) {
	expected, err := CurrentDatabaseIdentity(kind)
	if err != nil {
		return DatabaseIdentity{}, err
	}
	identity, err := readSchemaIdentity(ctx, database)
	if err != nil {
		return DatabaseIdentity{}, err
	}
	actual := DatabaseIdentity{ApplicationID: identity.applicationID, SchemaVersion: identity.version}
	if actual.ApplicationID != expected.ApplicationID || actual.SchemaVersion <= 0 {
		return DatabaseIdentity{}, fmt.Errorf(
			"unsupported %s upgrade database (application ID %d schema %d)",
			kind, actual.ApplicationID, actual.SchemaVersion,
		)
	}
	return actual, nil
}

func validateUpgradeDatabaseConnection(ctx context.Context, database *sql.DB) error {
	var foreignKeyFailures int
	rows, err := database.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("check upgrade database foreign keys: %w", err)
	}
	for rows.Next() {
		foreignKeyFailures++
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close upgrade database foreign-key check: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check upgrade database foreign keys: %w", err)
	}
	if foreignKeyFailures != 0 {
		return fmt.Errorf("upgrade database has %d foreign-key violations", foreignKeyFailures)
	}
	var integrity string
	if err := database.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		return fmt.Errorf("check upgrade database integrity: %w", err)
	}
	if integrity != "ok" {
		return fmt.Errorf("upgrade database integrity check returned %q", integrity)
	}
	return nil
}
