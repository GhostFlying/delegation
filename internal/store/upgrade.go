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
