package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
)

// PeerStore persists connector-owned managed worker reservations separately
// from the broker registry.
type PeerStore struct {
	db *sql.DB
}

func OpenPeer(ctx context.Context, path string) (*PeerStore, error) {
	resolved, err := preparePath(path)
	if err != nil {
		return nil, err
	}
	directory, err := openStateDirectoryGuard(filepath.Dir(resolved))
	if err != nil {
		return nil, err
	}
	directoryOpen := true
	closeDirectory := func() error {
		if !directoryOpen {
			return nil
		}
		directoryOpen = false
		return directory.Close()
	}
	db, err := sql.Open("sqlite", dataSourceName(resolved))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open peer state: %w", err), closeDirectory())
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)

	store := &PeerStore{db: db}
	closeOnError := func(err error) (*PeerStore, error) {
		return nil, errors.Join(err, db.Close(), closeDirectory())
	}
	if err := db.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("open peer state: %w", err))
	}
	if err := protectDatabaseFile(resolved); err != nil {
		return closeOnError(err)
	}
	if err := store.initializeSchema(ctx); err != nil {
		return closeOnError(err)
	}
	if err := enableStateWAL(ctx, db, "peer"); err != nil {
		return closeOnError(err)
	}
	if err := checkStateIntegrity(ctx, db, "peer"); err != nil {
		return closeOnError(err)
	}
	if err := protectDatabaseArtifacts(resolved); err != nil {
		return closeOnError(err)
	}
	if err := ValidatePath(resolved); err != nil {
		return closeOnError(err)
	}
	if err := directory.VerifyPath(); err != nil {
		return closeOnError(err)
	}
	if err := closeDirectory(); err != nil {
		return closeOnError(fmt.Errorf("close peer state directory: %w", err))
	}
	return store, nil
}

func (s *PeerStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PeerStore) initializeSchema(ctx context.Context) error {
	return withImmediateTransaction(ctx, s.db, "peer", func(connection *sql.Conn) error {
		identity, err := readSchemaIdentity(ctx, connection)
		if err != nil {
			return err
		}
		if identity.applicationID == peerStoreApplicationID && identity.version == peerSchemaVersion {
			return nil
		}
		if identity.applicationID != 0 || identity.version != 0 {
			return unsupportedPeerSchemaIdentity(identity)
		}
		var objectCount int
		if err := connection.QueryRowContext(ctx, `
SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
`).Scan(&objectCount); err != nil {
			return fmt.Errorf("inspect peer state schema: %w", err)
		}
		if objectCount != 0 {
			return unsupportedPeerSchemaIdentity(identity)
		}
		if _, err := connection.ExecContext(ctx, peerSchemaCurrent); err != nil {
			return fmt.Errorf("create peer state schema: %w", err)
		}
		return nil
	})
}

func unsupportedPeerSchemaIdentity(identity schemaIdentity) error {
	return fmt.Errorf(
		"unsupported peer state format (application ID %d, schema version %d); this runtime supports only the current pre-release format",
		identity.applicationID,
		identity.version,
	)
}
