package store

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaV1 = `
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    integer_value INTEGER NOT NULL
) STRICT;

INSERT INTO metadata(key, integer_value) VALUES ('registry_revision', 0);

CREATE TABLE credentials (
    controller_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('controller', 'device')),
    token_mac BLOB NOT NULL UNIQUE CHECK (length(token_mac) = 32),
    disabled INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1)),
    issued_at INTEGER NOT NULL CHECK (issued_at >= 0),
    PRIMARY KEY (controller_id, device_id)
) STRICT;

CREATE TABLE devices (
    controller_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('controller', 'device')),
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    runtime_version TEXT NOT NULL,
    protocol_version INTEGER NOT NULL CHECK (protocol_version > 0),
    features_json TEXT NOT NULL,
    online INTEGER NOT NULL CHECK (online IN (0, 1)),
    last_seen_at INTEGER NOT NULL CHECK (last_seen_at >= 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    PRIMARY KEY (controller_id, device_id)
) STRICT;

CREATE INDEX devices_by_controller_name
    ON devices(controller_id, name, device_id);

CREATE TABLE trees (
    controller_id TEXT NOT NULL,
    external_thread_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    root_agent_id TEXT NOT NULL,
    root_device_id TEXT NOT NULL,
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    PRIMARY KEY (controller_id, external_thread_id),
    UNIQUE (controller_id, tree_id)
) STRICT;

CREATE TABLE principals (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    parent_agent_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    PRIMARY KEY (controller_id, tree_id, agent_id),
    FOREIGN KEY (controller_id, tree_id)
        REFERENCES trees(controller_id, tree_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX principals_by_parent
    ON principals(controller_id, tree_id, parent_agent_id);

PRAGMA user_version = 1;
`

func (s *Store) migrate(ctx context.Context) error {
	return s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		var version int
		if err := connection.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
			return fmt.Errorf("read broker state schema version: %w", err)
		}
		switch version {
		case 0:
			if _, err := connection.ExecContext(ctx, schemaV1); err != nil {
				return fmt.Errorf("create broker state schema: %w", err)
			}
		case schemaVersion:
			return nil
		default:
			return fmt.Errorf("unsupported broker state schema version %d", version)
		}
		return nil
	})
}
