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

const schemaV2 = `
ALTER TABLE credentials ADD COLUMN pending INTEGER NOT NULL DEFAULT 0
    CHECK (pending IN (0, 1) AND (pending = 0 OR disabled = 1));

PRAGMA user_version = 2;
`

const schemaV3 = `
CREATE TABLE controller_registries (
    controller_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT;

WITH controllers(controller_id) AS (
    SELECT controller_id FROM credentials
    UNION
    SELECT controller_id FROM devices
    UNION
    SELECT controller_id FROM trees
)
INSERT INTO controller_registries(controller_id, revision)
SELECT
    controllers.controller_id,
    max(
        coalesce(
            (SELECT integer_value FROM metadata WHERE key = 'registry_revision'),
            0
        ),
        coalesce(
            (SELECT max(devices.revision)
             FROM devices
             WHERE devices.controller_id = controllers.controller_id),
            0
        )
    )
FROM controllers;

PRAGMA user_version = 3;
`

const schemaV4 = `
CREATE TABLE credentials_v4 (
    controller_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    token_mac BLOB NOT NULL UNIQUE CHECK (length(token_mac) = 32),
    disabled INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1)),
    issued_at INTEGER NOT NULL CHECK (issued_at >= 0),
    pending INTEGER NOT NULL DEFAULT 0
        CHECK (pending IN (0, 1) AND (pending = 0 OR disabled = 1)),
    PRIMARY KEY (controller_id, device_id)
) STRICT;

INSERT INTO credentials_v4(
    controller_id, device_id, token_mac, disabled, issued_at, pending
)
SELECT
    controller_id,
    device_id,
    token_mac,
    disabled,
    issued_at,
    0
FROM credentials
WHERE role = 'controller' OR (role = 'device' AND disabled = 1);

DROP TABLE credentials;
ALTER TABLE credentials_v4 RENAME TO credentials;

DROP INDEX devices_by_controller_name;

CREATE TABLE devices_v4 (
    controller_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    name TEXT NOT NULL,
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

INSERT INTO devices_v4(
    controller_id, device_id, name, os, arch, runtime_version,
    protocol_version, features_json, online, last_seen_at, revision
)
SELECT
    controller_id, device_id, name, os, arch, runtime_version,
    protocol_version, features_json, online, last_seen_at, revision
FROM devices;

DROP TABLE devices;
ALTER TABLE devices_v4 RENAME TO devices;

CREATE INDEX devices_by_controller_name
    ON devices(controller_id, name, device_id);

PRAGMA user_version = 4;
`

func (s *Store) migrate(ctx context.Context) error {
	return s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		var version int
		if err := connection.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
			return fmt.Errorf("read broker state schema version: %w", err)
		}
		if version < 0 || version > schemaVersion {
			return fmt.Errorf("unsupported broker state schema version %d", version)
		}
		if version == 0 {
			if _, err := connection.ExecContext(ctx, schemaV1); err != nil {
				return fmt.Errorf("create broker state schema: %w", err)
			}
			version = 1
		}
		if version == 1 {
			if _, err := connection.ExecContext(ctx, schemaV2); err != nil {
				return fmt.Errorf("migrate broker state schema to version 2: %w", err)
			}
			version = 2
		}
		if version == 2 {
			if _, err := connection.ExecContext(ctx, schemaV3); err != nil {
				return fmt.Errorf("migrate broker state schema to version 3: %w", err)
			}
			version = 3
		}
		if version == 3 {
			if _, err := connection.ExecContext(ctx, schemaV4); err != nil {
				return fmt.Errorf("migrate broker state schema to version 4: %w", err)
			}
		}
		return nil
	})
}
