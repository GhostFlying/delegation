package store

import (
	"context"
	"database/sql"
	"fmt"
)

const storeApplicationID = 0x444c4754 // "DLGT"

var schemaCurrent = fmt.Sprintf(`
CREATE TABLE credentials (
    controller_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    token_mac BLOB NOT NULL UNIQUE CHECK (length(token_mac) = 32),
    disabled INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1)),
    issued_at INTEGER NOT NULL CHECK (issued_at >= 0),
    pending INTEGER NOT NULL DEFAULT 0
        CHECK (pending IN (0, 1) AND (pending = 0 OR disabled = 1)),
    PRIMARY KEY (controller_id, device_id)
) STRICT;

CREATE TABLE controller_registries (
    controller_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT;

CREATE TABLE devices (
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

CREATE TABLE mailboxes (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    recipient_agent_id TEXT NOT NULL,
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
    PRIMARY KEY (controller_id, tree_id, recipient_agent_id),
    FOREIGN KEY (controller_id, tree_id, recipient_agent_id)
        REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE mailbox_messages (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    recipient_agent_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    message_id TEXT NOT NULL UNIQUE,
    source_agent_id TEXT NOT NULL,
    source_parent_agent_id TEXT NOT NULL,
    source_device_id TEXT NOT NULL,
    message TEXT NOT NULL CHECK (
        length(CAST(message AS BLOB)) BETWEEN 1 AND 1024
    ),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    PRIMARY KEY (controller_id, tree_id, recipient_agent_id, sequence),
    FOREIGN KEY (controller_id, tree_id, recipient_agent_id)
        REFERENCES mailboxes(controller_id, tree_id, recipient_agent_id) ON DELETE CASCADE,
    FOREIGN KEY (controller_id, tree_id, source_agent_id)
        REFERENCES principals(controller_id, tree_id, agent_id)
) STRICT;

CREATE TABLE mailbox_receipts (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    recipient_agent_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    message_id TEXT NOT NULL PRIMARY KEY,
    source_agent_id TEXT NOT NULL,
    source_parent_agent_id TEXT NOT NULL,
    source_device_id TEXT NOT NULL,
    message TEXT NOT NULL CHECK (
        length(CAST(message AS BLOB)) BETWEEN 1 AND 1024
    ),
    UNIQUE (controller_id, tree_id, recipient_agent_id, sequence),
    FOREIGN KEY (controller_id, tree_id, recipient_agent_id)
        REFERENCES mailboxes(controller_id, tree_id, recipient_agent_id) ON DELETE CASCADE,
    FOREIGN KEY (controller_id, tree_id, source_agent_id)
        REFERENCES principals(controller_id, tree_id, agent_id)
) STRICT;

CREATE INDEX mailbox_receipts_by_recipient
    ON mailbox_receipts(controller_id, tree_id, recipient_agent_id, sequence);

PRAGMA application_id = %d;
PRAGMA user_version = %d;
`, storeApplicationID, schemaVersion)

type schemaIdentity struct {
	applicationID int
	version       int
}

func (s *Store) initializeSchema(ctx context.Context) error {
	return s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		identity, err := readSchemaIdentity(ctx, connection)
		if err != nil {
			return err
		}
		if identity.applicationID == storeApplicationID && identity.version == schemaVersion {
			return nil
		}
		if identity.applicationID != 0 || identity.version != 0 {
			return unsupportedSchemaIdentity(identity)
		}
		var objectCount int
		if err := connection.QueryRowContext(ctx, `
SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
`).Scan(&objectCount); err != nil {
			return fmt.Errorf("inspect broker state schema: %w", err)
		}
		if objectCount != 0 {
			return unsupportedSchemaIdentity(identity)
		}
		if _, err := connection.ExecContext(ctx, schemaCurrent); err != nil {
			return fmt.Errorf("create broker state schema: %w", err)
		}
		return nil
	})
}

func (s *Store) requireCurrentSchema(ctx context.Context) error {
	identity, err := readSchemaIdentity(ctx, s.db)
	if err != nil {
		return err
	}
	if identity.applicationID != storeApplicationID || identity.version != schemaVersion {
		return unsupportedSchemaIdentity(identity)
	}
	return nil
}

func readSchemaIdentity(ctx context.Context, queryer rowQueryer) (schemaIdentity, error) {
	var identity schemaIdentity
	if err := queryer.QueryRowContext(ctx, "PRAGMA application_id").Scan(&identity.applicationID); err != nil {
		return schemaIdentity{}, fmt.Errorf("read broker state application ID: %w", err)
	}
	if err := queryer.QueryRowContext(ctx, "PRAGMA user_version").Scan(&identity.version); err != nil {
		return schemaIdentity{}, fmt.Errorf("read broker state schema version: %w", err)
	}
	return identity, nil
}

func unsupportedSchemaIdentity(identity schemaIdentity) error {
	return fmt.Errorf(
		"unsupported broker state format (application ID %d, schema version %d); this runtime supports only the current pre-release format",
		identity.applicationID,
		identity.version,
	)
}
