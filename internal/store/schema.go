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
	last_agent_sequence INTEGER NOT NULL DEFAULT 0
		CHECK (last_agent_sequence BETWEEN 0 AND 256),
	last_lifecycle_sequence INTEGER NOT NULL DEFAULT 0
		CHECK (last_lifecycle_sequence >= 0),
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

CREATE TABLE workspace_sync_receipts (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	source_agent_id TEXT NOT NULL,
	sync_id TEXT NOT NULL,
	source_device_id TEXT NOT NULL,
	target_device_id TEXT NOT NULL,
	git_url TEXT NOT NULL CHECK (length(CAST(git_url AS BLOB)) BETWEEN 1 AND 4096),
	source_path_digest BLOB NOT NULL CHECK (length(source_path_digest) = 32),
	status TEXT NOT NULL CHECK (status IN ('pending', 'inspected', 'prepared')),
	head_oid TEXT NOT NULL DEFAULT '',
	object_format TEXT NOT NULL DEFAULT '' CHECK (object_format IN ('', 'sha1', 'sha256')),
	working_directory TEXT NOT NULL DEFAULT '',
	source_clean INTEGER NOT NULL DEFAULT 0 CHECK (source_clean IN (0, 1)),
	source_snapshot_hash TEXT NOT NULL DEFAULT '',
	strategy TEXT NOT NULL DEFAULT '' CHECK (strategy IN ('', 'direct', 'thinBundle', 'selfContainedBundle')),
	manifest_hash TEXT NOT NULL DEFAULT '',
	warnings_json TEXT NOT NULL DEFAULT '[]',
	consumed_spawn_id TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, source_agent_id, sync_id),
	UNIQUE (controller_id, tree_id, sync_id),
	FOREIGN KEY (controller_id, tree_id, source_agent_id)
		REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, source_device_id)
		REFERENCES devices(controller_id, device_id),
	FOREIGN KEY (controller_id, target_device_id)
		REFERENCES devices(controller_id, device_id)
) STRICT;

CREATE INDEX workspace_sync_receipts_by_target
	ON workspace_sync_receipts(controller_id, target_device_id, status, updated_at);

CREATE TABLE agent_spawn_receipts (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 256),
	source_agent_id TEXT NOT NULL,
	spawn_id TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	target_device_id TEXT NOT NULL,
	task_name TEXT NOT NULL CHECK (
		length(CAST(task_name AS BLOB)) BETWEEN 1 AND 64
	),
	workspace_id TEXT NOT NULL DEFAULT '',
	prompt_digest BLOB NOT NULL CHECK (length(prompt_digest) = 32),
	status TEXT NOT NULL CHECK (status IN ('pending', 'started', 'failed')),
	failure_code TEXT NOT NULL CHECK (
		length(CAST(failure_code AS BLOB)) <= 64 AND
		((status = 'failed' AND length(failure_code) > 0) OR
		 (status != 'failed' AND failure_code = ''))
	),
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, source_agent_id, spawn_id),
	UNIQUE (controller_id, tree_id, sequence),
	UNIQUE (controller_id, tree_id, agent_id),
	UNIQUE (controller_id, tree_id, source_agent_id, task_name),
	FOREIGN KEY (controller_id, tree_id)
		REFERENCES trees(controller_id, tree_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, tree_id, source_agent_id)
		REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX agent_spawn_receipts_by_parent_sequence
	ON agent_spawn_receipts(controller_id, tree_id, source_agent_id, sequence);

CREATE TABLE agent_operation_receipts (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	source_agent_id TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	action TEXT NOT NULL CHECK (action IN ('send', 'followup', 'interrupt')),
	agent_id TEXT NOT NULL,
	payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
	outcome TEXT NOT NULL CHECK (
		(action = 'send' AND outcome IN ('pending', 'queued', 'steered', 'failed')) OR
		(action = 'followup' AND outcome IN ('pending', 'started', 'failed')) OR
		(action = 'interrupt' AND outcome IN ('pending', 'interrupted', 'failed'))
	),
	failure_code TEXT NOT NULL CHECK (
		length(CAST(failure_code AS BLOB)) <= 64 AND
		((outcome = 'failed' AND length(failure_code) > 0) OR
		 (outcome != 'failed' AND failure_code = ''))
	),
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, source_agent_id, operation_id),
	FOREIGN KEY (controller_id, tree_id)
		REFERENCES trees(controller_id, tree_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, tree_id, source_agent_id)
		REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX agent_operation_receipts_by_agent
	ON agent_operation_receipts(controller_id, tree_id, agent_id, created_at, operation_id);

CREATE TABLE peer_worker_sync_cursors (
	controller_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	active_connection_id TEXT NOT NULL CHECK (length(active_connection_id) = 36),
	active_lease_revision INTEGER NOT NULL CHECK (active_lease_revision > 0),
	applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
	PRIMARY KEY (controller_id, device_id),
	FOREIGN KEY (controller_id, device_id)
		REFERENCES devices(controller_id, device_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE agent_lifecycle_states (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	target_device_id TEXT NOT NULL,
	target_revision INTEGER NOT NULL CHECK (target_revision > 0),
	phase TEXT NOT NULL CHECK (
		phase IN ('reserved', 'pending', 'starting', 'preflight', 'ready',
		          'running', 'idle', 'interrupted', 'failed')
	),
	failure_code TEXT NOT NULL CHECK (
		length(CAST(failure_code AS BLOB)) <= 64 AND
		((phase = 'failed' AND length(failure_code) > 0) OR
		 (phase != 'failed' AND failure_code = ''))
	),
	lifecycle_sequence INTEGER NOT NULL CHECK (lifecycle_sequence > 0),
	observed_at INTEGER NOT NULL CHECK (observed_at >= 0),
	PRIMARY KEY (controller_id, tree_id, agent_id),
	UNIQUE (controller_id, tree_id, lifecycle_sequence),
	FOREIGN KEY (controller_id, target_device_id)
		REFERENCES devices(controller_id, device_id),
	FOREIGN KEY (controller_id, tree_id)
		REFERENCES trees(controller_id, tree_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES principals(controller_id, tree_id, agent_id) ON DELETE CASCADE,
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES agent_spawn_receipts(controller_id, tree_id, agent_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX agent_lifecycle_states_by_tree_sequence
	ON agent_lifecycle_states(controller_id, tree_id, lifecycle_sequence);

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
