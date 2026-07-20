package store

import "fmt"

const (
	peerStoreApplicationID = 0x444c4750 // "DLGP"
	peerSchemaVersion      = 1
)

var peerSchemaV1 = fmt.Sprintf(`
CREATE TABLE worker_reservations (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    parent_agent_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    workspace_path TEXT NOT NULL,
    codex_thread_id TEXT NOT NULL DEFAULT '',
    profile_version INTEGER NOT NULL CHECK (profile_version > 0),
    status TEXT NOT NULL CHECK (
        status IN ('reserved', 'starting', 'ready', 'running', 'idle', 'failed')
    ),
    active_turn_id TEXT NOT NULL DEFAULT '',
    failure_code TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
    PRIMARY KEY (controller_id, tree_id, agent_id)
) STRICT;

CREATE UNIQUE INDEX worker_reservations_by_thread
    ON worker_reservations(controller_id, codex_thread_id)
    WHERE codex_thread_id <> '';

CREATE INDEX worker_reservations_by_status
    ON worker_reservations(status, updated_at, controller_id, tree_id, agent_id);

PRAGMA application_id = %d;
PRAGMA user_version = %d;
`, peerStoreApplicationID, peerSchemaVersion)
