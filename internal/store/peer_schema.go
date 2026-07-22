package store

import "fmt"

const (
	peerStoreApplicationID = 0x444c4750 // "DLGP"
	peerSchemaVersion      = 5
)

var peerSchemaCurrent = fmt.Sprintf(`
CREATE TABLE peer_metadata (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	worker_revision INTEGER NOT NULL CHECK (worker_revision >= 0)
) STRICT;

INSERT INTO peer_metadata(singleton, worker_revision) VALUES (1, 0);

CREATE TABLE worker_reservations (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
	parent_agent_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	task_name TEXT NOT NULL,
	prompt_digest TEXT NOT NULL,
	workspace_path TEXT NOT NULL,
    codex_thread_id TEXT NOT NULL DEFAULT '',
    profile_version INTEGER NOT NULL CHECK (profile_version > 0),
    status TEXT NOT NULL CHECK (
		status IN ('reserved', 'pending', 'starting', 'preflight', 'ready', 'running', 'idle', 'interrupted', 'failed')
    ),
	retry_target TEXT NOT NULL DEFAULT '' CHECK (retry_target IN ('', 'pending', 'idle')),
    active_turn_id TEXT NOT NULL DEFAULT '',
    failure_code TEXT NOT NULL DEFAULT '',
	revision INTEGER NOT NULL CHECK (revision > 0),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
    PRIMARY KEY (controller_id, tree_id, agent_id)
) STRICT;

CREATE UNIQUE INDEX worker_reservations_by_thread
    ON worker_reservations(controller_id, codex_thread_id)
    WHERE codex_thread_id <> '';

CREATE INDEX worker_reservations_by_status
    ON worker_reservations(status, updated_at, controller_id, tree_id, agent_id);

CREATE TABLE worker_operation_receipts (
	controller_id TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	action TEXT NOT NULL CHECK (action IN ('send', 'followup', 'interrupt')),
	payload_digest TEXT NOT NULL CHECK (
		length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*'
	),
	status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
	outcome TEXT NOT NULL CHECK (
		outcome IN ('pending', 'queued', 'steered', 'started', 'interrupted', 'failed')
	),
	failure_code TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, operation_id),
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES worker_reservations(controller_id, tree_id, agent_id),
	CHECK (
		(status = 'pending' AND outcome = 'pending' AND failure_code = '') OR
		(status = 'succeeded' AND outcome IN ('queued', 'steered', 'started', 'interrupted') AND failure_code = '') OR
		(status = 'failed' AND outcome = 'failed' AND failure_code <> '')
	)
) STRICT;

CREATE INDEX worker_operation_receipts_by_worker
	ON worker_operation_receipts(controller_id, tree_id, agent_id, created_at, operation_id);

PRAGMA application_id = %d;
PRAGMA user_version = %d;
`, peerStoreApplicationID, peerSchemaVersion)
