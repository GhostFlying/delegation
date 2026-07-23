package store

import "fmt"

const (
	peerStoreApplicationID = 0x444c4750 // "DLGP"
	peerSchemaVersion      = 9
)

var peerSchemaCurrent = fmt.Sprintf(`
CREATE TABLE peer_metadata (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	worker_revision INTEGER NOT NULL CHECK (worker_revision >= 0)
) STRICT;

INSERT INTO peer_metadata(singleton, worker_revision) VALUES (1, 0);

CREATE TABLE prepared_workspaces (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	workspace_id TEXT NOT NULL,
	source_agent_id TEXT NOT NULL,
	source_device_id TEXT NOT NULL,
	target_device_id TEXT NOT NULL,
	git_url TEXT NOT NULL,
	head_oid TEXT NOT NULL,
	object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
	working_directory TEXT NOT NULL,
	source_clean INTEGER NOT NULL CHECK (source_clean IN (0, 1)),
	source_snapshot_hash TEXT NOT NULL CHECK (
		length(source_snapshot_hash) = 64 AND source_snapshot_hash NOT GLOB '*[^0-9a-f]*'
	),
	workspace_path TEXT NOT NULL,
	strategy TEXT NOT NULL CHECK (strategy IN ('direct', 'thinBundle', 'selfContainedBundle')),
	manifest_hash TEXT NOT NULL CHECK (
		length(manifest_hash) = 64 AND manifest_hash NOT GLOB '*[^0-9a-f]*'
	),
	source_warnings_json TEXT NOT NULL,
	warnings_json TEXT NOT NULL,
	status TEXT NOT NULL CHECK (status IN ('prepared', 'claimed')),
	claimed_agent_id TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, workspace_id),
	CHECK (
		(status = 'prepared' AND claimed_agent_id = '') OR
		(status = 'claimed' AND claimed_agent_id <> '')
	)
) STRICT;

CREATE INDEX prepared_workspaces_by_status
	ON prepared_workspaces(status, updated_at, controller_id, tree_id, workspace_id);

CREATE TABLE worker_reservations (
    controller_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
	parent_agent_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	task_name TEXT NOT NULL,
	prompt_digest TEXT NOT NULL,
	workspace_id TEXT NOT NULL DEFAULT '',
	workspace_path TEXT NOT NULL,
	working_directory TEXT NOT NULL DEFAULT '',
    codex_thread_id TEXT NOT NULL DEFAULT '',
    profile_version INTEGER NOT NULL CHECK (profile_version > 0),
    status TEXT NOT NULL CHECK (
		status IN ('reserved', 'pending', 'starting', 'preflight', 'ready', 'running', 'finalizing', 'idle', 'interrupted', 'failed')
    ),
	retry_target TEXT NOT NULL DEFAULT '' CHECK (retry_target IN ('', 'pending', 'idle')),
    active_turn_id TEXT NOT NULL DEFAULT '',
    failure_code TEXT NOT NULL DEFAULT '',
	final_target_status TEXT NOT NULL DEFAULT '' CHECK (
		final_target_status IN ('', 'idle', 'interrupted', 'failed')
	),
	final_failure_code TEXT NOT NULL DEFAULT '',
	revision INTEGER NOT NULL CHECK (revision > 0),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, agent_id),
	CHECK (
		(status <> 'finalizing' AND final_target_status = '' AND final_failure_code = '') OR
		(status = 'finalizing' AND active_turn_id <> '' AND retry_target = '' AND failure_code = '' AND (
			(final_target_status = 'idle' AND final_failure_code = '') OR
			(final_target_status IN ('interrupted', 'failed') AND final_failure_code <> '')
		))
	)
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

CREATE TABLE peer_changes_artifacts (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	turn_id TEXT NOT NULL,
	artifact_id TEXT NOT NULL UNIQUE,
	workspace_id TEXT NOT NULL,
	completion_target_status TEXT NOT NULL CHECK (
		completion_target_status IN ('idle', 'interrupted', 'failed')
	),
	completion_failure_code TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL CHECK (state IN ('capturePending', 'publishPending', 'published')),
	capture_status TEXT NOT NULL DEFAULT '' CHECK (
		capture_status IN ('', 'available', 'unchanged', 'captureFailed')
	),
	object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
	base_head_oid TEXT NOT NULL,
	base_clean INTEGER NOT NULL CHECK (base_clean IN (0, 1)),
	base_manifest_hash TEXT NOT NULL CHECK (
		length(base_manifest_hash) = 64 AND base_manifest_hash NOT GLOB '*[^0-9a-f]*'
	),
	base_snapshot_hash TEXT NOT NULL CHECK (
		length(base_snapshot_hash) = 64 AND base_snapshot_hash NOT GLOB '*[^0-9a-f]*'
	),
	result_head_oid TEXT NOT NULL DEFAULT '',
	result_snapshot_hash TEXT NOT NULL DEFAULT '',
	result_clean INTEGER NOT NULL DEFAULT 0 CHECK (result_clean IN (0, 1)),
	bundle_part_name TEXT NOT NULL DEFAULT '' CHECK (bundle_part_name IN ('', 'changes.bundle')),
	bundle_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (bundle_size_bytes BETWEEN 0 AND 268435456),
	bundle_sha256 TEXT NOT NULL DEFAULT '',
	overlay_part_name TEXT NOT NULL DEFAULT '' CHECK (overlay_part_name IN ('', 'changes-overlay.tar.zst')),
	overlay_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (overlay_size_bytes BETWEEN 0 AND 268435456),
	overlay_sha256 TEXT NOT NULL DEFAULT '',
	warnings_json TEXT NOT NULL DEFAULT '[]',
	failure_code TEXT NOT NULL DEFAULT '',
	retention_reserved INTEGER NOT NULL DEFAULT 0 CHECK (retention_reserved IN (0, 1)),
	reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK (reserved_bytes BETWEEN 0 AND 536870912),
	payload_bytes INTEGER NOT NULL DEFAULT 0 CHECK (payload_bytes BETWEEN 0 AND 536870912),
	broker_sequence INTEGER NOT NULL DEFAULT 0 CHECK (broker_sequence >= 0),
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, agent_id, turn_id),
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES worker_reservations(controller_id, tree_id, agent_id),
	CHECK (
		(completion_target_status = 'idle' AND completion_failure_code = '') OR
		(completion_target_status IN ('interrupted', 'failed') AND completion_failure_code <> '')
	),
	CHECK (
		(bundle_part_name = '' AND bundle_size_bytes = 0 AND bundle_sha256 = '') OR
		(bundle_part_name = 'changes.bundle' AND bundle_size_bytes > 0 AND
		 length(bundle_sha256) = 64 AND bundle_sha256 NOT GLOB '*[^0-9a-f]*')
	),
	CHECK (
		(overlay_part_name = '' AND overlay_size_bytes = 0 AND overlay_sha256 = '') OR
		(overlay_part_name = 'changes-overlay.tar.zst' AND overlay_size_bytes > 0 AND
		 length(overlay_sha256) = 64 AND overlay_sha256 NOT GLOB '*[^0-9a-f]*')
	),
	CHECK (payload_bytes = bundle_size_bytes + overlay_size_bytes),
	CHECK (
		(state = 'capturePending' AND capture_status = '' AND result_head_oid = '' AND
		 result_snapshot_hash = '' AND result_clean = 0 AND bundle_part_name = '' AND
		 overlay_part_name = '' AND warnings_json = '[]' AND failure_code = '' AND
		 payload_bytes = 0 AND broker_sequence = 0) OR
		(state = 'publishPending' AND capture_status <> '' AND broker_sequence = 0) OR
		(state = 'published' AND capture_status <> '' AND broker_sequence > 0)
	),
	CHECK (
		(capture_status = '' AND payload_bytes = 0) OR
		(capture_status = 'available' AND result_head_oid <> '' AND result_snapshot_hash <> '' AND
		 failure_code = '' AND (
			(payload_bytes > 0 AND retention_reserved = 1 AND reserved_bytes = payload_bytes) OR
			(payload_bytes = 0 AND retention_reserved = 0 AND reserved_bytes = 0 AND
			 base_clean = 0 AND result_head_oid = base_head_oid AND result_clean = 1)
		)) OR
		(capture_status = 'unchanged' AND result_head_oid = base_head_oid AND
		 result_snapshot_hash = base_snapshot_hash AND result_clean = base_clean AND payload_bytes = 0 AND
		 retention_reserved = 0 AND reserved_bytes = 0 AND failure_code = '') OR
		(capture_status = 'captureFailed' AND result_head_oid = '' AND
		 result_snapshot_hash = '' AND result_clean = 0 AND payload_bytes = 0 AND
		 retention_reserved = 0 AND reserved_bytes = 0 AND failure_code <> '')
	),
	CHECK (
		(retention_reserved = 0 AND reserved_bytes = 0) OR
		(retention_reserved = 1 AND reserved_bytes > 0)
	)
) STRICT;

CREATE INDEX peer_changes_artifacts_by_state
	ON peer_changes_artifacts(state, created_at, controller_id, tree_id, agent_id, turn_id);

PRAGMA application_id = %d;
PRAGMA user_version = %d;
`, peerStoreApplicationID, peerSchemaVersion)
