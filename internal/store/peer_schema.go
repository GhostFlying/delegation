package store

import (
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	peerStoreApplicationID = 0x444c4750 // "DLGP"
	peerSchemaVersion      = 13
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
	last_bound_turn_id TEXT NOT NULL DEFAULT '' CHECK (
		last_bound_turn_id = '' OR length(last_bound_turn_id) = 36
	),
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
	),
	CHECK (
		status NOT IN ('running', 'finalizing') OR active_turn_id = last_bound_turn_id
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

CREATE TABLE worker_turn_start_intents (
	controller_id TEXT NOT NULL CHECK (length(controller_id) = 36),
	tree_id TEXT NOT NULL CHECK (length(tree_id) = 36),
	agent_id TEXT NOT NULL CHECK (length(agent_id) = 36),
	intent_id TEXT NOT NULL UNIQUE CHECK (length(intent_id) = 36),
	device_id TEXT NOT NULL CHECK (length(device_id) = 36),
	managed_thread_id TEXT NOT NULL CHECK (length(managed_thread_id) = 36),
	previous_turn_id TEXT NOT NULL DEFAULT '' CHECK (
		previous_turn_id = '' OR length(previous_turn_id) = 36
	),
	package_id TEXT NOT NULL UNIQUE CHECK (length(package_id) = 36),
	operation_id TEXT NOT NULL DEFAULT '' CHECK (
		operation_id = '' OR length(operation_id) = 36
	),
	retry_target TEXT NOT NULL CHECK (retry_target IN ('pending', 'idle')),
	locator_status TEXT NOT NULL CHECK (locator_status IN ('available', 'unavailable')),
	codex_home TEXT NOT NULL DEFAULT '' CHECK (length(codex_home) <= %d),
	rollout_path TEXT NOT NULL DEFAULT '' CHECK (length(rollout_path) <= %d),
	rollout_offset INTEGER NOT NULL DEFAULT 0 CHECK (
		rollout_offset BETWEEN 0 AND %d
	),
	locator_failure_code TEXT NOT NULL DEFAULT '' CHECK (length(locator_failure_code) <= %d),
	reservation_limit_bytes INTEGER NOT NULL CHECK (
		reservation_limit_bytes BETWEEN 1 AND %d
	),
	state TEXT NOT NULL CHECK (state IN ('prepared', 'bound', 'rejected')),
	turn_id TEXT NOT NULL DEFAULT '' CHECK (turn_id = '' OR length(turn_id) = 36),
	rejection_failure_code TEXT NOT NULL DEFAULT '' CHECK (
		length(rejection_failure_code) <= %d
	),
	prepared_revision INTEGER NOT NULL CHECK (prepared_revision > 0),
	resolution_revision INTEGER NOT NULL DEFAULT 0 CHECK (resolution_revision >= 0),
	created_at INTEGER NOT NULL CHECK (created_at >= 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
	PRIMARY KEY (controller_id, tree_id, agent_id, intent_id),
	FOREIGN KEY (controller_id, tree_id, agent_id)
		REFERENCES worker_reservations(controller_id, tree_id, agent_id),
	CHECK (
		(retry_target = 'pending' AND operation_id = '') OR
		(retry_target = 'idle' AND operation_id <> '')
	),
	CHECK (
		(locator_status = 'available' AND codex_home <> '' AND rollout_path <> '' AND
		 locator_failure_code = '') OR
		(locator_status = 'unavailable' AND codex_home = '' AND rollout_path = '' AND
		 rollout_offset = 0 AND locator_failure_code <> '')
	),
	CHECK (
		(state = 'prepared' AND turn_id = '' AND rejection_failure_code = '' AND
		 resolution_revision = 0) OR
		(state = 'bound' AND turn_id <> '' AND rejection_failure_code = '' AND
		 resolution_revision > prepared_revision) OR
		(state = 'rejected' AND turn_id = '' AND rejection_failure_code <> '' AND
		 resolution_revision > prepared_revision)
	)
) STRICT;

CREATE UNIQUE INDEX worker_turn_start_intents_prepared_by_worker
	ON worker_turn_start_intents(controller_id, tree_id, agent_id)
	WHERE state = 'prepared';

CREATE UNIQUE INDEX worker_turn_start_intents_by_operation
	ON worker_turn_start_intents(controller_id, operation_id)
	WHERE operation_id <> '';

CREATE UNIQUE INDEX worker_turn_start_intents_by_turn
	ON worker_turn_start_intents(controller_id, tree_id, agent_id, turn_id)
	WHERE turn_id <> '';

CREATE INDEX worker_turn_start_intents_by_recovery
	ON worker_turn_start_intents(state, updated_at, controller_id, device_id, tree_id, agent_id);

CREATE TABLE peer_changes_artifacts (
	controller_id TEXT NOT NULL,
	tree_id TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	turn_id TEXT NOT NULL,
	artifact_id TEXT NOT NULL UNIQUE,
	workspace_id TEXT NOT NULL,
	workspace_source_device_id TEXT NOT NULL CHECK (length(workspace_source_device_id) = 36),
	workspace_target_device_id TEXT NOT NULL CHECK (length(workspace_target_device_id) = 36),
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
	base_warnings_json TEXT NOT NULL,
	result_head_oid TEXT NOT NULL DEFAULT '',
	result_snapshot_hash TEXT NOT NULL DEFAULT '',
	result_clean INTEGER NOT NULL DEFAULT 0 CHECK (result_clean IN (0, 1)),
	bundle_part_name TEXT NOT NULL DEFAULT '' CHECK (bundle_part_name IN ('', 'changes.bundle')),
	bundle_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (bundle_size_bytes BETWEEN 0 AND 268435456),
	bundle_sha256 TEXT NOT NULL DEFAULT '',
	overlay_part_name TEXT NOT NULL DEFAULT '' CHECK (overlay_part_name IN ('', 'changes-overlay.tar.zst')),
	overlay_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (overlay_size_bytes BETWEEN 0 AND 268435456),
	overlay_sha256 TEXT NOT NULL DEFAULT '',
	result_warnings_json TEXT NOT NULL DEFAULT '[]',
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
		 overlay_part_name = '' AND result_warnings_json = '[]' AND failure_code = '' AND
		 payload_bytes = 0 AND broker_sequence = 0) OR
		(state = 'publishPending' AND capture_status <> '' AND broker_sequence = 0) OR
		(state = 'published' AND capture_status <> '' AND broker_sequence > 0)
	),
	CHECK (
		(capture_status = '' AND result_warnings_json = '[]' AND payload_bytes = 0) OR
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
		 result_warnings_json = '[]' AND retention_reserved = 0 AND reserved_bytes = 0 AND
		 failure_code <> '')
	),
	CHECK (
		(retention_reserved = 0 AND reserved_bytes = 0) OR
		(retention_reserved = 1 AND reserved_bytes > 0)
	)
) STRICT;

	CREATE INDEX peer_changes_artifacts_by_state
		ON peer_changes_artifacts(state, created_at, controller_id, tree_id, agent_id, turn_id);

	CREATE TABLE peer_result_outbox (
		controller_id TEXT NOT NULL,
		tree_id TEXT NOT NULL,
		source_agent_id TEXT NOT NULL,
		source_device_id TEXT NOT NULL CHECK (length(source_device_id) = 36),
		package_id TEXT NOT NULL UNIQUE CHECK (length(package_id) = 36),
		state TEXT NOT NULL CHECK (
			state IN ('capturePending', 'publishPending', 'deliveryPending', 'delivered')
		),
		managed_thread_id TEXT NOT NULL DEFAULT '',
		turn_id TEXT NOT NULL DEFAULT '',
		lifecycle_revision INTEGER NOT NULL DEFAULT 0 CHECK (lifecycle_revision >= 0),
		manifest_bytes BLOB NOT NULL DEFAULT X'',
		manifest_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
			manifest_size_bytes BETWEEN 0 AND %d
		),
		manifest_sha256 TEXT NOT NULL DEFAULT '',
		parts_json TEXT NOT NULL DEFAULT '[]',
		reservation_limit_bytes INTEGER NOT NULL CHECK (reservation_limit_bytes BETWEEN 1 AND %d),
		reserved_bytes INTEGER NOT NULL CHECK (reserved_bytes BETWEEN 1 AND %d),
		package_bytes INTEGER NOT NULL DEFAULT 0 CHECK (package_bytes BETWEEN 0 AND %d),
		delivery_sequence INTEGER NOT NULL DEFAULT 0 CHECK (delivery_sequence >= 0),
		created_at INTEGER NOT NULL CHECK (created_at >= 0),
		updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
		PRIMARY KEY (controller_id, tree_id, source_agent_id, package_id),
		FOREIGN KEY (controller_id, tree_id, source_agent_id)
			REFERENCES worker_reservations(controller_id, tree_id, agent_id),
		CHECK (
			(state = 'capturePending' AND managed_thread_id = '' AND turn_id = '' AND
			 lifecycle_revision = 0 AND length(manifest_bytes) = 0 AND manifest_size_bytes = 0 AND
			 manifest_sha256 = '' AND parts_json = '[]' AND reserved_bytes = reservation_limit_bytes AND
			 package_bytes = 0 AND
			 delivery_sequence = 0) OR
			(state IN ('publishPending', 'deliveryPending') AND managed_thread_id <> '' AND
			 turn_id <> '' AND lifecycle_revision > 0 AND length(manifest_bytes) = manifest_size_bytes AND
			 manifest_size_bytes > 0 AND length(manifest_sha256) = 64 AND
			 manifest_sha256 NOT GLOB '*[^0-9a-f]*' AND reservation_limit_bytes >= package_bytes AND
			 reserved_bytes = package_bytes AND
			 package_bytes > 0 AND delivery_sequence = 0) OR
			(state = 'delivered' AND managed_thread_id <> '' AND turn_id <> '' AND
			 lifecycle_revision > 0 AND length(manifest_bytes) = manifest_size_bytes AND
			 manifest_size_bytes > 0 AND length(manifest_sha256) = 64 AND
			 manifest_sha256 NOT GLOB '*[^0-9a-f]*' AND reservation_limit_bytes >= package_bytes AND
			 reserved_bytes = package_bytes AND
			 package_bytes > 0 AND delivery_sequence > 0)
		)
	) STRICT;

	CREATE UNIQUE INDEX peer_result_outbox_by_turn
		ON peer_result_outbox(controller_id, tree_id, source_agent_id, turn_id)
		WHERE turn_id <> '';

	CREATE INDEX peer_result_outbox_by_state
		ON peer_result_outbox(state, updated_at, created_at, controller_id, tree_id, source_agent_id);

	CREATE TABLE peer_result_inbox (
		controller_id TEXT NOT NULL,
		tree_id TEXT NOT NULL,
		root_agent_id TEXT NOT NULL CHECK (length(root_agent_id) = 36),
		root_device_id TEXT NOT NULL CHECK (length(root_device_id) = 36),
		source_agent_id TEXT NOT NULL CHECK (length(source_agent_id) = 36),
		source_device_id TEXT NOT NULL CHECK (length(source_device_id) = 36),
		managed_thread_id TEXT NOT NULL CHECK (length(managed_thread_id) = 36),
		turn_id TEXT NOT NULL CHECK (length(turn_id) = 36),
		package_id TEXT NOT NULL UNIQUE CHECK (length(package_id) = 36),
		state TEXT NOT NULL CHECK (state IN ('receiving', 'available', 'evictionTombstone')),
		attempt_id TEXT NOT NULL CHECK (length(attempt_id) = 36),
		lease_expires_at INTEGER NOT NULL CHECK (lease_expires_at > 0),
		lifecycle_revision INTEGER NOT NULL CHECK (lifecycle_revision > 0),
		manifest_bytes BLOB NOT NULL,
		manifest_size_bytes INTEGER NOT NULL CHECK (manifest_size_bytes BETWEEN 1 AND %d),
		manifest_sha256 TEXT NOT NULL CHECK (
			length(manifest_sha256) = 64 AND manifest_sha256 NOT GLOB '*[^0-9a-f]*'
		),
		parts_json TEXT NOT NULL,
		package_bytes INTEGER NOT NULL CHECK (package_bytes BETWEEN 1 AND %d),
		created_at INTEGER NOT NULL CHECK (created_at >= 0),
		updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
		PRIMARY KEY (controller_id, tree_id, root_agent_id, package_id),
		CHECK (length(manifest_bytes) = manifest_size_bytes)
	) STRICT;

	CREATE INDEX peer_result_inbox_by_state
		ON peer_result_inbox(state, updated_at, created_at, controller_id, tree_id, root_agent_id);

	CREATE TABLE peer_result_inbox_parts (
		package_id TEXT NOT NULL,
		kind TEXT NOT NULL CHECK (kind IN ('changesBundle', 'changesOverlay', 'rollout')),
		size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
		sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
		next_offset INTEGER NOT NULL DEFAULT 0 CHECK (next_offset >= 0 AND next_offset <= size_bytes),
		PRIMARY KEY (package_id, kind),
		FOREIGN KEY (package_id) REFERENCES peer_result_inbox(package_id) ON DELETE CASCADE
	) STRICT;

	CREATE TABLE peer_result_inbox_chunks (
		package_id TEXT NOT NULL,
		attempt_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		offset_bytes INTEGER NOT NULL CHECK (offset_bytes >= 0),
		size_bytes INTEGER NOT NULL CHECK (size_bytes BETWEEN 1 AND %d),
		sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
		PRIMARY KEY (package_id, attempt_id, kind, offset_bytes),
		FOREIGN KEY (package_id, kind) REFERENCES peer_result_inbox_parts(package_id, kind)
			ON DELETE CASCADE
	) STRICT;

	CREATE TABLE peer_result_inbox_attempt_receipts (
		controller_id TEXT NOT NULL CHECK (length(controller_id) = 36),
		tree_id TEXT NOT NULL CHECK (length(tree_id) = 36),
		root_agent_id TEXT NOT NULL CHECK (length(root_agent_id) = 36),
		root_device_id TEXT NOT NULL CHECK (length(root_device_id) = 36),
		package_id TEXT NOT NULL CHECK (length(package_id) = 36),
		attempt_id TEXT NOT NULL CHECK (length(attempt_id) = 36),
		outcome TEXT NOT NULL CHECK (outcome IN ('cancelled', 'reclaimed')),
		phase TEXT NOT NULL CHECK (phase IN ('prepared', 'completed')),
		created_at INTEGER NOT NULL CHECK (created_at >= 0),
		updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
		PRIMARY KEY (package_id, attempt_id)
	) STRICT;

	CREATE INDEX peer_result_inbox_attempt_receipts_by_age
		ON peer_result_inbox_attempt_receipts(phase, updated_at, created_at, package_id, attempt_id);

	PRAGMA application_id = %d;
	PRAGMA user_version = %d;
	`,
	maximumWorkspacePath,
	maximumWorkspacePath,
	maximumWorkerRolloutOffset,
	maximumWorkerFailureCode,
	protocol.MaximumResultPackageBytes,
	maximumWorkerFailureCode,
	protocol.MaximumResultManifestBytes,
	protocol.MaximumResultPackageBytes,
	protocol.MaximumResultPackageBytes,
	protocol.MaximumResultPackageBytes,
	protocol.MaximumResultManifestBytes,
	protocol.MaximumResultPackageBytes,
	protocol.ResultPackageChunkBytes,
	peerStoreApplicationID,
	peerSchemaVersion,
)
