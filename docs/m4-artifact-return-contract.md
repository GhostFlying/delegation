# M4 Result Package Return Contract

This document fixes the behavior of the M4 reverse-artifact and safe root-apply checkpoints. It is
an implementation contract for the pre-release protocol, not a compatibility promise.

## Scope

Every managed worker turn produces one result package, including turns that have no managed Git
workspace. The package always contains a canonical `result-manifest.json` and attempts to include
the exact worker rollout segment for that turn. Git result payloads are optional and are present
only when the managed workspace changed.

Package return ends when the package is durably available in the original root peer's local inbox.
It does not mutate the root workspace or expose raw rollout contents to the model. A separate,
explicit `apply_agent_changes` call may consume the verified Git parts of an available package.

`resultPackageV3` is a required hello feature and replaces the pre-release `changesArtifactV1`
flow. A peer or broker missing the feature fails closed instead of silently publishing metadata
without delivering the bytes.

## Package Layout

A package directory uses fixed names and may contain only these regular files:

- `result-manifest.json`, always present, at most 64 KiB.
- `rollout.jsonl.zst`, present when rollout capture succeeds, at most 64 MiB compressed and 64 MiB
  after decompression.
- `changes.bundle`, present when the worker result HEAD differs from the prepared base, at most
  256 MiB.
- `changes-overlay.tar.zst`, present when the worker result is dirty, at most 256 MiB.

The aggregate manifest-plus-payload bound is 512 MiB. Payload descriptors reserve the full 64 KiB
manifest allowance before capture, while rollout, bundle, and overlay retain their individual
64/256/256 MiB bounds. If otherwise valid workspace artifacts would exceed the remaining aggregate
budget, the target omits both workspace artifacts and records `workspace_result_too_large` while
retaining a valid rollout and terminal manifest. A descriptor records the fixed part kind, byte
size, and lowercase SHA-256 digest. Descriptors are sorted and unique. The manifest does not
describe itself recursively.

The exact `result-manifest.json` bytes are authoritative. The target encodes a versioned Go struct
with `encoding/json`, with no optional or unknown fields, appends one newline, and hashes those
bytes. The metadata publish request carries the bounded bytes and their descriptor. The broker
verifies the digest and parses the same bytes with unknown fields and trailing JSON rejected; it
never attempts to re-encode a parsed value to reproduce the digest.

The canonical manifest records the network, tree, source agent, source peer, managed thread, turn,
required worker lifecycle revision, terminal outcome, capture time, and component status. Workspace
fields retain the explicit source and target peer IDs, object format, base and result HEADs,
snapshot hashes, manifest hash, clean flags, and source/result warnings. It never contains a
credential, provider configuration, Git URL, absolute path, prompt, or message body.

Rollout and workspace capture failures are independent component states. A rollout failure does not
discard valid Git changes, and a Git capture failure does not discard a valid rollout or prevent a
terminal result manifest from being returned.

The mandatory manifest cannot represent failure to publish itself. Transient filesystem or database
errors remain `capturePending` and retry with bounded backoff while the reserved worker is
`finalizing`. An invariant, integrity, or exhausted-reservation failure is fail-stop for the peer:
the connector becomes unhealthy, refuses new managed work, retains the reservation and any durable
bytes, and reports a bounded operator-facing error. Pre-release recovery requires repairing or
resetting that peer state; it must never release the slot or claim delivery without a durable
manifest.

## Durable States

The target peer owns the authoritative outbox payload and advances through:

1. `capturePending`: the managed turn is terminal and package capture is reserved.
2. `publishPending`: fixed files and manifest are durably published in the target outbox.
3. `deliveryPending`: the broker accepted authoritative metadata; the worker slot is released, but
   payload files remain protected from garbage collection.
4. `delivered`: the original root peer durably acknowledged the complete package; the target copy
   remains protected until the broker explicitly authorizes release.
5. `releasePending`: the target durably recorded the broker release before deleting the package
   directory. It fsyncs the outbox parent where the platform exposes a directory durability barrier
   and then removes the row; startup completes either side of that filesystem boundary. Windows
   relies on the durable intent and bounded retry because Go does not expose the same directory
   barrier and transient file locks may delay removal.

The root peer owns a separate inbox and advances through `receiving` and `available`. A finish
request validates every descriptor and digest, fsyncs the files and containing directory, atomically
publishes the package directory, and commits `available` before acknowledging success. Startup
removes or resumes bounded `receiving` state without exposing a partial package.

The broker persists package identity, authority, manifest metadata, destination root peer, and
delivery state only. It never stores rollout, bundle, overlay, or chunk bytes in SQLite, temporary
files, logs, status output, or errors.

## Acknowledgements And Sequence

Metadata publication and payload delivery are separate acknowledgements:

- The metadata acknowledgement moves the target to `deliveryPending` and releases its worker slot.
  Root unavailability must not keep `maxWorkerSlots` occupied.
- The root finish acknowledgement proves that the full package is durable and available. Only then
  does the broker mark the package delivered and allocate its tree event sequence.
- The target delivery acknowledgement changes the outbox to `delivered` but does not delete bytes.
  The broker first durably records that acknowledgement, then sends a separate idempotent `release`
  carrying the immutable package ID and delivery sequence. Only that release may delete the target
  copy. If the source deletes the bytes but the broker's release record is lost, replay sees the
  absent immutable package as success and completes the broker record without retransferring data.

Assigning sequence at metadata publication is forbidden: a later package could otherwise become
available first and advance a root cursor past an older pending package. Replays use `packageId` and
the source-agent/turn identity. Repeating an identical publish, begin, write, finish, or delivery
acknowledgement returns the recorded result. Reusing an identity with different metadata, bounds, or
bytes is a conflict and fences the affected transfer.

At the first successful begin, the root peer transactionally assigns a positive monotonic retention
ordinal. Receiving and already-available replays return that same ordinal. The broker supplies its
largest retained ordinal for that root as a floor, so a peer that rebuilt local state continues
above broker metadata that still exists. After root finish, the broker stores the ordinal with the
delivery sequence. The broker serializes that floor lookup, root transfer, and delivery mark per
root peer. This closes the cold-state window in which two concurrent relays could otherwise reuse
one ordinal between a root database rebuild and the first broker mark; relays to different roots
remain concurrent.

After source release, the broker may compact old manifest detail while preserving the tree sequence
high-water and controller lifetime counters. It ranks every delivered package per root peer by the
root retention ordinal, including unreleased packages, and retains the newest 64. The root uses that
same total order for admission eviction, so the broker window remains a superset even when relays
finish out of publication order. Only released packages outside that window are eligible. There is
no age-based expiry: compaction runs in bounded batches after broker startup and delivery progress,
and a compaction failure never changes the relay result. This avoids deleting metadata for a payload
that the root still retains without keeping the only remaining source copy indefinitely.

## Authority

Metadata publication is accepted only when the authenticated connection device equals the source
worker principal device, the principal has `artifact.publish.self`, the source is a started managed
spawn in the same controller and tree, and the manifest's agent, peer, thread, and turn identity
matches that durable worker lifecycle. For workspace results, the consumed workspace receipt,
source/target peers, base manifest, base HEAD, base snapshot, object format, and warnings must also
match exactly. Generalizing packages to no-workspace turns must not weaken these checks.

The worker lifecycle sync and broker store are extended with the managed Codex thread ID and the
active or finalizing turn ID. A finalizing result publication must match that durable receipt before
the broker accepts it; target-local state alone is not treated as broker authorization. The package
records the exact lifecycle revision that introduced the finalizing thread/turn receipt. The
connector waits until its lifecycle-sync acknowledgement has applied at least that revision before
publishing metadata. An early publication receives a retryable `lifecycle_not_ready` response and
does not create package state or fence the transfer; any other identity mismatch remains an
authority conflict.

The broker derives the sink from the tree's durable root principal and `rootDeviceId`. Root
`begin`, `write`, `finish`, `cancel`, availability lookup, and acknowledgement are accepted only on
that authenticated root connection and only for the same immutable package metadata. Neither source
nor sink may substitute a controller, tree, agent, peer, turn, workspace, or descriptor supplied by
the other side.

## Live Relay

The target connector asks the broker to deliver a published package. The broker derives the
destination from the tree's durable `rootDeviceId`; the target cannot choose another recipient. The
broker relays fixed 128 KiB chunks from the target outbox to the root inbox and validates offset,
kind, size, and digest metadata at every boundary.

Delivery coordination must run outside the source session's synchronous read loop. The broker has
to keep reading that same full-duplex WebSocket while it issues source `read` calls, including when
the source and root are the same peer. A synchronous request handler would deadlock both remote and
self-target delivery.

Disconnects leave the target in `deliveryPending`. A later connector session retries from durable
metadata; the root reports its recorded offset or `alreadyAvailable` state. The broker may have one
live relay for a package and deduplicates concurrent attempts. It does not queue payload bytes while
either peer is offline.

`begin` durably creates a `receiving` row and fixed temporary files at offset zero. A write is
accepted at exactly the recorded next offset. Before acknowledging it, the root writes and fsyncs
the chunk, then commits the new offset. If a crash leaves a file longer than the committed offset,
startup truncates it back to that offset; a file shorter than the committed offset is an integrity
failure. After a lost acknowledgement, a replay below the recorded offset is read back and must
match the already durable bytes before the recorded next offset is returned. A different overlap or
an offset beyond the recorded value is a conflict.

`finish` requires every committed offset to equal its descriptor size and re-hashes every file and
the exact manifest bytes. It then fsyncs every file, fsyncs the temporary package directory, renames
that directory atomically, fsyncs the parent directory, and only then commits `available`. Startup
reconciles a verified final directory to `available`, truncates a temporary directory to committed
offsets, and rejects any other database/filesystem disagreement.

Before that publication sequence, a root receiving a rollout component decodes exactly one zstd
stream with explicit decoder memory and window limits. It consumes the stream to a clean EOF while
enforcing the 64 MiB raw limit and verifies the manifest's exact raw byte count and raw SHA-256.
Trailing, concatenated, truncated, over-limit, or digest-mismatched rollout data fails `finish` and
keeps the package unavailable.

## Rollout Capture

Before each `turn/start`, the target durably records a start intent containing its generated intent
ID, managed thread ID, most recent locally bound turn ID, if any, and one rollout locator state:

- `available`, with the validated rollout path and current byte offset; or
- `unavailable`, with the bounded capture failure code already observed for that thread.

Only then may it write the request. A valid `turn/started` notification or successful response
atomically binds the returned Codex turn ID to that intent before ordinary monitoring continues;
contradictory identities fail-stop the peer. An explicit successful start with an unavailable
locator proceeds normally and produces a manifest whose rollout component is `captureFailed`.

A definitive JSON-RPC rejection for that exact request means no turn was created. Before following
the existing worker failure transition, the target durably marks the intent `rejected/noTurn` with
a bounded error code and releases its package-capacity reservation. Restart replays that terminal
no-turn transition instead of attempting rollout reconciliation. Transport loss, malformed or
contradictory responses, and any error whose response receipt is uncertain are not definitive and
remain ambiguous.

If the request outcome is ambiguous, the connector does not mark the worker interrupted, release
its reservation, or issue another turn. It retires the uncertain app-server process and reconciles
the intent after cold resume by confirming exactly one new turn with
`thread/read(includeTurns: true)`. The confirmed ID is durably bound and recovered whether the turn
is still active or already terminal. The saved locator remains the boundary for later result
capture, but is not needed to prove which turn was created. An
unavailable locator only degrades the rollout component to `captureFailed`; exact thread history is
still authoritative for start reconciliation. No turn, multiple turns, unsupported history, a
missing previous boundary, or conflicting thread, turn, or status evidence leaves the peer
fail-stop and the reservation intact for operator recovery; it must never guess or replay a prompt
that may already have executed. Startup performs the same reconciliation for every unresolved start
intent. Runtime app-server recovery performs it before reopening the operation fence: one replacement
app-server cold-resumes each prepared thread and reads its exact history, without issuing another
`turn/start`.

Once a turn ID is durably bound, connector or app-server loss may not move that worker directly to
idle, interrupted, or failed. Recovery first reserves finalization for the bound turn. If the exact
terminal rollout record becomes available, its outcome is used; otherwise bounded recovery records
an interrupted `app_server_lost` outcome and a `rollout.captureFailed` component. In both cases it
still captures the current managed Git result and returns the mandatory manifest before releasing
the worker slot. All bound turns from one shared app-server loss use the same seven inspection rounds
and six flush waits; the retry window is not multiplied by the number of worker slots.

After each managed `thread/start` and cold `thread/resume`, the connector calls
`thread/read(includeTurns: false)` and records the returned rollout path only after validating that
it is inside the managed `CODEX_HOME` session hierarchy and names the expected thread ID.

The Codex `Thread.path` field is unstable and optional. An unsupported `thread/read`, a missing
path, compressed or non-regular source, mismatched thread ID, path outside the managed hierarchy, or
other validation failure produces a bounded `rollout.captureFailed` component. It does not block the
turn or suppress other package parts, but repeated failures are visible in peer status and logs.

`turn/completed` can arrive before the final rollout write is flushed. Capture therefore retries for
a bounded interval until it finds a matching `task_started` record and the following terminal record
for the same turn: `task_complete` or `turn_aborted`. It compresses only that inclusive JSONL byte
segment, not the cumulative thread prefix or the file at EOF. This avoids duplicating prior turns
and prevents a concurrent follow-up turn from leaking into the result. Both raw and compressed sizes
and the raw SHA-256 digest are checked against their limits.

Raw rollout bytes remain local to the root inbox. `wait_agent`, MCP instructions, status output, and
broker metadata expose only a bounded package handle and component status.

## Retention And Admission

Each peer may retain at most 64 target-outbox packages and 64 root-inbox packages, with a 2 GiB byte
budget for each store. `capturePending`, `publishPending`, `deliveryPending`, `delivered`,
`releasePending`, and `receiving` entries are not eligible for ordinary age-based garbage
collection. A target outbox is removed only by broker release; local periodic maintenance merely
finishes a previously committed `releasePending` tombstone.

Each `receiving` row is bound to one broker-issued transfer attempt and a bounded lease. On relay
failure, the broker sends an exact `cancel` that removes only that attempt's partial root files; the
target remains `deliveryPending` and retains the authoritative outbox. If cancellation is lost, the
root periodically reclaims an expired receiving lease that has no active in-process writer. A retry
after reclamation starts at offset zero. Active or unexpired receiving state is never evicted for
admission, and expired partial state is reclaimed before a backlog error is returned.

Before accepting a new managed turn, the peer reserves enough package capacity for the configured
component bounds. If it cannot reserve safely, admission fails with a bounded backlog error; it does
not start a turn and later evict an undelivered result. Root `available` packages may be pruned
oldest-first only when admission needs capacity. Admission orders `receiving` and `available` rows
together by their durable retention ordinal. If the oldest row is still `receiving`, the root
returns retryable backpressure instead of evicting a newer available result; a later retry proceeds
after that row finishes or its lease is reclaimed. Target outbox packages are never pruned by local
admission.

`maxWorkerSlots` limits app-server concurrency; result-store capacity is an independent admission
resource. A peer can therefore admit more no-workspace turns than worst-case workspace turns. When
the 2 GiB result backlog cannot reserve another package, the target returns the retryable `busy`
outcome before `turn/start`; peer status identifies the result backlog. It does not queue the turn.

Outbox and inbox directories are distinct so self-target delivery cannot collide. All traversal and
publication use anchored `os.Root` operations, reject symlinks and unexpected entries, create files
with restrictive permissions, and use temporary files plus atomic rename.

Inbox pruning commits a durable local eviction tombstone before removing bytes, fsyncs the parent
after deletion, and then compacts the tombstone to an absent row. Startup completes either side of
that sequence, so tombstones do not accumulate. Broker result metadata deliberately does not claim
current byte availability. On each root `wait_agent` response, the same-peer local bridge decorates
a delivered handle as `available` only when its inbox row and directory are verified; an eviction
tombstone or absent row is `evicted`. It validates that the handle belongs to the current tree root
before doing so, and no local path is returned. This is unambiguous because the broker allocates a
sequence only after that root previously acknowledged durable availability. It keeps availability
authoritative after GC without sending root filesystem state to the broker.

## Root Visibility

`wait_agent` returns retained delivered package handles in tree sequence order. Compacted sequences
remain monotonic holes and are never renumbered; a later package still advances past those holes.
A handle includes package and
turn IDs, source agent/peer identity, terminal/component status, bounded Git metadata and warnings,
part kinds/sizes/digests, and locally decorated availability. It never contains a local path or raw
payload.

Each `wait_agent` response contains at most one result package, at most four lifecycle records, and
at most one 1 KiB worker message, with a hard 16 KiB serialized model-output limit. Package summaries
reuse the existing limits of 16 workspace warnings, 64 bytes per warning or failure code, fixed-size
IDs and digests, and at most four part descriptors. Manifest bytes and component payloads are never
embedded in the response.

`apply_agent_changes` accepts only a fresh `apply_id` and an `available` `package_id`. The root MCP
derives the source path from Codex's trusted sandbox metadata; a model cannot supply or redirect a
filesystem path. The same-user local bridge keeps that path local. Broker authorization receives
only its SHA-256 digest, the explicit Git URL, package identity, and retained workspace identity.

Before authorization and again immediately before mutation, the root peer verifies the current Git
HEAD, index, non-ignored worktree state, explicit origin, object format, working directory, and
warnings against the package's retained base manifest. It rejects active Git operations, shallow
repositories, replacement refs, includes, worktree redirection, fsmonitor, and executable clean,
smudge, or process filters. Repository hooks and `core.hooksPath` are accepted because every Git
command in the apply path overrides hooks to a private disabled path and none of the chosen plumbing
commands requires hooks. System and global Git configuration are disabled for all apply inspection
and mutation commands. Normal and linked worktrees are supported; a thread copied to a different
peer remains outside this authority.

`remote_git_full_history_fallback` is transfer-only metadata, so root inspection removes only that
warning before comparing the current repository's source warnings. The LFS and submodule warnings
remain exact-match inputs, and the retained base manifest hash and workspace snapshot must still
match. This allows a full-fallback result to return without treating its transport decision as root
workspace drift.

The root peer materializes only `changes.bundle` and `changes-overlay.tar.zst` into a private,
journaled staging directory. It never materializes `rollout.jsonl.zst` into the apply path. Worker
commits are flattened against the retained root HEAD, so their content becomes staged changes while
the worker's final staged, unstaged, deletion, rename, binary, intent-to-add, and non-ignored
untracked states remain distinct. Root HEAD, its symbolic ref, and the branch OID never move.
Repository-local configuration and ignored files are preserved.

Ignored root paths are enumerated with the repository's own exclude rules under the same isolated
Git profile. If an ignored file or directory has an exact, ancestor, descendant, or portable
case-folded collision with any path changed by the result, apply returns
`root_workspace_conflict` before the first destructive root write. Delegation never deletes an
ignored cache or build output to make room for a worker path.

Terminal apply state is first atomically reduced to a replay receipt and fsynced; only then are the
materialized package, bundle, staging checkout, and desired overlay removed. A crash in between is
completed by startup or exact replay. New apply admission is blocked when the peer already has 64
active apply journals or 4 GiB of persisted active journal data. At most 1,024 terminal receipts or
64 MiB are retained. Authorizing, building, or prepared journals that have not begun a root mutation
expire after 24 hours into a compact `root_workspace_recovery_required` receipt. A journal that may
have begun mutation instead retains its verified base, desired snapshot, and reconstruction payload
as active recovery data; it counts against the active count and byte ceilings until resolved rather
than being discarded by age. Terminal receipts expire after 30 days and are otherwise pruned
oldest-first for count or bytes; a new apply is rejected with a bounded backlog error rather than
deleting active recovery data. An exact completed replay returns the durable prior outcome while
that terminal receipt remains retained. Reusing a retained `apply_id` with different arguments is a
conflict. Base drift, unsafe repository state, or mutation-boundary drift returns
`needs_resolution` with `root_workspace_conflict` before Delegation writes the workspace or its Git
object database. An ambiguous crash or integrity failure returns `needs_resolution` with
`root_workspace_recovery_required` and preserves recovery data. After the root agent or user restores
the workspace to the recorded base or completes the desired snapshot, retrying the same `apply_id`
either resumes the apply or verifies and records success; it never reports success based on an
unverified partial state.

## Acceptance

The runnable checkpoint must cover:

- a no-workspace worker returning a rollout and manifest;
- unchanged, commit-only, dirty-only, and commit-plus-dirty Git results;
- root offline during publication and delivery after reconnect;
- disconnects and lost acknowledgements at every chunk and finish boundary;
- a lost `turn/start` response before the turn ID is attached, followed by cold reconciliation and
  package capture without re-executing the prompt;
- a definitive `turn/start` rejection followed by restart, with no fail-stop state or leaked package
  reservation;
- duplicate and conflicting package identities;
- concurrent remote and self-target delivery without read-loop deadlock;
- digest, offset, traversal, symlink, size, count, and byte-budget rejection;
- worker-slot release after metadata acknowledgement while payload delivery remains pending;
- root cursor ordering based only on durable availability;
- the complete `wait_agent` to `apply_agent_changes` path through the local bridge, broker
  authorization, and mutation of the originating root workspace;
- safe local apply for normal and linked worktrees with exact staged, unstaged, untracked, binary,
  rename, deletion, and commit-flattening fidelity;
- root config and ignored-file preservation, no HEAD/ref movement, exact replay, and zero-write
  conflicts for tracked, untracked, index, and ref drift at the mutation boundary;
- isolated Git configuration with executable local/worktree filters rejected, global filters unable
  to execute, and ordinary repository hooks accepted but disabled for apply commands; and
- connector, broker, and app-server restart recovery with no partial package exposed.
