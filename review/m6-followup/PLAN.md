# M6 Follow-up Checkpoint Plan

## Objective

Make Delegation upgrades safe and recoverable, qualify peers for real worker execution, and expose
current agent state without rewriting immutable spawn receipts. The milestone starts at
`v0.1.0-alpha.7` (`82491df5c9a1c03d75de14008f293b9d856d776a`).

## Checkpoints

### 1. Runtime acquisition and release trust

- Owner: runtime/release checkpoint worktree.
- Dependencies: none.
- Write set: `plugins/delegation`, `cmd/releasepack`, `.github/workflows`, focused tests and release
  trust documentation.
- Behavior: plugin launchers acquire an absent exact-version runtime through the existing immutable
  installer; upgrade artifacts are accepted only from the canonical attested GitHub release.
- Acceptance: POSIX and Windows plugin tests, releasepack tests, tamper and concurrent-install tests.

### 2. Execution readiness and TraeX repair

- Owner: readiness checkpoint worktree.
- Dependencies: none.
- Write set: protocol hello/readiness messages, connector and broker readiness state, peer store,
  managed-home validation/repair, local management transport, CLI/status surfaces and focused tests.
- Behavior: lifecycle synchronization and execution readiness are independent; readiness attempts
  persist across ordinary restarts; repair, upgrade, digest change, or explicit recheck starts a new
  attempt epoch; permanent failures require intervention immediately.
- Acceptance: readiness classification/backoff/exhaustion/restart/recheck tests, repair commit and
  rollback tests, CLI/config validation tests, and a real thread-start smoke path without turn start.

### 3. Current agent-state projection

- Owner: projection checkpoint worktree.
- Dependencies: checkpoint 2 readiness and connection snapshot contract.
- Write set: agent protocol types, broker/store list path, root MCP presentation and focused tests.
- Behavior: page immutable spawn receipts and left join current lifecycle; project freshness and
  execution readiness into an effective status without mutating receipt state.
- Acceptance: exhaustive mapping, stale/missing lifecycle, generation race, failure precedence, and
  stable pagination tests.

### 4. Atomic local service upgrade

- Owner: local-upgrade checkpoint worktree.
- Dependencies: checkpoints 1 and 2.
- Write set: upgrade journal/store, release verification, local management transport, user-service
  backends, activator command, CLI/status surfaces and platform tests.
- Behavior: prepare and arm a crash-consistent local transaction; an independent activator switches
  definition and shadow database, qualifies the new service, and rolls back only before the global
  commit boundary.
- Acceptance: every journal crash point plus systemd user, launchd, and Scheduled Task ownership,
  exact process-tree, shadow migration, and rollback tests.

### 5. Broker-coordinated upgrade

- Owner: coordinated-upgrade checkpoint worktree.
- Dependencies: checkpoint 4.
- Write set: protocol upgrade RPC, broker/connector coordinators, controller drain, persisted global
  transaction, CLI/status surfaces and fault-injection tests.
- Behavior: freeze responsive online participants, prepare and arm peers, persist an irreversible
  commit, activate peer-first and broker-last, then restore only qualified target-version peers.
- Acceptance: prepare/arm/activate response loss, post-switch disconnect, broker-switch failure,
  reconnect/qualification timeout, cancellation boundary, and bootstrap tests.

## Freeze, Review, and Integration

For every checkpoint, record the base, frozen commit and tree, focused acceptance command and raw
log path in `review/m6-followup/ROUNDS.md`. Review each frozen range once in a clean detached
worktree with an independent read-only agent. A tracked fix consumes another round; do not start a
fourth round. Rebase and fast-forward checkpoints onto this integration branch serially.

After combined validation, freeze and independently review the milestone tree. Final acceptance uses
new isolated homes, identities, databases, service names, and configurations for Linux Codex/TraeX,
macOS Codex/TraeX, and Windows 11 Codex. Windows TraeX must be rejected explicitly. The end-to-end
path includes plugin cold acquisition, a real worker turn and result return, peer-first/broker-last
upgrade, transient disconnect, reconnect, readiness qualification, and current agent projection.
