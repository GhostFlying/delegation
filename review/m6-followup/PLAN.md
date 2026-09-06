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

### Final-validation follow-up: broker hello activation synchronization

- Owner: milestone integration worktree.
- Dependencies: checkpoint 2 and the combined milestone tree.
- Write set: broker readiness test synchronization and review evidence only.
- Behavior: the readiness transition test waits for the broker handler to publish the accepted
  connection after the hello response instead of racing that handler continuation. Product runtime
  behavior and protocol ordering remain unchanged.
- Acceptance: the focused test passes 100 consecutive runs, the surrounding readiness and
  dispatchability tests pass under the race detector, and the full Linux suite passes.

### Final-validation follow-up: native Windows TraeX rejection coverage

- Owner: milestone integration worktree.
- Dependencies: checkpoint 2 and the combined milestone tree.
- Write set: CLI platform test placement, Windows acceptance runner, and review evidence only.
- Behavior: the existing startup-classification regression for unsupported Windows TraeX peers is
  compiled and executed on Windows instead of remaining hidden in a Linux-only test file. Product
  runtime behavior and the explicit Windows rejection contract remain unchanged.
- Acceptance: the focused test passes on Linux and native Windows, the Windows platform contract
  suite reports that the test executed, and the full Linux suite passes.

### Final-validation follow-up: portable TraeX managed-worker live smoke

- Owner: milestone integration worktree.
- Dependencies: checkpoint 2, the readiness-aware live fixture follow-up, and the combined
  milestone tree.
- Write set: TraeX live-test helper isolation, Linux/macOS process observation, and review evidence
  only.
- Behavior: the existing managed TraeX app-server/thread/turn/result/cold-resume smoke compiles and
  runs on both supported TraeX platforms. Linux keeps procfs observation; macOS uses native process
  metadata to match the managed runtime home and exact app-server arguments. Product runtime
  behavior remains unchanged.
- Acceptance: the complete smoke passes with pinned TraeX on fresh isolated Linux and macOS homes,
  compile-only validation passes on all supported build targets, and the full Linux suite passes.

### Final-validation follow-up: protected TraeX account reuse

- Owner: milestone integration worktree.
- Dependencies: checkpoint 2 and the portable TraeX managed-worker live smoke.
- Write set: peer configuration/setup, protected credential synchronization, managed-home policy,
  worker profile, readiness digest inputs, live acceptance, and review evidence.
- Behavior: a TraeX peer reuses one explicitly configured current-user-only host `auth.json` by
  atomically copying it into the isolated `TRAECLI_HOME`; the app-server can authenticate with the
  copy while model-generated worker tools are explicitly denied access to both source and copy.
  The normal TraeX home, configuration, plugins, rules, skills, and session history remain
  isolated.
- Acceptance: setup and runtime reject missing, aliased, broad-permission, malformed, and
  conflicting credential sources; a source-byte change starts a new readiness epoch after service
  restart; the real Linux TraeX shell cannot read either the host source or managed copy; and the
  live worker smoke uses the existing Trae account rather than a no-auth loopback provider. macOS
  must prove the same deny boundary before release acceptance; otherwise stop for an explicit
  security-model decision.

### Final-validation follow-up: native macOS credential-deny enforcement

- Owner: milestone integration worktree.
- Dependencies: the protected TraeX account-reuse checkpoint and native macOS real-account
  acceptance.
- Write set: TraeX credential-path validation, setup/connector/worker enforcement, worker
  filesystem-profile generation, focused/native tests, operator documentation, and review evidence.
- Behavior: retain literal exact-path denies for the explicit host credential and managed copy.
  On macOS, fail closed before setup persistence and again at service/worker startup when either
  path, lexically or through a filesystem alias, is beneath `/tmp`, `/private/tmp`, `/var/tmp`, or
  `/private/var/tmp`; Codex's minimal Seatbelt platform defaults grant those roots after dynamic
  profile rules are compiled. Other locations and platforms retain the existing policy.
- Acceptance: focused tests prove scoped literal entries for both protected paths and temporary-root
  rejection, including lexical and canonical aliases; the real macOS TraeX 0.201.6 smoke proves a worker turn and result,
  direct and symlink-alias denial of both paths, token non-leakage, app-server replacement, and
  same-thread cold resume. This is review round 3 for the protected-auth checkpoint; any remaining
  actionable finding requires human disposition rather than a fourth automated round.

### Final-review follow-up: reliable terminal readiness publication

- Owner: milestone integration worktree.
- Dependencies: checkpoint 2 and final milestone review round 1.
- Write set: connector readiness publication, focused concurrency tests, and review evidence only.
- Behavior: terminal readiness is deduplicated only after a validated broker acknowledgement; when
  the connector's supported in-flight RPC capacity is temporarily full, publication waits for a
  slot and retries within the caller's bounded context instead of suppressing the durable terminal
  snapshot. Lost acknowledgements continue to recover through the reconnect hello snapshot.
- Acceptance: a 128-call capacity test releases one slot without reconnecting and observes the
  terminal update and acknowledgement; invalid acknowledgements remain retryable; existing
  once-per-connection and lost-acknowledgement tests pass under the race detector.

### Rescoped follow-up: worker profile upgrade compatibility

- Human disposition: approved after protected-auth review round 3; this is a new checkpoint whose
  independent review starts at round 1 rather than extending the exhausted protected-auth review.
- Owner: milestone integration worktree.
- Dependencies: local atomic upgrade, protected TraeX authentication, and the frozen round-3
  finding at `c65a5de85032adc5a6ad7568f2cb1e98685275fc`.
- Write set: stopped-shadow peer database migration, upgrade preflight/activation wiring, focused
  migration and rollback tests, operator documentation, and review evidence.
- Behavior: when the target runtime's worker permission profile advances to version 7, the
  stopped-service activator may atomically rewrite retained version-5 alpha.4 or version-6 alpha.7
  worker reservations only in
  the peer shadow database and only after the existing blocker snapshot proves there is no occupied
  worker, pending operation, or unfinished result publication. Reject mixed, unknown, future, or
  occupied profile history. The canonical version-6 database remains untouched as rollback material
  until the normal database switch. Fresh databases and already-version-7 shadow databases remain
  idempotent. Broker databases are unchanged.
- Acceptance: exact alpha.4/profile-5 and alpha.7/profile-6 peer fixtures with `pending`, `idle`,
  `interrupted`, and `failed` history migrate to profile 7 and let worker-host authority validation
  start; occupied, unfinished, mixed-version, and unknown-version cases fail before switch; every activator crash
  point preserves either the original canonical database or a validated target shadow with intact
  rollback material; Linux tests, race tests, macOS/Windows compile checks, and the containerized
  peer-first/broker-last drill pass.

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
