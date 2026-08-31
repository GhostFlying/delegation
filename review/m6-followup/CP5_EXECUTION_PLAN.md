# Checkpoint 5 Execution Plan: Broker-Coordinated Upgrade

Base: `be9375ffc4210b89788c381ad0e9e857eedfd552`

## Contract

1. Add versioned broker-to-peer upgrade RPCs for status, prepare, arm, activate, and cancel. The
   connector exposes only its already configured local upgrade manager; the broker never supplies a
   repository, URL, binary, config path, environment path, or other peer-local authority. Requests
   are authenticated by the existing peer session and carry no root or worker principal.
2. Persist a separate protected controller journal beside the broker's local upgrade journal. It
   freezes the responsive upgrade-capable online participants, their exact connection generation,
   local transaction identities, target runtime/config digests, readiness baselines, per-peer
   progress, the broker-local transaction identity, the irreversible global COMMIT decision, a
   bounded completion deadline, and intervention results. Repeating the same target resumes this
   journal; a conflicting non-terminal target fails closed.
3. Enter controller-wide drain before probing or preparing participants. Reject new spawn, send,
   follow-up, mailbox-send, workspace-sync, and result-apply mutations before they create durable
   receipts; continue list, wait, status, lifecycle/readiness publication, result finalization, and
   explicit interrupt. New and replacement peer sessions inherit the drain.
4. PREPARE every frozen peer and the broker-local transaction, then ARM every prepared peer and the
   broker. Any pre-COMMIT error or timeout drives idempotent cancellation and restores dispatch only
   after every prepared local transaction has been canceled. Persist global COMMIT only after all
   participants and the broker report durable ARMED state.
5. After COMMIT, activate every peer first. Treat a lost activation response as ambiguous and
   forward-only; retry while connected and otherwise wait for the target version to reconnect.
   Launch the broker activator only after activation has been requested for every peer. The old or
   recovered broker remains drained and cannot dispatch to source-version peers.
6. On target broker startup, resume the controller journal before accepting dispatch. Require each
   participant to reconnect at the target version, finish lifecycle synchronization, and publish a
   strictly newer ready epoch whose runtime/config digests match its prepared transaction. Wait at
   most ten minutes from target-broker startup. Healthy target peers become usable when the journal
   finishes; missing or exhausted peers are marked `intervention_required`, remain non-dispatchable,
   and produce `completed_with_errors`.
7. Route non-bootstrap broker `service upgrade` through the protected local bridge. Keep peer
   invocation bootstrap-only. Permit cancellation only before global COMMIT. Extend `status --json`
   with the controller transaction, participant counts, completion deadline, and intervention
   state without exposing credentials or filesystem paths.

## Implementation Sequence

1. Define protocol v7 feature/method/payload validation and connector-side upgrade handlers.
2. Add broker drain and generation-pinned upgrade peer calls plus target-version compatibility
   gating.
3. Add the crash-consistent controller journal/store and the resumable coordinator state machine.
4. Wire broker runtime recovery, local bridge management, CLI cancellation/output, and status.
5. Add exhaustive transition, response-loss, disconnect/reconnect, timeout, cancellation-boundary,
   drain, identity, and bootstrap regression tests; update operator and release documentation.

## Acceptance

- Focused protocol, connector, broker, coordinated-upgrade, local-bridge, CLI, and status tests.
- Fault injection at PREPARE, ARM, activation acknowledgement, peer disconnect, broker activation,
  target reconnect/qualification timeout, and every persisted controller-journal transition.
- Repeated race tests for drain admission, connection-generation replacement, and resume.
- Full Linux suite and vet; Linux amd64, macOS arm64, and Windows amd64 compile-only validation;
  integration-tagged Codex E2E compile; POSIX plugin smoke; `git diff --check`.
- Freeze an exact commit/tree and obtain an independent clean detached read-only review. Do not
  start a fourth automated round; unresolved actionable findings after round 3 require human
  disposition.
