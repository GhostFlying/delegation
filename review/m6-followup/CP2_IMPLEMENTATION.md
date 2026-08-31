# Checkpoint 2 Implementation Plan

Base: `b406974120dcc95372a91c8d9297649fa665a431`

## Objective

Make lifecycle synchronization and execution qualification independent, durable gates. A peer is
dispatchable only while it is connected, protocol-compatible, not draining, lifecycle-synchronized,
and qualified by a real fresh-thread worker MCP probe. Provide explicit requalification and a
TraeX-only transactional repair path for managed worker configuration.

## Work

1. Rename the existing in-memory broker lifecycle gate from worker readiness to worker sync
   readiness. Extend protocol hello and acknowledged readiness updates, durable peer storage, and
   broker dispatch/status selection with independent execution-readiness state.
2. Add a connector qualification controller backed by durable state. Run a real fresh
   `thread/start` against the managed app-server and validate the complete worker MCP surface. Never
   use `turn/start` or `thread/resume`. Persist a five-attempt epoch with delays of 0, 10 seconds, 30
   seconds, 2 minutes, and 7 minutes across reconnects and restarts; classify permanent failures and
   exhaustion as intervention-required and best-effort publish terminal state once per active broker
   connection.
   Persisted terminal transitions are logged once by the peer service. A protected local-bridge
   long poll uses a revision cursor to notify root MCP sessions that were active at the transition,
   without replaying terminal state that predates MCP startup.
3. Add protected local-bridge `worker recheck --config ... [--json]` support. Start a new epoch only
   for an explicit recheck, runtime/config digest change, or successful repair/upgrade. Extend local
   and broker status JSON with all qualification fields.
4. Reject profile-selection arguments in setup/config sources. Add `service repair --config
   --environment-file [--json]` for TraeX peers: exactly remove profile selectors while preserving
   other argv, quarantine forbidden managed-home roots and non-system skills on the same filesystem
   with private permissions and a typed/mode/digest manifest, atomically replace config, run doctor
   and fresh-thread smoke validation, and roll back on failure with an explicit rollback-failed
   conflict.
5. Add focused protocol, persistence, retry, reconnect/restart, recheck, repair, rollback, status, and
   CLI validation tests. Run `gofmt`, focused suites, then `go test ./...`, preserving raw logs
   outside the repository. Commit the frozen checkpoint atomically and report commit/tree/test
   evidence for independent review.

## Acceptance

- Lifecycle sync remains independent from execution readiness at hello, update, persistence, and
  dispatch.
- Restart and reconnect cannot reset or exceed an epoch's attempt budget. Only contract-authorized
  events create a new epoch.
- Real probe tests prove `thread/start`, full worker MCP authority, persistent managed home, and no
  turn/resume calls.
- Repair success and every rollback branch leave a validated config or a visible
  `rollback_failed` conflict, with quarantine metadata sufficient for manual recovery.
- Terminal readiness rejects spawn before receipt creation, and its bounded failure code is logged
  once and best-effort notified once to root MCP sessions active at the transition.
- Focused and full Go suites pass from a clean worktree.
