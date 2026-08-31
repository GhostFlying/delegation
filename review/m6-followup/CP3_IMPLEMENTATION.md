# Checkpoint 3 Implementation Plan

Base: `23e3234203f8bd429e6e324f9ced0a9d3f333795`

## Objective

Expose the current observable state of every managed agent without mutating or disguising its
immutable spawn receipt. Keep pagination anchored only to the receipt sequence while joining the
latest durable lifecycle state and a generation-consistent live target snapshot.

## Work

1. Rename the public spawn receipt fields to `spawnStatus` and `spawnFailureCode`; remove the
   ambiguous `status` and `failureCode` aliases from `spawn_agent` and its root MCP output. Keep the
   durable receipt values and spawn outcome validation unchanged.
2. Add a distinct list-agent state contract containing the spawn receipt, optional lifecycle
   phase/failure/revision/observation fields, lifecycle freshness, effective status/failure source,
   and the target dispatchability observed for the same broker connection generation.
3. Read each receipt page and its latest lifecycle row through one read-only database transaction
   using a left join. Keep membership, ordering, and the next cursor based only on the immutable
   spawn sequence; lifecycle updates must not create, remove, reorder, or duplicate page members.
4. Around each database read, capture the broker's current connection generation plus, per target,
   connection presence, lifecycle-sync readiness, execution readiness, version compatibility, and
   draining state. Retry the complete capture/read/check operation at most three times. Return an
   unavailable error instead of a mixed snapshot if the generation keeps changing.
5. Project status with spawn failure taking precedence over lifecycle failure. A present non-failed
   lifecycle is current only on a connected, lifecycle-synchronized target; otherwise its last
   phase remains visible but effective status is indeterminate. A missing lifecycle is explicitly
   `missing`. `targetDispatchable` uses the exact CP2 five-part dispatch gate. Keep `wait_agent`
   activities unchanged as exact lifecycle transitions.
6. Update root MCP output and validation, protocol/store/broker tests, and documentation. Cover the
   full mapping table, conflicting failure codes, missing and stale lifecycle state, one-change
   retry, continuous generation churn, dispatchability, and pagination stability under lifecycle
   updates.

## Acceptance

- Spawn receipt values remain unchanged after lifecycle updates and keep their original database
  columns and transition rules.
- `list_agents` never reports a pending spawn as its current effective state when a current
  lifecycle says running, idle, interrupted, or failed.
- Offline or syncing targets never turn a non-failed stale lifecycle into a definitive effective
  state, while the last lifecycle fields remain available for diagnosis.
- A spawn failure always wins over a conflicting lifecycle failure; failure source and effective
  failure code identify the selected authority.
- Connection churn cannot produce a mixed page, and three consecutive generation changes fail
  closed.
- Focused race tests, the full Go suite, vet, and Linux/macOS/Windows compile validation pass from
  a clean checkpoint worktree before freeze.
