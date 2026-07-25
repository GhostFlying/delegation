# M4 Status Checkpoint Contract

This document fixes the observable status semantics for the M4 status checkpoint. It is an
implementation contract, not a compatibility promise for the pre-release wire format.

## Metrics

- `runningTurns` counts the latest worker lifecycle rows in `running` state whose target peer has
  a live, worker-synchronized broker connection.
- `occupiedWorkerSlots` counts `reserved`, `starting`, `preflight`, `ready`, `running`, and
  `finalizing` workers on live, synchronized target peers. These are the states used by peer slot
  admission.
- `dispatchesPending`, `dispatchesStarted`, and `dispatchesFailed` count durable spawn receipts by
  their current status. A started dispatch remains started after its worker becomes idle.
- `dispatchesStartedTotal` is the lifetime number of unique dispatches that first reached started.
  Replay and idempotent retries must not increment it again.
- `turnsStartedTotal` counts initial and follow-up turns once each. It requires a durable turn
  identity and must survive detail garbage collection.
- `devicesRegistered`, `devicesOnline`, `devicesConnected`, and `devicesSyncReady` are distinct.
  Persistent presence does not prove that a connector is currently connected or synchronized.

The product must not label an idle worker as completed or infer a lifecycle history from the
latest-state table.

## Surfaces

`delegation status [--json]` is role-aware. A broker reports the network snapshot. A peer reports
its local connector and worker-slot snapshot together with broker-authoritative network counters
when connected.

The broker serves the same bounded snapshot as server-rendered HTML and JSON. The status listener
is loopback-only by default. A non-loopback status listener requires a separate read-only token and
must not reuse a broker master token or peer token.

Status output never includes prompts, messages, Git URLs, local paths, rollout contents, tokens, or
provider configuration. Recent-detail collections have hard limits and all HTTP responses disable
caching and external content.
