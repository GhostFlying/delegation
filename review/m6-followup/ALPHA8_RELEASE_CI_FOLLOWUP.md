# Alpha.8 Release CI Follow-up

## Trigger

The first protected-main pull request for the reviewed alpha.8 source and manifest boundary ran the
same CI tree as both a branch push and a pull request. Both runs exposed two repeatable test-contract
failures while the race suite and all six cross-compilation jobs passed:

- `TestCodexPeerTopology` expected one Unix socket per peer home after M6 added the broker's
  controller-upgrade management socket. The cohosted broker/peer A home correctly contains the
  broker and peer endpoints; B and C contain only their peer endpoint.
- `TestPeerServicesRegisterDevicesAndServeRootBridge` allowed only two seconds for two full peer
  services to initialize and register. Both macOS runs exhausted that first bounded wait without a
  connector error, while focused and local runs passed.
- `TestCodexPeerTopology` randomized the broker listener but left its local status listener at the
  default `127.0.0.1:8788`. A supported host already running Delegation can legitimately own that
  port, causing the isolated test broker to exit before its health endpoint becomes ready.

## Checkpoint

- Owner: main integration thread.
- Base: reviewed manifest commit `12bbc7c8cac37e8e8a1334bf449cbaf9691ab939`.
- Write set: `tests/codex_peer_e2e/peer_topology_test.go`,
  `internal/cli/connector_runtime_test.go`, and this evidence file.
- Behavior: assert the exact expected peer and cohosted-broker socket paths instead of a directory
  cardinality, and use a bounded ten-second device-registration wait that reports the last observed
  store state or error on timeout. Randomize both broker listeners in the topology fixture and
  include the service log in a health-readiness failure.
- Non-goals: no runtime, protocol, persistence, release-workflow, or artifact-format change.

## Acceptance

- Run the two focused regressions repeatedly, including the Linux Codex topology with the pinned
  Codex binary used by CI.
- Run affected package tests under the race detector, the full Linux suite, vet, plugin contract,
  M6 support contract, format, and diff checks.
- Rebuild the six release archives twice and require byte identity. The new source changes Go test
  files only, so production archives and their manifest must remain byte-identical to the reviewed
  alpha.8 manifest.
- Freeze and obtain independent read-only review before replacing the remote release branch. Then
  recreate a direct-child manifest commit from the accepted source, review that one-file boundary,
  and rerun protected-main CI.
