# Alpha.11 Release Plan

## Objective

Publish `0.1.0-alpha.11` with the two upgrade-recovery fixes discovered during the isolated
alpha.7 to alpha.10 bootstrap, then use alpha.10 as the source for the first real
broker-coordinated upgrade acceptance.

## Accepted Behavior Boundary

- Runtime-alias checkpoint: `cd77ddade784cf231ebeb8536c85744fbe4a9838`, tree
  `d86adef75cf3cce3f245fc7b3c17c1b88d6ac6fc`; independent review accepted in round 2.
- Peer-first local qualification checkpoint: `b09d8645a2001853322bf15caad8fabc5989006b`, tree
  `a4c95d1952c30f2d327c8d996b9822b0eacbfffb`; its semantic patch is identical to the round-1
  accepted revision after the mechanical parent rebase.
- The combined exact `40c932f..b09d864` range was independently accepted in round 1.
- Full Linux tests, focused race tests, vet, plugin and support contracts, and ordinary plus
  `integration,live` compile-only checks for Darwin and Windows amd64/arm64 passed. A Windows
  Kross container also executed the runtime-alias regression test successfully.

## Source Boundary

- The source commit is a direct child of the accepted behavior boundary.
- It changes only the runtime, plugin, and tested CLI version to `0.1.0-alpha.11` and records this
  release plan.
- It deliberately retains the published alpha.10 checksum manifest until deterministic alpha.11
  builds exist.
- No protocol, schema, worker-profile, transport, or platform-support contract changes are part
  of the release commit. Windows TraeX remains unsupported.

## Immutable Release Boundary

1. Freeze and independently review source commit `S` while the tracked checksum manifest still
   names alpha.10.
2. Run full, race, vet, plugin, support, and cross-platform compile acceptance.
3. Build all six deterministic unsigned alpha.11 archives twice from `S`; require byte identity
   and verify both release directories.
4. Create direct child manifest commit `M` by changing only
   `plugins/delegation/release-artifacts.sha256`.
5. Freeze and independently review `S -> M`, then expose only `M` through the protected-main pull
   request.
6. After CI and rebase merge, prove the merged trees are equivalent to the accepted boundary, tag
   the merged manifest commit as `v0.1.0-alpha.11`, and dispatch the release workflow from that tag.
7. Verify the immutable GitHub Release, six archives, checksums, Sigstore provenance, cold launcher
   installation, and current-platform canonical acquisition.

## Coordinated Upgrade Acceptance

- Complete the isolated alpha.7 to alpha.10 forward recovery before starting the coordinated
  transaction.
- Start alpha.10 to alpha.11 through the broker without `--bootstrap`. Require fixed participants,
  PREPARE and ARMED acknowledgement, durable global COMMIT, peer-first and broker-last activation,
  reconnect, lifecycle synchronization, target-bound readiness, and resumed dispatch.
- Prove real worker execution and result return before and after the coordinated upgrade, plus
  lifecycle-derived `list_agents` projection rather than stale spawn receipt state.
- Keep any post-COMMIT failure forward-only and mark an unqualified participant
  `intervention_required` instead of silently rolling back the network.
