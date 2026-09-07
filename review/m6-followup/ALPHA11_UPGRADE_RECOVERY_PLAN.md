# Alpha.11 Upgrade Recovery Plan

## Objective

Close the two ordinary Linux bootstrap failures reproduced while upgrading the isolated
`m6-alpha8-bootstrap` deployment from alpha.7 to alpha.10, then publish alpha.11 and use alpha.10
as the source of the first broker-coordinated upgrade acceptance.

## Checkpoint 1: Runtime Path Identity

- Base: published alpha.10 tag commit `40c932f0628dabef6b1a5aa65fb95954a078199e`.
- Treat a protected target runtime path and the running executable as identical when both resolve
  through filesystem symlinks to the same path and the running bytes match the journal digest.
- Keep failures closed for an unresolved target path, a different resolved file, or a digest
  mismatch.
- Add a focused regression test for a logical home path that is a symlink to a physical home path.
- Freeze, run focused and full acceptance, and obtain an independent read-only review.

## Checkpoint 2: Peer-First Activation Qualification

- For every peer-first activation, require the target local runtime, protected configuration,
  service identity, and a newer target-bound execution-readiness epoch. Do not require the upgraded
  peer to reconnect to the old broker before the broker-last activation step.
- Keep the controller completion gate strict after the target broker starts: connection, target
  version, lifecycle synchronization, target-bound readiness, and committed peer status remain
  required before the participant qualifies and the drain can end.
- Update the operator contract and add table-driven tests covering disconnected local activation
  while retaining the existing coordinated reconnect and timeout/intervention tests.
- Freeze, run focused and full acceptance, and obtain an independent read-only review.

## Release And E2E

After both checkpoints are accepted, prepare the alpha.11 source and manifest checkpoints using the
existing immutable release workflow. Finish the alpha.7 to alpha.10 isolated bootstrap without
rolling back the already authorized peer transaction, prove alpha.10 worker execution, then run the
alpha.10 to alpha.11 broker-coordinated transaction and its post-upgrade worker/status acceptance.
