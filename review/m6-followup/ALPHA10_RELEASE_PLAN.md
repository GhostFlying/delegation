# Alpha.10 Release Plan

## Objective

Publish `0.1.0-alpha.10` as the narrow Linux systemd upgrade-activator repair discovered during
the first canonical alpha.7 bootstrap. Alpha.10 becomes the first supported Linux source for a
real broker-coordinated upgrade; alpha.8 and alpha.9 must not be used to claim that acceptance.

## Source Boundary

- Base: accepted activator checkpoint `006b0119b282640cae8a674470a0225ca598e560`, tree
  `bead8710fb0fcfd63ebe78f9d7a15e0471f55a51`.
- The source commit updates the runtime, plugin, and tested CLI version to `0.1.0-alpha.10`, records
  the accepted checkpoint review, and adds this release plan.
- The source commit deliberately retains the published alpha.9 checksum manifest as historical
  input. It must not claim alpha.10 artifacts before deterministic builds exist.
- No protocol, schema, worker-profile, transport, or platform-support contract changes in this
  release. Windows TraeX remains unsupported.

## Immutable Release Boundary

1. Freeze and independently review source commit `S` with all behavior-bearing and build-affecting
   alpha.10 content while the tracked checksum manifest still names alpha.9.
2. Run the full Linux suite and race suite, vet, plugin and support contracts, and all six ordinary
   plus `integration,live` compile-only targets. Run the real systemd activator round trip.
3. Build all six deterministic unsigned alpha.10 archives twice from `S`, require byte identity,
   and verify both release directories.
4. Create direct child manifest commit `M` by changing only
   `plugins/delegation/release-artifacts.sha256` to the generated alpha.10 manifest.
5. Freeze and independently review the exact `S -> M` boundary. Expose only `M` through the
   protected-main pull request.
6. After protected-main CI passes and the pull request is rebase-merged, prove the merged source
   and manifest trees are equivalent to the accepted boundary, tag the merged manifest commit as
   `v0.1.0-alpha.10`, and dispatch the `Release` workflow from that exact tag ref.
7. Verify the immutable GitHub Release, six archives, checksum manifest, custom Sigstore
   provenance, cold launcher installation, and current-platform canonical acquisition.

## Bootstrap And Coordinated Upgrade Acceptance

- Use only the existing isolated M6 bootstrap deployment or fresh isolated homes, configs,
  databases, identities, ports, and native-service names. Never touch the long-lived Delegation
  services.
- Use the canonical alpha.10 CLI to recover the isolated alpha.7 deployment peer-first and
  broker-last with explicit local `--bootstrap`. Require local health after each activation and
  verify the resulting service definitions and binaries are exactly alpha.10.
- Publish a strictly newer canonical alpha.11 with no behavior-bearing change, then start the first
  real broker-coordinated transaction from the alpha.10 broker CLI without `--bootstrap`. Require
  the fixed participant set, PREPARE and ARMED acknowledgements, durable global COMMIT, peer-first
  activation, broker-last activation, reconnect, lifecycle synchronization, a target-bound
  readiness epoch, and resumed dispatch.
- Prove a real worker turn and result return before and after the coordinated upgrade, a bounded
  participant disconnect during activation, and `list_agents` projection from lifecycle rather
  than a stale spawn receipt.
- A pre-COMMIT failure cancels without switching services. After COMMIT the transaction remains
  forward-only; a peer that does not reconnect or qualify stays non-dispatchable with
  `intervention_required`.
