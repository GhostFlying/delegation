# Alpha.9 Release Plan

## Objective

Publish `0.1.0-alpha.9` as the narrow POSIX managed-runtime permission follow-up to canonical
alpha.8. Alpha.9 is also the first strictly newer canonical target used to validate the
broker-coordinated, peer-first and broker-last upgrade protocol implemented by alpha.8.

## Source Boundary

- Base: accepted permission checkpoint `812220b876b61f12cee46c5558b2f82990be1be5`, tree
  `b80ba299ea040fa685a2053334194ed69373682c`.
- The source commit updates the runtime, plugin, and tested CLI version to `0.1.0-alpha.9`, records
  the accepted checkpoint review, and documents the POSIX private-directory operator contract.
- The source commit deliberately retains the published alpha.8 checksum manifest as historical
  input. It must not claim alpha.9 artifacts before deterministic builds exist.
- No protocol, schema, worker-profile, native-service, or platform-support contract changes in this
  release. Windows TraeX remains unsupported.

## Immutable Release Boundary

1. Freeze and independently review source commit `S` with all behavior-bearing and build-affecting
   alpha.9 content while the tracked checksum manifest still names alpha.8.
2. Run the full Linux suite and race suite, vet, plugin and support contracts, and all six ordinary
   plus `integration,live` compile-only targets.
3. Build all six deterministic unsigned alpha.9 archives twice from `S`, require byte identity, and
   verify both release directories.
4. Create direct child manifest commit `M` by changing only
   `plugins/delegation/release-artifacts.sha256` to the generated alpha.9 manifest.
5. Freeze and independently review the exact `S -> M` boundary. Expose only `M` through the
   protected-main pull request.
6. After protected-main CI passes and the pull request is rebase-merged, prove the merged source and
   manifest trees are equivalent to the accepted boundary, tag the merged manifest commit as
   `v0.1.0-alpha.9`, and dispatch the `Release` workflow from that exact tag ref.
7. Verify the immutable GitHub Release, six archives, checksum manifest, custom Sigstore
   provenance, cold launcher installation, and current-platform canonical acquisition.

## Coordinated Upgrade Acceptance

- Use only fresh isolated homes, configs, databases, identities, ports, and native-service names.
  Do not stop, replace, migrate, or reuse the long-lived Delegation network.
- Establish an alpha.8 broker and at least one alpha.8 peer with a private `bin` root. A canonical
  alpha.8 launcher-created broader directory must fail closed under the alpha.9 installer and may
  be secured only after explicit operator inspection; the installer never repairs it.
- Start the upgrade from the alpha.8 broker CLI without `--bootstrap`. Require a fixed participant
  set, PREPARE and ARMED acknowledgements, durable global COMMIT, peer-first activation, broker-last
  activation, reconnect, lifecycle synchronization, a newer target-bound readiness epoch, and
  resumed dispatch.
- Prove a real worker turn and result return before and after the upgrade, a bounded participant
  disconnect during activation, and `list_agents` projection from lifecycle rather than a stale
  spawn receipt.
- A pre-COMMIT failure cancels without switching services. After COMMIT the transaction remains
  forward-only; any peer that does not reconnect or qualify is retained as non-dispatchable with
  `intervention_required`.
