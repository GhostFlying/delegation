# M6 Follow-up Release Plan

## Objective

Publish the accepted M6 Follow-up implementation as `0.1.0-alpha.8` without rewriting the already
published alpha.7 version-to-artifact mapping. Alpha.8 is the first release that implements the
local and broker-coordinated upgrade protocol.

## Compatibility Boundary

- Installed alpha.4 and alpha.7 services do not implement the current local management or upgrade
  protocol. Use the alpha.8 CLI to perform an explicit local `--bootstrap` upgrade on every idle
  peer first and the broker last.
- Alpha.4 bootstrap additionally performs the bounded schema-3-to-4 config migration.
- Both alpha.4 and alpha.7 migrate broker schema 19 or peer schema 15 to 20 or 16. Alpha.7 peers
  also migrate homogeneous retained profile-6 worker history to profile 7 in the stopped shadow
  database.
- Once all online participants run alpha.8, later strictly newer canonical releases may use the
  broker-coordinated peer-first/broker-last path. A real coordinated-upgrade release acceptance
  therefore requires a later target release, expected to be alpha.9.

## Immutable Release Boundary

1. Create a source commit `S` containing the accepted implementation, version `0.1.0-alpha.8`,
   plugin metadata, operator documentation, and all build-affecting files. Leave the tracked
   alpha.7 checksum manifest unchanged in `S`; it is historical input, not an alpha.8 claim.
2. Freeze and independently review `S` as a new release-preparation checkpoint. Run the full Linux
   suite, race and vet checks, support/plugin checks, and Linux/macOS/Windows compile matrices.
3. Build all six deterministic unsigned alpha archives twice from `S`, require byte identity, and
   verify their exact two-file layouts and attribution notice.
4. Create a direct child manifest commit `M` that changes only
   `plugins/delegation/release-artifacts.sha256` to the freshly generated alpha.8 manifest.
5. Freeze and independently review the `S -> M` one-file boundary, then fast-forward and push
   `main` to `M`. Do not expose `S` alone as the installable marketplace head.
6. Create immutable tag `v0.1.0-alpha.8` at `M` and dispatch the current `Release` workflow from
   that exact tag. The workflow rebuilds and verifies all six archives against the manifest before
   publishing them.
7. Verify the published release contains the six archives, checksum manifest, and
   `release-provenance.sigstore.json`; verify the custom GitHub Actions/Sigstore attestation and
   ensure the canonical release verifier accepts the current-platform artifact.

## Acceptance And Stop Conditions

- Keep the optional native-signing candidate-and-promotion workflow disabled for this unsigned
  pre-1.0 alpha unless its protected environments and repository variable are deliberately enabled
  as a separate release-policy change.
- Do not tag or publish before the source checkpoint is clean and accepted.
- Do not claim coordinated cross-version E2E from alpha.7 to alpha.8; that boundary is local
  bootstrap. Use alpha.8 to alpha.9 for the first real coordinated-upgrade release E2E.
- Stop on missing GitHub-release environment approval, tag protection, immutable release support,
  manifest identity, or provenance. The disabled native-signing candidate flow adds its own
  release-signing and candidate-identity gates only if it is deliberately enabled later. Do not
  weaken the release trust policy.
