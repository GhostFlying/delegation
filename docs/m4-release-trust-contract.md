# M4 Release Trust Contract

This document fixes the release trust model for signed Delegation runtimes. It is an operational
contract for future releases; it does not claim that an existing release was retroactively signed.

## Trust Goals

A published runtime must have all of the following properties:

- the plugin pins the exact SHA-256 of every platform archive;
- macOS executables are Developer ID signed and accepted by Apple's notary service;
- Windows executables are Authenticode signed with an RFC 3161 timestamp;
- the published bytes are the same bytes that passed native signature verification;
- GitHub attestations bind the candidate build to its source commit and bind promotion to the
  immutable release tag; and
- no signing credential is available to pull-request or ordinary CI jobs.

The installer continues to enforce the plugin-bundled SHA-256 without requiring GitHub CLI,
Sigstore, or network access beyond downloading the selected archive. Attestations provide an
additional operator-verifiable provenance path; they do not weaken or replace checksum pinning.

## Why Release Builds Are Promoted

Notarization and timestamped Authenticode signatures are intentionally non-reproducible. A release
workflow therefore must not rebuild signed binaries after their checksums have been committed.
Delegation uses a two-stage candidate-and-promotion flow:

1. Candidate source commit `S` contains the final version and all build-affecting source. Its
   checksum file is not yet the candidate manifest.
2. The protected candidate workflow checks out `S`, builds on native runners, signs and verifies the
   macOS and Windows executables, packages all six archives, and generates the candidate manifest.
3. The workflow creates SLSA provenance for the six archives at source commit `S`, includes its
   Sigstore bundle, and uploads one immutable GitHub candidate artifact.
4. A normal commit `M` changes only `plugins/delegation/release-artifacts.sha256` to the reviewed
   candidate manifest. `M` must have `S` as its single parent.
5. Tag `v<VERSION>` points to `M`. The promotion workflow is dispatched with that exact tag selected
   as its workflow ref and is given the candidate run ID and artifact ID.
6. Promotion verifies the candidate workflow identity, source commit, artifact identity, manifest,
   native-signing evidence, and the exact `S -> M` one-file diff. It publishes the original candidate
   bytes without rebuilding them.
7. Promotion creates a custom attestation that binds the same archive digests to tag commit `M` and
   records the candidate workflow run and source commit `S`.

Any source, version, build flag, dependency, signing input, or packaging change invalidates the
candidate. Generate a new candidate rather than editing its payload or descriptor.

## Candidate Contents

The uploaded candidate contains exactly 15 regular files and no directories or links:

- six runtime archives for Linux, macOS, and Windows on amd64 and arm64;
- six canonical `evidence_<os>_<arch>.json` files;
- `release-artifacts.sha256`, covering exactly those six archives;
- `release-candidate.json`, a canonical machine-readable descriptor;
- and `candidate-provenance.sigstore.json`, the candidate SLSA provenance bundle.

The descriptor records schema version, repository, runtime version, source commit, workflow run ID,
candidate artifact name, a domain-separated payload digest, archive name, size, SHA-256, target
OS/architecture, and a bounded signing result. The payload digest covers all archives, evidence, and
the checksum manifest without attempting an impossible digest self-reference through the descriptor
or GitHub artifact ZIP. macOS evidence records successful strict code-signature verification,
hardened runtime, secure timestamp, and accepted notarization request identity. Windows evidence
records successful Authenticode chain, SHA-256 signing, and RFC 3161 timestamp verification. Linux
entries explicitly record that no platform code signature is expected.

GitHub assigns the immutable artifact ID and container digest only after upload. Promotion obtains
both from the Actions API, requires them to belong to the selected successful workflow run, relies
on the pinned download action's container-digest verification, and independently recomputes the
descriptor's payload digest. Neither an operator-supplied digest nor an artifact name alone is a
trust decision.

The descriptor never contains certificates' private material, passwords, API keys, access tokens,
temporary keychain paths, runner names, or local user paths.

## Native Signing Boundaries

The macOS and Windows candidate jobs use a protected `release-signing` GitHub environment restricted
to the default branch. Repository pull requests, ordinary CI, release-candidate mocks, Linux builds,
the assembly job, and promotion jobs cannot read its secrets. A non-secret mock in ordinary CI
exercises the same canonical descriptor and verifier without claiming that its synthetic evidence
is a signed release.

macOS jobs import a base64-encoded Developer ID Application certificate into an ephemeral keychain,
sign with hardened runtime and a secure timestamp, verify the signature, submit a temporary ZIP to
`notarytool`, and require an accepted result. Delegation remains a single-binary `tar.gz`; a raw
command-line executable cannot carry a stapled ticket, so notarization is verified before packaging
and Gatekeeper may retrieve the ticket online.

Windows jobs import a base64-encoded PFX only for the duration of the job, sign with SHA-256, request
an RFC 3161 timestamp from the configured HTTPS timestamp service, and require `signtool verify` to
validate both the Authenticode chain and timestamp before packaging.

Every signing step removes temporary credential files, imported certificates, and keychains in an
always-run cleanup path. Logs must not print secret values, decoded credential paths containing user
data, or full certificate exports.

## Promotion Validation

Promotion fails closed unless all of these conditions hold:

- the workflow runs from `refs/tags/v<VERSION>` and the tag resolves to commit `M`;
- the candidate run belongs to this repository, used the designated candidate workflow, completed
  successfully, and has head commit `S`;
- `M` has exactly one parent and that parent is `S`;
- the only `S -> M` path change is `plugins/delegation/release-artifacts.sha256`;
- the version in `M` matches the tag, descriptor, manifest, and archive names;
- the candidate artifact ID and GitHub container digest match the selected run, while its artifact
  name and recomputed payload digest match the descriptor;
- the candidate contains no unlisted files, directories, links, or duplicate archive targets;
- every archive digest matches the manifest and descriptor;
- every native target has the required successful verification evidence; and
- the candidate provenance verifies against the candidate workflow and source commit `S`.

The write-scoped publish job re-runs the same validation after its environment approval. It also
re-fetches the tag and rejects any movement before creating the immutable GitHub Release. It does
not receive platform signing secrets and never invokes the runtime build or packaging commands.
The release publishes every original candidate file plus the tag-bound promotion attestation bundle.

## Verification And Rotation

Release notes publish commands for verifying checksums, candidate provenance, and promotion
attestation with GitHub CLI. Verification pins this repository and the exact signer workflow rather
than trusting only the repository owner.

Certificate renewal or rotation requires a fresh candidate. Revoking a compromised credential does
not change already published bytes; operators must revoke or supersede the affected release and
publish a new version. GitHub environment approvals, protected tag rules, immutable releases, and
signing-provider audit logs remain deployment prerequisites rather than repository secrets.
