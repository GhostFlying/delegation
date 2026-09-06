# M6 Release Closure

This record closes the technical acceptance scope for M6 and its `0.1.0-alpha.8` follow-up.
Publication remains a separate release operation performed from the final reviewed commit.

## Supported Scope

| Host OS | Managed CLI | M6 disposition |
| --- | --- | --- |
| Linux | Codex | Supported |
| Linux | TraeX | Supported |
| macOS | Codex | Supported |
| macOS | TraeX | Supported |
| Windows 11 | Codex | Supported |
| Windows | TraeX | Unsupported |

Windows TraeX is a product-scope exclusion, not a successful qualification. Do not start another
Windows TraeX formal qualification for M6.

The original M6 platform qualification used only fresh independent deployments. The M6 follow-up
adds narrow in-place paths from alpha.4 and alpha.7. Both require the alpha.8 or newer CLI to run a
protected local bootstrap because neither source release implements the current upgrade RPC. An
exact alpha.4 schema-3 TCP service with broker schema 19 or peer schema 15 may move forward through
the bounded legacy migration. Alpha.7 already has config schema 4 but still migrates broker schema
19 or peer schema 15 to 20 or 16; a peer also migrates homogeneous retained profile-6 worker history
to profile 7 in the stopped shadow database. Arbitrary config/state reuse, transport migration,
downgrade, and operator-selected binaries remain unsupported.
After that bootstrap boundary, coordination-capable releases support a broker-owned, peer-first and
broker-last transaction over the existing authenticated connector sessions and protected local
management endpoint. The coordinator freezes only online compatible peers, persists COMMIT before
activation, resumes forward after response loss or restart, and leaves old-version offline peers
non-dispatchable until they are bootstrapped locally.

## Acceptance Disposition

- macOS alpha.7 passed fresh foreground and native-service qualification for separate Codex and
  TraeX domains.
- Linux isolated validation passed the broker-to-peer spawn, app-server thread start, exact worker
  MCP inventory, and turn-start boundaries for both Codex and TraeX. The deliberately unreachable
  test provider failed only after those connector boundaries.
- Windows 11 Codex passed connector recovery, required worker MCP initialization, managed turn,
  result delivery, and ready worker synchronization with the qualified Codex runtime.
- Windows TraeX remains unsupported. Its historical diagnostic receipt has
  `ENGINEERING_VERDICT=BLOCKED`; that immutable result is not overwritten or reinterpreted as
  `PASS`, and it does not block acceptance of the supported combinations.
- Windows TraeX diagnostic cleanup completed with zero owned processes and zero listeners across
  the retained diagnostic cycles.

The supported-platform technical acceptance verdict is `UNBLOCKED`, with no remaining M6
technical blocker.

The follow-up additionally passed real Linux and macOS TraeX account reuse with exact credential
file isolation, direct and symlink-alias denial, worker turn and result return, app-server
replacement, and cold resume. Runtime acquisition, execution readiness, current agent projection,
local atomic upgrade, coordinated upgrade, and retained worker-profile migration passed their
frozen checkpoint reviews. The complete follow-up tree passed the third and final combined
milestone review with no actionable finding.

## Release History Disposition

The release branch keeps the final behavior from accepted fixes while removing iterative delivery
and qualification noise:

- intermediate alpha.5 and alpha.6 version and checksum mappings are omitted;
- accepted Windows native process-helper fixes are represented by one final checkpoint;
- accepted Windows handoff-harness fixes are represented by one final checkpoint;
- Attempt-specific Windows qualification controllers and Attempt 7-24 runners are omitted;
- acquisition delivery bundles, Windows TraeX deep-diagnostic code, and legacy cleanup tooling are
  omitted; and
- provider-failure diagnostics discovered during the unsupported Windows TraeX investigation are
  deferred to a future milestone.

The follow-up release must retain the two-commit source/manifest boundary: the alpha.8 source commit
contains all build-affecting and plugin control-plane content, and its direct child changes only
`plugins/delegation/release-artifacts.sha256`.

## Publication Gate

Before publishing:

1. run the full Go test and vet suites with `ts_omit_logtail`;
2. compile the complete Linux, macOS, and Windows amd64/arm64 test matrix;
3. build the unsigned release twice and require byte-identical output;
4. verify both builds and the tracked checksum manifest;
5. freeze and independently review the complete M6 range and final tree;
6. fast-forward and push `main`;
7. create and push `v0.1.0-alpha.8` at the manifest commit; and
8. dispatch the release workflow and verify all six archives, `release-artifacts.sha256`, and
   `release-provenance.sigstore.json`.

The first real broker-coordinated release upgrade must use alpha.8 as the source and a later
canonical release as the target. Alpha.7 to alpha.8 is intentionally the local bootstrap boundary,
not a coordinated-upgrade claim.
