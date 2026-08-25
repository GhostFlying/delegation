# M6 Release Closure

This record closes the technical acceptance scope for `0.1.0-alpha.7`. Publication remains a
separate release operation performed from the final reviewed commit.

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

M6 supports only fresh independent deployments. Migration, in-place upgrade, downgrade, rollback,
config conversion, state reuse, and native-service replacement remain unsupported.

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

The final release must retain the two-commit source/manifest boundary: the alpha.7 source commit
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
7. create and push `v0.1.0-alpha.7` at the manifest commit; and
8. dispatch the `Release` workflow and verify all six archives plus
   `release-artifacts.sha256`.
