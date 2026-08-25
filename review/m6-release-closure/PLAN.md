# M6 Release Closure Plan

## Objective

Publish `0.1.0-alpha.7` from a compact, reviewable M6 history while preserving the accepted runtime
tree and deterministic release artifacts.

## Base

- Base tag: `v0.1.0-alpha.4`
- Original M6 integration head: `b0635b337e120428c36209a2a3f4cfe7b7813c73`
- Accepted alpha.7 source: `5161316956cf6a6f7a8e4f0f2531a045a3f04404`
- Accepted alpha.7 manifest: `605a44357bb417c559cfc6b8b11ff59016e342f0`
- Backup refs:
  - `refs/backup/m6-pre-squash-main`
  - `refs/backup/m6-qualification-operation`

## Commit Structure

1. Embedded Tailscale configuration, protected enrollment keys, and persisted runtime configuration.
2. Tailscale path authority and safe runtime transport.
3. Fresh setup, authority preflight, token authentication, and safe status metadata.
4. Deterministic attributed runtime archives.
5. M6 operator documentation.
6. Windows native qualification process helper, including all accepted follow-up fixes.
7. Managed-worker network access.
8. macOS LaunchAgent startup acceptance.
9. Native-service Tailscale readiness.
10. Alpha.7 source version.
11. Alpha.7 release manifest.
12. Final Windows handoff validation harness, including all accepted follow-up fixes.
13. Review-process update removing the former Claude-review requirement.
14. M6 support disposition and release-closure record.

Intermediate alpha.5 and alpha.6 version or manifest commits are omitted. Attempt-specific Windows
qualification controllers, Attempt 7-24 runners, acquisition delivery bundles, deep TraeX
diagnostics, and legacy cleanup tooling are not release content. Their historical evidence remains
outside the release history.

## Support Disposition

- Linux: Codex and TraeX supported.
- macOS: Codex and TraeX supported.
- Windows 11: Codex supported.
- Windows and TraeX: unsupported and waived for M6.
- M6 supports fresh independent deployments only. Migration, in-place upgrade, and rollback remain
  unsupported.
- Historical Windows TraeX `BLOCKED` evidence remains immutable and is not reinterpreted as `PASS`.

## Acceptance

1. Compare the rewritten alpha.7 source tree against the accepted source tree, excluding only
   intentionally omitted intermediate release mappings and newly added closure documentation.
2. Run focused documentation and support-matrix consistency checks.
3. Run `gofmt` verification, `go test -tags=ts_omit_logtail -timeout 20m ./...`, and
   `go vet -tags=ts_omit_logtail ./...`.
4. Cross-compile tests for Linux, macOS, and Windows on amd64 and arm64.
5. Build release artifacts twice and require recursive byte identity.
6. Run `releasepack verify-release`, compare the generated manifest with the tracked manifest, and
   run strict SHA-256 verification.
7. Freeze the exact milestone commit and tree in a detached clean worktree.
8. Obtain one independent read-only Codex review of the full M6 range and final tree.
9. Fast-forward `main`, push it, create and push `v0.1.0-alpha.7`, dispatch the release workflow,
   and verify the six archives plus checksum manifest.
