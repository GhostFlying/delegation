# Current CLI Acceptance: 2026-10-31

The current baseline is Codex **0.163.0**, TraeX **0.213.1**, warmpool **0.1.0**, and Go **1.26.5**.
CI now installs the exact Codex baseline. These results refresh compatibility evidence; they do not
publish a new Delegation runtime or replace historical M6 release records.

## Frozen Source

- Baseline checkpoint: `893d53966e09092c848ed1aa456170c2e499b0c9`.
- Baseline tree: `1aaa8c4dbd5e9950f1c2570e0c0a2b59f7a8d66b`.
- Runtime/default-suite checkpoint: `cc2ad7d0ac1f8deb05c52f1cf56815147f35f7b9`.
- Runtime/default-suite tree: `6a6bf689b9179edd08a96f256bfce76c3f2c5727`.

The earlier checkpoint includes the stale-idle test's bounded startup wait and cleanup ordering fix.
The baseline checkpoint changes CI, documentation, and integration-tagged test fixtures only.
Production Go sources, default-tagged tests, and launcher/service scripts are unchanged between
these checkpoints. Default-suite and native service results below were obtained on the earlier
checkpoint and reused after an executable source-equivalence check. Both macOS runtime builds also
produced the same SHA-256:
`869f379450726034361c1951ea1db9a14989c4cbe1d32221b51d055b5df8bc59`.
The final evidence-only documentation change does not alter these executable inputs.

## Executed Results

All native lanes used Go 1.26.5. macOS and Windows used fresh, isolated build environments; their
temporary environments were deleted and absence was verified after testing. No account credentials
were transferred to either remote environment.

| Environment | Executed check | Result |
| --- | --- | --- |
| Linux amd64 | `go test -count=1 -timeout 20m ./...`; `go vet ./...` | PASS |
| Linux amd64 | `sh tests/m6_support_contract_test.sh`; `sh tests/posix_plugin_test.sh` | PASS |
| Linux amd64, Codex 0.163.0 | Complete `tests/codex_peer_e2e` suite with `-count=1 -v -timeout 20m -tags=ts_omit_logtail,integration` | PASS |
| Linux amd64, Codex 0.163.0 | Isolated plugin install, cache invalidation smoke, and all three pinned plugin validators | PASS |
| Linux amd64, TraeX 0.213.1 / warmpool 0.1.0 | `TestManagedWorkerTraeXWarmpoolLiveSmoke` with `integration,live` tags and an existing protected account file | PASS |
| macOS 26.3 (25D125), arm64, Codex 0.163.0 | `TestManagedWorkerReturnsResultPackageCrossPlatform` and `TestManagedWorkerUsesEnvOnlyProviderAndColdResume` | PASS |
| macOS 26.3, arm64 | Complete default Go suite, `go vet ./...`, and POSIX launcher tests | PASS |
| macOS 26.3, arm64 | `TestDarwinLaunchAgentRoundTrip` with native integration enabled in an available GUI user domain | PASS |
| Windows Server 2019 container, NT 10.0.17763, amd64 | `go test -p=1 -tags=ts_omit_logtail -timeout 20m ./...`; `go vet -tags=ts_omit_logtail ./...` | PASS |
| Same Windows container, Codex 0.163.0 | `TestManagedWorkerReturnsResultPackageCrossPlatform` (dirty/commit and clean-commit cases), `TestManagedWorkerUsesEnvOnlyProviderAndColdResume` | PASS |
| Same Windows container | Stale-idle regression test repeated 20 times | PASS |
| Same Windows container, PowerShell 7.6.9 | Launcher, installer, native process helpers, and qualification-handoff fixtures | PASS |
| Same Windows container, PowerShell 5.1.17763.1490 | Native process helpers and qualification-handoff fixtures | PASS |
| Linux, macOS, Windows; amd64 and arm64 | Integration/live-tagged package compilation for all six targets | PASS (compile only) |

The Linux Codex suite includes three-peer Git synchronization, root MCP, concurrent dispatch,
recovery, result packages, and cold resume. The macOS/Windows Codex lanes execute the two named
cross-platform scenarios; they do not execute the Linux-only topology scenario. Codex provider
responses are mocked in these tests; this is actual CLI execution, not live model-provider
acceptance. The Linux TraeX smoke executes a real account-backed turn, result handling, credential
access restrictions, application-server replacement, and cold resume.

The compile-only command was
`CGO_ENABLED=0 GOOS=<os> GOARCH=<arch> go test -tags=ts_omit_logtail,integration,live -run '^$' -exec /bin/true ./tests/codex_peer_e2e`.
It does not establish native runtime acceptance for an unexecuted architecture.

## Explicit Gaps

| Check | Status | Reason |
| --- | --- | --- |
| Windows 11 logged-in-user qualification | SKIP | No logged-in Windows 11 machine is available. |
| Real Windows Task Scheduler round trip and runtime readiness | SKIP | Requires that supported interactive machine; a Windows Server build container is insufficient. Both opt-in tests were read back as skipped. |
| Current macOS TraeX real-account smoke | SKIP | No account credentials were provisioned in the isolated macOS environment. Linux TraeX success does not cover this row. |

Windows TraeX is unsupported and excluded from the supported-path acceptance matrix. No support
scope or Windows CI job was removed. The subprocess crash-helper test's intentional harness skip
does not stand in for a named acceptance test.

## Resolved Initial Failures

- The old topology mock model no longer supplied tool-search metadata to the current Codex CLI.
  The mock now uses catalogued Responses model `gpt-5.5`; the complete Linux suite passed.
- The Windows mock selected `shell_command` by operating system while the current CLI offered
  `exec_command`. It now selects a shell tool from the actual request, retaining tool-exposure
  assertions. Both Windows Codex scenarios passed on the frozen baseline.
- The first macOS default-suite run placed credential fixtures under the build environment's
  default temporary directory, which violated the existing credential-source restriction. A fresh
  mode-0700 temporary directory beneath the isolated home corrected the invocation; the full suite
  passed without weakening the restriction or changing source.
- The first Windows cold-resume network probe used PowerShell 5.1 and lacked its network marker.
  Adding the already installed PowerShell 7 to the test command's PATH matched CI; the complete
  scenario passed. This is not a PowerShell 5.1 cold-resume acceptance claim.
- The first local plugin cache smoke ran before its validator directory was prepared. After
  preparing the exact CI-pinned validator source, the cache smoke and all three validators passed.

Raw command output and superseded failures are retained outside tracked files. No release version,
release checksum mapping, credential boundary, or historical M6 evidence was changed.
