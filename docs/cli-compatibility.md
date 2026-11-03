# Current CLI Acceptance Baseline

The current acceptance inputs are Codex `0.163.0`, TraeX `0.213.1`, warmpool `0.1.0`, and Go
`1.26.5`. These pins identify the versions to test; a version observation or successful compilation
alone is not execution acceptance. Historical M6 review and release records retain the versions
that actually produced those results.

Current Codex-backed tests use `gpt-6.1-sol`; TraeX acceptance may continue with `gpt-5.6`.
Codex mock coverage exercises Responses Lite declarations in `additional_tools`, actual
`functions.exec` calls and runtime `ALL_TOOLS` discovery. Managed workers enable code mode through
the controlled runtime profile and disable native agents with `agents.enabled = false`; provider
JSON cannot override those capability controls. Live Codex smoke rejects a different model.

Ordinary CI installs exact Codex `0.163.0` for native managed-worker tests and the plugin contract.
TraeX real-account acceptance runs separately because it requires an explicitly provided protected
account file. Do not copy account credentials into CI configuration or evidence.

## Managed Worker Profile

Managed worker profile 8 enables the Codex code-mode tool surface required by code-mode-only
models such as `gpt-6.1-sol`. Native Codex agents remain disabled with the runtime-controlled
`agents.enabled=false` setting and the existing multi-agent feature flags. The Delegation worker
MCP remains available. Provider JSON cannot override these controls. TraeX retains its own tool
surface and model selection; the Codex-only controls are not passed to TraeX.

The common profile version also applies to TraeX history. The stopped-shadow upgrade accepts
homogeneous inactive histories from profiles 5, 6, or 7 and rewrites them to profile 8. Active work,
unfinished result publication, mixed generations, and foreign authority remain upgrade blockers.
This does not change filesystem permissions or credential-file protections. Historical M6 records
continue to describe the profile and runtime that produced their evidence.

## Executable Acceptance

For Linux and macOS, build the exact checkpoint and run the Codex mock integration tests with the
pinned executable:

```sh
codex --version
go build -tags=ts_omit_logtail -trimpath -buildvcs=false -o <test-binary> ./cmd/delegation
DELEGATION_E2E_BINARY=<test-binary> CODEX_BINARY=<codex-executable> \
  go test -count=1 -v -timeout 20m -tags=ts_omit_logtail,integration \
  ./tests/codex_peer_e2e
```

On supported TraeX platforms, run the existing isolated real-account smoke with exact TraeX and
warmpool executables and an existing current-user-only account file:

```sh
traecli --version
warmpool --version
DELEGATION_E2E_BINARY=<test-binary> TRAE_X_BINARY=<traex-executable> \
  WARMPOOL_BINARY=<warmpool-executable> TRAE_AUTH_FILE=<protected-account-file> \
  go test -count=1 -v -timeout 15m -tags=ts_omit_logtail,integration,live \
  -run '^TestManagedWorkerTraeXWarmpoolLiveSmoke$' ./tests/codex_peer_e2e
```

Record the command, actual tool versions, exact source commit/tree, platform and environment class,
exit status, and whether the named test executed. A passing Go command that skipped its acceptance
test is not a passing acceptance result.

## Windows Environment Boundary

No logged-in Windows 11 machine is currently available for this acceptance campaign. Tests requiring
that machine are `SKIP`, with that reason retained in the result. This does not remove Windows
Codex from the supported product scope or disable the existing Windows CI jobs.

A fresh Windows build container can execute compatible native Go tests, PowerShell launcher and
process tests, qualification-handoff fixtures, and Codex mock integration tests. Record those as
Windows-container results, including the actual Windows Server and PowerShell versions. They do
not establish Windows 11 interactive-user Task Scheduler or real-machine qualification.

Keep real-machine-only Task Scheduler and logged-in-user qualification as separate `SKIP` rows.
Windows TraeX remains unsupported; its exclusion is not a passing or skipped supported-path test.

See the [2026-11-04 real Sol execution record](../review/codex-sol-live-acceptance/ACCEPTANCE.md)
for the successful real-provider tool/result probe. That isolated probe used the configured
provider's case-sensitive wire ID `GPT-6.1-Sol`; the lowercase ID was not an alias for it. This is
the same requested model family, with no fallback to an older model. Repository mock inputs and
the named Go live-smoke model assertion remain `gpt-6.1-sol`; that named live smoke was not rerun
under a different ID. The successful probe does not establish a published canonical upgrade.

The [2026-11-01 Sol execution record](../review/codex-sol-acceptance/ACCEPTANCE.md) and
[2026-10-31 execution record](../review/cli-baseline-refresh/ACCEPTANCE.md) retain their original
results and source boundaries. Historical M6 evidence remains unchanged.
