# Current CLI Acceptance Baseline

The current acceptance inputs are Codex `0.163.0`, TraeX `0.213.1`, warmpool `0.1.0`, and Go
`1.26.5`. These pins identify the versions to test; a version observation or successful compilation
alone is not execution acceptance. Historical M6 review and release records retain the versions
that actually produced those results.

Ordinary CI installs exact Codex `0.163.0` for native managed-worker tests and the plugin contract.
TraeX real-account acceptance runs separately because it requires an explicitly provided protected
account file. Do not copy account credentials into CI configuration or evidence.

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

See the [2026-10-31 execution record](../review/cli-baseline-refresh/ACCEPTANCE.md) for actual
results and remaining gaps. Historical M6 evidence remains unchanged.
