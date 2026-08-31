# M6 Follow-up Review Rounds

This file records frozen checkpoint and milestone review evidence. Raw logs live outside the
repository. No checkpoint may start a fourth automated review round.

## Checkpoint 1: Runtime Acquisition And Release Trust

- Review round: 1
- Base commit: `b406974120dcc95372a91c8d9297649fa665a431`
- Frozen commit: `8d62abfe4964d69c49c28c9dfa6a351db603d2fd`
- Frozen tree: `c6ba9449e430367f2887120c72ce464717022a13`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Disposition: accepted and fast-forwarded into the integration branch

Executable acceptance at the frozen revision:

- `go test -count=1 -tags=ts_omit_logtail -timeout 20m ./...` passed.
- `go vet -tags=ts_omit_logtail ./...` passed.
- `GO=go ./tests/posix_plugin_test.sh` passed.
- `tests/windows_plugin_test.ps1` passed under Windows PowerShell 5.1 and PowerShell 7.5.2.
- The independently reviewed focused checks `go test ./internal/releaseverify
  ./cmd/releasepack` and `tests/posix_plugin_test.sh` passed.
- The reviewed range passed `git diff --check`; the detached worktree remained clean.

## Checkpoint 2: Execution Readiness And TraeX Repair

- Review round: 1
- Base commit: `b406974120dcc95372a91c8d9297649fa665a431`
- Frozen commit: `c0d151aca9f53d93759704da238ad0d436e9c0c1`
- Frozen tree: `672c2daed5b2a63ea52d7486c5e9dcccf5dc2e20`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Residual risks: none identified
- Disposition: accepted and cherry-picked serially after Checkpoint 1
- Integration commit: `18ab4af2509d344f6497a886549d2772643cdcf9`
- Integration tree before this evidence commit: `149ad970d6d6547b659f4e031ea89b0fa5ff64ec`

The integration cherry-pick merged the independently modified `README.md` automatically.
`git range-diff` reported the frozen and integrated Checkpoint 2 commits as equivalent, and both
patches had stable patch ID `af8ef9435872695ccff9e3e6ba2e4689b9b4fdf3`; no semantic
adjustment or conflict occurred.

Executable acceptance at the frozen revision:

- `go test -count=1 -race -tags=ts_omit_logtail ./internal/workerreadiness
  ./internal/localbridge ./internal/rootmcp ./internal/broker ./internal/connector ./internal/store
  ./internal/traexrepair ./internal/workerhost ./internal/cli` passed.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` passed.
- `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS arm64, and Windows amd64 compile validation passed with `CGO_ENABLED=0`.
- The terminal readiness and active-session notification race tests passed 50 consecutive runs
  under the race detector.

Independent review acceptance at the frozen revision:

- The reviewer inspected the complete frozen range, including durable retry epochs, dispatch
  gating, fresh-thread qualification, typed pre-receipt rejection, transactional repair and
  rollback, terminal intervention long-poll behavior, active-session-only notification, and the
  protected offline status fallback.
- The reviewer reran focused package tests and focused readiness/notification race tests; all
  passed.
- The reviewed range passed `git diff --check`; the detached worktree remained clean.

Post-integration acceptance:

- Focused protocol, readiness, local bridge, root MCP, broker, connector, store, repair, worker
  host, and CLI tests passed serially.
- The terminal readiness and active-session notification tests passed 50 consecutive runs under
  the race detector.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` and
  `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS arm64, and Windows amd64 compile validation passed with `CGO_ENABLED=0`.

## Checkpoint 3: Agent Current-State Projection

- Base commit: `23e3234203f8bd429e6e324f9ced0a9d3f333795`
- Base tree: `10c6ec8b8cdd0cdcbf99c41b178eb9247f89f12a`
- Accepted review range:
  `23e3234203f8bd429e6e324f9ced0a9d3f333795..c00ac422cbc8a89b5de9f39f79908c7f08022dbf`
- Integration method: fast-forward; no rebase, conflict, or semantic adjustment occurred
- Integration commit: `c00ac422cbc8a89b5de9f39f79908c7f08022dbf`
- Integration tree before this evidence commit: `4c646db4a624a969b38db05c38ad4f54376a9329`

### Review Round 1

- Frozen commit: `738b13d04983944bb13cd8465afbfb12a58a0215`
- Frozen tree: `a267378aa350c75ee4b8e8b457ad7f784afa71d3`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `FINDINGS`
- Finding 1: a valid 31- or 32-agent projected response could exceed the 16 KiB root MCP
  output limit, making the documented maximum page fail in an ordinary full-page list request.
- Finding 2: the local bridge version remained 3 after the breaking agent response schema change,
  allowing incompatible old and new processes to pass the version gate before payload decoding.
- Disposition: both findings were actionable compatibility and operability defects. The output limit
  was raised to 32 KiB with worst-case 32-agent coverage, and the local bridge version was raised
  to 4 with legacy request and response rejection coverage.

### Review Round 2

- Frozen commit: `c00ac422cbc8a89b5de9f39f79908c7f08022dbf`
- Frozen tree: `4c646db4a624a969b38db05c38ad4f54376a9329`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Confirmed dispositions: both round 1 findings were resolved and covered by regression tests
- Residual risks: deliberate protected-state corruption and hostile same-UID protocol forgery are
  outside ordinary supported workflows
- Disposition: accepted and fast-forwarded into the integration branch at the exact frozen commit
  and tree

Executable acceptance at the accepted frozen revision:

- `go test -count=1 ./internal/rootmcp ./internal/localbridge ./internal/protocol
  ./internal/broker ./internal/store` passed.
- The projection mapping, pagination, connection-generation churn, 32-agent output bound, local
  bridge compatibility, spawn/list durability, and exact wait-activity tests passed 50 consecutive
  runs under the race detector.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` passed.
- `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS arm64, and Windows amd64 compile validation passed with `CGO_ENABLED=0` and
  `go test -exec=true -run '^$' -tags=ts_omit_logtail ./...`.
- `go test -count=1 -tags=integration -run '^$' ./tests/codex_peer_e2e` passed.
- The reviewed range passed `git diff --check`; the detached worktree remained clean.

Post-integration acceptance after the exact fast-forward and evidence commit:

- Core root MCP, local bridge, protocol, broker, and store package tests passed.
- The focused projection, pagination, generation-churn, output-bound, compatibility, and exact
  wait-activity tests passed 50 consecutive runs under the race detector.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` and
  `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS arm64, and Windows amd64 compile validation passed with `CGO_ENABLED=0`.
- Integration-tagged Codex peer E2E compilation and `git diff --check` passed.
- Validation used `go1.26.5 linux/amd64`; the integration worktree remained clean.

## Checkpoint 4: Local Atomic Service Upgrade

- Base commit: `9d4b444292a32e5a2b9ef83a16e9f489e79da582`
- Base tree: `fc7366d25e167d2c8e755cf281bf74564d223817`

### Review Round 1

- Frozen commit: `f3d77215cf2a31ea4cbdea0bbb37a47dfa6ab199`
- Frozen tree: `91cb26a922f784336230d9120f6d7dc78fc0e9db`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `FINDINGS`
- Finding 1: the documented alpha.4 bootstrap path rejected the real schema-3 config and
  broker-19 or peer-15 database before the service could be upgraded.
- Finding 2: peer qualification could commit after local process start without requiring lifecycle
  synchronization and a new target-bound execution-readiness epoch.
- Disposition: both findings were actionable supported-path operability defects. The round-2
  revision adds an exact schema-3 TCP config conversion, exact broker 19-to-20 and peer 15-to-16
  shadow migrations, crash-consistent protected configuration switching, and identity-bound peer
  qualification that requires lifecycle sync plus a newer matching ready epoch.
- Residual risks: platform service-manager execution remains fixture/fake tested; deliberate
  protected-state corruption and hostile same-UID protocol forgery remain outside ordinary
  supported workflows.

Round-1 reviewer verification passed the focused packages, full Linux suite, vet, focused race,
Darwin and Windows cross-compilation, and `git diff --check`.
