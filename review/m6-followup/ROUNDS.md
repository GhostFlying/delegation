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

### Review Round 2

- Frozen commit: `cdf7c9b2b9631198bb334e3d2a2f5dea760d84f3`
- Frozen tree: `c0a1674053c51ef1ecaef7c9c5ec8c3471c00765`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Confirmed dispositions: the exact alpha.4 config/database bootstrap path and target-bound peer
  qualification findings from round 1 are resolved and covered by regression tests
- Residual risks: native systemd, launchd, and Windows Scheduled Task behavior remains
  fixture/fake tested; deliberate protected-state corruption and hostile same-UID protocol forgery
  remain outside ordinary supported workflows
- Disposition: accepted for serial fast-forward into the integration branch

Executable acceptance at the accepted frozen revision:

- `go test -count=1 ./internal/config ./internal/store ./internal/localupgrade ./internal/cli`
  and the corresponding focused race run passed.
- The real alpha.4 config/database migration, configuration switch/resume/rollback, and peer/broker
  qualification regression tests passed repeated runs.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` and
  `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS arm64, and Windows amd64 compile validation passed with `CGO_ENABLED=0`.
- Integration-tagged Codex peer E2E compilation, the POSIX plugin smoke test, and
  `git diff --check` passed.
- The independent reviewer reran focused packages and race tests, the full Linux suite, vet, all
  three platform compile checks, integration-tagged E2E compilation, and a credential-pattern scan.
- Validation used `go1.26.5 linux/amd64`; the detached review worktree remained clean.

Post-integration acceptance after the exact fast-forward and evidence commit:

- Accepted review range:
  `9d4b444292a32e5a2b9ef83a16e9f489e79da582..cdf7c9b2b9631198bb334e3d2a2f5dea760d84f3`
- Integration method: fast-forward; no rebase, conflict, or semantic adjustment occurred.
- Integration commit before this evidence commit: `299b8355bd591693ebcc2b4d64a3bb74ad66fb4e`
- Integration tree before this evidence commit: `ecad55322c298c514cc62e4e8dbce37d364efdc6`
- Focused config, store, local-upgrade, local-bridge, user-service, and CLI tests and their race
  variants passed.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` and
  `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS arm64, and Windows amd64 compile validation passed with `CGO_ENABLED=0`.
- Integration-tagged Codex peer E2E compilation, the POSIX plugin smoke test, and
  `git diff --check` passed.
- Validation used `go1.26.5 linux/amd64`; the integration worktree remained clean before this
  evidence-only update.

## Checkpoint 5: Broker-Coordinated Upgrade

- Base commit: `be9375ffc4210b89788c381ad0e9e857eedfd552`
- Base tree: `c0ed91e1868e11ecfcd958bef0518ec6669fe5e0`
- Accepted review range:
  `be9375ffc4210b89788c381ad0e9e857eedfd552..f1d77501f06a1fe1c5da9bd8ee45e341074ef3f8`

### Review Round 1

- Frozen commit: `f0ae6d47657df36c50f22f8215d8db101796393e`
- Frozen tree: `276b81fefef0ccc4121321b1773a4f2e86fbed26`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `FINDINGS`
- Finding: a peer or broker could durably complete PREPARE and bind its local journal, lose the
  response before the controller persisted the returned local transaction ID, and then be skipped
  by pre-COMMIT cancellation. The stranded transaction blocked later upgrades and dispatch could
  resume before all prepared work was canceled.
- Disposition: actionable response-loss and operability defect. The controller now reserves and
  persists every peer and broker local transaction ID before PREPARE, carries that exact identity
  in the protocol request, and requires local preparation to create or resume the reserved ID with
  its controller binding.

Round-1 reviewer verification passed the focused protocol, connector, broker, coordinated-upgrade,
local-bridge, and CLI tests and `git diff --check`; the detached worktree remained clean. An initial
reviewer attempt produced no verdict and was replaced on the same unchanged frozen revision, so it
did not consume a separate review round.

### Review Round 2

- Frozen commit: `021fcf90918e571f6352b77d92deda92111fffce`
- Frozen tree: `dd7d8103f058fc0ae50c11d7608d7606c8e1f649`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `FINDINGS`
- Confirmed disposition: the Round-1 durable-PREPARE response-loss path was fixed.
- Finding: on a later upgrade, a previous terminal local journal could mask that a newly reserved
  transaction had never been created. Cancellation returned an identity conflict instead of
  authenticated NotFound, leaving the controller in `canceling` and the broker-wide mutation drain
  active indefinitely.
- Disposition: actionable ordinary retry-path operability defect. A mismatched terminal journal now
  classifies the newly reserved transaction as absent, while a mismatched active journal remains a
  fail-closed conflict. Regression tests cover both cases.

Round-2 reviewer verification passed the six focused packages, response-loss tests under the race
detector, and `git diff --check`; the detached worktree remained clean.

### Review Round 3

- Frozen commit: `f1d77501f06a1fe1c5da9bd8ee45e341074ef3f8`
- Frozen tree: `9ee4e8fac8b62e82a8a463b9fab3b6f2c63e7b3a`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Confirmed dispositions: local transaction identities are durable before PREPARE; exact-ID
  create/resume and controller binding survive response loss; a prior terminal journal does not
  mask an absent reserved transaction; and conflicting active state still fails closed.
- Disposition: accepted for serial fast-forward into the integration branch. This was the third and
  final permitted automated review round.

Executable acceptance at the accepted frozen revision:

- The protocol, local-upgrade, CLI, connector, broker, and coordinated-upgrade focused tests passed.
- PREPARE response-loss and cancellation regression tests passed 20 consecutive runs under the
  race detector.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` and
  `go vet -tags=ts_omit_logtail ./...` passed.
- The coordinated-upgrade, local-bridge, broker, connector, CLI, local-upgrade, and protocol race
  suites passed.
- Linux amd64, macOS arm64, and Windows amd64 ordinary and `integration,live` compile-only
  validation passed with `CGO_ENABLED=0`.
- `GO=go ./tests/posix_plugin_test.sh`, `./tests/m6_support_contract_test.sh`,
  `git diff --check`, and the full-tree gofmt check passed.
- Validation used `go1.26.5 linux/amd64`; the accepted writer and detached review worktrees remained
  clean.

## Follow-up Checkpoint: Bounded TraeX Rollout Compatibility

- Base commit: `b0e16fed3f899c2a45996c944bdc2f8d8ce31f05`
- Accepted review range:
  `b0e16fed3f899c2a45996c944bdc2f8d8ce31f05..66df723e81d36a0f37f4145598ad01ac349abc6e`
- Review round: 1
- Frozen commit: `66df723e81d36a0f37f4145598ad01ac349abc6e`
- Frozen tree: `0775c121a18c59853e38795d2021d31e37e05b4e`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Disposition: accepted and committed directly on the integration branch

This checkpoint raises only the bounded pre-start scan budget to the existing 64 MiB raw-rollout
limit. It keeps pre-start content excluded from the captured result and digest while accepting the
approximately 1.41 MiB `session_meta` emitted by the supported TraeX 0.201.6 runtime.

Executable acceptance at the accepted frozen revision:

- The focused rollout-capture package tests passed, including the retained small-limit rejection
  case and a 2 MiB session-metadata regression case that returns only the exact requested turn.
- `go test -count=1 -tags=ts_omit_logtail -timeout=30m ./...` passed.
- `go vet -tags=ts_omit_logtail ./...` passed.
- The reviewed range passed `git diff --check` and the full-tree gofmt check; the detached review
  worktree remained clean.

## Follow-up Checkpoint: Readiness-aware Live E2E Fixtures

- Base commit: `66df723e81d36a0f37f4145598ad01ac349abc6e`
- Accepted review range:
  `66df723e81d36a0f37f4145598ad01ac349abc6e..79513871a3e0c5c37c427dcdd06d0608d13b5f99`
- Review round: 1
- Frozen commit: `79513871a3e0c5c37c427dcdd06d0608d13b5f99`
- Frozen tree: `0f18584b7fe92ed003a6f64453661d9cb113a778`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Disposition: accepted and committed directly on the integration branch

This checkpoint makes the result-package fixture enter through a production-equivalent,
digest-bound persisted readiness epoch and removes prohibited TraeX profile-loading arguments from
the live smoke path.

Executable acceptance at the accepted frozen revision:

- The independently reviewed artifact live E2E passed both dirty-workspace-and-commit and
  clean-commit-only result-package scenarios with Codex 0.144.1.
- The independently reviewed TraeX 0.201.6 plus warmpool live smoke passed a real worker turn,
  result packaging, app-server process replacement, and cold resume.
- Fresh result-package E2E passed on Linux amd64 in 2.02 seconds, macOS arm64 in 7.04 seconds, and
  Windows amd64 in 45.67 seconds using binaries built from the frozen tree.
- The reviewed range passed `git diff --check`; the detached review worktree remained clean.

## Follow-up Checkpoint: Protected TraeX Account Reuse

- Base commit: `9aee2bad99a89bcd924fe3b916627d1875c7303a`
- Accepted review range:
  `9aee2bad99a89bcd924fe3b916627d1875c7303a..5e040f61178c4e1aff98d1f35448a8b821faf520`

This checkpoint keeps the managed TraeX home isolated while copying only an explicitly configured,
validated host `auth.json` into the managed CLI home. Both the host source and managed copy remain
outside the worker tool filesystem profile, including access through workspace symlink aliases.
The auth path and exact bytes participate in readiness and upgrade digests; authentication failures
are persisted as `authentication_invalid`.

### Review Round 1

- Frozen commit: `2bd7e12aff43964b1027e1a3a0cbdc135ff6621b`
- Frozen tree: `c8c03d0aba0c96f3a1ef6db2d089c520dad76862`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `FINDINGS`
- Finding 1: the live process matcher compared a lexical home path with the canonical managed home,
  causing ordinary symlinked user homes to report that no TraeX app-server existed.
- Finding 2: the first TraeX app-server run created bounded runtime-owned state that managed-home
  validation rejected during process replacement and cold resume.
- Disposition: both findings were actionable supported-path operability defects. Process discovery
  now uses platform-specific exact argument inspection against the canonical managed CLI home.
  Managed-home validation accepts only the known TraeX-generated paths and clears that bounded
  state before each app-server start or reuse.

The reviewer reproduced both findings with TraeX 0.201.6 and warmpool 0.1.0. The reviewed range
passed `git diff --check` and Linux, macOS amd64, macOS arm64 compile checks; the detached worktree
remained clean.

### Review Round 2

- Frozen commit: `5e040f61178c4e1aff98d1f35448a8b821faf520`
- Frozen tree: `2cb13f6493a46437733b24c970d2349370881aa6`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Confirmed dispositions: canonical platform process discovery resolves symlinked-home matching,
  and bounded TraeX-generated state no longer prevents app-server replacement or cold resume.
- Residual risk: native macOS execution of the auth reuse and filesystem-denial path remains a
  release gate; cross-compilation and static inspection are not treated as native acceptance.
- Disposition: accepted at the exact frozen commit and tree. No security-model relaxation was
  required.

Executable acceptance at the accepted frozen revision:

- `go test -count=1 -buildvcs=false -tags=ts_omit_logtail -timeout=30m ./...` passed.
- `go vet -tags=ts_omit_logtail ./...` passed.
- Focused config, TraeX auth, path guard, managed-home, worker-host, readiness, local-upgrade, CLI,
  and repair suites passed under the race detector.
- Linux amd64, macOS amd64, macOS arm64, and Windows amd64 core plus `integration,live` compile-only
  validation passed.
- `GO=go ./tests/posix_plugin_test.sh`, `./tests/m6_support_contract_test.sh`, the full-tree gofmt
  check, and `git diff --check` passed.
- The real Linux TraeX 0.201.6 account smoke passed in 39.04 seconds. It covered a real worker turn,
  result publication and acknowledgement, direct and symlink-alias denial of the source and copy,
  token non-leakage, app-server process replacement, same-thread cold resume, and the follow-up
  result marker. No token value entered tracked files or test output.
- The round-2 reviewer reran the focused packages, full Linux suite, vet, live-tag compilation, and
  Darwin arm64 and Windows amd64 cross-compilation; the detached worktree remained clean.

## Follow-up Checkpoint: Native macOS Test Portability

- Base commit: `c4544d0443803423dca7bf29a63282498331867a`
- Accepted review range:
  `c4544d0443803423dca7bf29a63282498331867a..510ccd62d908b88b0115dda073821c851e466580`
- Review round: 1
- Frozen commit: `510ccd62d908b88b0115dda073821c851e466580`
- Frozen tree: `17876fa1f3bebd49ee9d5f287a7f7d165742c78d`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Disposition: accepted at the exact frozen commit and tree

Native macOS execution exposed two test-fixture assumptions rather than product defects. The
qualification assertion compared the lexical temporary path under `/tmp` with the production
canonical path under `/private/tmp`, and the CLI MCP fixture wrote a relative `os.Args[0]` into a
configuration that correctly requires absolute executable paths. The tests now compare the
canonical managed home and reuse the existing absolute test-binary helper. Production behavior and
security validation are unchanged.

Executable acceptance at the accepted frozen revision:

- `go test -count=1 ./internal/cli ./internal/workerhost` passed on Linux.
- The affected CLI, worker-host, and qualification tests passed 20 consecutive runs under the race
  detector on Linux.
- The independent reviewer reran the full Linux suite, full CLI and worker-host race suites,
  Darwin arm64 and Windows amd64 cross-compilation, and `git diff --check`; all passed.
- Kross workload `m6-traex-auth-iosdev-20260901-1922` used macOS 26.3 arm64, Xcode 26.3, and
  Apple Git 2.50.1. Locally cross-compiled test binaries were verified by SHA-256 after transfer.
  The complete `internal/config`, `internal/traexauth`, `internal/pathguard`,
  `internal/codexconfig`, `internal/workerhost`, `internal/workerreadiness`,
  `internal/localupgrade`, and `internal/cli` packages passed natively after the fixture fixes.
- The Kross workload and local transferred test artifacts were deleted after validation. No real
  TraeX credential was uploaded because the available workspace had no secret mount. Native macOS
  real-account TraeX execution therefore remains a release gate rather than an inferred pass.

## Follow-up Checkpoint: Reliable Terminal Readiness Publication

- Base commit: `8025bebda747acb5b0847a9c97587d67127ae0b9`
- Accepted review range:
  `8025bebda747acb5b0847a9c97587d67127ae0b9..fa178470a97aa6691ea3eee51319825dbf5506e5`
- Review round: 1
- Frozen commit: `fa178470a97aa6691ea3eee51319825dbf5506e5`
- Frozen tree: `6c28c60b8d3e8576e0fb6eb3245c350d8fdc3d28`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `CLEAN`
- Findings: none
- Disposition: accepted at the exact frozen commit and tree

This checkpoint resolves the combined milestone round-1 finding that terminal readiness could be
suppressed permanently after the connector's 128 supported in-flight RPC slots were occupied. A
terminal epoch is now deduplicated only after a validated matching broker acknowledgement. The
connector waits eventfully for capacity within the bounded publication context, concurrent
same-epoch callers share the in-flight result, and the readiness controller retries the latest
durable snapshot without requiring another readiness transition. Lost acknowledgements continue to
recover through the durable readiness snapshot in the reconnect hello.

Executable acceptance at the accepted frozen revision:

- The capacity-release, concurrent same-epoch, mismatched-acknowledgement, lost-acknowledgement, and
  controller publication-retry tests passed 50 consecutive runs under the race detector.
- The original pending-call capacity and reconnect recovery test passed together with the new
  capacity-release path for 20 consecutive race-detector runs.
- Full connector and worker-readiness race suites passed.
- `go test -count=1 -buildvcs=false -tags=ts_omit_logtail -timeout=30m ./...` and
  `go vet -tags=ts_omit_logtail ./...` passed.
- Linux amd64, macOS amd64, macOS arm64, and Windows amd64 compile validation passed, including the
  `integration,live` E2E package.
- `GO=go ./tests/posix_plugin_test.sh`, `./tests/m6_support_contract_test.sh`, the full-tree gofmt
  check, and `git diff --check` passed.
- The independent reviewer reran focused packages, full Linux tests, full affected race suites, 20
  focused race iterations, and macOS arm64 and Windows amd64 compile checks. The detached worktree
  remained clean.

## Combined Milestone Review

- Review base commit: `b406974120dcc95372a91c8d9297649fa665a431`

### Review Round 1

- Frozen commit: `8025bebda747acb5b0847a9c97587d67127ae0b9`
- Frozen tree: `41cf23f9a6c2669e7e2ede014b7345863dd1d6ce`
- Review checkout: clean detached worktree at the frozen commit and tree
- Independent review result: `FINDINGS`
- Finding: if all 128 supported connector RPC slots were occupied when readiness became terminal,
  the first publication returned `ErrBusy` after marking the epoch as published. Later same-epoch
  calls returned false success without sending, and the controller did not retry. The broker could
  therefore remain pending and non-dispatchable indefinitely while local durable state was ready.
- Disposition: actionable ordinary-concurrency operability defect. The reliable terminal readiness
  publication follow-up above adds acknowledgement-based deduplication, event-driven capacity
  retry, controller-owned durable snapshot retry, and focused regression coverage. The corrected
  combined tree will be frozen for milestone review round 2.

Round-1 validation otherwise passed the full Linux suite, race suites, vet, support contract, POSIX
plugin smoke, format and credential checks, and Linux, macOS, and Windows compile validation. The
reviewer reproduced the finding using the public 128-call capacity without modifying the frozen
worktree.
