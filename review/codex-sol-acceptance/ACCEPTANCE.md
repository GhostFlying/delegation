# Codex Sol Acceptance: 2026-11-01

Current Codex acceptance uses **gpt-6.1-sol** with Codex **0.163.0** and Go **1.26.5**.
TraeX may continue using **gpt-5.6**; its model configuration was not changed. TraeX CLI remains
**0.213.1**, with warmpool **0.1.0**. This record supersedes the current Codex model policy and
preserves the [October 31 execution record](../cli-baseline-refresh/ACCEPTANCE.md) as historical
evidence of its actual model and source.

## Frozen Implementation

- Production profile checkpoint: `31c1ac3df794cb8782919d491f40543a73fe0740`.
- Production profile tree: `e088f54c503a53a02a693b63df501237ef7c3f16`.
- Sol profile and model fixtures: `b509b38b4ccb40328621d67d9f831269ddf66203`.
- Sol profile and fixture tree: `cead724d3ded72d3986f86336370e782fda45758`.

- Result-publication ordering checkpoint: `49291b284e7aeedd9979f0905efccddc3eb8db42`.
- Result-publication ordering tree: `06687fccae48c45ab4d42473a06cb337f0c47abb`.
- Alpha.12 candidate source: `f7108d6245a2168ad3a363e278636daeb969e752`.
- Alpha.12 candidate tree: `c64b5f21118f6f9e4519b4aa063c821b57178611`.

The two Sol checkpoints received independent, read-only round-1 review of their complete frozen ranges in
clean detached worktrees. Both reviews were clean. The fixture dependency rebase was conflict-free;
its patch comparison showed only documentation blob-index changes. Final evidence edits do not
change executable source or test fixtures.

Profile 8 enables Codex code mode and explicitly disables native agents with `agents.enabled=false`,
in addition to the existing multi-agent, fanout and plugin restrictions. The worker MCP remains
limited to its two mailbox tools. TraeX receives neither new Codex-only setting. Stopped-shadow
migration accepts inactive homogeneous histories from profiles 5, 6 and 7; active work, unfinished
result publication, mixed profiles and foreign authority remain blockers.

## Result-Publication Ordering

The faster CI execution exposed a normal race: a worker could publish before the broker committed
its successful spawn acknowledgement. The old pending-receipt classification returned permanent
Forbidden and stopped the connector. A deterministic broker/connector interleaving reproduced the
old failure before the spawn reply could return. The fix keeps matching pending publication
retryable and inserts nothing until the receipt is started and all original finalizing authority
checks pass. Failed, missing and foreign receipts remain rejected. Independent round-1 review was
clean; focused race repeated 20 times, full Go/vet and the complete Sol mock suite passed on this
checkpoint.

## Executed Checks

The macOS/Windows build-environment, TraeX live and compile-only results below refer to the Sol
profile/fixture checkpoint. Linux full Go/vet and complete mock integration were also rerun on the
result-publication checkpoint. These source boundaries are intentional; compilation and earlier
native execution are not claimed as later combined-tree execution.

| Environment | Check | Result |
| --- | --- | --- |
| Linux amd64 | Complete default Go suite and `go vet ./...` on the result-publication checkpoint | PASS |
| Linux amd64 | Focused production-profile, provider-control and migration race suites | PASS |
| Linux amd64, Codex 0.163.0 | Complete Codex mock integration suite with `gpt-6.1-sol` and the production profile, without diagnostic overlays | PASS |
| Linux amd64, alpha.12 candidate | Support contract and POSIX plugin/installer smoke | PASS |
| Linux amd64, TraeX 0.213.1 / warmpool 0.1.0 | Existing isolated real-account smoke, including source/managed credential-file protections, app-server replacement and cold resume | PASS |
| macOS 26.3 arm64, fresh isolated build environment | Supported Codex mock cases and complete Go/vet | PASS |
| Windows Server 2019 amd64, isolated build container | Supported Codex mock cases and complete serialized Go/vet | PASS |
| Linux/macOS/Windows, amd64/arm64 | Integration/live-tagged package compilation for all six targets | PASS, compile only |

The Linux mock suite executed result-package dirty/commit and clean-commit scenarios, environment-only
provider plus cold resume, direct/thin/full Git delegation, managed root MCP, concurrent dispatch
and recovery. Tools execute through the actual CLI code-mode runtime and consume actual shell/MCP
outputs. Inventory assertions prohibit native/root collaboration and retain exact worker-mailbox
checks. Sandbox and credential-denial assertions remain enabled. These tests mock model responses;
they establish CLI/tool compatibility and do not establish real model-provider acceptance.

The subprocess crash-helper's intentional harness skip is not an acceptance-test skip. Full topology
is Linux-tagged; the macOS and Windows lanes execute the two supported cross-platform scenarios.
No provider account credentials are transferred into remote build environments. All three temporary
remote environments were deleted and their absence was verified after validation.

Initial environment failures remain in the raw evidence: long remote setup command transport errors
were resolved with short task-local scripts; a Windows dependency-fetch TLS failure was resolved
with verified vendored public dependencies; inherited proxy settings initially interfered with
Windows loopback fixtures. The final named Windows mock scenarios passed after removing those
proxy settings only from the test environment. The original full native suite itself passed and
was not rerun. None of these setup/failed attempts is counted as a passing or skipped named test.

## Alpha.12 Candidate

The repository candidate is **0.1.0-alpha.12**, not a published release. A new identity is necessary
because the profile and publication fixes change runtime bytes. The initial deterministic-package
CI failure compared the new runtime with the old alpha.11 checksum map; its two new builds agreed
with each other. Candidate version pins and the unsigned six-target map are prepared together;
alpha.11 release artifacts and historical records remain unchanged.

The two complete unsigned builds passed `verify-release` and compared byte-for-byte across their
full output directories. Both copies of all six archives matched the generated checksum map.
The extracted Linux amd64 executable returned `0.1.0-alpha.12` in plain and JSON version output.
Complete Go, focused version/CLI/package tests and vet passed. The source-equivalence check confirms
that the candidate changes only four version pins and the generated map relative to the tested
publication checkpoint; it changes no other behavior. The six-target build establishes compilation
and packaging, not native execution of an otherwise unexecuted architecture.

Unsigned manifest SHA-256:
`01dc9410d2debe20d880921e6c85305f50947cd72fd40e4d57802b0d0b3a72e1`.

Independent round-1 review of the complete candidate checkpoint was clean. It independently
verified archive hashes, manifest, historical preservation and the extracted executable version.
The version-sensitive support contract and POSIX installer smoke also passed on this candidate.

This candidate has not been tagged, signed or published. It is concrete release-preparation
material; canonical acquisition and coordinated upgrade remain separate acceptance steps.

## Real Provider Attempt And Remaining Boundaries

The real `gpt-6.1-sol` attempt is **BLOCKED**. The existing protected acceptance provider returned
HTTP **422**, **model not found: gpt-6.1-sol**, before tool execution. The failed turn, immutable
started receipt and current failed lifecycle projection were preserved. No older model was used as
a replacement or counted as Sol acceptance.

That attempt used a clearly labeled development build of the frozen production profile checkpoint,
with only a private build-version overlay. It is not an official alpha.11 artifact or canonical
upgrade evidence. Its fresh, isolated services were stopped and disabled after the attempt. The
previous acceptance deployment's configurations, service units and upgrade journals remained
byte-identical.

| Remaining check | Status | Reason |
| --- | --- | --- |
| Real Codex 6.1 Sol execution, tools and result delivery | BLOCKED | The configured provider does not supply the requested model. |
| Canonical coordinated upgrade under the new model requirement | BLOCKED | Needs a published runtime containing profile 8 and a provider supporting the requested model. Released alpha.10/alpha.11 lack the profile fix. |
| Windows 11 logged-in-user qualification and real Task Scheduler readiness | SKIP | No suitable logged-in Windows 11 machine is available; a Windows Server container cannot establish this. |
| macOS TraeX real-account smoke | SKIP | The isolated remote environment has no provisioned account credentials. |

Windows TraeX remains unsupported. This checkpoint does not publish a release or complete the M6
canonical upgrade acceptance. Historical upgrade journals were not reset or rewritten. Reissuing a
same-target command against a terminal journal returns that old result, so a retry must use fresh
isolated transaction state after its prerequisites are ready.

Raw command logs, superseded setup failures, private probe details and review reports remain outside
tracked files. The evidence consistency check verifies frozen source, model-policy changes,
historical-file preservation, executed tests, result distinctions and owned-resource cleanup.
