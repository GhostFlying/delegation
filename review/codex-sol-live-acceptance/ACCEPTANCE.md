# Real Codex Sol Acceptance: 2026-11-04

The previously blocked real-provider tool/result probe now has **PASS** status with the requested
**GPT-6.1-Sol** model, Codex **0.163.0**, Go **1.26.5**, and the unsigned **0.1.0-alpha.12** candidate
on Linux amd64. This supersedes only the real-provider blocker in the
[November 1 record](../codex-sol-acceptance/ACCEPTANCE.md). Its failed attempt remains historical
evidence, and canonical coordinated upgrade acceptance remains incomplete.

## Frozen Source And Runtime

- Merged source: `09ba4d0921ec0e45e05a8a53e03a3d33ee44e598`.
- Source tree: `aea0a61bb75305ae4ff1b621929c3ae71ebe78b7`.
- Candidate artifact source: `f7108d6245a2168ad3a363e278636daeb969e752`.
- Linux amd64 executable SHA-256:
  `c64d5525a53b24c021f028d539b71fd18f985837841f600885f0749d06310333`.
- Linux amd64 archive SHA-256:
  `753340ea960a25d4c6af2257b9ec7bd971c80911f4491fbb0c54e691275c1948`.

The candidate's executable inputs are identical to the merged source; only the two earlier
documentation files differ. The actual executable hash matches the executable read from the
candidate archive, and the archive matches the merged checksum map. No development-version
overlay was used. These checks identify a local unsigned candidate, not a published artifact.

## Model Route Correction

The configured provider exposes `GPT-6.1-Sol`, with case-sensitive model IDs. Using the same protected
acceptance credential, model retrieval returned 200 for that ID and 404 for `gpt-6.1-sol`. The
provider had no alias between those IDs. The earlier HTTP 422 attempt used the lowercase ID;
it did not establish that the provider lacked the requested model family.

A fresh isolated acceptance instance received a private copy of the existing provider environment.
Only its model ID changed to `GPT-6.1-Sol`; provider configuration and credential variables remained
otherwise identical. Shared gateway configuration, old instances and historical journals were not
modified. Repository mock model inputs and the named Go live-smoke assertion remain unchanged.
This was a separate root-MCP execution probe, not a claim that the named Go live test ran.

## Executed Acceptance

The bounded probe used the real managed-worker profile and root MCP to dispatch one worker to its
own isolated peer. It required a fresh nonce printed by an actual nested shell tool call through
`functions.exec`. Rollout inspection paired the exact prescribed JavaScript invoking
`tools.exec_command` with its successful tool result and final assistant nonce. Merely echoing a
command or nonce was insufficient.

The probe passed all of these checks together:

- Every fresh turn context identified `GPT-6.1-Sol`.
- The exact prescribed code-mode call had a paired successful shell result containing the nonce.
- The final assistant response contained that same nonce.
- Root MCP delivered an available durable result with terminal outcome `completed`.
- The result's turn and managed-thread identities matched the fresh rollout.
- The immutable spawn receipt was started, and the fresh lifecycle projection was current and idle.
- The root MCP process exited with status 0.

The initial setup attempt failed before peer installation because the private runner added quotes
around a JSON environment-file value. That failure is retained. A second fresh instance used the
required raw JSON encoding and passed; the failed setup is not counted as acceptance execution.

All services installed by either attempt were stopped and disabled, and their owned unit files were
archived outside the repository and removed. Nine protected prior configuration, environment,
service and journal files were byte-identical before and after the run. Private command logs,
model-route checks, rollout, MCP messages and cleanup evidence remain outside tracked files.

The complete `go test -count=1 -timeout 20m ./...` suite passed under the host's normal `umask 022`.
An earlier run under the private runner's `umask 077` failed a permission fixture: `os.Mkdir(0755)`
created a private directory instead of the intended unsafe parent. That failure is retained;
no test or production code was changed. Executable evidence checks also rejected a wrong nonce,
wrong turn identity and different shell command.

## Remaining Acceptance

| Check | Status | Boundary |
| --- | --- | --- |
| Real Sol shell execution and durable result delivery | PASS | Fresh isolated Linux probe on the identified unsigned candidate. |
| Published compatible runtime and canonical coordinated upgrade | INCOMPLETE | Candidate publication and a fresh upgrade transaction remain separate steps. |
| Windows 11 logged-in-user qualification | SKIP | No suitable logged-in machine is available. |
| macOS TraeX real-account qualification | SKIP | No account is provisioned in the isolated remote environment. |

Earlier native/mock, packaging and TraeX results retain their original source boundaries.
Windows TraeX remains unsupported. This record creates no tag, publishes no runtime and makes no
canonical upgrade, cross-platform real-Sol, or full M6 closure claim.
