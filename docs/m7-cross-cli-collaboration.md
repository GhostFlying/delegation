# M7 Cross-CLI Collaboration

## Objective And Current Status

Allow a user-created Codex root to delegate to a TraeX peer, and a user-created TraeX root to
delegate to a Codex peer, through one explicit Delegation control plane. Preserve the task's
supported inputs, tool requirements, communication, lifecycle, result, and resource-policy
semantics across the CLI boundary. Unsupported requirements must fail explicitly before work
starts rather than silently disappear.

M6 is closed by the owner disposition in [project status](project-status.md). This document starts
M7 with scope, checkpoints, and executable acceptance. It does not claim an implemented mixed
network or a passing cross-CLI task. The source baseline is M6 closure commit
`974283f0b8ebab29f3e530fb91edbf5b06a9baab`.

## Product Boundary

- Each peer instance continues to host exactly one CLI. Mixed collaboration selects another peer;
  it does not switch an existing peer's executable or combine its Codex and TraeX managed homes.
- Start with fresh, explicitly configured mixed networks. Existing homogeneous deployments keep
  their current behavior and authority. In-place conversion of their config, database, or token
  domain is outside this initial M7 scope and must not be performed implicitly.
- Target discovery must expose the peer's actual CLI kind. A dispatch selects a device and states
  the expected CLI kind; the target must match. Runtime identity is retained through retries,
  lifecycle, result retrieval, and recovery. Omitted selection preserves homogeneous behavior;
  it must never silently select a different CLI.
- Roots retain spawn, device discovery, descendant control, workspace synchronization, and explicit
  result application. Managed workers retain their restricted mailbox and self-publication
  authority. Cross-CLI collaboration grants no recursive delegation or cross-tree authority.
- Use explicit independent task context and workspace transfer. Full native conversation fork,
  inherited live tool sessions, native child-agent UI integration, sibling communication, and
  recursive worker dispatch are outside this initial scope. Requests requiring those semantics
  must receive an unsupported-capability result rather than an equivalent-fork claim.
- Codex acceptance uses the requested 6.1 Sol model; the known provider's case-sensitive wire ID is
  recorded when used. TraeX may continue with 5.6. Keep the pinned CLI baseline in
  [CLI compatibility](cli-compatibility.md).
- Keep the supported host/CLI matrix. Windows TraeX remains unsupported. A Windows Codex root
  targeting a supported Linux/macOS TraeX peer does not turn Windows into a TraeX host.
- The deferred alpha.11-to-alpha.12 upgrade campaign remains separate from M7. This milestone does
  not require resuming that campaign or relabeling its failed or unexecuted checks.

## Verified Source Gaps

These are implementation observations at the source baseline, not tests of a new implementation.

| Area | Current boundary | Source |
| --- | --- | --- |
| Admission | Broker and connector both reject differing host kinds. | [Broker](../internal/broker/server.go), [connector](../internal/connector/client.go). |
| Discovery | Hello carries hostKind, but its descriptor, stored device, and root MCP projection omit it. | [Protocol](../internal/protocol/protocol.go), [control model](../internal/control/model.go), [schema](../internal/store/schema.go), [root MCP](../internal/rootmcp/server.go). |
| Dispatch | Requests select a device; there is no expected runtime-kind field in the spawn contract. | [Agent protocol](../internal/protocol/agent.go), [root spawn](../internal/rootmcp/agent.go). |
| Persistence | Broker metadata binds one network host_kind; dispatch identity and retry checks need an explicit compatibility design. | [Store](../internal/store/store.go), [device epoch](../internal/store/device.go), [spawn state](../internal/store/agent_spawn.go). |
| Worker adaptation | Existing hosting already adapts thread creation, isolation, and terminal observation by CLI kind. | [Host](../internal/workerhost/host.go), [profile](../internal/workerhost/profile.go), [monitor](../internal/workerhost/monitor.go). |
| Context and control | Spawn accepts bounded text/workspace input; worker MCP exposes only upstream messaging and receive. New capability requirements need a versioned contract. | [Agent protocol](../internal/protocol/agent.go), [worker MCP](../internal/workermcp/server.go). |

Simply removing the admission equality checks is insufficient: discovery would hide the selected
CLI, persistent state would still encode a homogeneous network, and older endpoints would reject
the connection.

## Checkpoints

Each checkpoint gets one owner and writable worktree. The owner integrates only a frozen,
independently reviewed revision with executable acceptance. Shared protocol/schema write sets are
serialized; independent implementation can proceed after its required contract is frozen.

| Checkpoint | Owner | Dependencies | Primary write set | Acceptance behavior |
| --- | --- | --- | --- | --- |
| M7-0: scope and baseline | Planning owner | Accepted M6 closure | This document and project status | Source/link consistency and existing baseline regressions; plan review only. |
| M7-1: runtime identity | Protocol/identity owner | M7-0 | control, protocol device types, store device schema, root discovery projection, focused tests | Discovery reports actual per-peer CLI kind, including reconnect/reopen; legacy homogeneous identity remains valid; unknown or conflicting identity is explicit. Mixed admission remains disabled. |
| M7-2: mixed admission and explicit dispatch | Admission/dispatch owner | M7-1 | config, setup, broker/connector admission, spawn protocol/store/root MCP, focused tests | Fresh mixed networks admit compatible Codex and TraeX peers. Exact device/kind binding, root authority, and idempotent dispatch hold in both directions; incompatible old endpoints fail before mutation. |
| M7-3: task capability contract | Task-input owner | Frozen M7-2 dispatch contract | Versioned task/context transfer, required-capability validation, runtime profile adaptation, focused tests | Supported context and workspace/artifact references arrive intact. Missing tools, unsupported permissions/history semantics, oversize inputs, or unknown capabilities reject before execution. |
| M7-4: resource admission and usage | Resource-policy owner | Frozen M7-2 dispatch contract | Root/controller admission and durable resource accounting, protocol usage projection, focused tests | Cross-CLI work consumes the configured concurrency/accounting limits; retries and reconnects do not double-charge or release early; missing usage remains unknown. Unsupported strict policies reject explicitly. |
| M7-5: lifecycle and recovery | Lifecycle owner | M7-2; M7-3/M7-4 contracts | Existing agent control, mailbox, workerhost/result paths and focused fault tests | Send, follow-up, interrupt, wait, durable completion/failure, reconnect, and retained-thread resume preserve the selected runtime and exactly one logical dispatch. |
| M7-6: end-to-end and delivery | Integration owner | M7-1 through M7-5 | Cross-CLI integration/live fixtures, operator/plugin docs, release metadata | Both directions pass actual execution, result retrieval/application, capability denials, resource limits, and recovery at one frozen combined tree. |

The task-input and resource-policy checkpoints can proceed independently after the dispatch
contract is frozen, with non-overlapping implementation files and serialized shared-contract edits.
Follow the [checkpoint workflow](checkpoint-workflow.md), including its three-round review limit.
Release any runtime or behavior-bearing plugin changes through matching version/checksum/tag
publication; a development test binary is not an installable release.

## Required Acceptance Scenarios

All scenarios below are planned. Passing existing homogeneous tests does not pass these new paths.

| Scenario | Required evidence |
| --- | --- |
| Codex root to TraeX worker | Discovery, explicit target/kind, actual 5.6 tool execution, upstream question and response, follow-up, terminal result, and exact worker thread/turn identities. |
| TraeX root to Codex worker | The same chain, with actual 6.1 Sol code-mode execution on Codex and no model fallback. |
| Context, tools, and permissions | Exact bytes/digests for supported input references; required tool inventory; explicit unsupported/denied results before spawn or protected effects, including supported inputs exceeding the old short-message envelope. |
| Git work and long results | Prepared HEAD plus dirty workspace identity, full supported result retrieval beyond inline text limits, warning preservation, and explicit safe root apply. Delivery alone is not application. |
| Retry and authority | Same logical operation creates at most one worker; changed target/kind under the same ID conflicts; foreign tree/device and worker-origin spawn are denied. |
| Interrupt and recovery | Active turn interruption, idle follow-up, response loss, reconnect/restart, preserved terminal outcome, and same-runtime/thread continuation. |
| Limits and usage | Capacity exhaustion in both directions, retry/restart accounting, and explicit unknown or unsupported usage/policy states. Native parent budgets are not implicitly inherited; any promised limit needs a defined enforcement boundary. |
| Compatibility and hosts | Existing homogeneous tests pass; old/incompatible endpoints fail closed; Linux/macOS supported paths and compatible Windows-container cases run under their actual environment labels. Windows 11 interactive-only checks keep an explicit SKIP if no suitable machine is available. |

Real-account checks require isolated supported credentials; a missing account is a documented
SKIP or blocker for that specific live path, not a passing mock result. The selected cross-CLI live
paths need actual execution before the milestone can claim end-to-end capability alignment.

## Executable Starting Points

These existing tests establish the current boundary and provide extension points. Their rejection
assertions stay valid until an explicit M7 compatibility contract replaces them.

```sh
go test ./internal/broker ./internal/connector \
  -run 'TestTraeXBrokerRejectsCodexPeerBeforeRegistryMutation|TestTraeXConnectorRejectsCodexBrokerFeatures|TestAgentRPCRoutesToExplicitRemotePeer'
go test ./internal/rootmcp \
  -run 'TestRootMCPFailsClosedOnMismatchedRootBinding|TestRootMCPSpawnsAndListsDurableAgents'
go test ./internal/store -run TestBrokerEpochBindsStateHostKind
go test ./internal/workerhost \
  -run 'TestHostIsolatesRuntimeHomeForHostKind|TestHostAdaptsThreadStartProtocolByHostKind'
```

The first runtime implementation checkpoint is M7-1. Its exit is truthful, persisted per-peer CLI
identity with unchanged homogeneous behavior; it does not yet enable cross-CLI dispatch.
