# Delegation

Delegation is a Codex plugin for assigning bounded work across trusted peers with different operating
systems, toolchains, or local capabilities. Every participating device can host user-created root
tasks and managed workers.

The project is delivered in reviewed milestones. The current M4 checkpoint includes the plugin and
runtime bootstrap, a broker-backed equal-peer registry, isolated managed Codex workers with durable
lifecycle and messaging controls, Git workspace synchronization, and operational status surfaces.
It synchronizes an exact Git HEAD plus the root task's dirty index and worktree state, then reliably
returns each terminal rollout and Git result package to the originating root peer. The root agent can
explicitly apply a selected package without moving HEAD: descendant commits become staged changes,
dirty state is preserved, and conflicts stop before mutation. Result relay, replay, deduplication,
garbage collection, safe apply recovery, and the future signed release-candidate design are covered
by the M4 contracts.

## Install The Plugin

```bash
codex plugin marketplace add GhostFlying/delegation
codex plugin add delegation@delegation
```

Start a new Codex task after installation, then invoke `$delegation-setup`. The plugin does not
silently download native code from its MCP launcher: setup installs the exact runtime version after
verifying the SHA-256 pinned in the plugin.

The marketplace snapshot from `main` is the source of truth for the rolling plugin control plane;
GitHub Releases distribute only immutable native runtime archives. Each tagged release is a
compatibility checkpoint: its plugin control plane, runtime version, checksums, and native binaries
must match. Any native runtime change, or any behavior-bearing control-plane change including
skills, scripts, or MCP configuration, requires updating the plugin and runtime versions together
and publishing the matching tag and Release as one release operation. Do not announce that revision
as installable until the Release succeeds. Documentation-only changes do not require a runtime
release.

For source development, build the native runtime and point the plugin at it:

```bash
go build -o ./bin/delegation ./cmd/delegation
export DELEGATION_BINARY="$PWD/bin/delegation"
plugins/delegation/scripts/delegation-mcp version --json
```

## Development

Use Go 1.26.5.

```bash
go test ./...
python3 /path/to/plugin-creator/scripts/validate_plugin.py plugins/delegation
python3 /path/to/skill-creator/scripts/quick_validate.py \
  plugins/delegation/skills/delegation-setup
python3 /path/to/skill-creator/scripts/quick_validate.py \
  plugins/delegation/skills/cross-device-delegation
```

The Python paths above refer to the validators bundled with the local Codex installation; they are
not runtime dependencies of Delegation.

## Prepare A Release

Pre-1.0 alpha releases use deterministic unsigned archives. For every runtime compatibility
checkpoint, update `plugins/delegation/VERSION` and the plugin manifest version together, freeze all
behavior-bearing control-plane and build-affecting source on `main`, run
`go run ./cmd/releasepack -out dist`, and replace
`plugins/delegation/release-artifacts.sha256` with the generated manifest in a normal commit. CI
rebuilds all six Linux, macOS, and Windows archives twice and requires byte-identical output. Before
tagging, rebuild and commit the manifest from the exact release source tree. Later development does
not rewrite the runtime version-to-checksum mapping for an already published version; publish a new
version instead. The release workflow checks the mapping again at the immutable tag.

After the checksum commit passes CI, create `v<VERSION>` at that exact commit. Dispatch `Release`
from `main` and provide the tag as its `tag` input. The workflow rejects stable versions, validates
that the prerelease tag matches the version and is contained in `main`, rebuilds the six unsigned
archives at the tagged commit, and verifies them against the tracked manifest before its
write-scoped job publishes a GitHub prerelease. The `main` merge necessarily precedes that
publication. If a marketplace refresh observes the new version during this interval, setup fails
closed because the exact Release asset is absent; it never substitutes an older runtime. Retry setup
after the Release workflow succeeds.

Configure the required-reviewer `github-release` environment to accept deployments only from
`main`. It needs no platform signing credential. Add a tag ruleset that prevents updates and
deletion for `v*`, and enable immutable releases. The signed candidate-and-promotion design remains
documented in [the M4 release trust contract](docs/m4-release-trust-contract.md), but is not the
release path for the current unsigned alpha series. Keep the repository variable
`DELEGATION_ENABLE_SIGNED_RELEASE` unset or different from `true`; setting it to `true` explicitly
enables the future `Release candidate` gate.

## Configure The Network

The runtime installer deliberately creates no `PATH` shim. `$delegation-setup` resolves the
plugin's launcher and uses it for every command. In a source checkout on Linux or macOS, inspect
the installation flags through the same launcher:

```bash
plugins/delegation/scripts/delegation-mcp setup broker --help
plugins/delegation/scripts/delegation-mcp setup peer --help
plugins/delegation/scripts/delegation-mcp doctor --help
plugins/delegation/scripts/delegation-mcp status --help
```

On Windows, use `plugins\delegation\scripts\delegation-mcp.cmd` instead. The runtime writes a
versioned local configuration without embedding token material.

`controllerId` is the stable Delegation network or trust-domain ID. It is not the identity of a
controller device. Token authentication is the default. Peer setup accepts only a token file path;
the token itself is never accepted as a command-line value. Pass the same `--device-id` used when
the broker issued that token. Setup validates the complete peer configuration before creating
local credentials and never overwrites an existing configuration.

`instanceId` names one local Delegation deployment. It is not a principal, credential, protocol
target, or network identity. `hostKind` identifies the CLI family hosted by one homogeneous network
and currently accepts only `codex` or `traex`; a broker rejects peers from the other family before
registry mutation, and broker state is permanently bound to its first host family. Existing
version-3 configs that omit both fields are interpreted as the `default` Codex instance. New setup
commands persist both fields explicitly:

```bash
plugins/delegation/scripts/delegation-mcp setup broker \
  --instance <local-instance-name> \
  --host-kind <codex-or-traex>
```

TraeX peer setup remains disabled until the configurable non-shell CLI launch specification is
available. The `default` instance keeps the existing `~/.delegation/broker.json`,
`~/.delegation/peer.json`, local bridge, and native service identities. A named instance uses
`~/.delegation/instances/<instanceId>/` for its default broker and peer configuration, state,
secrets, managed Codex home, and workspaces. Its local bridge is also instance-scoped. Named broker
setup requires explicit `--listen` and `--status-listen` addresses because the runtime does not
guess free ports. Explicit `--config`, path, and listener arguments remain authoritative. Named
instances can run as foreground processes, while native service installation fails closed until
instance-scoped systemd, LaunchAgent, and Scheduled Task identities land in the next checkpoint.
Set `DELEGATION_INSTANCE=<instanceId>` in the Codex or TraeX host environment before it loads the
plugin to make the shipped root MCP select that named peer, or set `DELEGATION_CONFIG` to the exact
peer config. An explicit instance selector must match the selected config. Named setup refuses an
inherited `DELEGATION_CONFIG` unless `--config` is also explicit, preventing a split namespace.

Enroll each peer from the broker installation before running peer setup. Choose a new stable UUID
for the peer and start the broker once so its state is initialized, then run:

```bash
plugins/delegation/scripts/delegation-mcp credential issue \
  --config <broker-config> \
  --device-id <device-uuid> \
  --out <protected-staging-token-file>
```

Transfer that file over an authenticated encrypted channel and preserve or re-establish
current-user-only file protection on the target. Never paste its contents into a Codex task, shell
argument, config file, or broker URL. Run `setup peer` with the network's `controllerId`, the exact
issued `deviceId`, and the protected token path. Run `doctor --config <peer.json>` and remove any
intermediate transfer copy after verification. Revoke a lost or retired credential only from the
broker installation:

```bash
plugins/delegation/scripts/delegation-mcp credential revoke \
  --config <broker-config> \
  --device-id <device-uuid>
```

Revocation closes access on the next broker frame and marks the peer offline. Revoked IDs remain
tombstoned and require a new device UUID.

Pre-1.0 releases provide no upgrade support. Config, broker state, peer state, wire protocol, local
bridge, and native service definitions are versioned independently. Discard configuration and state
created by earlier development checkouts, then run `setup broker` and `setup peer` again; the
current runtime never converts or deletes them automatically.

Run `doctor --config <path>` after setup. Broker and peer may coexist on one device through
`broker.json` and `peer.json`; commands that could be ambiguous require an explicit config. Install
each configured process as a separate current-user service with:

```bash
plugins/delegation/scripts/delegation-mcp service install --config <broker.json>
plugins/delegation/scripts/delegation-mcp service install \
  --config <peer.json> \
  --environment-file <protected-peer.env>
```

The peer environment file is a current-user-only file containing literal `NAME=value` lines. It
must define `DELEGATION_CODEX_CONFIG_JSON` and the provider credential variables referenced by that
JSON. It may also contain the target host's existing `CODEX_ACCESS_TOKEN`, `CODEX_API_KEY`, or
`OPENAI_API_KEY`; these authenticate the managed app-server and are excluded from worker shells.
Values are not shell-expanded. Keep the file outside the managed worker `CODEX_HOME` and workspace
root, and never put a broker or peer token in it. A foreground `service run --config <peer.json>`
inherits the same values from the current environment; add `--environment-file` to test the
native-service source directly.

Installation writes a disabled definition first, then enables, starts, and verifies it through the
native service manager. The definitions are `delegation-broker.service` and
`delegation-peer.service` on Linux, matching `.broker` and `.peer` LaunchAgents on macOS, and
`Delegation Broker` and `Delegation Peer` Scheduled Tasks on Windows. They are currently reserved
for the `default` instance; named installation is rejected before writing an artifact. Installation
refuses foreign definitions and managed definitions whose executable or configuration path has
drifted. A command whose effect cannot be reconciled returns
`indeterminate` and leaves the definition in place for inspection.

Linux requires a working systemd user manager. macOS uses the current GUI launchd domain and thus
requires that user to have a GUI login. The Windows task uses an interactive user token and likewise
requires a logged-in user. A nonzero Windows service exit is retried once per minute for up to 255
attempts; a clean exit is not restarted. Runtime-path updates require explicit native service
replacement.
Restart the peer service after rotating provider credentials or replacing the environment file.

Inspect either process through its explicit role config:

```bash
plugins/delegation/scripts/delegation-mcp status --config <broker.json>
plugins/delegation/scripts/delegation-mcp status --config <peer.json> --json
```

Peer status is read from the same-user local connector bridge and distinguishes local worker state
from the revision acknowledged by the broker. A successful status read proves the peer service is
running; `connectionState=ready` separately proves that its broker connection and worker lifecycle
sync are usable. Protocol v4 identifies the CLI host family and sends both the pre-recovery startup
baseline and the current worker revision: the broker uses the baseline for rollback detection but
does not make the peer dispatchable until it has acknowledged the current revision. A replaced peer
database is reported as `stateRecoveryRequired` with `peer_worker_revision_rollback` and both the
local and broker worker revisions instead of as a generic disconnect. Broker status combines
durable network counters with
the current synchronized connection set. It includes registered/online/connected/sync-ready device
counts, current and lifetime dispatch/turn counts, occupied worker slots, and bounded artifact
counts. Broker result status separates current delivery/detail retention from lifetime delivered,
source-acknowledged, source-released, and compacted-detail totals. Neither surface includes prompts,
messages, Git URLs, workspaces, rollout contents, credentials, or provider configuration.

When status reports `stateRecoveryRequired`, stop the peer service and restore the original peer
database before restarting it. An automated reset policy is intentionally not defined yet because
discarding or terminalizing managed worker state requires an explicit product decision.

`setup broker` configures the Web status listener as `127.0.0.1:8788` by default. It serves HTML at
`/status` and the same snapshot as JSON at `/v1/status`. The listener is separate from the broker
WebSocket listener and is restricted to loopback. To inspect it from another host, use an
authenticated tunnel instead of exposing it directly, for example:

```bash
ssh -L 8788:127.0.0.1:8788 <broker-host>
```

Then open `http://127.0.0.1:8788/status` locally. Status responses disable caching and external
content. An older pre-release broker config without `statusListen` must be recreated with the
current `setup broker` command; pre-1.0 configuration migration is intentionally unsupported.

Managed worker process cleanup is lifecycle ownership, not an OS security boundary. On Unix-like
hosts, a deliberately detached same-UID process is outside the current threat model; on macOS, an
immediately daemonizing double-fork may reparent before it becomes discoverable to the connector.
Windows managed workers use Codex's `:danger-full-access` permission profile because
restricted read profiles require a separately provisioned elevated Windows sandbox. The broker and
worker MCP capability checks constrain the normal worker principal only; they are not a security
boundary against the worker process itself. Treat a Windows worker as fully trusted same-user code:
it can read or modify same-user credentials, configuration, and state; use the peer credential to
impersonate or fence that peer; and access the network without restriction. Do not delegate an
untrusted prompt, repository, or program to a Windows peer. Elevated sandbox provisioning is
deferred until it can be installed and verified without an interactive prompt in the peer service.

## Discover Peers

The bundled root MCP initializes without broker access so setup remains available when the runtime
or connector is offline. Its instructions are static and do not inject a device roster. Call
`list_devices` for a current, tree-authorized view of the network registry, then call
`describe_device` when the full runtime feature list or current presence details matter. The root
MCP exposes peer identity, `isCurrentDevice`, OS, architecture, runtime and protocol versions,
features, online state, and last-seen time. It does not expose a device role or arbitrary toolchain,
hardware, tag, or capacity metadata; do not infer those properties from a peer name.

The root MCP talks only to the same-user local connector bridge. It neither reads the connector's
broker token nor opens a broker connection itself. Device calls lazily bind the Codex task's
`_meta.threadId` to a Delegation tree and the broker validates the resulting root principal and
capabilities. Any non-managed, user-created Codex task on any peer may become a root when it first
uses a Delegation tool. Trees remain bound to the originating peer. A managed worker thread will
remain a worker; opening its history does not promote it.

## Dispatch Managed Workers

For repository work, first call `sync_workspace` with a fresh `sync_id`, the online
`target_device_id`, and an explicit SSH or HTTPS Git URL. The target clones and checks out the exact
source `HEAD` when it can; otherwise the peers relay a scoped thin or self-contained Git bundle.
The bundle contains only objects reachable from `HEAD`, not unrelated branches or tags. A
`remote_git_full_history_fallback` warning means that HEAD-reachable history may include deleted
content. Dirty sources add a bounded, deterministic `tar.zst` overlay that preserves staged and
unstaged content, binary files, rename/delete semantics, intent-to-add entries, and non-ignored
untracked files. The target applies it only inside the managed workspace and verifies the resulting
Git snapshot before publishing it. Non-portable paths, unsupported index states or special files,
and absolute or escaping dirty symlinks fail closed instead of being partially synchronized. LFS
payloads and submodule repositories are not embedded; heed the corresponding sync warnings.
Changes-artifact metadata keeps those prepared-workspace conditions in `base_warnings` and reports
conditions observed while capturing the worker result separately in `result_warnings`. Treat the
two lists as independent evidence; a clean result-warning list does not erase a base transfer
warning.

Call `spawn_agent` with a fresh `spawn_id` UUID, the same target, the returned `workspace_id`, a
unique lowercase `task_name`, and a self-contained `message`. Tasks that do not need repository
state may omit `workspace_id`. The target may be the current peer: self-targeting still creates a
separate managed Codex thread in the connector's app-server. The broker persists the worker
principal and dispatch receipt before contacting the target, so a lost response can be retried with
the same spawn ID and exactly the same arguments.

The returned durable status is `started`, `failed`, or `pending`, and the dispatch attempt also has
an `outcome`. `busy` means the target had no worker slot. `indeterminate` means no definitive target
result could be confirmed or durably recorded; the worker may already have started. Both retain one
pending receipt and must be retried with the same spawn ID and exactly the same arguments. `started`
and `failed` outcomes are terminal. Use `list_agents` to inspect the current tree's durable receipts
and terminal failure codes. Task names identify agents within a root tree and cannot be reused for
another spawn.

Use `send_message` to steer a running worker or queue a message for an idle worker,
`followup_task` to start a new turn for an idle worker, and `interrupt_agent` to stop an active turn.
`wait_agent` returns bounded lifecycle, worker-message, legacy artifact metadata, and verified result
package handles; call it again immediately while `has_more` is true before concluding that the
result is complete. `available` means the package bytes are currently durable in the root peer;
`evicted` means only broker metadata remains after local retention. Raw rollouts remain local and
never enter model context. To accept a workspace-backed result, call `apply_agent_changes` with a
fresh `apply_id` and the available `package_id`. The runtime derives the trusted root cwd locally,
applies only after the retained base still matches, never moves root HEAD or its ref, and returns
`needs_resolution` without Delegation writes when the workspace has drifted. Worker commits become
staged changes for root review; the worker's staged, unstaged, and untracked distinctions remain
visible. Ignored cache/build paths are never overwritten when they collide with result paths.
After an ambiguous mid-apply interruption, recovery material remains local and the root agent can
restore the recorded base or finish the desired state before retrying the same `apply_id`.
Completed apply journals retain a bounded replay receipt, not the materialized result payload.
Apply is explicit and is never performed merely because a worker completed.

## License

Apache-2.0
