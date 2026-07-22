# Delegation

Delegation is a Codex plugin for assigning bounded work across trusted peers with different operating
systems, toolchains, or local capabilities. Every participating device can host user-created root
tasks and managed workers.

The project is being delivered in reviewed milestones. M0 provides the plugin, skills, native
runtime bootstrap, and release foundation. M1 provides the broker and persistent registry. M1.1
makes every device an equal peer and exposes root MCP discovery from any user-created Codex task.
M2 runs isolated managed Codex threads on selected peers; its first checkpoint provides durable
spawn and agent discovery. Git workspace transport remains M3 work.

## Install The Plugin

```bash
codex plugin marketplace add GhostFlying/delegation
codex plugin add delegation@delegation
```

Start a new Codex task after installation, then invoke `$delegation-setup`. The plugin does not
silently download native code from its MCP launcher: setup installs the exact runtime version after
verifying the SHA-256 pinned in the plugin.

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

Run the `Release candidate` workflow from the exact commit intended for release. It builds all six
runtime archives twice, requires byte-identical output, and uploads a candidate containing the
generated `release-artifacts.sha256`. Review that candidate, then replace the comments in the
plugin's checksum file with the reviewed manifest in a normal commit.

Before enabling releases, configure the `github-release` environment to accept deployments only
from `main` and require approval. Also add a tag ruleset that prevents updates and deletion for
`v*`, and enable immutable releases. The ruleset closes the force-move window while a release is
being published; immutability protects its tag and assets after publication.

After the checksum commit passes CI, create `v<VERSION>` at that exact commit. Dispatch the
`Release` workflow from `main` with that tag. Separate jobs validate the tag against `main`, build
the six archives at the tagged commit, and verify them against the reviewed checksum manifest
before the write-scoped job publishes them. Candidate preparation never creates a tag or release.
Release signing and provenance hardening are deferred to M4.

## Configure The Network

The runtime installer deliberately creates no `PATH` shim. `$delegation-setup` resolves the
plugin's launcher and uses it for every command. In a source checkout on Linux or macOS, inspect
the installation flags through the same launcher:

```bash
plugins/delegation/scripts/delegation-mcp setup broker --help
plugins/delegation/scripts/delegation-mcp setup peer --help
plugins/delegation/scripts/delegation-mcp doctor --help
```

On Windows, use `plugins\delegation\scripts\delegation-mcp.cmd` instead. The runtime writes a
versioned local configuration without embedding token material.

`controllerId` is the stable Delegation network or trust-domain ID. It is not the identity of a
controller device. Token authentication is the default. Peer setup accepts only a token file path;
the token itself is never accepted as a command-line value. Pass the same `--device-id` used when
the broker issued that token. Setup validates the complete peer configuration before creating
local credentials and never overwrites an existing configuration.

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

Delegation has not published a runtime release yet. Config, broker state, peer state, wire protocol,
local bridge, and native service definitions are versioned independently and have no upgrade
support. Discard configuration and state created by earlier development checkouts, then run `setup
broker` and `setup peer` again; the current runtime never converts or deletes them automatically.

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
`Delegation Broker` and `Delegation Peer` Scheduled Tasks on Windows. Installation refuses foreign
definitions and managed definitions whose executable or configuration path has drifted. A command
whose effect cannot be reconciled returns
`indeterminate` and leaves the definition in place for inspection.

Linux requires a working systemd user manager. macOS uses the current GUI launchd domain and thus
requires that user to have a GUI login. The Windows task uses an interactive user token and likewise
requires a logged-in user. Runtime-path updates require explicit native service replacement.
Windows restart-on-failure hardening is deferred to M4.
Restart the peer service after rotating provider credentials or replacing the environment file.

Managed worker process cleanup is lifecycle ownership, not an OS security boundary. On Unix-like
hosts, a deliberately detached same-UID process is outside the M2 threat model; on macOS, an
immediately daemonizing double-fork may reparent before it becomes discoverable to the connector.
Windows managed workers use Codex's `:danger-full-access` permission profile in M2 because
restricted read profiles require a separately provisioned elevated Windows sandbox. The broker and
worker MCP capability checks constrain the normal worker principal only; they are not a security
boundary against the worker process itself. Treat a Windows worker as fully trusted same-user code:
it can read or modify same-user credentials, configuration, and state; use the peer credential to
impersonate or fence that peer; and access the network without restriction. Do not delegate an
untrusted prompt, repository, or program to a Windows peer in M2. Elevated sandbox provisioning is
deferred until it can be installed and verified without an interactive prompt in the peer service.

## Discover Peers

The bundled root MCP initializes without broker access so setup remains available when the runtime
or connector is offline. Its instructions are static and do not inject a device roster. Call
`list_devices` for a current, tree-authorized view of the network registry, then call
`describe_device` when the full runtime feature list or current presence details matter. M1.1
exposes peer identity, `isCurrentDevice`, OS, architecture, runtime and protocol versions, features,
online state, and last-seen time. It does not expose a device role or arbitrary toolchain, hardware,
tag, or capacity metadata; do not infer those properties from a peer name.

The root MCP talks only to the same-user local connector bridge. It neither reads the connector's
broker token nor opens a broker connection itself. Device calls lazily bind the Codex task's
`_meta.threadId` to a Delegation tree and the broker validates the resulting root principal and
capabilities. Any non-managed, user-created Codex task on any peer may become a root when it first
uses a Delegation tool. Trees remain bound to the originating peer. A managed worker thread will
remain a worker; opening its history does not promote it.

## Dispatch Managed Workers

Call `spawn_agent` with a fresh `spawn_id` UUID, an online `target_device_id` returned by
`list_devices`, a unique lowercase `task_name`, and a self-contained `message`. The target may be
the current peer: self-targeting still creates a separate managed Codex thread in the connector's
app-server. The broker persists the worker principal and dispatch receipt before contacting the
target, so a lost response can be retried with the same spawn ID and exactly the same arguments.

The returned status is `started`, `failed`, or `pending`. `pending` means the durable receipt exists
but the broker could not establish a definitive target result; retry the exact request rather than
inventing another spawn ID. Use `list_agents` to inspect the current tree's receipts and terminal
failure codes. Task names identify agents within a root tree and cannot be reused for another
spawn.

This M2 checkpoint starts workers in an empty managed workspace. It does not yet expose root-side
message, follow-up, interrupt, or wait tools; those lifecycle controls are the next M2 checkpoint.
Repository synchronization and change artifacts arrive in M3. Do not claim that a worker received
the root repository until workspace transport reports that explicitly.

## License

Apache-2.0
