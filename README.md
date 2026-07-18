# Delegation

Delegation is a Codex plugin for assigning bounded work to managed devices with different operating
systems, toolchains, or local capabilities. The root Codex task stays on its original device while
remote workers run in isolated, prepared workspaces.

The project is being delivered in reviewed milestones. M0 provides the plugin, skills, native
runtime bootstrap, and release foundation. M1 provides the broker, persistent device registry,
controller and device connectors, and the root MCP device-discovery tools. Managed Codex workers
and Git workspace transport remain M2 and M3 work respectively.

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

## Configure A Role

The runtime installer deliberately creates no `PATH` shim. `$delegation-setup` resolves the
plugin's launcher and uses it for every command. In a source checkout on Linux or macOS, inspect
the role-specific flags through the same launcher:

```bash
plugins/delegation/scripts/delegation-mcp setup broker --help
plugins/delegation/scripts/delegation-mcp setup controller --help
plugins/delegation/scripts/delegation-mcp setup device --help
plugins/delegation/scripts/delegation-mcp doctor --help
```

On Windows, use `plugins\delegation\scripts\delegation-mcp.cmd` instead. The runtime writes a
versioned local configuration without embedding token material.

Token authentication is the default. Controller and device setup accept only a token file path;
the token itself is never accepted as a command-line value. Pass the same `--device-id` used when
the broker issued that token. Setup validates the complete role configuration before creating
local credentials and never overwrites an existing configuration.
Schema v2 introduced an absolute broker `stateFile`, which schema v3 retains. Credential commands
use that configured path directly instead of deriving a database location from their environment.
Schema v3 makes plaintext non-loopback acknowledgement consistent for every authentication mode
and role. Schema v1 and v2 are not migrated in place. Before moving an old broker configuration
aside, preserve every setting and identify the state database previously used by credential
commands. Rerun `setup broker` with the existing controller ID, listener, authentication mode,
master token path when used, state path, and `--allow-insecure-nonloopback` for any non-loopback
listener. For an old controller or device configuration, preserve its identity, name, broker URL,
authentication mode, and token path; add the acknowledgement only for a non-loopback `ws://` URL.
Do not accept a new default unless it matches the existing setting or database.
Run `doctor` through the launcher after setup to validate the local schema, authority paths, state
path, and protected token file. A broker, controller, or device can run in the foreground with
`service run`. Install and activate the configured role as the current user's managed service with:

```bash
plugins/delegation/scripts/delegation-mcp service install
```

Installation writes a disabled definition first, then enables, starts, and verifies it through the
native service manager. It refuses foreign definitions and managed definitions whose executable or
configuration path has drifted. A command whose effect cannot be reconciled returns
`indeterminate` and leaves the definition in place for inspection.

Linux requires a working systemd user manager. macOS uses the current GUI launchd domain and thus
requires that user to have a GUI login. The Windows task uses an interactive user token and likewise
requires a logged-in user. M1 does not yet migrate a service when a runtime update changes its
versioned executable path; verify ownership, remove the old native definition explicitly, and rerun
`service install`. Windows restart-on-failure hardening is deferred to the reliability milestone.

## Discover Managed Devices

The bundled root MCP initializes without broker access so setup remains available when the runtime
or connector is offline. Its instructions are static and do not inject a device roster. Call
`list_devices` for a current, tree-authorized view of the controller registry, then call
`describe_device` before choosing a target whose toolchains, tags, or capacity matter.

The root MCP talks only to the same-user local connector bridge. It neither reads the connector's
broker token nor opens a broker connection itself. Device calls lazily bind the Codex task's
`_meta.threadId` to a Delegation tree and the broker validates the resulting root principal and
capabilities. M1 exposes discovery only; worker spawn, messaging, and workspace synchronization are
added by later milestones.

## License

Apache-2.0
