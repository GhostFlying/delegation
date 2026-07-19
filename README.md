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

Enroll each controller or device from the broker installation before running target setup. Choose
a new stable UUID for the device, then run the broker's launcher with the matching role:

```bash
plugins/delegation/scripts/delegation-mcp credential issue \
  --config <broker-config> \
  --role <controller-or-device> \
  --device-id <device-uuid> \
  --out <protected-staging-token-file>
```

Transfer that file over an authenticated encrypted channel and preserve or re-establish
current-user-only file protection on the target. Never paste its contents into a Codex task, shell
argument, config file, or broker URL. Run `setup controller` or `setup device` on the target with
the broker's `controllerId`, the exact issued `deviceId`, and the protected token path. After
`doctor` succeeds, remove any intermediate transfer copy. Revoke a lost or retired credential only
from the broker installation:

```bash
plugins/delegation/scripts/delegation-mcp credential revoke \
  --config <broker-config> \
  --device-id <device-uuid>
```

Revocation closes the credential's access on its next broker frame and marks the device offline.
Re-enrollment after revocation requires a new device UUID.

Schema v2 introduced an absolute broker `stateFile`, which schema v3 retains. Credential commands
use that configured path directly instead of deriving a database location from their environment.
Schema v3 makes plaintext non-loopback acknowledgement consistent for every authentication mode
and role. Schema v1 and v2 configs are not migrated in place. Preserve non-secret identity and
endpoint settings locally, move the old config aside, and rerun setup with explicit values rather
than accepting changed defaults. Schema v1 used one shared bearer token: never reuse it as either
the private M1 broker master or a device credential. Move the old shared token aside with the config
and verify the selected new master-token path does not exist. Omit `--token-file` only after checking
that setup's default token path is absent, or pass a different nonexistent path so setup creates a
fresh private M1 master. Then issue a fresh credential for every target device, transfer each token
as a protected file, and only then configure that target. Schema v2 may retain an existing protected
M1 master or per-device token path while rerunning setup with all other values explicit.

M0 Windows homes require a clean re-home because they used ordinary inherited profile-directory
ACLs. Stop the managed service, rename the entire old Delegation home to an offline backup outside
the new home, and run the M1 installer and setup against an empty default home or a new
`DELEGATION_HOME`. Do not point any new config, token, state, or service definition back into the
backup. Recreate the broker trust state and issue fresh per-device credentials; never print or copy
old token contents through a shell or Codex task. After `doctor` and all re-enrolled devices pass,
remove the offline backup according to the host's credential-retirement policy. M1 intentionally
refuses to change permissions on an existing M0 Windows home, so moving only `config.json` is not a
supported upgrade path.

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
`describe_device` when the full runtime feature list or current presence details matter. M1 exposes
device identity, role, OS, architecture, runtime and protocol versions, features, online state, and
last-seen time. It does not expose arbitrary toolchain, hardware, tag, or capacity metadata; do not
infer those properties from a device name.

The root MCP talks only to the same-user local connector bridge. It neither reads the connector's
broker token nor opens a broker connection itself. Device calls lazily bind the Codex task's
`_meta.threadId` to a Delegation tree and the broker validates the resulting root principal and
capabilities. M1 exposes discovery only; worker spawn, messaging, and workspace synchronization are
added by later milestones.

## License

Apache-2.0
