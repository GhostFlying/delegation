# Delegation

Delegation is a Codex plugin for assigning bounded work to managed devices with different operating
systems, toolchains, or local capabilities. The root Codex task stays on its original device while
remote workers run in isolated, prepared workspaces.

The project is being delivered in reviewed milestones. M0 provides the plugin, skills, native
runtime bootstrap, and release foundation. Broker, connector, worker lifecycle, and Git workspace
transport arrive in later milestones.

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

On Windows, use `plugins\delegation\scripts\delegation-mcp.cmd` instead. The M0 runtime writes a
versioned local configuration without embedding token material.

Token authentication is the default. Controller and device setup accept only a token file path;
the token itself is never accepted as a command-line value. Pass the same `--device-id` used when
the broker issued that token. Setup validates the complete role configuration before creating
local credentials and never overwrites an existing configuration.
Run `doctor` through the launcher after setup to validate the local schema and protected token
file. Broker connectivity starts in M1. Prepare the current platform's user-service definition
through the launcher with:

```bash
plugins/delegation/scripts/delegation-mcp service install
```

M0 leaves the systemd user unit, LaunchAgent, or Windows Scheduled Task inactive. It does not load,
enable, start, replace, or remove an existing service; the long-running service runtime starts in
M1.

## License

Apache-2.0
