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

## License

Apache-2.0
