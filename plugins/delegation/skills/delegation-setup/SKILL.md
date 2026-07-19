---
name: delegation-setup
description: Install, update, or diagnose the Delegation native runtime on Linux, macOS, or Windows. Use when Delegation reports a missing runtime, when preparing a device for later controller or connector configuration, or after updating the plugin or runtime.
---

# Delegation Setup

Set up the native runtime that matches this plugin version.
Never download `latest`, skip checksum verification, or expose a broker token to a Codex child process.

## Resolve The Plugin

Resolve the plugin root from this skill directory. Read `VERSION` and
`release-artifacts.sha256` there before installing anything. Treat `VERSION` as the exact release
version; do not substitute a newer version. Resolve the runtime launcher as
`scripts/delegation-mcp` on Linux or macOS and `scripts/delegation-mcp.cmd` on Windows. Use that
launcher for every runtime command below.

## Install The Runtime

1. If `DELEGATION_BINARY` is set, run the launcher with `version --json` and verify the reported
   version. The launcher applies the override for development, offline, or managed enterprise
   installations.
2. Otherwise run `scripts/install-runtime` on Linux or macOS, or
   `scripts/install-runtime.cmd` on Windows. The installer must select the current OS and
   architecture, fetch the exact GitHub Release version, verify the pinned SHA-256, and install
   atomically under the user's Delegation home.
3. Stop on a missing checksum, version mismatch, unsupported platform, or failed verification.
   Do not fall back to an unverified binary.
4. Run the launcher with `version --json` after installation. Do not invoke a bare `delegation`
   command or assume setup created a `PATH` shim; the runtime remains in its versioned directory.

## Configure The Role

Choose exactly one role and run the launcher with `setup <role> --help` before writing configuration.
Before broker, controller, or device setup, or before issuing/revoking a credential, read
[role configuration](references/role-configuration.md) and follow its enrollment and transport
rules. Token authentication is the default. Never pass token material as a command-line value;
configuration stores only an absolute token file path and refuses to overwrite an existing config.

Before migrating an older installation, changing a versioned runtime, or installing a user service,
read [migration and services](references/migration-and-services.md). Do not infer old settings from
new defaults, reuse any schema v1 broker or target token, or modify an unsafe inherited Windows home
in place.

## Verify And Hand Off

Run the launcher with `version --json` and confirm it exactly matches `VERSION`, then run the
launcher with `doctor`. Report the installed version, configured role, configuration path, and
checks without printing credentials. For a broker foreground run, verify its `/healthz` endpoint
without printing or transmitting the master token.

After a plugin or runtime update, tell the user to start a new Codex task so the updated skills and
MCP configuration are loaded. Do not attempt to reload a managed worker in place.
