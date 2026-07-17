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
version; do not substitute a newer version.

## Install The Runtime

1. If `DELEGATION_BINARY` is set, run that binary with `version --json` and verify the reported
   version. Keep the override for development, offline, or managed enterprise installations.
2. Otherwise run `scripts/install-runtime` on Linux or macOS, or
   `scripts/install-runtime.cmd` on Windows. The installer must select the current OS and
   architecture, fetch the exact GitHub Release version, verify the pinned SHA-256, and install
   atomically under the user's Delegation home.
3. Stop on a missing checksum, version mismatch, unsupported platform, or failed verification.
   Do not fall back to an unverified binary.

## Configure The Role

Choose exactly one role and run `delegation setup <role> --help` before writing configuration:

- Use `setup broker` to create a trust domain and listener configuration. Token authentication is
  the default and creates a protected token file when none is supplied. For auth mode `none`, keep
  the listener on loopback unless the user explicitly accepts `--allow-insecure-nonloopback`.
- Use `setup controller` for a device that hosts root tasks. Supply the broker URL, controller ID,
  and an existing token file when token authentication is enabled.
- Use `setup device` for a worker-only device. Supply the same controller ID, the broker URL, and
  that device's token file. Let setup generate a stable device ID unless one is already assigned.

Never pass token material as a command-line argument. Configuration stores only the absolute token
file path and refuses to overwrite an existing configuration.

## Verify And Hand Off

Run `delegation version --json` through the installed path and confirm it exactly matches `VERSION`,
then run `delegation doctor`. Report the installed version, configured role, configuration path, and
checks without printing credentials. M0 validates configuration locally; broker connectivity is
added in M1.

After a plugin or runtime update, tell the user to start a new Codex task so the updated skills and
MCP configuration are loaded. Do not attempt to reload a managed worker in place.
