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

## Verify And Hand Off

Run `delegation version --json` through the installed path and confirm it exactly matches `VERSION`.
Report the installed version and path without printing credentials. Runtime installation alone does
not configure a broker, controller, connector, or user service.

After a plugin or runtime update, tell the user to start a new Codex task so the updated skills and
MCP configuration are loaded. Do not attempt to reload a managed worker in place.
