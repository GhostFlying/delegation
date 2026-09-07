---
name: delegation-setup
description: Install, update, configure, or diagnose the Delegation native runtime on Linux, macOS, or Windows. Use when Delegation reports a missing runtime, when joining a device to a Delegation peer network, when configuring a cohosted broker, or after updating the plugin or runtime.
---

# Delegation Setup

Set up the native runtime that matches this plugin version.
Never download `latest`, skip checksum verification, or expose a broker token to a managed CLI
child process.

For embedded Tailscale, create only fresh named deployments. Delegation embeds one `tsnet` node in
each broker or peer process and does not use system `tailscaled`. Do not convert or reuse an old
config, database, token domain, or Tailscale state directory. A current managed native service may
move forward through the verified local upgrade transaction, which preserves all those identities.
Require `--auth-mode token` for every embedded Tailscale broker and peer; no unauthenticated
Tailscale mode is supported. Keep Codex and TraeX in separate instance, broker, controller, token,
state, hostname, and service domains.

Windows TraeX is unsupported. M6 supports Codex and TraeX on Linux and macOS, and Codex on
Windows 11. Stop instead of configuring, qualifying, or installing a Windows TraeX deployment.

## Resolve The Plugin

Resolve the plugin root from this skill directory. Read `VERSION` and
`release-artifacts.sha256` there before installing anything. Treat `VERSION` as the exact release
version; do not substitute a newer version. Resolve the runtime launcher as
`scripts/delegation-mcp` on Linux or macOS and `scripts/delegation-mcp.cmd` on Windows. Use that
launcher for every runtime command below.

## Acquire Or Recover The Runtime

1. Run the launcher with `version --json`. If the canonical versioned runtime is absent, the
   launcher invokes the bundled installer automatically. The installer selects the current OS and
   architecture, fetches the exact GitHub Release version, verifies the SHA-256 pinned in the
   plugin, and publishes atomically under the user's Delegation home.
2. The launcher independently requires a non-linked regular executable that reports the exact
   plugin `VERSION`. A present but invalid runtime fails closed and is not automatically replaced.
3. If automatic acquisition fails, run `scripts/install-runtime` on Linux or macOS, or
   `scripts/install-runtime.cmd` on Windows, to see installer diagnostics and recover explicitly.
   Stop on a missing checksum, version mismatch, unsupported platform, failed verification, or
   failed postcondition. Do not fall back to an unverified binary.
4. If `DELEGATION_BINARY` is set, the launcher preserves it as an explicit direct-execution
   override for development, offline, or managed enterprise installations. Managed-runtime
   version and symlink policy do not apply to this override.
5. Do not invoke a bare `delegation` command or assume setup created a `PATH` shim; the runtime
   remains in its versioned directory.

On Linux and macOS, the Delegation home and each managed `bin`, version, platform, and staging
directory must be owned by the current user with mode `0700`. The installer creates new directories
with that protection and validates the complete hierarchy on cold and warm launcher paths. It never
changes permissions on an existing directory. If an alpha.8 cold installation reports a broader
directory, stop and inspect its owner and contents before an operator deliberately secures or
recreates it; do not let setup silently take ownership of that path.

## Configure The Installation

An installation may host a broker, a peer, or both. Run `setup broker --help` or `setup peer --help`
before writing configuration. The `default` instance keeps the separate top-level `broker.json`
and `peer.json` files. A named instance defaults under `instances/<instanceId>/`; named brokers
require explicit broker and status listeners. Before setup or issuing/revoking a credential, read
[role configuration](references/role-configuration.md) and follow its enrollment and transport
rules. Token authentication is the default. Never pass token material as a command-line value;
configuration stores only an absolute token file path and refuses to overwrite an existing config.
TraeX peer setup additionally requires `--trae-auth-file` pointing to the host's protected
`TRAECLI_HOME/auth.json`; keep that source outside the isolated managed home, workspace, service
environment, Delegation authority, and Tailscale authority. The runtime copies it into the managed
home for the app-server while denying managed worker tools access to both the source and the copy.
On macOS, neither the source nor the managed home may be beneath `/tmp`, `/private/tmp`, `/var/tmp`,
or `/private/var/tmp`, including through a symbolic-link alias, because the worker sandbox grants
model tools access to those temporary roots.
Set `DELEGATION_INSTANCE` in the CLI host environment before plugin startup to select a named
peer's root MCP; an explicit `DELEGATION_CONFIG` remains authoritative and must identify the same
instance.

Before installing a fresh user service, read
[native services](references/native-services.md). Install broker and peer processes independently,
and stop when the native service manager reports a foreign, drifted, or indeterminate definition.
Use the exact named config when installing a named instance; its `instanceId` selects a distinct
native service identity.

## Upgrade A Managed Service

Read [native services](references/native-services.md) before upgrading. Upgrade only an existing,
idle managed service to a strictly newer canonical release. The transaction verifies release
provenance, exact service ownership, immutable config/environment identity, database schemas, and
embedded-Tailscale compatibility before it can stop the service. It never accepts a caller-selected
repository, URL, or binary. `DELEGATION_BINARY` and development builds are outside this path.

The first move from alpha.4 or alpha.7 requires `service upgrade ... --bootstrap` locally on each
host, peers first and broker last, because neither release has the current upgrade RPC. Use the
alpha.8 or newer CLI. A peer bootstrap also requires its existing protected `--environment-file`; a
broker bootstrap forbids that flag. After the network runs a coordination-capable
version, invoke `service upgrade` without `--bootstrap` against the broker config. The broker drains
mutations, freezes the online compatible peers, upgrades them first, and upgrades itself last. Offline
old-version peers later require local bootstrap. Retrying the same target resumes the protected
controller journal. Bootstrap discovers and verifies the old runtime through the exact current-user
native service definition and process identity. Peer
activation migrates only homogeneous retained alpha.4/profile-5 or alpha.7/profile-6 worker history
to the target profile in the stopped-service shadow database; mixed or unknown profiles, occupied
workers, pending operations, and unfinished result publication are rejected, and the original
database remains rollback material.
`service upgrade --cancel --config <path> --transaction-id <uuid>` is valid only before
durable commit authorization. After authorization, do not downgrade or restore the old definition;
inspect `status --config <path> --json`, repair the target release, and retry forward. Windows TraeX
remains unsupported.

## Verify And Hand Off

Run the launcher with `version --json` and confirm it exactly matches `VERSION`, then run the
launcher with `doctor --config <path>` for each broker and peer config. Report the installed version,
configured processes, configuration paths, and checks without printing credentials. Boundedly poll
`status --config <path>` after each service starts. For a broker, verify both its `/healthz`
endpoint and loopback-only `/status` page without printing or transmitting the master token. Keep
the status listener on loopback; use an authenticated tunnel for remote inspection.

For a fresh embedded Tailscale deployment, qualify every broker and peer with foreground
`service run --config <path>` before installing a native service. Use an explicit config for every
command and boundedly poll status until the broker responds or the peer reports both
`connectionState=ready` and `workerSyncReady=true`. Track a bootstrap broker while issuing peer
credentials, then stop and wait for its complete process tree before starting the qualification
broker. On Windows, resolve and start the native `delegation.exe` directly instead of tracking the
`.cmd` wrapper. Dot-source `scripts/windows-process.ps1` and use its shell-free native process
functions; they preserve exact argv on Windows PowerShell 5.1 and PowerShell 7, retain the native
process object and PID, and perform exact-PID process-tree cleanup. Use them only with direct native
executables, never `.cmd` or `.bat` wrappers. Never replace a live service for qualification.
Status may report only the transport and optional Tailscale hostname from the transport
configuration; do not print enrollment-key paths, state directories, lease paths, or tokens.

After a plugin or runtime update, tell the user to start a new task in the configured CLI so the
updated skills and MCP configuration are loaded. Do not attempt to reload a managed worker in place.
