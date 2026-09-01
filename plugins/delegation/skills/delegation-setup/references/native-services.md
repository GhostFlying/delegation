# Native Services

M6 supports native Codex services on Linux, macOS, and Windows 11, and native TraeX services on
Linux and macOS. Windows TraeX is unsupported; do not configure or install it.

For a fresh embedded Tailscale deployment, qualify each broker and peer before installation. Run
`doctor --config <path>`, then start `service run --config <path>` in the foreground with the same
explicit config and peer environment source intended for the service. Use bounded polling rather
than a single immediate status probe; require broker status success or peer
`connectionState=ready` plus `workerSyncReady=true`. Fail on timeout or tracked-process exit.

On POSIX, record the launched PID and stop and wait for that exact process in a trap. On Windows,
do not track `delegation-mcp.cmd`: resolve the same native `delegation.exe` selected by the launcher,
dot-source `scripts/windows-process.ps1`, and call `Start-DelegationNativeProcess` with one exact
argv element per `-ArgumentList` entry. The helper uses `ProcessStartInfo.ArgumentList` when
available and the equivalent standard Windows quoting rules on Windows PowerShell 5.1. Pass
environment additions or removals through `-Environment`, and pass literal output and error paths
through `-StandardOutputPath` and `-StandardErrorPath`. Retain the returned native process object
and its `Id`. Use the helper only with direct native executables, never `.cmd` or `.bat` wrappers.
Use `Invoke-DelegationNativeProcessCapture` for bounded status commands and
`Stop-DelegationNativeProcessTree` in `finally`; cleanup runs
`taskkill.exe /PID <native-runtime-pid> /T /F` and waits for that process handle. Never build a
command string, use `Start-Process -ArgumentList`, match an image name, or issue an unscoped kill.
The runtime's Windows Job Object ownership covers its managed app-server descendants.

Credential issuance uses the same ownership rules: start and boundedly poll a tracked bootstrap
broker, issue while it is ready, then stop and wait for its complete process tree before starting
the later qualification broker. Stop and wait for all qualification processes before installation.
Do not qualify by replacing or stopping a live service.

Run the broker and peer installs separately:

```text
service install --config <broker.json>
service install --config <peer.json> --environment-file <peer.env>
```

The peer environment file is required for native service installation because systemd, launchd,
and Task Scheduler do not reliably inherit the setup shell. It must be an absolute,
current-user-only protected file outside the managed worker `CODEX_HOME` and workspace root. The
file uses literal `NAME=value` lines with no shell expansion or quote processing. It must contain
`DELEGATION_CODEX_CONFIG_JSON` and the credential variables referenced by that JSON. It may also
contain `CODEX_ACCESS_TOKEN`, `CODEX_API_KEY`, or `OPENAI_API_KEY` from the target host so the
managed app-server can use the host's existing authentication; worker shells exclude these values.
Do not put broker or peer tokens in this file.
For embedded Tailscale, also keep the environment file outside the Tailscale enrollment-key file,
state directory, and derived `<state-dir>.tailscale.lock` path. Never put the Tailscale enrollment
key or a Delegation token in the environment file.

For an interactive foreground peer, `service run --config <peer.json>` reads the provider config,
referenced credentials, and any available standard Codex/OpenAI authentication variables from the
current environment. Add `--environment-file <peer.env>` to exercise the same source used by the
native service.
The native identities are:

- Linux: `delegation-broker.service` and `delegation-peer.service`.
- macOS: `com.github.ghostflying.delegation.broker` and
  `com.github.ghostflying.delegation.peer`.
- Windows: `Delegation Broker` and `Delegation Peer` Scheduled Tasks.

Those names remain exact for the `default` instance. A named config selects:

- Linux: `delegation-<instanceId>-broker.service` or
  `delegation-<instanceId>-peer.service`.
- macOS: `com.github.ghostflying.delegation.<instanceId>.broker` or
  `com.github.ghostflying.delegation.<instanceId>.peer`.
- Windows: `Delegation <instanceId> Broker` or `Delegation <instanceId> Peer`.

Installation writes a disabled definition before enabling, starting, and verifying it. It refuses
foreign definitions, definitions owned by another instance, and managed definitions whose
executable or configuration path differs. Treat `indeterminate` as partial activation that requires
inspection through the native service manager; do not overwrite or delete the definition
automatically.

Linux requires a working systemd user manager. macOS needs the current user's GUI launchd domain.
Windows needs an interactive login. A nonzero Windows service exit is retried once per minute for
up to 255 attempts; a clean exit is not restarted. A managed service may move to a strictly newer
canonical runtime through the local atomic upgrade below. The config and environment-file paths
remain immutable; use a fresh named deployment when either path or another service identity must
change.

## Managed Service Upgrade

The first move from alpha.4 requires explicit local bootstrap authorization because alpha.4 has no
upgrade RPC. Upgrade every idle peer first, then the broker last:

```text
service upgrade --config <peer.json> --environment-file <peer.env> \
  --target-version <newer-version> --bootstrap --timeout 30m --json
service upgrade --config <broker.json> \
  --target-version <newer-version> --bootstrap --timeout 30m --json
```

Once every online peer and the broker run a coordination-capable version, start a controller-wide
transaction from the broker host without `--bootstrap`:

```text
service upgrade --config <broker.json> \
  --target-version <newer-version> --timeout 30m --json
```

The broker drains new mutations, freezes the online compatible upgrade-capable peers, prepares and
arms every participant, persists irreversible global COMMIT, activates peers first, and activates
itself last. Pre-COMMIT failure cancels every prepared transaction. Offline peers do not block; an
old-version peer remains non-dispatchable after reconnecting and requires local bootstrap. The new
broker bounds reconnect and readiness qualification and reports `completed_with_errors` when a
participant requires intervention.

Preparation accepts only the canonical Delegation GitHub release and verifies its manifest,
Sigstore provenance, tag commit, workflow identity, platform, architecture, and artifact digest. It
also verifies the exact current-user service definition, executable/config/environment identity,
database and embedded-Tailscale compatibility, and the absence of occupied workers or unfinished
operations. The new CLI performs bootstrap discovery directly from that native definition and its
running process, so an alpha.4 service does not need the current local-bridge protocol or upgrade
RPC. Alpha.4 bootstrap accepts only the exact schema-3 TCP config and broker-19 or peer-15
database. After the service stops, the activator preserves rollback material, migrates protected
shadows to schema 4 and broker-20 or peer-16, and switches them atomically. A same-target retry
resumes the existing journal. Peer completion requires target runtime/config digests, lifecycle
synchronization, dispatchability, and a ready epoch newer than the source epoch.

Arming installs a transaction-specific one-shot activator without changing the main service.
Activation first writes durable commit authorization, then the independent activator stops the exact
process tree, checkpoints and shadows the database, switches the definition and database, starts
the target, and performs local qualification. `prepared` or `armed` may be cancelled with:

```text
service upgrade --cancel --config <config> --transaction-id <uuid>
```

After authorization, recovery is forward-only. `status --config <config> --json` reports the
bounded transaction even while the service is stopped during activation. A
`forward_recovery_required` state requires repairing the target-version material and retrying; do
not silently restore the old runtime. The protected local Unix socket or Windows named pipe is the
only running-service management channel; broker listeners expose no upgrade API. Windows activators
invoke the native `delegation.exe` directly.

Each embedded Tailscale service owns one `tsnet` node and one exclusive Tailscale state-directory
lease. It does not use system `tailscaled`. Keep broker and peer state directories distinct, and
require `--auth-mode token` for every embedded broker and peer. Keep Codex and TraeX in separate
named service domains. A second foreground or native-service process cannot share the same config
and state authority.

`status --config <peer.json>` distinguishes the local process from broker readiness. A successful
status call with `connectionState=connecting` means the service is alive but not synchronized.
`stateRecoveryRequired` plus `peer_worker_revision_rollback` means the configured peer database is
behind the broker cursor, commonly after replacing or losing `peer.sqlite3`. Stop the peer service
and restore the original database before restarting it. Automated reset semantics are intentionally
not defined yet because managed worker state must not be silently discarded or terminalized.

Transport status adds only `transport` and optional `tailscaleHostname`; it does not expose the
enrollment-key path, Tailscale state or lease paths, or Delegation tokens. Native services do not
support downgrade, config/transport conversion, service-identity replacement, federation, high
availability, Funnel, Serve, public ingress, or mixed Codex and TraeX networks.
