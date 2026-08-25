# Native Services

For a fresh embedded Tailscale deployment, qualify each broker and peer before installation. Run
`doctor --config <path>`, then start `service run --config <path>` in the foreground with the same
explicit config and peer environment source intended for the service. Use bounded polling rather
than a single immediate status probe; require broker status success or peer
`connectionState=ready` plus `workerSyncReady=true`. Fail on timeout or tracked-process exit.

On POSIX, record the launched PID and stop and wait for that exact process in a trap. On Windows,
do not track `delegation-mcp.cmd`: resolve the same native `delegation.exe` selected by the launcher,
start it directly with shell-free `System.Diagnostics.ProcessStartInfo`, set `UseShellExecute` to
`$false`, and call `ArgumentList.Add()` once per exact argv element. Do not use an argument-string
interface or build a command string: executable, config, environment-file, and log paths may contain
spaces. Open log paths with literal .NET file APIs rather than shell redirection. Retain the native
process object and PID. Clean up that exact Windows PID with a process-tree scoped
`taskkill.exe /PID <native-runtime-pid> /T /F` in `finally`, then wait for the process handle. Never
use image-name matching or an unscoped kill. The runtime's Windows Job Object ownership covers its
managed app-server descendants.

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
up to 255 attempts; a clean exit is not restarted. Runtime-path changes require explicit native
service replacement. Restart the peer service after rotating its provider credential or replacing
the environment file. Changing the environment file path changes the managed service identity and
requires explicit replacement.

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
add support for migration, upgrade, rollback, replacement, federation, high availability, Funnel,
Serve, public ingress, or mixed Codex and TraeX networks.
