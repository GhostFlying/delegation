# Native Services

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

`status --config <peer.json>` distinguishes the local process from broker readiness. A successful
status call with `connectionState=connecting` means the service is alive but not synchronized.
`stateRecoveryRequired` plus `peer_worker_revision_rollback` means the configured peer database is
behind the broker cursor, commonly after replacing or losing `peer.sqlite3`. Stop the peer service
and restore the original database before restarting it. Automated reset semantics are intentionally
not defined yet because managed worker state must not be silently discarded or terminalized.
