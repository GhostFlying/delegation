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

Installation writes a disabled definition before enabling, starting, and verifying it. It refuses
foreign definitions and managed definitions whose executable or configuration path differs. Treat
`indeterminate` as partial activation that requires inspection through the native service manager;
do not overwrite or delete the definition automatically.

Linux requires a working systemd user manager. macOS needs the current user's GUI launchd domain.
Windows needs an interactive login. Runtime-path changes require explicit native service
replacement. Restart the peer service after rotating its provider credential or replacing the
environment file. Changing the environment file path changes the managed service identity and
requires explicit replacement.
