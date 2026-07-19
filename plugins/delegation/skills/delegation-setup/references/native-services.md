# Native Services

Run `service install --config <broker.json>` and `service install --config <peer.json>` separately.
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
replacement.
