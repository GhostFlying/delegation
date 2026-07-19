# Migration And Services

Schema v4, broker-store v4, wire protocol v2, and local-bridge v2 form one coordinated, fail-closed
upgrade. It is not rolling-compatible with M1. Before migration, stop the legacy service and back up
the config, broker state, token files, and persistent service definition.

Use `migrate config --from <schema-v3-config> --to <broker.json-or-peer.json>`; the source remains
unchanged and the destination is never replaced.

- A legacy broker becomes a v4 broker with the same network `controllerId`, state path, listener,
  auth mode, and master-token path. Do not generate a new master token: retained controller
  credential MACs depend on it. Start this v2 broker before running credential administration. It
  migrates the state store while holding the broker instance lease.
- A legacy controller becomes a peer with the same device identity and credential. That credential
  was already root-capable.
- A legacy active device credential was target-only and must never be promoted. After the broker
  migration deletes it, issue a fresh peer credential for the same `deviceId` to a different token
  file. Run config migration with `--token-file <fresh-peer-token>`. Revoked or incomplete legacy
  enrollments remain tombstoned and need a new device ID.

Do not run `credential issue` or `credential revoke` against a v3 state file. Administrative opens
refuse to migrate it without the broker lease. Do not reuse a legacy device token path or token
value. The legacy source files remain available for rollback only while the old services are
stopped.

Schema v1 and v2 installations should first follow their version-specific secure migration rules to
reach a protected v3 installation. Never reuse a schema-v1 shared bearer token as a v4 broker master
or peer credential. M0 Windows homes used inherited profile-directory ACLs and need an offline
re-home; do not modify them in place.

Run `service install --config <broker.json>` and `service install --config <peer.json>` separately.
The native identities are:

- Linux: `delegation-broker.service` and `delegation-peer.service`.
- macOS: `com.github.ghostflying.delegation.broker` and
  `com.github.ghostflying.delegation.peer`.
- Windows: `Delegation Broker` and `Delegation Peer` Scheduled Tasks.

Stop and remove the legacy single `delegation.service`, `com.github.ghostflying.delegation`, or
`Delegation Connector` definition through the native service manager before starting the migrated
peer. Verify its Delegation ownership marker and config path first. Service installation refuses
foreign definitions and managed definitions with a different executable or config path. Treat
`indeterminate` as partial activation requiring native service-manager inspection.

Linux requires a systemd user manager. macOS needs the current user's GUI launchd domain. Windows
needs an interactive login. Runtime-path changes require explicit native service replacement.
