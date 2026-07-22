# Network Configuration

Run the plugin launcher with `setup broker --help` or `setup peer --help` before writing
configuration. One device may host both processes. Their default configs and native services are
separate.

- `setup broker` creates or joins the authority for one Delegation network. The wire field
  `controllerId` is the stable network or trust-domain ID, not a controller device role. Token
  authentication is the default. Keep plaintext listeners on loopback unless the user explicitly
  accepts `--allow-insecure-nonloopback` behind an encrypted private network or tunnel.
- `setup peer` joins a physical device to that network. Every peer can host user-created root tasks
  and can later receive managed workers. Supply the broker URL, network `controllerId`, broker-bound
  `deviceId`, display name, and protected peer-token path. Setup generates a device ID only in
  `none` auth mode.

For token authentication, enroll every peer from the configured broker:

1. Choose a stable device UUID.
2. Start the broker once so its state is initialized, then verify it with `doctor`.
3. On the broker, run `credential issue --config <broker.json> --device-id <device-uuid> --out
   <protected-staging-token-file>`.
4. Transfer the token file over an authenticated encrypted channel and preserve or restore
   current-user-only protection at its final absolute path. Never paste token contents into chat,
   shell arguments, configuration, or a URL.
5. On the peer, run `setup peer` with the network ID, exact issued device ID, and
   `--token-file <protected-peer-token-file>`. Run `doctor --config <peer.json>` and remove transfer
   copies only after verification succeeds.
6. Revoke a lost or retired credential with `credential revoke --config <broker.json> --device-id
   <device-uuid>`. A revoked ID remains tombstoned; replacement needs a new identity.

`none` authentication trusts every client that can reach the broker. Such a client can join,
enumerate, dispatch, impersonate, or fence a same-ID peer. Tailscale narrows reachability but still
means trusting the entire tailnet. This authentication warning is separate from the plaintext
transport warning.

After setup, use `service run --config <path>` for a foreground process. A peer foreground process
loads `DELEGATION_CODEX_CONFIG_JSON` and its referenced provider credential variables from the
current environment. To install a role-specific current-user service, use `service install
--config <broker.json>` for a broker or `service install --config <peer.json> --environment-file
<peer.env>` for a peer. Read the native-service reference before creating that protected file.
