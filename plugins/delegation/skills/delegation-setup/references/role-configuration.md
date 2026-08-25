# Network Configuration

Run the plugin launcher with `setup broker --help` or `setup peer --help` before writing
configuration. One device may host multiple isolated instances. The `default` instance preserves
the legacy top-level paths and service identities; named instances default under
`instances/<instanceId>/` and use distinct local bridges and native services. Supply explicit
`--listen` and `--status-listen` values for every named broker.

Set `DELEGATION_INSTANCE=<instanceId>` in the CLI host environment before loading the plugin so its
root MCP uses that named peer. `DELEGATION_CONFIG` may instead select an exact config path; when
both selectors are present, the config's `instanceId` must match. Named setup requires an explicit
`--config` when `DELEGATION_CONFIG` is inherited, so setup never splits a named config from its
default resource namespace.

Embedded Tailscale is a fresh-deployment transport. Each role runs an independent userspace `tsnet`
node; Delegation does not use system `tailscaled`. Do not convert an existing TCP deployment or
reuse its config, database, token domain, state, or service definition. M6 has no migration,
in-place upgrade, or rollback workflow.

- `setup broker` creates or joins the authority for one Delegation network. The wire field
  `controllerId` is the stable network or trust-domain ID, not a controller device role. Token
  authentication is the default. Keep plaintext listeners on loopback unless the user explicitly
  accepts `--allow-insecure-nonloopback` behind an encrypted private network or tunnel. The
  separate status listener defaults to `127.0.0.1:8788` and must remain on loopback; use an
  authenticated tunnel to reach `/status` from another device.
- `setup peer` joins a physical device to that network. Every peer can host user-created root tasks
  and receive managed workers. Supply the broker URL, network `controllerId`, broker-bound
  `deviceId`, display name, and protected peer-token path. For embedded Tailscale, the `deviceId`
  must exactly match the identity for which the broker issued that token. Codex keeps
  `--codex-binary` as a compatible shorthand; both Codex and TraeX accept `--cli-command`,
  repeatable `--cli-argument`, `--cli-launcher`, and repeatable
  `--cli-launcher-prefix-argument`. TraeX requires the structured command and launcher. Every
  repeated value becomes one exact shell-free argv element; setup performs no shell parsing or
  expansion. Use `--flag=-value` for values beginning with `-`.

The current runtime accepts only schema version 4. It rejects version-3 and other old configs
instead of reading, converting, or migrating them. Create a new config with the current
`setup broker` or `setup peer`; do not hand-edit an old config to change its schema version.

For an embedded Tailscale broker, use all of these exact transport flags:

```text
setup broker
  --config <broker-config>
  --instance <instance-id>
  --host-kind <codex-or-traex>
  --controller-id <controller-uuid>
  --listen :<broker-port>
  --status-listen 127.0.0.1:<status-port>
  --auth-mode token
  --token-file <broker-master-token-file>
  --transport tailscale
  --tailscale-hostname <broker-tailscale-hostname>
  --tailscale-auth-key-file <broker-tailscale-auth-key-file>
  --tailscale-state-dir <broker-tailscale-state-dir>
```

For an embedded Tailscale peer, add the same `--transport`, `--tailscale-hostname`,
`--tailscale-auth-key-file`, and `--tailscale-state-dir` flags to the complete host-specific peer
setup, together with `--auth-mode token` and its issued `--token-file`. Its broker URL must be
exactly:

```text
ws://<broker-tsnet-hostname>:<broker-port>/v1/connect
```

Tailscale mode requires `ws://` with the explicit `/v1/connect` path and forbids
`--allow-insecure-nonloopback`. `<broker-tsnet-hostname>` is the broker node's configured
`--tailscale-hostname` when that short MagicDNS name is unique, or the broker node's full MagicDNS
FQDN. The embedded peer classifies the name or Tailscale IP as one currently online tailnet peer
before dialing it; it never falls back to the host network.

For a TraeX peer launched through warmpool:

```text
setup peer
  --instance <traex-instance-id>
  --host-kind traex
  --cli-command <traex-executable>
  --cli-argument=<traex-cli-argument>
  --cli-launcher <traex-launcher-executable>
  --cli-launcher-prefix-argument=<traex-launcher-prefix-argument>
```

Managed TraeX workers use this exact shell-free launch. The connector adapts TraeX's app-server
thread source, workspace-root response, idle completion, and `TRAECLI_HOME` rollout layout while
preserving the isolated worker profile and worker-only MCP authority. Keep Codex and TraeX in
separate instances; cross-CLI dispatch and mixed-CLI networks are not supported.

Codex and TraeX require separate brokers, instance IDs, controller IDs, broker and peer tokens,
Tailscale hostnames, Tailscale state directories, configs, databases, managed homes, service
identities, and peer URLs. Do not federate the brokers or share one controller domain.

The launcher must preserve stdio. On Linux it must `exec` the target CLI; on macOS and Windows it
must remain attached and must not daemonize or detach. The connector assembles the configured
launcher prefix, resolved native TraeX command, exact CLI arguments, and app-server arguments
without invoking a shell.

`--codex-home` and `peer.codexHome` are the current flag and field names for the managed CLI home;
they do not imply support for an older config schema. Codex receives the path as `CODEX_HOME`;
TraeX receives it as `TRAE_HOME` and uses its `cli` child as `TRAECLI_HOME`. Never point this field
at the user's normal CLI home: setup, doctor, and worker launch reject user authentication,
instructions, profiles, plugins, hooks, model providers, execution rules, and non-system skills.
Setup and doctor validate the command, launcher, paths, and protected configuration without
starting the CLI.

For token authentication, enroll every peer from the configured broker:

1. Choose a stable device UUID.
2. Start a tracked bootstrap broker with `service run --config <broker.json>`. Use bounded polling
   until `status --config <broker.json>` succeeds; a single immediate probe is insufficient.
3. On the broker, run `credential issue --config <broker.json> --device-id <device-uuid> --out
   <protected-staging-token-file>`.
4. Stop the tracked bootstrap broker and wait for its complete process tree to exit. Do not start
   the later qualification broker until this exit is confirmed.
5. Transfer the token file over an authenticated encrypted channel and preserve or restore
   current-user-only protection at its final absolute path. Never paste token contents into chat,
   shell arguments, configuration, or a URL.
6. On the peer, run `setup peer` with the network ID, exact issued device ID, and
   `--token-file <protected-peer-token-file>`. Run `doctor --config <peer.json>` and remove transfer
   copies only after verification succeeds.
7. Revoke a lost or retired credential with `credential revoke --config <broker.json> --device-id
   <device-uuid>`. A revoked ID remains tombstoned; replacement needs a new identity.

The Tailscale enrollment key is not the Delegation token. Each embedded role needs a protected
`tskey-auth-...` file supplied through `--tailscale-auth-key-file`. Setup reads it offline, stores
only its absolute path, and does not consume or delete it. The runtime reads it at every start, so
retain it with current-user-only protection. Retain the broker master token and issued peer token
as separate protected files. Never put either kind of secret in a URL, command argument, provider
environment file, config contents, or task.

Give every role its own persistent `--tailscale-state-dir`. Do not share it or its derived
`<state-dir>.tailscale.lock` lease path, and keep both isolated from config, token, database,
managed-home, workspace, and peer environment paths. `TSNET_FORCE_LOGIN` must be unset or false.

Embedded Tailscale supports only `--auth-mode token` for brokers and peers. Tailnet membership
limits transport reachability but never replaces the broker master token or broker-issued peer
token.

After setup, use `service run --config <path>` for a foreground process. A peer foreground process
loads `DELEGATION_CODEX_CONFIG_JSON`, its referenced provider credential variables, and the target
host's available standard Codex/OpenAI authentication variables from the current environment. To
install a role-specific current-user service, use `service install
--config <broker.json>` for a broker or `service install --config <peer.json> --environment-file
<peer.env>` for a peer. Read the native-service reference before creating that protected file.

After each foreground process starts, boundedly poll `status --config <broker.json>` or
`status --config <peer.json>` instead of probing once. The broker command reads the aggregate
loopback Web endpoint; the peer command reads the same-user local connector bridge. Peer
qualification requires `connectionState=ready` and `workerSyncReady=true`. Add `--json` for
machine-readable output. Fail on timeout or tracked-process exit. Do not copy raw state databases
or tokens to diagnose status.

For transport metadata, status exposes only `transport` and optional `tailscaleHostname`. It never
exposes the enrollment key or path, Tailscale state or lease paths, or Delegation tokens. M6 does
not support federation, high availability, Tailscale Funnel or Serve, public ingress, mixed-CLI
networks, or shared embedded state.
