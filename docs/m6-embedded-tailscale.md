# M6 Embedded Tailscale Operator Guide

M6 adds a private transport for fresh Delegation deployments. Each broker and peer process embeds
its own userspace Tailscale node through `tsnet`. It does not use, configure, or require a system
`tailscaled`, its socket, or the system Tailscale CLI. The broker listens inside its embedded
tailnet node; each peer resolves and dials that broker through its own embedded node.

This guide uses placeholders only. Replace every `<...>` value with a value for that one deployment
without reusing a Codex value in a TraeX deployment or a TraeX value in a Codex deployment.

## Deployment Contract

Embedded Tailscale is fresh-deployment-only:

- Create new named instances, configs, controller UUIDs, device UUIDs, Delegation tokens, databases,
  Tailscale state directories, hostnames, and native service identities.
- Do not convert a TCP config, hand-edit an older config, reuse an older database or Tailscale state
  directory, or replace a running service definition.
- M6 defines no migration, in-place upgrade, downgrade, or rollback procedure. Preserve an earlier
  deployment only as a separately stopped installation.
- Run setup offline, qualify every process in the foreground, clean up the recorded processes, and
  only then install fresh native services.

Codex and TraeX are two independent Delegation networks. They must use different:

- `instanceId` values;
- brokers and embedded Tailscale broker hostnames;
- `controllerId` trust domains;
- broker master tokens and issued peer tokens;
- broker and peer configs, databases, Tailscale state directories, and managed homes;
- device IDs, peer Tailscale hostnames, status ports, broker ports, and peer WebSocket URLs.

Do not connect a Codex peer to a TraeX broker or vice versa. The broker permanently binds its state
to one `hostKind`, and mixed-CLI networks are unsupported.

## Credentials And Persistent State

Tailscale enrollment and Delegation authentication are separate layers:

1. `--tailscale-auth-key-file` points to an operator-managed Tailscale enrollment key. It must be a
   protected current-user-only regular file containing one `tskey-auth-...` line. Exact mode `0600`
   is required on Unix. Setup reads and validates the file but does not enroll a node.
2. `--token-file` points to a Delegation bearer token. A broker uses its master token; a peer uses
   the token issued for its exact `deviceId`. Tailnet membership does not replace this token.

Every embedded Tailscale broker and peer must use `--auth-mode token`. No unauthenticated
Tailscale mode is supported.

Both files are retained. The runtime reads the Tailscale enrollment-key file at every start and
never deletes it. Do not remove it after the first enrollment. The Delegation token files are also
required for normal authentication and must remain protected. Never put either secret in a command
argument, URL, config value, provider environment file, log, or task.

Each process also owns a persistent `--tailscale-state-dir`. The runtime creates it privately when
the embedded node first starts and holds an exclusive sibling
`<tailscale-state-dir>.tailscale.lock` lease. Never share a state directory between roles,
instances, processes, or CLI families, and do not place configs, tokens, provider environment
files, managed CLI homes, or workspaces inside it.

`TSNET_FORCE_LOGIN` must be unset or false. Interactive forced login is unsupported.

## Create The Codex Domain

Create a protected Tailscale enrollment-key file for the Codex broker, then configure a fresh named
broker. Named brokers require explicit broker and status listeners:

```text
<launcher> setup broker
  --config <codex-broker-config>
  --instance <codex-instance-id>
  --host-kind codex
  --controller-id <codex-controller-uuid>
  --listen :<codex-broker-port>
  --status-listen 127.0.0.1:<codex-status-port>
  --state <codex-broker-database>
  --auth-mode token
  --token-file <codex-broker-master-token-file>
  --transport tailscale
  --tailscale-hostname <codex-broker-tailscale-hostname>
  --tailscale-auth-key-file <codex-broker-tailscale-auth-key-file>
  --tailscale-state-dir <codex-broker-tailscale-state-dir>
```

Bootstrap the broker in the foreground so its Delegation database is initialized. Track that
bootstrap process with the platform-specific ownership method in
[Foreground Qualification](#foreground-qualification), and use bounded polling until
`status --config <codex-broker-config>` succeeds. While that tracked bootstrap process is still
running, issue a distinct Delegation peer token:

```text
<launcher> credential issue
  --config <codex-broker-config>
  --device-id <codex-peer-device-uuid>
  --out <codex-peer-staging-token-file>
```

Immediately after issuance, stop the tracked bootstrap process, wait for its complete process tree
to exit, and clear its PID or process handle. Do not configure or qualify the peer until that exit
is confirmed. The later qualification phase starts a new broker process; the bootstrap broker and
qualification broker must never overlap on the broker or embedded Tailscale state leases.

Transfer the issued file over an authenticated encrypted channel, restore current-user-only
protection at `<codex-peer-token-file>`, and retain that final file. Configure the Codex peer:

```text
<launcher> setup peer
  --config <codex-peer-config>
  --instance <codex-instance-id>
  --host-kind codex
  --controller-id <codex-controller-uuid>
  --device-id <codex-peer-device-uuid>
  --device-name <codex-peer-display-name>
  --broker-url ws://<codex-broker-tsnet-hostname>:<codex-broker-port>/v1/connect
  --auth-mode token
  --token-file <codex-peer-token-file>
  --codex-binary <codex-executable>
  --git-binary <codex-peer-git-executable>
  --codex-home <codex-managed-cli-home>
  --workspace-root <codex-workspace-root>
  --state <codex-peer-database>
  --transport tailscale
  --tailscale-hostname <codex-peer-tailscale-hostname>
  --tailscale-auth-key-file <codex-peer-tailscale-auth-key-file>
  --tailscale-state-dir <codex-peer-tailscale-state-dir>
  --max-worker-slots <codex-max-worker-slots>
```

The peer URL is exactly `ws://<codex-broker-tsnet-hostname>:<codex-broker-port>/v1/connect`.
Embedded Tailscale requires `ws://`, an explicit `/v1/connect` path, no credentials, query, or
fragment, and no `--allow-insecure-nonloopback`. `<codex-broker-tsnet-hostname>` is the broker
`tsnet` node hostname from `--tailscale-hostname` when that short MagicDNS name is unique, or that
node's full MagicDNS FQDN.

## Create The TraeX Domain

Repeat the process with a separate broker, controller UUID, tokens, state, and peer URL:

```text
<launcher> setup broker
  --config <traex-broker-config>
  --instance <traex-instance-id>
  --host-kind traex
  --controller-id <traex-controller-uuid>
  --listen :<traex-broker-port>
  --status-listen 127.0.0.1:<traex-status-port>
  --state <traex-broker-database>
  --auth-mode token
  --token-file <traex-broker-master-token-file>
  --transport tailscale
  --tailscale-hostname <traex-broker-tailscale-hostname>
  --tailscale-auth-key-file <traex-broker-tailscale-auth-key-file>
  --tailscale-state-dir <traex-broker-tailscale-state-dir>
```

Start that TraeX bootstrap broker with tracked process-tree ownership, use bounded polling until
`status --config <traex-broker-config>` succeeds, and keep it running only while issuing the peer
credential:

```text
<launcher> credential issue
  --config <traex-broker-config>
  --device-id <traex-peer-device-uuid>
  --out <traex-peer-staging-token-file>
```

Then stop and wait for the complete bootstrap process tree before peer setup. Never leave the
bootstrap broker running for the later qualification start.

TraeX requires a structured CLI command and an attached shell-free launcher:

```text
<launcher> setup peer
  --config <traex-peer-config>
  --instance <traex-instance-id>
  --host-kind traex
  --controller-id <traex-controller-uuid>
  --device-id <traex-peer-device-uuid>
  --device-name <traex-peer-display-name>
  --broker-url ws://<traex-broker-tsnet-hostname>:<traex-broker-port>/v1/connect
  --auth-mode token
  --token-file <traex-peer-token-file>
  --cli-command <traex-executable>
  --cli-argument=<traex-cli-argument>
  --cli-launcher <traex-launcher-executable>
  --cli-launcher-prefix-argument=<traex-launcher-prefix-argument>
  --git-binary <traex-peer-git-executable>
  --codex-home <traex-managed-cli-home>
  --workspace-root <traex-workspace-root>
  --state <traex-peer-database>
  --transport tailscale
  --tailscale-hostname <traex-peer-tailscale-hostname>
  --tailscale-auth-key-file <traex-peer-tailscale-auth-key-file>
  --tailscale-state-dir <traex-peer-tailscale-state-dir>
  --max-worker-slots <traex-max-worker-slots>
```

Repeat `--cli-argument` and `--cli-launcher-prefix-argument` once per exact argv element. Setup does
not perform shell parsing. The TraeX peer URL is exactly
`ws://<traex-broker-tsnet-hostname>:<traex-broker-port>/v1/connect`, where the host is the TraeX
broker `tsnet` node's unique short MagicDNS hostname or full MagicDNS FQDN. It must not name the
Codex broker.

## Foreground Qualification

Run `doctor --config <config>` for every new config before starting it. Qualify one domain at a
time, without an installed service for the same config. Use explicit config paths for every command.
Every readiness check must use bounded polling. A process may need time to enroll its embedded node,
open its local status surface, connect to the broker, and synchronize worker state; a single status
probe immediately after start is not qualification.

On a POSIX host, record only the PIDs returned by the shell and clean up those exact processes.
The same pattern applies to a bootstrap broker; stop and wait for its PID immediately after
credential issuance, then reset `broker_pid` before starting the qualification broker.

```sh
broker_pid=
peer_pid=
cleanup() {
  if [ -n "$peer_pid" ]; then
    kill "$peer_pid" 2>/dev/null || true
    wait "$peer_pid" 2>/dev/null || true
  fi
  if [ -n "$broker_pid" ]; then
    kill "$broker_pid" 2>/dev/null || true
    wait "$broker_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT HUP INT TERM

<launcher> service run --config <broker-config> >"<broker-log>" 2>&1 &
broker_pid=$!

attempt=0
broker_successes=0
broker_ready=false
while [ "$attempt" -lt <qualification-max-attempts> ]; do
  if <launcher> status --config <broker-config> >"<broker-status>" 2>&1; then
    broker_successes=$((broker_successes + 1))
    if [ "$broker_successes" -ge <qualification-success-count> ]; then
      broker_ready=true
      break
    fi
  else
    broker_successes=0
  fi
  kill -0 "$broker_pid" 2>/dev/null || break
  attempt=$((attempt + 1))
  sleep <qualification-poll-seconds>
done
if [ "$broker_ready" != true ]; then
  exit 1
fi

# For bootstrap, issue here, then stop before qualification:
<launcher> credential issue \
  --config <broker-config> \
  --device-id <peer-device-uuid> \
  --out <peer-staging-token-file>
kill "$broker_pid"
wait "$broker_pid"
broker_pid=

# Start a new, non-overlapping broker process for qualification:
<launcher> service run --config <broker-config> >"<broker-log>" 2>&1 &
broker_pid=$!
# Repeat the bounded broker status loop above before starting the peer.

<launcher> service run --config <peer-config> \
  --environment-file <protected-peer-environment-file> >"<peer-log>" 2>&1 &
peer_pid=$!

attempt=0
peer_successes=0
peer_ready=false
while [ "$attempt" -lt <qualification-max-attempts> ]; do
  if <launcher> status --config <peer-config> >"<peer-status>" 2>&1 &&
      grep -Fxq 'broker connection: ready' "<peer-status>" &&
      grep -Fxq 'worker sync ready: true' "<peer-status>"; then
    peer_successes=$((peer_successes + 1))
    if [ "$peer_successes" -ge <qualification-success-count> ]; then
      peer_ready=true
      break
    fi
  else
    peer_successes=0
  fi
  kill -0 "$peer_pid" 2>/dev/null || break
  attempt=$((attempt + 1))
  sleep <qualification-poll-seconds>
done
if [ "$peer_ready" != true ]; then
  exit 1
fi
```

On Windows, do not start or track `delegation-mcp.cmd`: its PID belongs to the wrapper, not the
native runtime. Resolve the native `delegation.exe` with the launcher's exact rule:
`DELEGATION_BINARY` when set, otherwise
`<delegation-home>\bin\<plugin-version>\windows-<architecture>\delegation.exe`. Verify it with
`version --json`, then start that executable directly with shell-free
`System.Diagnostics.ProcessStartInfo`. Set `FileName` to the resolved native path,
`UseShellExecute` to `$false`, and call `ArgumentList.Add()` once for each exact argv element. For
example, add `service`, `run`, `--config`, and `<broker-config>` separately; for a peer, also add
`--environment-file` and `<protected-peer-environment-file>` separately. Do not build one command
string or use a PowerShell argument-string interface, because config, environment-file, executable,
and log paths may contain spaces. Create a `System.Diagnostics.Process`, assign its `StartInfo`,
call `Start()`, and retain that native runtime process object and PID. Configure output and error
redirection on `ProcessStartInfo` when qualification logs are required, and open each log path with
a literal .NET file API instead of embedding shell redirection in an argument.

Poll by starting the same native executable with a new shell-free `ProcessStartInfo` and adding
`status`, `--config`, `<broker-config>`, and `--json` as four separate `ArgumentList` entries. Repeat
until `<qualification-success-count>` consecutive successful results or
`<qualification-timeout-seconds>` expires. For a peer, parse each successful JSON result and
require both `connectionState` equal to `ready` and `workerSyncReady` equal to `true` for the same
number of consecutive samples before the deadline. Fail immediately if the tracked runtime exits.
In `finally`, run the process-tree-scoped
`taskkill.exe /PID <native-runtime-pid> /T /F`, tolerate only the already-exited case, and
`WaitForExit()` on the tracked process object. `/PID` must be the exact native PID; never use an
image name or an unscoped kill. The `/T` cleanup and the runtime's own Windows Job Object ownership
ensure managed app-server descendants do not survive qualification.

Use this same direct-native, bounded-polling, process-tree cleanup for each Windows bootstrap
broker. Issue the credential only after the bootstrap poll succeeds, then terminate and wait for
that tree before starting the qualification broker.

On every platform, stop qualification processes before installing a native service; a second
process cannot share the broker, peer, or embedded Tailscale state lease.

A successful status command proves that the selected config matches the running process. For the
transport addition, status exposes only:

- `transport`, with value `tailscale`; and
- `tailscaleHostname`, with the configured embedded node hostname.

The surrounding broker and peer status fields remain available. No status surface exposes the
Tailscale enrollment key or its path, Tailscale state or lease paths, Delegation tokens, or provider
configuration. Peer qualification is complete only when its normal status also reports a ready
broker connection and worker synchronization.

After qualification, let the cleanup complete, confirm the recorded processes exited, and install
fresh native services with explicit configs:

```text
<launcher> service install --config <broker-config>
<launcher> service install
  --config <peer-config>
  --environment-file <protected-peer-environment-file>
```

## Unsupported Topologies And Operations

M6 does not support:

- migration between TCP and embedded Tailscale;
- in-place upgrade, downgrade, rollback, config conversion, or service replacement;
- broker federation, broker failover, active/active or active/passive high availability, or
  multiple brokers for one controller domain;
- Tailscale Funnel, Tailscale Serve, public ingress, public DNS exposure, or an Internet-facing
  reverse proxy to the embedded listener;
- system `tailscaled` integration, a tailscaled socket, or management through the Tailscale CLI;
- mixed Codex and TraeX peers, cross-CLI dispatch, or shared controller/token domains;
- shared Tailscale state between any two processes.

If any unsupported capability is required, stop and design a new checkpoint rather than extending
these commands by hand.
