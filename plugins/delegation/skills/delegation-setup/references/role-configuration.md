# Role Configuration

Run the plugin launcher with `setup <role> --help` before writing configuration.

- `setup broker` creates a trust domain and listener. Token authentication is the default and
  creates a protected token file when none is supplied. Keep plaintext listeners on loopback unless
  the user explicitly accepts `--allow-insecure-nonloopback` behind an encrypted private network or
  tunnel. Use `--state` during setup only when the default state path is unsuitable.
- `setup controller` configures a device that hosts root tasks. Supply its broker URL, controller
  ID, broker-assigned device ID, and protected token file. Prefer `wss://`; non-loopback `ws://`
  needs the same explicit protected-network acknowledgement.
- `setup device` configures a worker-only device with the same identity, token, and transport rules.
  Setup generates a device ID only when authentication mode is `none`.

For token authentication, enroll every target from the configured broker before target setup:

1. Choose a new stable device UUID and credential role, `controller` or `device`.
2. On the broker, run `credential issue --config <broker-config> --role <role> --device-id
   <device-uuid> --out <protected-staging-token-file>`.
3. Transfer the token file over an authenticated encrypted channel and preserve or restore
   current-user-only protection at its final absolute path. Never paste token contents into chat,
   shell arguments, configuration, or a URL.
4. On the target, run `setup controller` or `setup device` with the broker controller ID, exact
   issued device ID, and `--token-file <protected-target-token-file>`. Run `doctor`; remove transfer
   copies only after verification succeeds.
5. Revoke a lost or retired credential with `credential revoke --config <broker-config> --device-id
   <device-uuid>`. A revoked ID cannot be reissued; replacement needs a new identity.

After setup, `service run --config <path>` runs the configured role in the foreground.
