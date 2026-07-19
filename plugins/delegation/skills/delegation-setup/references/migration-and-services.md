# Migration And Services

Schema v3 makes plaintext non-loopback acknowledgement consistent for every auth mode and role.
Schema v1 and v2 are not migrated in place. Back up and move an old broker config aside, then rerun
`setup broker` with its controller ID, listener, auth mode, actual state database, and non-loopback
acknowledgement. Do not accept changed defaults or create a fresh state database accidentally.

Schema v1 used a shared bearer token. Never reuse it as the private M1 broker master. Move
the old config and token file to an offline backup, then verify the selected new master-token path
does not exist. Omit `--token-file` only when the default path is confirmed absent, or pass a
different nonexistent path so setup creates a fresh protected master. Then issue fresh per-device
credentials. Schema v2 brokers may retain their existing protected M1 master-token path.

For a schema v1 controller or device, preserve only non-secret identity, name, endpoint, and auth
mode. Its old shared token is not a device-bound M1 credential: enroll a fresh credential at the M1
broker, transfer the new protected token file, and pass that path to target setup. Never reuse or
print the old token. Schema v2 targets may retain their protected token path while rerunning setup
with all explicit settings.

M0 Windows homes used inherited profile-directory ACLs and need an offline re-home. Stop the service,
rename the entire old home outside the new home, then install and configure an empty protected home.
Do not point new config, token, state, or service paths into the backup. Recreate broker trust state
and per-device credentials. Remove the backup only after `doctor` and every device pass.

Run `service install --config <path>` to write, enable, start, and verify the current platform user
service. It refuses foreign definitions and managed definitions with a different executable or config
path. Treat `indeterminate` as partial activation requiring native service-manager inspection.

Linux requires a systemd user manager. macOS needs the current user's GUI launchd domain. Windows
needs an interactive login. M1 does not migrate versioned runtime paths in place: verify that the old
definition is Delegation-owned, remove it through the native service manager, and reinstall. Windows
restart-on-failure remains deferred to the reliability milestone.
