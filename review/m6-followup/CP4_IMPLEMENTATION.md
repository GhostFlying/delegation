# Checkpoint 4: Local Atomic Service Upgrade

## Objective

Add a crash-consistent, resumable local upgrade transaction for an already managed Delegation
broker or peer service on systemd user services, launchd LaunchAgents, and Windows Scheduled Tasks.
The transaction changes only the immutable runtime binary path; it preserves the exact
configuration, environment file, instance, role, user identity, and native service name.

Checkpoint 4 owns local prepare, activation, cancellation, forward recovery, and status. Checkpoint
5 will own controller-wide drain, participant selection, the global COMMIT decision, peer-first
ordering, and broker-last coordination.

## Base And Ownership

- Base commit: `9d4b444292a32e5a2b9ef83a16e9f489e79da582`
- Base tree: `fc7366d25e167d2c8e755cf281bf74564d223817`
- Writable owner: `feat/m6-local-atomic-upgrade`
- Writable worktree: the dedicated `feat/m6-local-atomic-upgrade` worktree
- Review will use a separate clean detached worktree at an exact frozen commit and tree.

## Durable Contract

One protected journal is authoritative for one role/instance/target-version transaction. Retrying
the same target resumes that journal. A different target is rejected while a non-terminal journal
exists. Journal transitions are atomic, current-user-only, and directory-synced:

```text
prepared -> armed -> activating -> started -> qualified -> committed
                         |             |            |
                         +-------------+------------+-> forward_recovery_required
```

The journal records source and target versions/digests, config identity, immutable invocation, old
and new service-definition digests, database kind and paths, platform/architecture, activation
substeps, commit authorization, bounded failure code, and timestamps. It never stores config
contents, environment values, credentials, or raw service-manager output.

The local transaction remains cancellable while it is `prepared` or `armed`. Legacy rollback states
remain readable for journal-format compatibility, but the current manager never starts service or
database mutation before authorization. Activation writes
`commitAuthorized` durably before launching the independent activator, so every service or database
mutation occurs on the forward-only side of that boundary. Once authorization is durable, rollback
to the source runtime is forbidden; failures become `forward_recovery_required` and retries continue
toward the target. Checkpoint 5 will bind this same local authorization bit to its global COMMIT
decision.

## Prepare

Preparation must fail closed unless all of the following hold:

- the current native definition is owned by the exact role/instance and is enabled/running;
- its executable, config path, environment path, native identity, and current user match the
  requested invocation;
- the target runtime is a strictly newer canonical GitHub release verified through
  `internal/releaseverify`, and its extracted binary reports the exact target version;
- the new service definition differs only in the binary path;
- config and platform are supported, including explicit Windows TraeX rejection;
- broker/peer database schema, managed home, and embedded-Tailscale generation are compatible;
- no occupied worker, pending operation, workspace transfer, result transfer/finalization, or
  other upgrade-blocking durable work exists.

Preparation writes immutable target material, renders the new definition, snapshots the exact old
definition, and creates the journal. Arming installs a platform-specific one-shot activator which
invokes the target runtime independently of the old service process. Neither step stops or mutates
the live service definition or canonical database.

## Activation

The one-shot activator reopens and validates the protected journal and reconciles durable state
before every action. It then:

1. stops only the recorded managed role/instance and waits for its exact process tree to exit;
2. acquires the role database lease, checkpoints WAL, creates and verifies a same-filesystem shadow
   database, and retains the old database as rollback material;
3. atomically replaces only the recorded owned service definition;
4. atomically switches the canonical database to the verified shadow;
5. starts the recorded service and checks local role/instance identity, target version, and target
   runtime digest;
6. records `started`, performs bounded local qualification, records `qualified`, and commits;
7. retains rollback material until terminal journal durability and removes the one-shot activator.

Every filesystem or manager response-loss point is reconciled from the definition bytes, database
digests, journal flags, exact process identity, and local status before another mutation.

## Platform Rules

- systemd: exact user-unit path, empty drop-ins, exact ExecStart, MainPID/ControlPID/cgroup stop,
  daemon-reload, start, and bounded local status qualification.
- launchd: exact GUI-domain label/path/program/argv, bootout with bounded unload/PID wait, atomic
  plist replacement, bootstrap/kickstart, and bounded local status qualification.
- Windows: exact current-user task path/SID/action, exact running engine process tree, bounded End
  and exact-PID cleanup, refusal to overwrite an occupied task, Run, and named-pipe status
  qualification.

## Database Rules

- Stop the service before checkpoint, copy, migration, or switch.
- Use a same-directory shadow and rollback copy; never migrate the canonical file in place.
- Include committed WAL data by checkpointing before the copy.
- Validate application ID, supported source version, target migration, foreign keys, and
  `integrity_check` on the shadow.
- Keep the original database and old definition as recovery evidence until commit. Cancellation is
  allowed before authorization, when neither has been switched; after authorization recovery is
  forward-only.
- Reject unsupported schema gaps without touching the live definition or canonical database.

## Public And Internal Surfaces

- A protected per-service Unix socket or Windows named pipe is the only running-service management
  channel. No broker listener or remote HTTP endpoint gains upgrade methods.
- `delegation service upgrade --config PATH --target-version VERSION [--environment-file PATH]
  [--bootstrap] [--timeout 30m] [--json]` prepares/resumes a local transaction. Peer configs require
  explicit bootstrap or the later broker coordinator; broker coordination is completed in CP5.
  Bootstrap discovers the old executable and verifies the exact current-user native definition and
  running process locally. It therefore supports the alpha.4 transition without requiring that
  legacy service to speak the current local-bridge protocol or expose upgrade RPCs.
- `delegation service upgrade --cancel --config PATH --transaction-id UUID` works only before
  `commitAuthorized`.
- A hidden activator entrypoint accepts only a protected journal under the canonical upgrade root.
- `status --json` gains a bounded, non-secret local upgrade snapshot.

## Acceptance

- Unit tests cover journal validation/atomicity/idempotence and every transition/crash point.
- Database tests cover WAL checkpoint/shadow copy/integrity, switch reconciliation, rollback-material
  validation, and unsupported schema rejection.
- Platform tests cover exact definition ownership, executable-only replacement, process-tree stop,
  target start/qualification, cancellation before authorization, and forward-only behavior.
- CLI/management tests cover protected local transport, same-target resume, conflicting target,
  cancellation boundary, Windows TraeX rejection, bounded status, and no remote HTTP surface.
- Run focused race tests, the complete suite, vet, and Linux amd64/macOS arm64/Windows amd64
  compile-only validation with Go 1.26.5 before freezing.
