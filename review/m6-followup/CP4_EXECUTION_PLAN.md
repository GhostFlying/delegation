# Checkpoint 4 Execution Plan

Base: `9d4b444292a32e5a2b9ef83a16e9f489e79da582`

1. Add one protected, atomically replaced local-upgrade journal per managed
   role/instance. Validate every immutable identity and state transition, keep
   `commitAuthorized` monotonic, and expose only a bounded non-secret snapshot.
2. Add stopped-service database preparation and reconciliation: acquire the
   existing broker/peer lease, checkpoint WAL, copy into same-directory shadow
   and rollback files, migrate and validate the shadow, then switch or restore
   by digest without mutating the canonical database in place.
3. Extend `userservice` with read-only exact-owned-definition inspection and
   activation operations for systemd user units, launchd LaunchAgents, and
   current-user Windows Scheduled Tasks. Fence exact recorded process identity,
   atomically replace only owned definitions, and reconcile response loss.
4. Implement a resumable one-shot activator. Revalidate the protected journal
   before each idempotent substep. No service or database mutation occurs before
   durable commit authorization; pre-authorization cancellation only removes the
   one-shot definition. After authorization, enter forward-only recovery on any
   failure. The activator consumes authorization and never invents a controller-wide
   CP5 COMMIT decision.
5. Extend the existing protected local bridge and CLI with prepare/resume,
   cancel, hidden activation, bootstrap restrictions, and bounded upgrade
   status. Bind qualification to exact role/instance, target version/runtime
   digest, config digest, and a new CP2 readiness epoch. Do not add remote HTTP
   upgrade methods.
6. Test journal crash points, database WAL/shadow/switch/recovery material, all platform
   definition/process contracts with fakes, CLI/local transport restrictions,
   and pre/post-COMMIT recovery. Run focused race, full suite, vet, and compile
   checks for Linux amd64, macOS arm64, and Windows amd64 using Go 1.26.5.

## Round 1 Finding Disposition Plan

The frozen round-1 review found two actionable gaps. Before freezing round 2:

1. Make the alpha.4 bootstrap path consume exactly the known schema-3 TCP
   configuration, validate it as an alpha.4 document, and derive a schema-4
   target configuration by adding only the explicit TCP transport. Record the
   source/target config identities and protected same-directory shadow and
   rollback paths in the journal. The activator must prepare and atomically
   switch this config after stopping the service, reconcile every crash point,
   and retain the source bytes for forward recovery. Current-schema upgrades
   keep one identical source/target configuration.
2. Accept only the known alpha.4 database identities (broker 19, peer 15) or
   the exact target identity. On the stopped shadow copy, apply only the audited
   one-step schema addition for broker 19-to-20 or peer 15-to-16, set the target
   user version, checkpoint, and validate before switching. Reject every other
   source identity or version gap without touching canonical state.
3. For peers, capture the source readiness epoch during preparation. Target
   qualification must read one identity-bound local status snapshot and require
   the target runtime version, ready connection/lifecycle synchronization,
   dispatchability, a strictly newer ready readiness epoch, and matching target
   runtime/config digests. Broker qualification remains role-local and binds
   role, instance, version, runtime digest, and target config digest; CP5 owns
   controller-wide peer reconnection after the broker switches.
4. Add real alpha.4 config/database fixtures, config and database switch crash
   coverage, unsupported-gap rejection, readiness pending/failure/stale-epoch/
   digest/sync rejection, and successful peer/broker qualification tests.
