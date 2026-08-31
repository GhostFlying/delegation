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
