# Alpha.8 Windows CI Follow-up

## Trigger

Protected-main CI for the reviewed alpha.8 release boundary passed Linux, macOS, plugin, race,
cross-compilation, and deterministic packaging, but the native Windows job exposed test-fixture
assumptions that do not hold on a real inherited Windows temporary directory. The production
security checks correctly reject those inherited DACLs. The same job also exposed one
millisecond-scale app-server close-contract assertion whose caller deadline can expire before the
forced-termination branch starts on a loaded Windows runner.

## Checkpoint

- Owner: main integration thread.
- Base: reviewed alpha.8 manifest commit `45ad6b46f86a6d807d4c22fc4321f183f6c8c289`.
- Write set: Windows-facing test fixtures, platform-specific installed-runtime permission validation,
  and this evidence file.
- Behavior: create security-sensitive test roots through the production current-user-only directory
  primitive, keep intentionally unsafe fixtures explicit, and make the close-contract test
  synchronize on forced termination before applying its bounded unconfirmed-exit deadline.
- Non-goals: no weakening of production DACL, protected-file, runtime-publication, process
  ownership, protocol, persistence, upgrade, or release verification behavior.

The native Kross rerun also showed that the test-only app-server launcher used a two-second
handshake deadline, substantially shorter than the thirty-second production default, and that two
TraeX upgrade fixtures attempted to exercise an explicitly unsupported Windows host. The test
launcher now keeps a bounded ten-second handshake/fifteen-second startup budget, while Windows
skips only those TraeX-specific cases. No production timeout or host support contract changes.

The full Windows suite additionally exposed a changes-artifact acknowledgement test whose
twenty-five-millisecond call budget could expire while the local broker response was being
scheduled, closing the session before the intended local acknowledgement timeout. That test keeps
the production retry path but uses a bounded half-second call budget so it exercises the intended
failure rather than runner scheduling latency.

The next full-suite run exposed the same scheduling assumption in the three local-bridge readiness
fixtures: their one-second probe contexts were shorter than the production ten-second service
readiness window and could expire while a Windows Named Pipe frame was being scheduled. The
fixtures now use a shared five-second bound; production readiness and local-bridge deadlines remain
unchanged.

The archive format continues to require exact `0755` and `0644` modes on every platform. After
extraction, POSIX installations require those exact modes, while Windows installations require
each file to remain a current-user-only protected file under the protected staging directory.

## Acceptance

- Reproduce the original failures on a native Kross Windows workload using Go 1.26.5 and the exact
  reviewed commit.
- Run every affected package test and the full native Windows suite after the fix.
- Run the focused close-contract test repeatedly and under the race detector where supported.
- Run the full Linux suite, race suite, vet, format, plugin contract, M6 support contract, and all
  six ordinary and `integration,live` compile-only targets.
- Rebuild all six release archives twice, require byte identity, and verify both complete release
  directories. Freeze the source checkpoint and use its generated manifest to create a separate
  direct-child commit that changes only `plugins/delegation/release-artifacts.sha256`.
- Obtain independent read-only reviews of both the frozen source checkpoint and the recreated
  direct-child source-to-manifest boundary before updating the protected-main pull request.
