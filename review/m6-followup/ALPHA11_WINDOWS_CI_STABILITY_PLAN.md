# Alpha.11 Windows CI Stability Checkpoint

## Trigger

The frozen alpha.11 tree passed the Windows native job twice on the pull-request branch, but the
post-merge run and its failed-job retry each timed out in different packages. The first run timed
out across broker runtime and worker-host tests; the retry passed those packages and timed out in
a broker heartbeat integration test. These tests all pass on the same source tree when the runner
is not saturated.

## Change

- Keep Linux and macOS native test behavior unchanged.
- Run the Windows package test phase with `go test -p=1` so process-heavy packages do not compete
  for the hosted runner at the same time.
- Do not change production code, test deadlines, platform support, runtime bytes, or the alpha.11
  release manifest.

## Acceptance

1. Run the full Windows native Go suite with `-p=1` in a fresh Kross Windows workload.
2. Freeze the exact commit and tree and obtain an independent read-only review.
3. Merge through a protected-main pull request and require the complete main CI, including the
   serialized Windows native job, to pass before tagging alpha.11.
4. Rebuild and compare the six release artifacts against the reviewed alpha.11 manifest to prove
   that the workflow-only commit did not alter release bytes.
