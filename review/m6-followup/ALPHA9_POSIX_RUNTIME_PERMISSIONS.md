# Alpha.9 POSIX Runtime Permission Follow-up

## Trigger

The canonical alpha.8 POSIX plugin cold-start path installed the immutable version and platform
runtime directories with the caller's ordinary umask, producing mode `0755` under a private
Delegation home. The canonical service-upgrade runtime acquisition path requires every managed
runtime directory to be owned by the current user with mode `0700`, so an ordinary launcher cold
install could make a later service upgrade fail before PREPARE.

## Checkpoint

- Owner: main integration thread.
- Base: merged canonical alpha.8 commit `4d4c42fa09c925e0b250dd3566343e2d34a254fc`.
- Write set: `plugins/delegation/scripts/install-runtime`, POSIX plugin regression tests, this
  checkpoint evidence file, and review-round evidence after the revision is frozen.
- Behavior: create every managed runtime parent, version, platform, and staging directory with
  current-user-only permissions; validate existing managed directories before reuse; never repair
  or take over an existing directory with broader permissions, a different owner, or a symlink.
- Compatibility: already complete immutable runtime installations remain reusable only when their
  managed directories satisfy the same private-directory contract as service upgrade acquisition.
- Non-goals: no service mutation, no permission-model relaxation, no release-version bump, no
  manifest refresh, and no change to Windows installation behavior.

## Acceptance

- Run the POSIX plugin contract test on Linux and require explicit fresh-install mode checks for
  the `bin`, version, platform, and staging directories.
- Prove pre-existing `0755` `bin`, version, and platform directories fail closed without chmod or
  content mutation.
- Preserve concurrent-install, checksum, archive-shape, symlink, and exact-version coverage.
- Run shell syntax checks, `git diff --check`, and the complete Go test suite.
- Freeze an exact commit and tree, then obtain an independent read-only review in a clean detached
  worktree. Any tracked fix consumes another review round; do not start a fourth round.
