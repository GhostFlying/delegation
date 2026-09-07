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
- Write set: `plugins/delegation/scripts/install-runtime`, `plugins/delegation/scripts/delegation-mcp`,
  POSIX plugin regression tests, this checkpoint evidence file, and review-round evidence after the
  revision is frozen.
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
- Prove the warm launcher also validates an already-present runtime layout and does not bypass the
  installer checks or redownload a valid private installation.
- Preserve concurrent-install, checksum, archive-shape, symlink, and exact-version coverage.
- Run shell syntax checks, `git diff --check`, and the complete Go test suite.
- Freeze an exact commit and tree, then obtain an independent read-only review in a clean detached
  worktree. Any tracked fix consumes another review round; do not start a fourth round.

## Review Rounds

### Round 1

- Base commit: `4d4c42fa09c925e0b250dd3566343e2d34a254fc`.
- Frozen commit: `32c565feb94c56c44af03931480eaa093a6869c5`.
- Frozen tree: `b22bddda7ceaebcb6a32b0634030dc756611af02`.
- Review checkout: clean detached worktree at the frozen commit and tree.
- Independent review result: `CHANGES_REQUIRED`.
- Executable checks: POSIX plugin contract, full Go suite, `/bin/sh` and `dash` syntax checks, and
  `git diff --check` passed.

Finding and disposition:

1. The POSIX launcher invoked `install-runtime` only when the runtime binary was absent. An updated
   plugin therefore continued to run a binary from the canonical alpha.8 `0755` cold-install
   layout, bypassing the new directory validation and preserving the later service-upgrade failure.
   - Disposition: fixed by making every non-override launcher invocation run the idempotent
     installer validation path. A valid private warm installation performs no download; a legacy
     broad layout fails closed before execution or download. Regression coverage now exercises both
     warm cases.

The tracked fix requires Round 2 on a new frozen commit and tree.
