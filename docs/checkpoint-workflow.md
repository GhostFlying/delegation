# Lightweight Checkpoint Workflow

Use this workflow for milestones that benefit from multiple agents or parallel work. Keep it
lightweight: the goal is to make checkpoints small and reviews stable, not to build a second project
management system.

## 1. Plan Small Checkpoints

Define each checkpoint by one behavior, its owner, dependencies, write set, and executable
acceptance path. A checkpoint is too large when parts of it could be accepted or reverted
independently, or when its review requires several unrelated explanations.

There is no fixed concurrency limit. Start independent checkpoints in parallel when their
dependencies and write sets permit it. Give each checkpoint one writable worktree and serialize
updates to the milestone integration branch.

Give workers only the objective, base revision, write set, dependencies, acceptance commands, and
required result summary. Store complete command output outside tracked files and return concise
results with the command, exit status, and artifact path.

## 2. Implement And Freeze

Implement, fix findings, rebase, and validate only in the checkpoint's writable worktree. Before a
review round:

1. run focused acceptance and require a clean worktree;
2. record the base commit, head commit, and tree hash;
3. create a separate detached worktree at that head; and
4. verify that the detached worktree is clean and has the recorded tree.

Any tracked fix creates a new frozen revision and consumes another review round.

## 3. Review The Frozen Revision

In each round, an independent read-only agent and local Claude Code review the same complete
checkpoint range and frozen tree. Neither reviewer edits the worktree, delegates review, or follows
a branch that can move.

A finding is actionable only when it includes an ordinary supported user path and a material
correctness, security, data-loss, compatibility, or operability impact. Record contrived invariant
violations as residual risk unless they cross a promised security boundary or have a real
reproduction.

Accept when both reviewers are clean and executable acceptance passes. After any tracked fix, review
the new frozen revision. Do not start round four: after three non-converged rounds, stop and request
a human decision to accept, defer, or rescope. Use the same human path when a later rebase would
require review after the checkpoint has exhausted its three-round budget.

## 4. Rebase And Integrate

The checkpoint owner rebases onto the latest integration head in the writable worktree. After a
rebase, compare the old and new patch and rerun focused acceptance. A no-op or mechanical
no-conflict rebase does not need another checkpoint review. Any semantic patch difference requires
a new frozen review even when Git reports no conflict.

If the rebase has a conflict or needs a manual semantic adjustment, the owner resolves it in the
checkpoint worktree, validates it, and freezes the resulting commit for review in a new detached
worktree. Never resolve the conflict while reviewing or directly in the integration checkout.

Fast-forward the integration branch one checkpoint at a time. Verify the expected tree and run
combined-tree validation before integrating the next checkpoint.

## 5. Review The Milestone

After all checkpoints integrate and combined validation passes, freeze the milestone commit and
tree. Review that exact tree once with both reviewers for cross-checkpoint interactions and
end-to-end behavior.

Turn actionable holistic findings into new small checkpoints with their own implementation,
acceptance, and frozen review. Do not edit the frozen milestone review worktree. If three milestone
review sequences do not converge, request a human decision instead of starting a fourth.

## 6. Keep Evidence Proportional

For each checkpoint, retain only enough evidence to reconstruct:

- its objective, owner, dependencies, write set, and base;
- each frozen commit, tree, review round, reviewer result, and finding disposition;
- focused acceptance commands and results;
- rebase or conflict-review evidence when applicable; and
- integration and combined-validation results.

Documentation and process checkpoints need an executable consistency or smoke check; prose review
alone is insufficient. Metrics such as token use, elapsed time, log size, review rounds, or
validation count are observational. They must not reduce testing, review depth, security checks, or
cross-platform coverage.
