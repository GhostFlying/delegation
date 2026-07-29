# Delegation Engineering Guide

## Development

- Use Go 1.26.5 and keep Linux, macOS, and Windows behavior covered.
- Prefer the standard library and small, established dependencies with a concrete need.
- Run `gofmt` on Go changes and `go test ./...` before review.
- Keep tracked files free of credentials, private endpoints, machine-specific paths, session IDs,
  personal identifiers, and company-only information.

## Git Workflow

- Use English Conventional Commit titles and keep each commit focused on one behavior.
- Include the implementation, focused tests, and required schema or documentation updates in the
  same commit.
- Keep commits atomic, but review at runnable functional checkpoints instead of reviewing every
  commit. Each checkpoint must cover one independently understandable and reversible behavior with
  an executable acceptance test or smoke path. Split a checkpoint when it contains independently
  acceptable behaviors or cannot be reviewed as one focused change.
- Do not cap useful checkpoint concurrency. Run independent checkpoints in parallel according to
  their dependencies and write sets, give each checkpoint one owner and writable worktree, and
  serialize updates to the integration branch.
- Freeze each checkpoint at an exact commit and tree before review. Obtain an independent,
  read-only agent review of the complete checkpoint range in a clean detached worktree. Reviewers
  must not edit the reviewed worktree or review a moving branch. Accept only when both reviews are
  clean and executable acceptance passes; any tracked fix requires a new frozen revision and review
  round.
- Also run a read-only local Claude Code review of each frozen checkpoint with `claude -p` and
  `claude-opus-5`. Use the highest available effort, trying `--effort max` first and `xhigh` only
  when the installed CLI rejects `max`. Record the exact model, effort, unavailable model, failed
  invocation, or fallback explicitly; this supplements rather than replaces the independent agent
  review.
- Treat one review round as one frozen checkpoint submitted to both the independent agent and
  Claude review, followed by disposition of that round's findings. Record the round number and the
  exact frozen diff or commit range.
- Do not start a fourth review round for the same checkpoint. If actionable findings, recurring
  findings, or reviewer disagreement remain after three rounds, stop automated review and request
  human intervention with the unresolved findings, risk assessment, and concrete accept, defer,
  or rescope options. Use the same human path when a later rebase would require review after round
  three. The checkpoint remains unaccepted or unintegrated until a human records the disposition.
- Treat a review finding as actionable only when it has a concrete, reachable trigger in an
  ordinary supported user workflow and a material correctness, security, data-loss, compatibility,
  or operability impact. The reviewer must state that trigger and impact.
- Record deliberately corrupted protected state, hostile same-UID protocol forgery, manual edits to
  internal files, unrealistic resource extremes, and similarly contrived invariant violations as
  residual risks instead of extending the fix/review loop. Escalate one only when it crosses an
  explicitly promised security boundary or has a real-world reproduction.
- After a rebase, compare the patch and rerun focused acceptance. If the comparison shows a semantic
  change, or the rebase has a conflict or needs a manual semantic adjustment, freeze the resulting
  commit and review it in a new detached worktree before integration.
- Validate the combined integration tree and review the final milestone at a frozen commit and tree.
  Turn actionable holistic findings into new small checkpoints rather than editing the reviewed
  tree.
- For documentation or process checkpoints, prose review is not acceptance evidence; run a focused
  executable consistency or smoke check. Treat token, duration, log-size, and validation-count
  measurements as observations, never as reasons to reduce testing, review, security, or
  cross-platform coverage.
- Follow [the lightweight checkpoint workflow](docs/checkpoint-workflow.md) for multi-checkpoint
  work. Keep raw logs outside tracked files and send agents compact task and result summaries.
- Use `feat/<topic>`, `bugfix/<topic>`, `refactor/<topic>`, `test/<topic>`, `docs/<topic>`,
  `chore/<topic>`, or `release/<version>` for parallel implementation branches. Do not include
  personal names, device identifiers, or task identifiers in branch names.

## Phase Boundaries

- Complete, review, and report the active milestone before starting the next milestone.
