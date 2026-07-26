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
  commit. A checkpoint must expose an end-to-end behavior with an executable acceptance test or
  smoke path.
- At each checkpoint, obtain an independent, read-only agent review of the complete checkpoint
  commit range and current tree. Fix and re-review until no actionable findings remain before
  declaring the checkpoint accepted. Intermediate commits do not require separate reviews.
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
  or rescope options. The checkpoint remains unaccepted until a human records the disposition.
- Treat a review finding as actionable only when it has a concrete, reachable trigger in an
  ordinary supported user workflow and a material correctness, security, data-loss, compatibility,
  or operability impact. The reviewer must state that trigger and impact.
- Record deliberately corrupted protected state, hostile same-UID protocol forgery, manual edits to
  internal files, unrealistic resource extremes, and similarly contrived invariant violations as
  residual risks instead of extending the fix/review loop. Escalate one only when it crosses an
  explicitly promised security boundary or has a real-world reproduction.
- Use `feat/<topic>`, `bugfix/<topic>`, `refactor/<topic>`, `test/<topic>`, `docs/<topic>`,
  `chore/<topic>`, or `release/<version>` for parallel implementation branches. Do not include
  personal names, device identifiers, or task identifiers in branch names.

## Phase Boundaries

- Complete, review, and report the active milestone before starting the next milestone.
