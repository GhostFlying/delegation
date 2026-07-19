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
- Also run a read-only local Claude Code review of each frozen checkpoint with `claude -p`, trying
  `--model claude-fable-5` first. Record any unavailable model, failed invocation, or fallback
  explicitly; this supplements rather than replaces the independent agent review. Fix actionable
  findings and rerun the Claude review until it is clean.
- Use `feat/<topic>`, `bugfix/<topic>`, `refactor/<topic>`, `test/<topic>`, `docs/<topic>`,
  `chore/<topic>`, or `release/<version>` for parallel implementation branches. Do not include
  personal names, device identifiers, or task identifiers in branch names.

## Phase Boundaries

- Complete, review, and report the active milestone before starting the next milestone.
- Do not add M2 managed-worker behavior until the M1.1 peer topology and its pre-release v1 cleanup
  have been independently reviewed and accepted.
