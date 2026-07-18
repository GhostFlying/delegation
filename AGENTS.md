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
- Use `feat/<topic>`, `bugfix/<topic>`, `refactor/<topic>`, `test/<topic>`, `docs/<topic>`,
  `chore/<topic>`, or `release/<version>` for parallel implementation branches. Do not include
  personal names, device identifiers, or task identifiers in branch names.

## Phase Boundaries

- Complete, review, and report the active milestone before starting the next milestone.
- Do not add M1 broker or device-control behavior while working on the M0 plugin and release
  scaffold.
