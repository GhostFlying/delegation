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
- Before every non-documentation commit, stage the intended diff and obtain an independent,
  read-only agent review. Fix and re-review until no actionable findings remain, then commit.
- A documentation-only exemption applies only when every changed file is prose or documentation
  media. Manifests, configuration, scripts, CI, schemas, lockfiles, and tests require review.
- Use `feat/<topic>`, `bugfix/<topic>`, `refactor/<topic>`, `test/<topic>`, `docs/<topic>`,
  `chore/<topic>`, or `release/<version>` for parallel implementation branches. Do not include
  personal names, device identifiers, or task identifiers in branch names.

## Phase Boundaries

- Complete, review, and report the active milestone before starting the next milestone.
- Do not add M1 broker or device-control behavior while working on the M0 plugin and release
  scaffold.
