# M6 Release Closure Review Rounds

## Round 1

- Base: `bc00def530c3d8eb67eafc7c5a22c17138e0ca72`
- Frozen commit: `70efdac066a6331e3e5b777d158e15eeb28a764c`
- Frozen tree: `7b7bfe5d6bac1b8ca4e56eee4fea660dcee301a5`
- Reviewer: independent local Codex `0.149.1`, read-only detached worktree
- Verdict: `CHANGES_REQUIRED`
- Executable checks:
  - `./tests/m6_support_contract_test.sh`: `PASS`
  - `git diff --check`: `PASS`

Findings and disposition:

1. The setup skill and native-service reference still directed supported-platform operators toward
   in-place native-service replacement after a runtime or environment-file path change.
   - Disposition: fixed by requiring a fresh named deployment instead.
2. The support contract test did not assert every supported matrix row, used an incomplete
   Windows TraeX string, and did not reject the contradictory service-replacement guidance.
   - Disposition: fixed with complete matrix assertions, exact unsupported wording, fresh-only
     assertions, and a negative assertion for the old replacement text.

Any tracked fix requires Round 2 on a new frozen commit and tree.

## Round 2

- Base: `bc00def530c3d8eb67eafc7c5a22c17138e0ca72`
- Frozen commit: `06be492014f24cfda16ec7f3e8a4864d1770ef95`
- Frozen tree: `3d5b539f170b7b88c341f5b94db9dfc476a55500`
- Reviewer: independent local Codex `0.149.1`, read-only detached worktree
- Verdict: `CLEAN`
- Executable checks:
  - `./tests/m6_support_contract_test.sh`: `PASS`
  - `/bin/sh -n tests/m6_support_contract_test.sh`: `PASS`
  - `git diff --check bc00def530c3d8eb67eafc7c5a22c17138e0ca72..06be492014f24cfda16ec7f3e8a4864d1770ef95`:
    `PASS`
  - Copied negative fixture changing `Windows TraeX is unsupported.` to a supported claim:
    contract test failed as required

No actionable findings remained. The M6 release-closure checkpoint is accepted at the Round 2
frozen commit and tree.
