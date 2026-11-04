# Project Status

## Milestone Disposition: 2026-11-04

| Item | Disposition | Evidence boundary |
| --- | --- | --- |
| M6 | CLOSED | Historical technical acceptance remains UNBLOCKED; the project owner deferred the additional live-upgrade campaign. |
| Current published runtime | 0.1.0-alpha.12 | Release commit `ec291b9f8e24e6dab7c07e4e969ff57635bf3917`; official publication, archive checksums, and Sigstore provenance verified. |
| Current CLI/model baseline | Recorded | See the [CLI acceptance baseline](cli-compatibility.md); real Sol execution passed on the identified Linux candidate, separately from published upgrade acceptance. |
| M7 | IN PROGRESS: scope and acceptance defined; runtime implementation pending | [Cross-CLI collaboration and capability alignment](m7-cross-cli-collaboration.md) between Codex and TraeX. M6 closure review and integration completed first. |

The [M6 closure record](m6-release-closure.md) preserves historical supported-platform acceptance
and the current owner disposition. Closure is a scope decision, not a claim that every subsequent
release or environment combination passed live acceptance.

## Deferred Acceptance

| Follow-up | Retained result | Disposition |
| --- | --- | --- |
| Published alpha.11 to alpha.12 coordinated upgrade | Previous attempt BLOCKED during preparation; no global COMMIT; source stayed alpha.11. | DEFERRED; a fresh retry has NOT_RUN. |
| Post-upgrade Sol code-mode execution and durable result | NOT_RUN on an upgraded deployment. | DEFERRED until a successful coordinated upgrade. |
| Retained-thread cold resume after the published upgrade | NOT_RUN. | DEFERRED until a successful coordinated upgrade. |
| Current-baseline Windows 11 logged-in-user qualification | SKIP: no suitable logged-in machine. | Keep separate from Windows container/CI evidence. |
| Current-baseline macOS TraeX real-account qualification | SKIP: no isolated account provisioned. | Keep separate from historical macOS acceptance. |

Official-release acquisition through the unchanged production Go path passed in an alternate
network environment. That diagnostic provides no coordinated transaction, target activation,
retained-worker migration, or post-upgrade execution evidence. No retry or new remote deployment is
needed to close M6 under the owner's decision.

These follow-ups are outside M7 unless explicitly selected later. Windows TraeX remains unsupported.
M7 selection does not change the current homogeneous-host-kind network contract; implementation
requires its own design, checkpoints, executable acceptance, and frozen independent review.
