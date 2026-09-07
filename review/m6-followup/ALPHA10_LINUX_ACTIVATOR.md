# Alpha.10 Linux Upgrade Activator Checkpoint

## Objective

Repair the ordinary Linux systemd user-service upgrade path discovered during the first canonical
alpha.7-to-alpha.8 bootstrap. `systemctl --user link` loads the activator through a canonical link
under the user's systemd configuration directory, so systemd reports that link as `FragmentPath`
rather than the protected transaction definition that it targets. The released implementation
compared `FragmentPath` directly with the transaction definition and therefore rejected every real
Linux activator before ARM. Its cleanup also used `systemctl revert`, which does not remove a unit
created by `systemctl link`.

The observed transaction `995fddd0-6598-40c0-af32-2068d643b859` remained `prepared` with
`commitAuthorized=false`; the alpha.7 peer service, configuration, and database were not switched.

## Implementation Plan

1. Resolve the one canonical persistent user-unit link path for the activator name. Respect an
   absolute `XDG_CONFIG_HOME`; otherwise use the current user's `.config/systemd/user` directory.
2. Accept an installed activator only when systemd reports that canonical path, `UnitFileState` is
   exactly `linked`, there are no drop-ins, and the path is a current-user-owned symbolic link whose
   absolute target is exactly the protected transaction definition.
3. Remove an exact linked activator with `systemctl --user disable`, reload the manager, and require
   the unit to become absent. Never remove a regular file, a foreign target, a shadowed fragment, or
   a unit with drop-ins.
4. Recover the crash window where the canonical link exists before `daemon-reload`, and treat a
   failed `disable` as successful only when the exact link is already absent before the mandatory
   reload and final absence check.
5. Update unit tests to model real `systemctl link` semantics and cover link-target, owner, fragment,
   state, drop-in, and configuration-home mismatches. Add an opt-in real systemd user-manager
   link/install/remove round-trip test.
6. Run the focused tests and the real round trip, then the full Go, race, vet, plugin, support, and
   cross-platform compile acceptance. Freeze the exact source commit and tree for independent
   read-only review before publication.

## Release And E2E Disposition

Alpha.8 and alpha.9 cannot be Linux coordinated-upgrade sources because ARM executes this broken
source-version path. Publish the repair as canonical alpha.10, use its CLI for explicit peer-first
and broker-last local bootstrap, then publish a strictly newer alpha.11 and exercise the first real
broker-coordinated transaction from alpha.10 to alpha.11. Do not claim alpha.8-to-later coordinated
upgrade acceptance.
