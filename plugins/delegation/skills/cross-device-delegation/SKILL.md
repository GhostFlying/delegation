---
name: cross-device-delegation
description: Delegate a bounded Codex subtask to an appropriate trusted peer and collaborate with the resulting managed worker. Use for platform-specific builds or validation, advertised runtime features, self-targeted isolation, or any task whose required operating system or environment differs from the root task.
---

# Cross-device Delegation

Every non-managed, user-created Codex task on a configured peer may become a temporary root when it
first uses Delegation. Keep that root task on its originating peer. Delegate only bounded work that
benefits from another environment; do not fork the root conversation onto the target peer.

## Select A Peer

Delegation MCP instructions contain no peer roster. Call `list_devices` for current availability,
then `describe_device` for the full advertised feature list and current presence details. Match the
task against reported OS, architecture, runtime and protocol versions, features, and online state.
Use `isCurrentDevice` to recognize the local peer; self-targeting is valid once spawn exists and
creates a separate managed worker. M1.1 does not report arbitrary toolchains, hardware, tags, or
capacity; do not infer them from a peer name. Ask the user for missing placement facts or validate them explicitly once worker
execution exists.

If Delegation tools are unavailable, use `$delegation-setup`; do not replace cross-device
delegation with an unrelated remote environment without telling the user.

## Prepare And Spawn

Proceed only when the installed runtime exposes the tools below. M1.1 exposes peer discovery but
does not yet expose worker or workspace tools; report that milestone boundary instead of pretending
to delegate.

1. Use `sync_workspace` from the trusted root workspace when the task needs repository content.
   Confirm the selected device, explicit Git URL, HEAD, transfer strategy, and warnings before
   spawning work.
2. Call `spawn_agent` with the selected peer, prepared workspace, a short task name, and a
   self-contained task description. Include the requested output, validation command, and relevant
   platform constraint; do not send the whole root transcript.
3. Keep unrelated subtasks local. Spawn multiple remote workers only when their work is independent.

## Collaborate

Use `send_message` to steer a running worker. Use `followup_task` when an idle worker must resume and
start another turn. Use `wait_agent` to collect worker messages and completion; messages remain in
the mailbox when the root task is not waiting. Use `interrupt_agent` only when continuing would be
wasteful or unsafe.

Remote workers do not receive the peer roster and cannot recursively delegate in v0. A managed
worker thread permanently remains a worker; opening its history does not promote it to a root. Start
a separate normal Codex task on that peer when the user wants a new root. Answer a worker's
environment question from the root or change the target from the root tools.

## Review Results

Treat worker output as evidence from a different environment, not as an automatically accepted
change. Check the reported commands, platform, workspace revision, warnings, and artifact metadata.
The v0 flow does not write worker changes back into the root workspace automatically.

Summarize the delegated task, target peer, result, verification evidence, and any artifact that
still requires an explicit root-side apply decision.
