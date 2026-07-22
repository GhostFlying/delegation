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
creates a separate managed worker. Delegation does not report arbitrary toolchains, hardware, tags,
or capacity; do not infer them from a peer name. Ask the user for missing placement facts or have a
worker validate them explicitly.

If Delegation tools are unavailable, use `$delegation-setup`; do not replace cross-device
delegation with an unrelated remote environment without telling the user.

## Prepare And Spawn

Proceed only when the installed runtime exposes `spawn_agent` and `list_agents`. Report the
installed checkpoint boundary instead of pretending unavailable lifecycle or workspace tools
exist.

1. Generate a fresh UUID `spawn_id` for the logical dispatch. Call `spawn_agent` with that ID, the
   selected `target_device_id`, a unique lowercase `task_name`, and a self-contained `message`.
   Include the requested output, validation command, and relevant platform constraint; do not send
   the whole root transcript.
2. Treat `pending` as an indeterminate delivery result. Retry with the same spawn ID and exactly the
   same arguments. Never create a second agent merely because the first response was lost.
3. Use `list_agents` to inspect durable status. Keep unrelated subtasks local and spawn multiple
   workers only when their work is independent.

The current M2 checkpoint starts workers in an empty managed workspace. Do not delegate repository
work that requires local files until workspace synchronization is available, unless the task can
fetch all required public inputs itself.

## Collaborate

Use root-side message, follow-up, wait, and interrupt tools only when the installed runtime actually
exposes them. The initial M2 dispatch checkpoint exposes spawn and agent listing but not those
lifecycle controls; report the spawned status and do not claim that later collaboration happened.

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
