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
installed checkpoint boundary instead of pretending unavailable workspace tools exist.

1. For repository work, call `sync_workspace` first with a fresh `sync_id`, the selected
   `target_device_id`, and the repository's explicit SSH or HTTPS Git URL. Pass its returned
   `workspace_id` to `spawn_agent`. Disclose `remote_git_full_history_fallback`: the transfer
   includes HEAD-reachable history that may contain deleted content. The current checkpoint handles
   clean workspaces through direct clone or scoped bundle fallback; report a dirty-overlay error
   instead of pretending local modifications were synchronized.
2. Generate a fresh UUID `spawn_id` for the logical dispatch. Call `spawn_agent` with that ID, the
   selected `target_device_id`, an optional prepared `workspace_id`, a unique lowercase `task_name`,
   and a self-contained `message`. Include the requested output, validation command, and relevant
   platform constraint; do not send the whole root transcript.
3. Inspect the dispatch `outcome`. `busy` means the target had no worker slot. `indeterminate`
   means no definitive target result could be confirmed or durably recorded, so the worker may
   already have started. Both keep one pending receipt: retry with the same spawn ID and exactly the
   same arguments. Never create a second agent merely because capacity was full or a result was
   lost.
4. Use `list_agents` to inspect durable status. Keep unrelated subtasks local and spawn multiple
   workers only when their work is independent.

## Collaborate

Use `send_message` to steer a running worker or queue a message for an idle worker. Use
`followup_task` to start a new turn only when the worker is idle, and `interrupt_agent` to stop an
active turn. `wait_agent` consumes bounded lifecycle and worker-message pages; call it again
immediately while `has_more` is true before concluding that no unread result remains. Reuse the
same operation ID only when retrying the exact same logical action.

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
