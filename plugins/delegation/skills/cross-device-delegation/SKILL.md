---
name: cross-device-delegation
description: Delegate a bounded Codex subtask to an appropriate managed device and collaborate with the resulting remote worker. Use for platform-specific builds or validation, access to device-local tools or hardware, or any task whose required operating system or environment differs from the root task.
---

# Cross-device Delegation

Keep the root task on its current device. Delegate only the bounded work that benefits from a
different registered environment; do not fork the root conversation onto the target device.

## Select A Device

Use the bounded device summary in the Delegation MCP instructions only as an initial hint. Call
`list_devices` for current availability, then `describe_device` when toolchains, hardware, tags, or
capacity affect the choice. Match the task's explicit platform and environment requirements before
preferring proximity or idle capacity.

If Delegation tools are unavailable, use `$delegation-setup`; do not replace cross-device
delegation with an unrelated remote environment without telling the user.

## Prepare And Spawn

1. Use `sync_workspace` from the trusted root workspace when the task needs repository content.
   Confirm the selected device, explicit Git URL, HEAD, transfer strategy, and warnings before
   spawning work.
2. Call `spawn_agent` with the selected device, prepared workspace, a short task name, and a
   self-contained task description. Include the requested output, validation command, and relevant
   platform constraint; do not send the whole root transcript.
3. Keep unrelated subtasks local. Spawn multiple remote workers only when their work is independent.

## Collaborate

Use `send_message` to steer a running worker. Use `followup_task` when an idle worker must resume and
start another turn. Use `wait_agent` to collect worker messages and completion; messages remain in
the mailbox when the root task is not waiting. Use `interrupt_agent` only when continuing would be
wasteful or unsafe.

Remote workers do not receive the device roster and cannot recursively delegate in v0. Answer a
worker's environment question from the root or change the target from the root tools.

## Review Results

Treat worker output as evidence from a different environment, not as an automatically accepted
change. Check the reported commands, platform, workspace revision, warnings, and artifact metadata.
The v0 flow does not write worker changes back into the root workspace automatically.

Summarize the delegated task, target device, result, verification evidence, and any artifact that
still requires an explicit root-side apply decision.
