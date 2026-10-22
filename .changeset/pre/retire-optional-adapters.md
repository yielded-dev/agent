---
"@yielded/agent": patch
"@yielded/agent-platform-node": patch
---

Retire command queues, command-shaped Sandbox execution, the ephemeral approval adapters, and `redactedTranscript`; compose native Run hooks and Effect child-process services where needed.
**BEHAVIOR CHANGE:** remove `@yielded/agent-sandbox-local`, `@yielded/agent-workflow`, and NodeWorkflow; finish pending Agent work and parent Workflow notifications with the existing bridge deployment before upgrading.
