---
"@yielded/agent-platform-node": patch
---

Keep Node host admission and worker lifecycle separate from runtime administration. BEHAVIOR CHANGE: access inspection, retry, wake, work discovery, and recovery methods through the `DurableAgentRuntime` service supplied by the host Layer instead of `NodeDurableHost`.
