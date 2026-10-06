---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
---

Resume durable Runs from canonical continuations and referenced context, expose `@yielded/agent/run-continuation`, and remove `ThreadStore.recoveryCheckpoints`. **BEHAVIOR CHANGE:** use fresh layout-19 storage and `effect-agent/thread@2` archives; predecessor stores and formats are rejected.
