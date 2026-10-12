---
"@yielded/agent": patch
"@yielded/agent-ai-decision": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-pr-review": patch
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-testing": patch
"@yielded/agent-workflow": patch
---

Reduce tracing overhead by keeping operation spans and removing private helper spans and stack frames. BEHAVIOR CHANGE: Update filters that use private helper span names to use the enclosing agent, model, tool, storage, or recovery operation.
