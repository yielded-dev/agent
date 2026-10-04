---
"effect-agent": patch
"@effect-agent/ai-decision": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/pr-review": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/testing": patch
"@effect-agent/workflow": patch
---

Reduce tracing overhead by keeping operation spans and removing private helper spans and stack frames. BEHAVIOR CHANGE: Update filters that use private helper span names to use the enclosing agent, model, tool, storage, or recovery operation.
