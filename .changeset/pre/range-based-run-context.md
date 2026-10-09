---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Store durable context as verified canonical ranges, eliminate duplicate hot SQL batch payloads, and skip unused token estimates. **BEHAVIOR CHANGE:** use fresh stores for the revised unreleased `effect-agent/thread@3` format; custom adapters must provide narrow `readPrompt` and snapshot-bound full history reads.
