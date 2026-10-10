---
"@yielded/agent": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-memory": patch
---

Reduce Run continuation storage by referencing Thread-local canonical positions with full SHA-256 integrity. BEHAVIOR CHANGE: require fresh `effect-agent/thread@6` records; earlier record formats are rejected without migration.
