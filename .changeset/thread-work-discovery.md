---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
---

Discover unfinished Thread work through `@yielded/agent/thread-work`, recover bounded pages, and explicitly rebuild disposable indexes. **BEHAVIOR CHANGE:** follow `runRecovery().cursor` to finish a scan and use fresh layout-21 stores; factual effect closure remains available after execution decisions and Run settlement.
