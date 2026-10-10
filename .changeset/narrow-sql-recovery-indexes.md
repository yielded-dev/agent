---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-postgres": patch
---

Reduce SQL Thread storage by indexing only the records used for operation and worker recovery. BEHAVIOR CHANGE: Export existing Thread stores with their matching adapter release and import into fresh layout-22 stores before upgrading.
