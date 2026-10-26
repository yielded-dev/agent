---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-platform-cloudflare": patch
---

Repair SQL work-index upgrades while preserving unfinished operations. SQL discovery advances incomplete upgrades in bounded passes; Cloudflare maintenance continues them automatically, including previously parked recovery. Operations with unknown external outcomes remain unresolved.
