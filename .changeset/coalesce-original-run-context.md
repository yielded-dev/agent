---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
---

Store the initial evaluated Run context in its canonical start record before model dispatch. BEHAVIOR CHANGE: Use fresh layout-23 stores with record format `effect-agent/thread@5`; retain older stores with their matching release.
