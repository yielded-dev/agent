---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-testing": minor
"@yielded/agent-workflow": minor
---

Store in-flight Run progress as one current row and retire it at settlement; use one scoped process owner per Thread and reject late results locally. BEHAVIOR CHANGE: use matching runtime and adapter packages on fresh layout-24 stores with thread@6 records; distributed leases, persisted attempts, tamper evidence and historical progress replay are no longer supported.
