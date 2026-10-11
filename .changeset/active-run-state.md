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

Persist only current Run state while a single process owns each Thread, and retire it at settlement. BEHAVIOR CHANGE: use matching packages on fresh layout-25 stores with thread@6 records; transfers require an idle Thread and a completed archive, and distributed ownership, tamper evidence and historical progress replay are no longer supported.
