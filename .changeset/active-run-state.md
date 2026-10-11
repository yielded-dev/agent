---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
---

Persist current Run progress atomically with conversation history and delete it at settlement, keeping unresolved obligations independently. BEHAVIOR CHANGE: use matching runtime and storage packages with fresh layout-23 stores and thread@5 history; verification checks schema, identity, order and atomicity without tamper evidence or historical progress replay.
