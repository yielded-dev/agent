---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
---

Persist current Run progress atomically with conversation history and retire it after settlement and execution closure. BEHAVIOR CHANGE: use matching runtime and storage packages with fresh layout-22 stores and thread@4 history; verification no longer replays discarded checkpoint revisions.
