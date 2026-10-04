---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
---

Export complete Thread archives and atomically import them into empty Threads with rebuilt ledger state and preserved admission facts.

BEHAVIOR CHANGE: Quiesce the source and export/import into fresh storage for record-format changes; retain queued or externally linked work when import rejects it, and re-export older archives that lack batch identities.
