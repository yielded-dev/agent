---
"@yielded/agent": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-testing": minor
---

Bind durable Attempts to scoped storage sessions and reduce repeated ownership and tail reads in managed Node hosts.

BEHAVIOR CHANGE: provide `RunStorage` in manual runtime assemblies and provide `ThreadReader` for canonical read helpers (stock adapters include it). Managed Node hosts keep `SqlClient`, `ThreadStore`, and `SubmissionLedger` private and reject custom SQLite triggers; compose application SQL with a separate client.
