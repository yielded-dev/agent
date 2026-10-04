---
"effect-agent": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/storage-sql": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-postgres": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/testing": minor
---

Bind durable Attempts to scoped storage sessions and reduce repeated ownership and tail reads in managed Node hosts.

BEHAVIOR CHANGE: provide `RunStorage` in manual runtime assemblies and provide `ThreadReader` for canonical read helpers (stock adapters include it). Managed Node hosts keep `SqlClient`, `ThreadStore`, and `SubmissionLedger` private and reject custom SQLite triggers; compose application SQL with a separate client.
