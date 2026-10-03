---
"effect-agent": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/testing": patch
---

Bind durable Attempts to scoped storage sessions and reduce repeated ownership and tail reads in managed Node hosts.

BEHAVIOR CHANGE: provide `RunStorage` in manual runtime assemblies and provide `ThreadReader` for canonical read helpers (stock adapters include it). Managed Node hosts keep `SqlClient`, `ThreadStore`, and `SubmissionLedger` private and reject custom SQLite triggers; compose application SQL with a separate client.
