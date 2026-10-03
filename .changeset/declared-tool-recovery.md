---
"effect-agent": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
---

Recover unfinished tools from their committed model declarations and remove the separate preparation write and outstanding-operation index.

BEHAVIOR CHANGE: a crash after declaration can leave a mutating tool outcome unknown; tool call IDs must be unique within a Run, and thread stores require fresh storage or format 16. Use runtime `explain` in place of `readOutstanding`, and `DeclaredToolCallEvidence` in custom reconcilers.

Supply JSON tool arguments and results in history used by function-based approval hooks.
