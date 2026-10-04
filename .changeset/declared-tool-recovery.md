---
"effect-agent": minor
"@effect-agent/storage-sql": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-postgres": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/storage-memory": minor
---

Recover unfinished tools from their committed model declarations and remove the separate preparation write and outstanding-operation index.

BEHAVIOR CHANGE: a crash after declaration can leave a mutating tool outcome unknown; durable Runs require unique tool call IDs and reject responses exceeding 4,096 distinct IDs with `RunJournalError` before commit or dispatch; thread stores require fresh storage or format 16. Use runtime `explain` in place of `readOutstanding`, and `DeclaredToolCallEvidence` in custom reconcilers.

Supply JSON tool arguments and results in history used by function-based approval hooks.
