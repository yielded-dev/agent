---
"@yielded/agent": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-storage-memory": minor
---

Recover unfinished tools from their committed model declarations and remove the separate preparation write and outstanding-operation index.

BEHAVIOR CHANGE: a crash after declaration can leave a mutating tool outcome unknown; durable Runs require unique tool call IDs and reject responses exceeding 4,096 distinct IDs with `RunJournalError` before commit or dispatch; thread stores require fresh storage or format 16. Use runtime `explain` in place of `readOutstanding`, and `DeclaredToolCallEvidence` in custom reconcilers.

Supply JSON tool arguments and results in history used by function-based approval hooks.
