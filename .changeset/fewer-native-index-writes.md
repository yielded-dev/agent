---
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
---

Remove the redundant batch-end index from SQLite and PostgreSQL stores while preserving existing tables, Threads and receipts.
