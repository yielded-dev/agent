---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sqlite": patch
---

Reduce database work for bounded prompt reads and uncached Durable Object history reads while
preserving byte limits and archive handling.

BEHAVIOR CHANGE: Use SQLite 3.43 or later when supplying a custom SQL client.
