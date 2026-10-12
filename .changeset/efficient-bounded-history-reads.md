---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Reduce database work for bounded prompt reads and uncached Durable Object history reads while
preserving byte limits and archive handling.
