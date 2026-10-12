---
"@yielded/agent": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
---

Reduce redundant validation when decoding stored record JSON for Durable Object reads and SQL Run preparation, preserving complete wire values and schema validation.
