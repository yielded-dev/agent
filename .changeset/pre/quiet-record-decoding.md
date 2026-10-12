---
"@yielded/agent": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
---

Reduce redundant record decoding and encoding in storage reads, SQL maintenance, and thread export while preserving complete wire values and schema validation.
