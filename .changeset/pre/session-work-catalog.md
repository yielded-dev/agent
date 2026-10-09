---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Avoid repeated work-index catalog probes within a storage session, invalidating schema presence after maintenance, rebuilds, and failed queries.
