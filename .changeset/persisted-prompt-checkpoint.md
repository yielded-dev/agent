---
"@yielded/agent": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
---

Reuse verified prompt history across consecutive durable Runs and seed it from a persisted Cloudflare checkpoint after eviction. Amortize checkpoint refreshes while rebuilding from canonical history whenever the cached prefix or suffix cannot be verified.
