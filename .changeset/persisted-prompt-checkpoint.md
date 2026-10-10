---
"@yielded/agent": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
---

Reuse a verified persisted prompt checkpoint when starting a fresh Cloudflare Run after eviction. Rebuild from canonical history whenever the checkpoint cannot be reused.
