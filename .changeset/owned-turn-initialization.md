---
"@yielded/agent": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
---

Reuse verified state held by the current owner when starting durable Cloudflare turns. Retain canonical validation and recovery when the view is absent or no longer matches.
