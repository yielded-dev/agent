---
"@yielded/agent": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Coalesce receipt admission and readiness for initialized Cloudflare Threads to avoid a redundant storage transaction while preserving initialization, recovery, and lifecycle publication.
