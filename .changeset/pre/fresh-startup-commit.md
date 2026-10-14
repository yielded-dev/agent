---
"@yielded/agent": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Commit fresh Cloudflare Run inputs, original context, and applied markers atomically before model dispatch. Preserve configured projections and lifecycle publication while avoiding a second initial checkpoint write.
