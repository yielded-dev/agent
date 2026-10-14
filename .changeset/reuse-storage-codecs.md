---
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-platform-cloudflare": patch
---

Reuse storage and live-text codecs across Thread operations to reduce repeated schema construction while preserving validation.
