---
"@yielded/agent": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
---

Reduce Worker module initialization by importing Effect modules directly on the durable execution path.
