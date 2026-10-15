---
"@yielded/agent": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Reduce fresh durable Run preparation by reading root identity and empty execution evidence together under the current owner. Reuse captured startup and checkpoint values while preserving recovery validation and atomic persistence before model dispatch.
