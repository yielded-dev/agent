---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
---

Remove per-Thread conversation quotas and reuse messaging and subscription capacity after work settles or its retention window expires, preserving exact receipts and host capacity bounds. **BEHAVIOR CHANGE:** use fresh layout-26 stores and version-4 standalone Cloudflare subscription stores with matching runtime and storage packages; custom Thread stores must count peer sends at the request's `nowMillis`.
