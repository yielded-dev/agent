---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
---

Remove per-Thread conversation quotas and reuse messaging and subscription capacity after work settles or its retention window expires, preserving exact receipts and host capacity bounds. Upgrade released layout-25 stores and standalone Cloudflare subscription stores v3 atomically on open; custom Thread stores must count peer sends at the request's `nowMillis`.
