---
"@yielded/agent": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Reduce repeated validation and dictionary work during durable turns. Capture canonical records by index and read execution arrays once, preserving their bounds even with custom iterators or getters.
