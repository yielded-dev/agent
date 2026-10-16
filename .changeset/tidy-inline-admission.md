---
"@yielded/agent": patch
"@yielded/agent-platform-cloudflare": patch
---

Start newly admitted Cloudflare work from the submit event without waiting for the post-event dispatcher. Keep receipts independent of execution and retain alarm recovery after interruption.
