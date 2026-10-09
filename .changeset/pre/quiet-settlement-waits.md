---
"@yielded/agent": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
---

Avoid rereading unfinished submissions on canonical progress by separating progress and settlement wake hints. Preserve broad wake behavior for existing schedulers and external notifications.
