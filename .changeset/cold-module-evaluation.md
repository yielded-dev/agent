---
"@yielded/agent": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Avoid loading the optional memory adapter for Thread storage ownership, and derive archive, remote-port, and admin encoders only when used.
