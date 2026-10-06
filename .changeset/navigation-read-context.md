---
"@yielded/agent-platform-cloudflare": patch
---

Retry a read whose page context a navigation destroys before the page reports its new URL, and observe a document still parsing after 2 s instead of failing the read.
