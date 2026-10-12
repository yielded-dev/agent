---
"@yielded/agent-platform-cloudflare": patch
---

Cancel remote settlement observations when their client Effect is interrupted, using the same bounded best-effort cleanup as progress observations. Keep the admitted Run active so another observer can await its completion.
