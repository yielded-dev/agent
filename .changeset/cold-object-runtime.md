---
"@yielded/agent-platform-cloudflare": patch
---

Reduce Thread Object initialization work by sharing runtime assembly with maintenance and using effect-cf's constructor gate.
