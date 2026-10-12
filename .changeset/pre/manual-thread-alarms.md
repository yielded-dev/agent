---
"@yielded/agent-platform-cloudflare": patch
---

Delegate Thread maintenance scheduling to effect-cf 0.56 and compose application alarms through `ThreadObject.make`.

BEHAVIOR CHANGE: shared hosts must register `ThreadObject.alarms`.
