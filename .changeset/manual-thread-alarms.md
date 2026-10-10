---
"@yielded/agent-platform-cloudflare": patch
---

Move Thread maintenance scheduling to effect-cf 0.55.0 and compose application alarms through `ThreadObject.make`.

BEHAVIOR CHANGE: shared hosts must register `ThreadObject.alarms`.
