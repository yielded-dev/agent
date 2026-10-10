---
"@yielded/agent-platform-cloudflare": patch
---

Move Thread maintenance scheduling to effect-cf 0.54.0 and compose application alarms through `ThreadObject.make`.

BEHAVIOR CHANGE: shared hosts must register `ThreadObject.alarms`; eviction during deferred maintenance can postpone recovery to effect-cf's one-hour guard.
