---
"@yielded/agent": patch
---

Cap in-process attached child execution at the parent's remaining deadline. Add `SubagentPolicy.parentReserveDuration` so callers can leave time to handle a typed child duration failure and finish.
