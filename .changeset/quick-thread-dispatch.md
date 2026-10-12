---
"@yielded/agent-platform-cloudflare": patch
---

Enable post-response alarm dispatch by default for Thread Objects to start due work sooner. BEHAVIOR CHANGE: receipt delivery on cold Objects may take longer; provide `DurableObjectAlarm.ScheduleConfiguration` with `{ dispatchAfterEvent: false }` to opt out.
