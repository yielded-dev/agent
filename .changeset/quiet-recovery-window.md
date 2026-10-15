---
"@yielded/agent-platform-cloudflare": minor
---

Configure a recovery window to reuse alarms across nearby requests while keeping alarm-driven work prompt.

BEHAVIOR CHANGE: Without a client, recovery can now take up to the configured window (60 seconds by default). Lower `recoveryWindowMillis` for faster recovery at the cost of an alarm write before the first model request; `0` restores the previous recovery deadlines.
