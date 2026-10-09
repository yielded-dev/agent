---
"@yielded/agent-platform-cloudflare": patch
---

Reduce Cloudflare admission latency by reusing the pre-armed alarm; larger `alarmBackoffBase` values can now delay healthy work, and custom hosts calling `ThreadObject.submit` must provide `DurableAlarmService`.
