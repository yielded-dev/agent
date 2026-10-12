---
"@yielded/agent": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
---

Reuse captured tool-cycle commits and settled prompt records, and retain each active Run continuation once. BEHAVIOR CHANGE: Execution checkpoints use wire version 2; native stores upgrade atomically from layouts 25 or 26 to 27, which older releases cannot reopen.
