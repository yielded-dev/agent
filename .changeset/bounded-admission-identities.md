---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Keep work-discovery cursors compact and bound fresh Memory identities independently of imported facts. Reject fresh admissions that cannot fit within a complete 32 MiB transfer page before retaining any work.
