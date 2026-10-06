---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Keep work-discovery cursors compact for accepted Thread identities and preserve their Thread binding. Reject fresh admissions that cannot fit within a complete 32 MiB transfer page before retaining any work.
