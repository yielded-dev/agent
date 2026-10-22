---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Commit peer messages, worker reports, and parent updates atomically with their pending deliveries through paired native storage. Drain unresolved source publications with the previous release before upgrading an existing SQL store.
