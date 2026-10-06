---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
---

Provide a mutation-scoped `ProgressAppendReader` when validating canonical Run progress. Update custom adapters to call `validateProgressAppend(records)` inside their write boundary.
