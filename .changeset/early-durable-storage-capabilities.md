---
"@yielded/agent": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-testing": patch
---

Reject incomplete durable storage adapters during runtime and work-discovery Layer construction, before accepting work. Handle `ThreadStoreError` when constructing custom runtimes or certifying adapters; prompt checkpoints and fresh-admission accelerators remain optional.
