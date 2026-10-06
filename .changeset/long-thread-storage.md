---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-testing": minor
---

Remove cumulative Thread record, worker-input, peer-message, and delivery limits while preserving live capacity and per-Run bounds. **BEHAVIOR CHANGE:** Use fresh layout-21 stores and `effect-agent/thread@3` records, stream snapshot-bound transfer pages, and configure compaction within retained model-context bounds.
