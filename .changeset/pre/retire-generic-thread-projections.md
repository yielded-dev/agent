---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-testing": minor
---

Retire generic `ThreadCheckpoint` storage and `ThreadProjection` replay APIs, including their TravelPlanner helpers. Discard only the retired derived cache in the layout 28 → 29 upgrade, preserving conversation and operational bytes and retaining supported older upgrades.

BEHAVIOR CHANGE: Keep application projections in application-owned storage; `ExecutionCheckpoint` and `PromptCheckpoint` remain supported.
