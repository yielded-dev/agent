---
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-platform-node": minor
---

Add a per-store `synchronous: "NORMAL"` option while keeping `FULL` as the default. `NORMAL` preserves process-crash recovery but can lose acknowledged commits after power loss or an OS crash, allowing external effects to repeat.
