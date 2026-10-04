---
"@effect-agent/storage-sqlite": patch
"@effect-agent/platform-node": patch
---

Add a per-store `synchronous: "NORMAL"` option while keeping `FULL` as the default. `NORMAL` preserves process-crash recovery but can lose acknowledged commits after power loss or an OS crash, allowing external effects to repeat.
