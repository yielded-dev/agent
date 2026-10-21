---
"@yielded/agent": patch
---

Remove the disconnected `ModelContext` text-only preparation and compaction API. **BEHAVIOR CHANGE:** Use `RunContextPreparation` from `@yielded/agent/run-options` and `ContextCompactor` from `@yielded/agent/context-compactor` for native prompt preparation and compaction.
