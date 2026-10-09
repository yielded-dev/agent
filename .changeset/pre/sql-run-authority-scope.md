---
"@yielded/agent-storage-sql": patch
---

Preserve newer SQL writer fences and independent canonical appends when an exclusive Run session's cached state is stale. Keep valid sessions usable after failed claims and bind operation resources to the caller's Scope.
