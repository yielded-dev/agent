---
"@yielded/agent": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-storage-memory": patch
---

**BEHAVIOR CHANGE:** Use the exported run-storage, browser adapter, and memory schedule layers directly as values instead of calling zero-argument factories; each build retains fresh scoped state. Reject non-finite numeric record fields while retaining the unbounded policy limit's `Infinity`/`null` round trip.
