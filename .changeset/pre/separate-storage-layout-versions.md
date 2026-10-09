---
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-cloudflare": patch
---

Track table layouts separately from record formats and apply pending layout steps atomically when opening supported storage.

BEHAVIOR CHANGE: Opening layout 16 advances its layout header to 17 without rewriting records; use the read-only export entry point when preserving the source for a format change.
