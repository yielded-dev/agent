---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Apply saved-context limits after compaction so new Runs can follow long compacted Runs. Reject unrepresentable queued facts before admission and detect orphan or out-of-range canonical data during Cloudflare startup verification.
