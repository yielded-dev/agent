---
"@yielded/agent": minor
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Publish original input and its applied marker atomically on every durable adapter. BEHAVIOR CHANGE: custom SubmissionLedger implementations must provide `appendInput` to commit the input, current Run state, and marker together under the Thread owner.
