---
"@yielded/agent": patch
---

Reduce durable response capture and progress-accounting work while preserving canonical record bytes and recovery validation. BEHAVIOR CHANGE: `ModelResponseRecorded.messages` is now typed as encoded Prompt messages; encode a `Prompt` before constructing the record.
