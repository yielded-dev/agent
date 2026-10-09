---
"@yielded/agent": minor
"@yielded/agent-storage-sql": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-postgres": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-testing": minor
---

Publish settlement intent atomically in the canonical log and remove the separate settlement reservation protocol. Combine eligible SQL receipt finalization with publication and exclusive-session input markers with their canonical append.

BEHAVIOR CHANGE: custom durable assemblies must provide a co-owned `SettlementPublisher`; pair Memory ledger and thread layers with `Layer.provideMerge`, and use fresh thread storage or format 16.
