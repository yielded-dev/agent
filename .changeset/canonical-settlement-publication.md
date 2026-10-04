---
"effect-agent": minor
"@effect-agent/storage-sql": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-postgres": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/storage-memory": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/platform-node": minor
"@effect-agent/testing": minor
---

Publish settlement intent atomically in the canonical log and remove the separate settlement reservation protocol. Combine eligible SQL receipt finalization with publication and exclusive-session input markers with their canonical append.

BEHAVIOR CHANGE: custom durable assemblies must provide a co-owned `SettlementPublisher`; pair Memory ledger and thread layers with `Layer.provideMerge`, and use fresh thread storage or format 16.
