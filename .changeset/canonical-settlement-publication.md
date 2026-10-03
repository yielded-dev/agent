---
"effect-agent": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/testing": patch
---

Publish settlement intent atomically in the canonical log and remove the separate settlement reservation protocol.

BEHAVIOR CHANGE: custom durable assemblies must provide a co-owned `SettlementPublisher`; pair Memory ledger and thread layers with `Layer.provideMerge`, and use fresh thread storage or format 16.
