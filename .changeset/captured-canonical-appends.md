---
"effect-agent": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-cloudflare": patch
---

Capture canonical appends before asynchronous work so later caller mutations cannot change the persisted value or invalidate its digest. Reuse captured record JSON across hashing and SQL writes, and commit eligible readonly responses with their completed results.

BEHAVIOR CHANGE: custom SQL adapters must prepare raw append requests with `prepareSqlAppend`; `RawAppendRequest` is now a typed value instead of a Schema factory, and `RawRecord` is removed.
