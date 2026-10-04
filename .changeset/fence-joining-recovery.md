---
"effect-agent": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sql": minor
"@effect-agent/storage-cloudflare": minor
---

Prevent recovery from reverting joined input while its host is still processing it. Recheck canonical input after acquiring recovery ownership and fence rollback against the current host.

BEHAVIOR CHANGE: Custom `SubmissionLedger` adapters must validate a supplied `RevertJoiningRequest.guard` atomically with rollback; requests without a guard retain their existing behavior.
