# First scripted-model boundary: existing counter evidence

Terminal public counters are finished: portable 50-turn replay matches all counters, SQL breakdowns, visits, model snapshots, importer inventory, and fingerprints. The 12 SHA-256 entries covering its 13-file text-only package were rechecked. Its REPORT.md, summary.json and PUBLISH.md are in the separate publish-terminal-reads directory; that frozen package is unchanged.

This note reads existing 250/1000 captures only. No new instrumentation, product/bench edits, baseline repeats, deployment, or full gate. It covers the first scripted model callback, not deployed submission-to-provider transport latency. Counter start precedes the inline turn; the model snapshot precedes the benchmark transcript conversion.

## Exact Effect split

208,859 is the baseline first-model count at 1000, whose readPrompt count is 151,084. Pairing it with the candidate readPrompt count of about 141k would mix variants.

| Turns | Variant | Before first model | readPrompt | Outside readPrompt | After first model |
|---:|---|---:|---:|---:|---:|
| 250 | baseline | 56,123 | 37,839 | 18,284 | 22,046 |
| 250 | prompt-reads | 52,065 | 35,320 | 16,745 | 22,046 |
| 250 | terminal-reads | 52,075 | 35,318 | 16,757 | 22,046 |
| 1,000 | baseline | 208,859 | 151,084 | 57,775 | 22,046 |
| 1,000 | prompt-reads | 192,609 | 141,000 | 51,609 | 22,046 |
| 1,000 | terminal-reads | 192,687 | 141,041 | 51,646 | 22,046 |

The post-first-model 22,046 is exactly 2,381 (model 1 → 2) + 7 × 2,323 (models 2 → 9) + 3,404 (last model → settled turn). Every interval matches across both sizes and all three variants. All history readPrompt work, metadata visits, and journal fold visits have already completed at the first callback; their counters never increase afterward.

## What the existing hooks quantify outside readPrompt

| Terminal candidate metric | 250 | 1000 |
|---|---:|---:|
| Residual Effects | 16,757 | 51,646 |
| Metadata visits | 1,087 | 4,341 |
| Journal fold visits | 1,087 | 4,341 |
| Journal projections | 1 | 1 |
| Residual schema nodes | 35,733 | 119,733 |
| Residual root parser calls | 572 | 1,572 |
| Extra canonical JSON decodes | 5 | 5 |
| Extra canonical JSON bytes | 3,993 | 3,995 |
| Full-envelope decodes | 4 | 4 |

Outside-readPrompt schema calls are identical across the three variants at each size: 35,733 → 119,733, an increase of 84,000. They are unscoped parser calls, not a measured history-digest subtotal. Five extra canonical decodes remain at both sizes: ThreadCreated ×1, UserInputRecorded ×1, RunContinuation ×2, RunContextRecorded ×1. This does not show a second full historical JSON decode.

The 1000-turn input filter removes 6,166 residual Effects relative to baseline; terminal filtering removes 6,129. Those are measured aggregate reductions outside readPrompt. They cannot be assigned wholly to the fold because page orchestration also changes.

## Source mapping and attribution limits

All source references below are at baseline 07f0272e; the candidates only replace the SQL selection module.

- Context collection: durable/internal/initial-context.ts:103,120,223 collects prompt pages, retains records, scans dependencies and sorts the facts. Page/stream orchestration sits outside the native readPrompt scope. In terminal captures the selected population is 1,086 → 4,340 and prompt pages 5 → 17. The loop sizes are source-grounded, but no separate Effect subtotal exists.
- Metadata: durable/DurableAgentRuntime.ts:8583 feeds the collector; durable/internal/journal-metadata.ts:91 records per-Run response/terminal maps and Tool declaration spans. The collector and snapshot copy Maps/arrays in synchronous JavaScript. Existing visit counts are exact; they do not represent one Effect evaluation per entry.
- Journal projection: durable/RunJournal.ts:1256 uses Stream.runForEach with an Effect generator per envelope. At 250/1000 terminal counts it folds 1,087/4,341 entries, including 417/1,667 ModelResponseRecorded and 167/667 ToolCallSettled. Its native-Prompt guard at line342 skips another full message decode, but still performs Effect composition, response/tool-ID checks and prompt construction. accountResponse at line1093 validates declared Tool identities even for historical responses. Counts do not isolate those Effect operations.
- Initial prompt and history digest: engine/internal/agent-runtime.ts:4518 copies prior history into the initial prompt. durable/DurableAgentRuntime.ts:5769 calls digestRunHistory before persisting the context. durable/internal/run-context.ts:20 encodes the full prior native Prompt through Schema.toCodecJson; durable/Digest.ts:23,44 canonicalizes JSON, UTF-8 encodes it and hashes it. This is history-sized encoding/traversal identified in source. The current hooks do not count digest calls/bytes or split its schema/Effect totals. Do not label all 119,733 residual schema nodes as digest work.
- Admission and continuation: the measured path is submitRegistered then processThreadResolved (examples/durable-bench/src/yielded.ts:197). Admission input encoding/digest and ledger/materialization execute before the callback, as do the two observed continuation decodes. There are no stage boundaries for their separate Effect totals.
- Materialization: storage-cloudflare/src/internal/do-journal.ts:494 reads thread/transfer state and can return through its idempotent path; it does not replay the entire canonical history. The existing counters do not quantify its own Effects or calls. No history-sized materialization Effect subtotal can be claimed.

## Existing SQL evidence outside prompt queries

Whole-Run cursor counts expose two changing query groups from 250 → 1000. They are not first-model SQL snapshots: the probe saves SQL totals only at stop.

| Query group | Calls at both sizes | Rows read 250 → 1000 |
|---|---:|---:|
| DO ledger ownership join, ORDER BY lease expiry LIMIT 1 | 2 | 504 → 2,004 |
| Canonical journal-range UPDATE RETURNING | 12 | 72 → 180 |

All other outside-prompt SQL aggregates are identical between these two captures. The ownership query is in storage-cloudflare/src/DoSubmissionLedger.ts:1649 (claim path). The 1,500 extra reads and 108 range-update reads are real database work, but they cannot be assigned to before/after first provider using the current snapshot format. Row reads are not Effect evaluations or latency.

## Reproduce these derived values

For each local yielded-VARIANT-N.json: first = modelSnapshots[0]; before-first = first.effectEvaluations; readPrompt = first["readPrompt.effectEvaluations"]; residual = before-first − readPrompt; post-first = counts.effectEvaluations − before-first. Apply the same subtraction to schema and JSON counters. Difference adjacent modelSnapshots for model intervals. summary.json records the exact raw-capture hashes. No model/benchmark execution is required.

This sidecar publication allowlist is REPORT.md, summary.json, SHA256SUMS only. All are text without record payloads or workstation paths. Do not include parent archives, databases, generated bundles or raw captures.
