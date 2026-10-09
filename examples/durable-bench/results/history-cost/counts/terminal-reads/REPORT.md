# Terminal-settlement filtering: deterministic counts

Baseline `07f0272e7ba49a494064b6b74c6318b55514ae19`; isolated SQL candidate `4417a0955e9c61cd3a45ac4cf4ad1e5c8f431228` (`terminal-reads`). Native baseline `Records.ts` is used. The earlier input filter and rejected codec candidate are absent.

All four captures match baseline seed and measured transcript fingerprints. This candidate reduces JSON decoding, schema work, Effect evaluations, and journal visits, while its correlated SQL membership queries increase observed database row reads. The tradeoff warrants preserving count evidence; it does not establish a latency or CPU improvement.

## Prompt read and whole-Run work

Values are baseline → terminal-reads. JSON bytes are raw UTF-8 canonical records passed to JSON.parse inside readPrompt. Effects are actual runLoop evaluations in the stated scope.

| Seed turns | Prompt records | Prompt JSON bytes | readPrompt Effects | Whole-Run Effects | Metadata visits = projection visits |
|---:|---:|---:|---:|---:|---:|
| 50 | 268 → 218 | 204,113 → 154,263 | 7,667 → 7,096 | 37,112 → 36,202 | 269 → 219 |
| 250 | 1,334 → 1,086 | 1,024,025 → 776,769 | 37,839 → 35,318 | 78,169 → 74,121 | 1,335 → 1,087 |
| 1,000 | 5,334 → 4,340 | 4,112,455 → 3,121,437 | 151,084 → 141,041 | 230,905 → 214,733 | 5,335 → 4,341 |
| 3,500 | 18,668 → 15,188 | 14,430,077 → 10,960,517 | 528,733 → 493,618 | 740,267 → 683,741 | 18,669 → 15,189 |

| Turns | Retained settlements | Historical inputs, unchanged | Encoded passes, unchanged | Native passes, unchanged | Prompt pages |
|---:|---:|---:|---:|---:|---:|
| 50 | 0 | 50 | 184 | 184 | 2 → 1 |
| 250 | 2 | 250 | 917 | 917 | 6 → 5 |
| 1,000 | 6 | 1,000 | 3,667 | 3,667 | 21 → 17 |
| 3,500 | 20 | 3,500 | 12,834 | 12,834 | 73 → 60 |

The predicate omits a settlement only when an earlier RunCompleted/RunFailed for the same Run lies strictly after the logical request lower bound. It applies in planning and hydration. Retained settlement counts are consistent with that page-boundary guard; the unchanged probe reports aggregate visits rather than identifying retained records. All historical UserInputRecorded records remain selected. Every selected historical record is decoded once, then visited once by metadata and once by the journal fold. Each journal collector also visits the new input once.

## Schema and decoding detail

| Turns | readPrompt schema nodes | Decoder schema nodes | Decoder root calls | Decoder Effects | Decoder nodes / selected record |
|---:|---:|---:|---:|---:|---:|
| 50 | 12,494 → 11,132 | 9,524 → 8,723 | 86 → 85 | 7,533 → 7,029 | 35.537 → 40.014 |
| 250 | 62,107 → 55,399 | 47,367 → 43,398 | 423 → 422 | 37,437 → 34,980 | 35.507 → 39.961 |
| 1,000 | 248,287 → 221,401 | 189,382 → 173,474 | 1,688 → 1,684 | 149,674 → 139,899 | 35.505 → 39.971 |
| 3,500 | 868,946 → 774,830 | 662,795 → 607,102 | 5,907 → 5,894 | 523,833 → 489,592 | 35.504 → 39.972 |

A schema-node call is an invocation of an interpreted AST parser, including structural and primitive nodes. Root calls separately count SchemaParser.runWithCompiler. Decoder scope excludes SQL/planning. Both native Prompt message traversals remain unchanged. Average nodes per surviving record rises because removed settlements are cheaper than the retained mix.

| Turns | Whole-Run canonical decodes | Whole-Run canonical bytes | Whole-Run schema nodes |
|---:|---:|---:|---:|
| 50 | 284 → 234 | 224,583 → 174,733 | 36,669 → 35,307 |
| 250 | 1,350 → 1,102 | 1,044,505 → 797,249 | 126,172 → 119,464 |
| 1,000 | 5,350 → 4,356 | 4,132,937 → 3,141,919 | 396,352 → 369,466 |
| 3,500 | 18,684 → 15,204 | 14,450,569 → 10,981,009 | 1,297,043 → 1,202,927 |

Whole-Run canonical decode/byte reductions equal prompt reductions, so omitted historical settlements are not decoded elsewhere in this window. Both variants perform one journal projection, four full-envelope decodes, and nine scripted model calls.

## Correlated-query SQL cost

| Turns | Planning rows | Hydration rows | Prompt rows total | All SQL rows | Net additional rows |
|---:|---:|---:|---:|---:|---:|
| 50 | 269 → 369 | 270 → 366 | 539 → 735 | 1,220 → 1,416 | 196 |
| 250 | 1,335 → 1,839 | 1,340 → 1,840 | 2,675 → 3,679 | 5,652 → 6,656 | 1,004 |
| 1,000 | 5,335 → 7,347 | 5,355 → 7,360 | 10,690 → 14,707 | 15,275 → 19,292 | 4,017 |
| 3,500 | 18,669 → 25,709 | 18,741 → 25,765 | 37,410 → 51,474 | 47,355 → 61,419 | 14,064 |

| Turns | Additional planning rows | Additional hydration rows | Prompt SQL calls | All SQL calls | All SQL row-read increase |
|---:|---:|---:|---:|---:|---:|
| 50 | 100 | 96 | 4 → 2 | 206 → 204 | 16.066% |
| 250 | 504 | 500 | 12 → 10 | 243 → 241 | 17.764% |
| 1,000 | 2,012 | 2,005 | 42 → 34 | 273 → 265 | 26.298% |
| 3,500 | 7,040 | 7,024 | 146 → 120 | 377 → 351 | 29.699% |

The captured SQL contains the NOT EXISTS subquery in both planning and hydration, explicitly INDEXED BY effect_agent_records_call. Every additional observed row read is in those two prompt-query groups; outside-prompt reads are unchanged. SQL call counts decrease as pages shrink, and all observed row writes remain 544 per Run.

These are completed workerd cursor.rowsRead/rowsWritten counters, covering outer scans and correlated probes together. The net differences above measure the SQL cost of this candidate, including changed page boundaries. The existing hooks do not separately count correlated-subquery invocations or isolate inner versus outer index visits. No invented per-probe cost is assigned. Counts are neither physical disk I/O nor Cloudflare billed rows.

At 3500 turns, the candidate removes 3,480 prompt records (18.642%), 3,469,560 JSON bytes (24.044%), 35,115 readPrompt Effects (6.641%), and 56,526 whole-Run Effects (7.636%). SQL row reads increase 14,064 (29.699%). These values are measured, not extrapolated.

## Method and limits

- Exact candidate SqlThreadNativeReads.ts is substituted into the baseline bundle before the unchanged counters are applied. Records.ts is byte-identical to baseline and candidate. Removing the task-local variant selectors restores original instrumentation byte-for-byte. Probe and transfer helpers are unchanged. The commit also contains documentation/changeset changes with no runtime effect.
- Baseline captures are reused. One new candidate capture ran at each size against the same retained canonical archive. Normalized importer tables and both fingerprints match baseline at every size. Portable source replay validation is recorded in provenance.json. No full ready gate was repeated; the owner handles product validation.
- Seed workload is examples/durable-bench/src/plan.ts: repeating 1/1/0 tool cycle, 256-byte results with every 97th result 8 KiB, no compaction. 1000/3500 fixtures continued private copies of the existing 250 seed. The measured input is `turn count-0 tools=8`, with nine scripted model calls and successful settlement. This benchmark does not prove other recovery, compaction, failure, or late-owner cases.
- The real canonical importer creates each disposable database. Historical Attempt rows are omitted by import; no attempt/claim rows are copied. Fresh Miniflare then executes the existing inline DurableAgentRuntime admission/process path. Native alarms are disabled in the counting bundle to avoid concurrent maintenance. Public RPC dispatch, polling, deployed startup, setup/import, and fingerprinting are outside the window.
- Hooks are in-memory esbuild substitutions. JSON.parse bytes use TextEncoder. Schema hooks wrap existing interpreted parsers; Effect hooks count runLoop iterations without adding schemas or Effect operations. Hooks add JavaScript work. Global scopes apply only to this isolated single-Run experiment. SQL coverage includes the Effect DO SQLite adapter and direct due-queue queries.
- Effect 4.0.0, Bun 1.4.2, Miniflare 5.20260811.1-alpha, macOS arm64. No timing, allocation, memory, billing, or deployment improvement is claimed. No provider calls, credentials, product edits, deployments, publication, or sibling-fixture writes were needed. No committed tests or infrastructure were added.
- See REPRODUCE.md for commands and PUBLISH.md for the text-only allowlist. Exact IDs/timestamps in retained fixtures matter for raw byte totals; regenerated seeds can match transcripts but differ in bytes. Raw captures and archives stay local and are bound by SHA-256 in provenance.json. Earlier codec/pi and prompt-reads evidence remains separate.

## Fingerprints

Both variants match. The measured fingerprint covers the transcript supplied to the last scripted model request; counts continue through successful settlement.

| Seed turns | Seed fingerprint | Measured fingerprint |
|---:|---|---|
| 50 | `b017b487524e44a4` | `7b3b83c4979d51bb` |
| 250 | `dcea9f30b0917245` | `c29eb4d490969f50` |
| 1,000 | `ac520308146f2a8f` | `02f7ac7f9eb06bee` |
| 3,500 | `0a8c8e4b0d9a0794` | `6dd3a56608e79a1d` |
