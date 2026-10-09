# Prompt-read admission filtering: deterministic counts

Baseline `07f0272e7ba49a494064b6b74c6318b55514ae19`; SQL candidate `87158447db05a1a2f9cfe4c38ea0aff4bceb4e3d` (`prompt-reads`). Native baseline `Records.ts` is used in both. The rejected codec candidate is not part of this comparison.

All four captures completed and matched baseline seed and measured transcript fingerprints. The SQL predicate removes exactly one historical `UserInputRecorded` per seeded turn from prompt hydration, decoding, metadata, and projection. Encoded/native message validation is unchanged. SQL cursor reads remain almost unchanged. These are operation counts, with no latency or CPU improvement claim.

## Prompt read and decode

Values are baseline → prompt-reads. Raw JSON bytes are UTF-8 canonical record payloads passed to JSON.parse within readPrompt.

| Seed turns | Records decoded | Raw JSON bytes | Schema-node calls | Effect evaluations | Pages |
|---:|---:|---:|---:|---:|---:|
| 50 | 268 → 218 | 204,113 → 185,973 | 12,494 → 11,032 | 7,667 → 7,109 | 2 → 1 |
| 250 | 1,334 → 1,084 | 1,024,025 → 933,135 | 62,107 → 54,845 | 37,839 → 35,320 | 6 → 5 |
| 1,000 | 5,334 → 4,334 | 4,112,455 → 3,748,565 | 248,287 → 219,239 | 151,084 → 141,000 | 21 → 17 |
| 3,500 | 18,668 → 15,168 | 14,430,077 → 13,153,687 | 868,946 → 767,290 | 528,733 → 493,499 | 73 → 60 |

| Turns | Encoded passes, unchanged | Native passes, unchanged | Decoder schema nodes | Decoder root calls | Decoder Effect evaluations | Decoder nodes / selected record |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 184 | 184 | 9,524 → 8,623 | 86 → 85 | 7,533 → 7,042 | 35.537 → 39.555 |
| 250 | 917 | 917 | 47,367 → 42,866 | 423 → 422 | 37,437 → 34,982 | 35.507 → 39.544 |
| 1,000 | 3,667 | 3,667 | 189,382 → 171,378 | 1,688 → 1,684 | 149,674 → 139,855 | 35.505 → 39.543 |
| 3,500 | 12,834 | 12,834 | 662,795 → 599,782 | 5,907 → 5,894 | 523,833 → 489,467 | 35.504 → 39.543 |

A schema-node call is an invocation of an interpreted AST parser, including structural and primitive nodes; root calls separately count SchemaParser.runWithCompiler. Decoder-only scope excludes SQL and planning. Root calls equal the page decodes plus native Prompt callbacks. Every selected historical record is JSON-decoded once. Both message schema traversals remain. The average schema calls per surviving record rises because the omitted input records were cheaper than the retained mix.

## Journal and whole Run

| Turns | Metadata visits = projection visits | Whole-Run canonical decodes | Whole-Run canonical bytes | Whole-Run schema nodes | Whole-Run Effect evaluations |
|---:|---:|---:|---:|---:|---:|
| 50 | 269 → 219 | 284 → 234 | 224,583 → 206,443 | 36,669 → 35,207 | 37,112 → 36,215 |
| 250 | 1,335 → 1,085 | 1,350 → 1,100 | 1,044,505 → 953,615 | 126,172 → 118,910 | 78,169 → 74,111 |
| 1,000 | 5,335 → 4,335 | 5,350 → 4,350 | 4,132,937 → 3,769,047 | 396,352 → 367,304 | 230,905 → 214,655 |
| 3,500 | 18,669 → 15,169 | 18,684 → 15,184 | 14,450,569 → 13,174,179 | 1,297,043 → 1,195,387 | 740,267 → 683,496 |

Metadata and fold each visit every selected historical record once, plus the new input once. Each run has one journal projection, four full-envelope decodes, and nine scripted model calls in both variants. The historical omitted inputs are not JSON-decoded elsewhere in this measured window: whole-Run canonical decode and byte reductions equal the readPrompt reductions. Newly written/current-Run records remain outside the historical selection.

## Observed SQL cursor reads

| Turns | Planning rows, unchanged | Hydration rows | Prompt SQL calls | Prompt rows total | All SQL rows | All SQL calls |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 269 | 270 → 268 | 4 → 2 | 539 → 537 | 1,220 → 1,218 | 206 → 204 |
| 250 | 1,335 | 1,340 → 1,336 | 12 → 10 | 2,675 → 2,671 | 5,652 → 5,648 | 243 → 241 |
| 1,000 | 5,335 | 5,355 → 5,346 | 42 → 34 | 10,690 → 10,681 | 15,275 → 15,266 | 273 → 265 |
| 3,500 | 18,669 | 18,741 → 18,713 | 146 → 120 | 37,410 → 37,382 | 47,355 → 47,327 | 377 → 351 |

The added predicate filters returned records; it does not reduce the measured planning cursor scan. All observed row writes remain 544 per Run. These numbers are workerd cursor.rowsRead/rowsWritten, including writes that read rows, not disk I/O or Cloudflare billing. Fewer hydrated/decoded records must not be described as a proportional reduction in database row reads.

At 3500 turns, measured reductions are 3500 prompt records (18.749%), 1,276,390 prompt JSON bytes (8.845%), 101,656 readPrompt schema-node calls (11.699%), and 35,234 readPrompt Effect evaluations (6.664%). Metadata and fold each lose 3500 visits. Whole-Run Effect evaluations drop 56,771 (7.669%). Prompt SQL cursor reads drop only 28 (0.075%). These percentages are calculated from the captures, not extrapolated.

## Method, reproducibility, and limits

- Same retained canonical archives, baseline dependencies, probe, AST/Effect hooks, SQL cursor hooks, and measured boundaries as the baseline. Only exact candidate SqlThreadNativeReads.ts is selected before instrumentation. Removing this source selector recovers the previous instrument.ts byte-for-byte. Records.ts is byte-identical to baseline and to this candidate commit. The candidate commit also has a documentation comment and changeset; neither changes runtime behavior.
- Baseline captures are reused without rerunning. One new candidate capture was taken at each size. The portable packaging check is recorded in provenance.json. No full ready gate was repeated; product validation remains owned separately. This benchmark does not prove compaction, late-owner, recovery, or other semantic cases absent from its workload.
- Seed plan is examples/durable-bench/src/plan.ts: repeating 1/1/0 tool cycle, 256-byte results with every 97th result 8 KiB, no compaction. Measured input is `turn count-0 tools=8`; nine scripted model calls run through successful settlement. 1000/3500 fixtures continued private copies of the existing 250 seed.
- Real canonical export/import restores each disposable database. Normalized importer table inventories match baseline at all sizes; historical Attempt rows are omitted by import, and no attempt/claim rows are copied. Fresh Miniflare then executes the existing inline DurableAgentRuntime admission/process path. Native alarms are disabled in the counting bundle to avoid concurrent maintenance. Public RPC dispatch, polling, and deployed startup are outside the measured window.
- Import/export, fixture copying, fingerprint checks, and reporting are outside counters. JSON.parse bytes use TextEncoder; schema hooks wrap existing interpreted parsers; Effect hooks count runLoop iterations. Hooks add JavaScript work but no schemas or Effect operations. Global scopes are only suitable for this isolated single-Run experiment.
- Effect 4.0.0, Bun 1.4.2, Miniflare 5.20260811.1-alpha, macOS arm64. No live model calls, credentials, deployment, publication, product edits, or sibling fixture writes were needed. No committed tests or infrastructure were added.
- See REPRODUCE.md and reproduce.sh for exact commands, source hooks, and SQL patch reconstruction. Original fixtures/archives preserve exact IDs and timestamps; independently regenerated seeds can match fingerprints while raw bytes differ. Raw captures stay local; provenance.json binds them by SHA-256. PUBLISH.md lists the compact text-only publication contents. The earlier codec/pi bundle is retained separately.

## Fingerprints

Both variants match at every size. The measured digest is the transcript supplied to the final scripted model request; successful Run settlement is included in counts.

| Seed turns | Seed fingerprint | Measured fingerprint |
|---:|---|---|
| 50 | `b017b487524e44a4` | `7b3b83c4979d51bb` |
| 250 | `dcea9f30b0917245` | `c29eb4d490969f50` |
| 1,000 | `ac520308146f2a8f` | `02f7ac7f9eb06bee` |
| 3,500 | `0a8c8e4b0d9a0794` | `6dd3a56608e79a1d` |
