# Deterministic history operation counts

Candidate 1 was rejected for lack of demonstrated warm improvement. These operation counts are diagnostic evidence, not a recommendation to adopt it.

Baseline `07f0272e`; candidate `40485f33`; Effect `4.0.0`; pi-durable `1.0.4`; Bun `1.4.2`; Miniflare `5.20260811.1-alpha`; macOS arm64. No timing or CPU claim.

The exact candidate Records.ts was substituted into the baseline bundle. The only other commit difference is a changeset. Both revisions used identical hooks and the same canonical import archives.

## Yielded: one new Run, eight sequential lookups

| Seed turns | Selected records | Raw JSON bytes | Encoded message passes B → C | Native passes B = C | Schema nodes in readPrompt B → C | Effect evaluations in readPrompt B → C |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 268 | 204,113 | 184 → 0 | 184 | 12,494 → 10,614 | 7,667 → 7,737 |
| 250 | 1,334 | 1,024,025 | 917 → 0 | 917 | 62,107 → 52,767 | 37,839 → 38,196 |
| 1,000 | 5,334 | 4,112,455 | 3,667 → 0 | 3,667 | 248,287 → 210,947 | 151,084 → 152,506 |
| 3,500 | 18,668 | 14,430,077 | 12,834 → 0 | 12,834 | 868,946 → 738,266 | 528,733 → 533,692 |

Each selected historical record is JSON-decoded once. SQL visits it for size planning and again for payload hydration. Journal metadata and projection each visit every selected record once, plus the new input. No historical Run verification or second JSON decode occurs in those journal visits.

## Decoder-only work and SQL

| Turns | Decoder schema nodes B → C | Decoder schema nodes per selected record B → C | Root decoder calls B → C | Decoder Effect evaluations B → C | Prompt SQL rows B = C | All observed SQL rows B = C |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 9,524 → 7,644 | 35.537 → 28.522 | 86 → 2 | 7,533 → 7,603 | 539 | 1,220 |
| 250 | 47,367 → 38,027 | 35.507 → 28.506 | 423 → 6 | 37,437 → 37,794 | 2,675 | 5,652 |
| 1,000 | 189,382 → 152,042 | 35.505 → 28.504 | 1,688 → 21 | 149,674 → 151,099 | 10,690 | 15,275 |
| 3,500 | 662,795 → 532,115 | 35.504 → 28.504 | 5,907 → 73 | 523,833 → 528,798 | 37,410 | 47,355 |

| Turns | Canonical JSON decodes (whole Run) | Canonical JSON bytes | Journal metadata / projection visits | Planning SQL rows / hydration SQL rows |
|---:|---:|---:|---:|---:|
| 50 | 284 | 224,583 | 269 / 269 | 269 / 270 |
| 250 | 1,350 | 1,044,505 | 1,335 / 1,335 | 1,335 / 1,340 |
| 1,000 | 5,350 | 4,132,937 | 5,335 / 5,335 | 5,335 / 5,355 |
| 3,500 | 18,684 | 14,450,569 | 18,669 / 18,669 | 18,669 / 18,741 |

A schema-node call is one invocation of an interpreted AST parser, including primitive fields and structural nodes. It is not one public Schema API call. Root decoder calls separately count calls through SchemaParser.runWithCompiler within row decoding: one per page plus baseline native Prompt callbacks. The measured schema reduction does not imply fewer Effect evaluations: this candidate slightly increases that count.

## pi comparison

| Turns | Persisted entries | Historical entry JSON parses | Immutable historical JSON bytes | Context derivations / scans | Projection entry visits | Observed SQL rows |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 169 | 3,044 | 1,080,912 | 18 / 18 | 3,204 | 4,500 |
| 250 | 835 | 15,086 | 5,515,302 | 18 / 18 | 15,192 | 16,596 |
| 1,000 | 3,335 | 60,266 | 22,409,010 | 18 / 18 | 60,192 | 61,956 |
| 3,500 | 11,669 | 210,854 | 79,377,432 | 18 / 18 | 210,204 | 213,120 |

pi storage parses entry JSON directly; its SQLite read path has no Effect Schema validation. This is source evidence, not a claim that all pi runtime input is unvalidated. Its two context reads per model response cause 18 range scans for nine model calls; paginated lookahead and point reads add repeated entry parses. Most original entries are decoded 18 times, with boundary entries decoded 20 times. Raw visit histograms are retained.

pi entries and Yielded canonical records have different shapes and durability duties, so bytes per record and validation costs are not interchangeable. pi performs more JSON parsing in this pinned version; Yielded performs native-schema validation and Effect interpretation. Neither count alone establishes which is faster.

## Method and scope

- Seed workload is examples/durable-bench/src/plan.ts: repeating 1/1/0 tool cycle, 256-byte results with every 97th result 8 KiB, no compaction. The measured new input is `turn count-0 tools=8`; all targets make nine scripted model calls and settle successfully.
- 50/250 snapshots are read-only copies from an existing benchmark fixture checkout. 1000/3500 continue a private copy of its 250 snapshot through the existing baseline seeder. No original fixture is modified. Original fixture metadata contains the seeder's incidental durations; those are not used in this count report.
- Yielded exports canonical archives, imports via the real importer, and verifies the seed transcript. Only historical Attempt rows are omitted by import. No attempt/claim rows are copied. Fresh Miniflare instances then run the existing inline DurableAgentRuntime admission/process path through settlement. The 3500 archive exceeds RPC's 32 MiB value limit, so setup transfers it through streamed Request/Response bodies. Native alarm handling is disabled in the counting bundle to avoid background maintenance in the window. Public RPC dispatch, settlement polling, and deployed startup are outside this diagnostic scope.
- pi uses a fresh instance over its copied SQLite seed and the existing Harness submit/wait/idle path. A pre-window MAX(id) query marks immutable seeded entries, without opening the Harness. Import/copy, verification, fingerprint computation, reporting, and SQL summary queries are excluded from counts.
- Hooks are esbuild onLoad text substitutions in memory. Shared node_modules and product source are unchanged. JSON.parse is wrapped; UTF-8 bytes use TextEncoder. AST interpreter parsers and Effect.runLoop iterations are counted without adding schemas or Effect operations. readPrompt and its row-decoding scope use existing generator yield boundaries.
- SQL rows use completed workerd cursor.rowsRead. Coverage includes the Effect DO SQLite adapter, direct host due-queue SQL, and the pi benchmark SQL adapter. This reports observed engine reads, including writes that read rows. It is not a physical disk-I/O metric or a claim about Cloudflare billed rows. SQL statement breakdowns are retained in raw JSON.
- Journal metadata/fold counts are attached at the existing collectors/loops. pi context counters attach to deriveContext/scanRange and actual contributions mapping. No committed tests or instrumentation infrastructure were added.
- 50/250/1000 Yielded baseline/candidate core counts (records, bytes, message/schema-node passes, and Effect evaluations) repeated exactly while detailed attribution hooks were added. Final complete SQL breakdowns match baseline/candidate at all four sizes; 3500 has one final capture per revision. pi operation counts repeated; generated new-entry JSON varied by 96 bytes in two samples, so the main comparison reports immutable historical bytes. Earlier trials are retained. All final outputs revalidate the model-visible seed fingerprint, and baseline/candidate/pi measured transcript digests match at every reported size.
- No timing, allocation, memory, billing, or deployment improvement is claimed. Hooks add JavaScript work. Global scopes are appropriate only for the isolated inline single-Run experiment. No credential access, live models, deployment, publication, product edits, or sibling fixture writes occurred.
- The product full ready gate passed separately. Local evidence consists of the measurement workflow, matched fingerprints, repeated core counts, and formatting checks.

## Reproduction and publication contents

See `REPRODUCE.md` for commands and `PUBLISH.md` for the exact publication allowlist. `summary.json` retains the reported count values. Source hooks are unchanged from the capture; only local input/output paths were made explicit CLI parameters. The small candidate patch reconstructs the exact measured Records.ts from the baseline.

This compact package deliberately omits canonical archives, JavaScript bundles, local databases, raw logs, environment diagnostics, local paths, and third-party source copies. Exact source-fixture IDs and timestamps influence byte totals: without the original local fixtures a regenerated workload must still match the transcript fingerprints, but raw byte totals need not match. The original fixtures and full raw captures remain local for exact replay and future candidates. No binary fixture is required in the public evidence branch.

## Fingerprints

| Turns | Seed | After measured Run (last model request) |
|---:|---|---|
| 50 | `b017b487524e44a4` | `7b3b83c4979d51bb` |
| 250 | `dcea9f30b0917245` | `c29eb4d490969f50` |
| 1,000 | `ac520308146f2a8f` | `02f7ac7f9eb06bee` |
| 3,500 | `0a8c8e4b0d9a0794` | `6dd3a56608e79a1d` |
