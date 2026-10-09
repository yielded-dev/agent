| Seed turns | TTFT (ms) | State | Yielded (ms) | pi 1.0.4 (ms) | tardie 0.44.0 (ms) | Yielded ÷ pi |
| --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 759 [733–1,084] | 748 [671–753] | 1,976 [1,944–2,012] | 1.01× |
| 50 | 0 | cold | 1,292 [1,265–1,715] | 967 [894–1,077] | 2,616 [2,490–2,631] | 1.34× |
| 50 | 400 | warm | 4,972 [4,951–4,997] | 4,796 [4,723–4,895] | 6,006 [5,735–6,335] | 1.04× |
| 50 | 400 | cold | 5,553 [5,432–5,595] | 5,127 [4,967–5,260] | 6,371 [6,354–6,854] | 1.08× |
| 250 | 0 | warm | 1,069 [999–1,193] | 686 [670–779] | 2,238 [1,861–2,689] | 1.56× |
| 250 | 0 | cold | 1,535 [1,530–1,922] | 898 [893–907] | 2,777 [2,596–3,452] | 1.71× |
| 250 | 400 | warm | 5,093 [5,025–5,145] | 4,860 [4,858–4,909] | 5,808 [5,696–5,955] | 1.05× |
| 250 | 400 | cold | 5,768 [5,625–5,923] | 5,167 [5,093–5,256] | 6,911 [6,449–7,020] | 1.12× |

Driver-observed turn latency: **median [Q1–Q3] of three Object medians**, rounded milliseconds. Each Object has one cold turn, one excluded settling turn, then three warm repeats. Q1/Q3 use linear interpolation (type 7). The ratio divides the two displayed targets' unrounded medians; it is not a paired same-Object effect.

Warm Yielded ranges from **759–1,069 ms at zero TTFT** and **4,972–5,093 ms at 400 ms TTFT**. Its warm differences from pi are no larger than the biggest repeat range in the corresponding cell, so this small run does not establish a stable warm ranking between them. Tardie has the highest median in each cell, with substantial spread. Every accepted Yielded turn used exactly one maintenance alarm pass.

This measures **main at the start of the run, aa7855a6c85e9a2a9395098c66f7cfcfe47c8467**, including #821, #825 and #826, against pi-durable 1.0.4 and tardie 0.44.0. There are **180 verified main turns and 1,620 matching provider receipts**: 36 cold and 108 warm turns feed the headline table; 36 settling turns are retained but excluded from it. Inline Yielded was not measured. These are small descriptive cohorts; differences within the repeat or Object spread are unresolved.

**Admission and model gaps.** Milliseconds; admission is Yielded submit → public receipt. Pi and tardie expose completion through their native benchmark paths, so this harness does not report a comparable separate admission receipt for them. Gaps are the median of the eight provider-end → next-provider-arrival intervals, reduced within each Object first.

| Seed / TTFT | State | Yielded admission | Yielded gap | pi gap | tardie gap |
| --- | --- | --- | --- | --- | --- |
| 50 / 0 | warm | 155 | 63 (3/3) | 66 (3/3) | 184 (3/3) |
| 50 / 0 | cold | 253 | 81 (3/3) | 76 (3/3) | 189 (3/3) |
| 50 / 400 | warm | 70 | 62 (3/3) | 65 (3/3) | 169 (3/3) |
| 50 / 400 | cold | 231 | 73 (3/3) | 72 (3/3) | 173 (3/3) |
| 250 / 0 | warm | 266 | 72 (3/3) | 67 (3/3) | 195 (3/3) |
| 250 / 0 | cold | 715 | 81 (3/3) | 67 (3/3) | 187 (3/3) |
| 250 / 400 | warm | 75 | 60 (3/3) | 72 (3/3) | 173 (3/3) |
| 250 / 400 | cold | 223 | 66 (3/3) | 78 (3/3) | 185 (3/3) |

**Last model response → client.** Conditional clock bounds in ms; parentheses show Objects with complete repeated evidence. This includes finalization and completion observation. It does not isolate storage notification. Unavailable means the before/after clock probes and provider colos could not support a bound.

| Seed / TTFT | State | Yielded | pi | tardie |
| --- | --- | --- | --- | --- |
| 50 / 0 | warm | 73…79 (2/3) | 49…56 (1/3) | 98…105 (2/3) |
| 50 / 0 | cold | 102…107 (2/3) | 57…62 (1/3) | 166…175 (2/3) |
| 50 / 400 | warm | 66…72 (2/3) | 28…33 (1/3) | unavailable (0/3) |
| 50 / 400 | cold | 87…92 (2/3) | 27…32 (1/3) | unavailable (0/3) |
| 250 / 0 | warm | 79…83 (3/3) | 51…56 (1/3) | 114…119 (2/3) |
| 250 / 0 | cold | 122…123 (3/3) | 52…58 (1/3) | 121…128 (2/3) |
| 250 / 400 | warm | 71…77 (2/3) | 40…47 (2/3) | 66…72 (2/3) |
| 250 / 400 | cold | 104…108 (2/3) | 44…49 (2/3) | 73…80 (2/3) |

**CPU per target invocation.** Cloudflare-attributed ms, median across Objects; parentheses show complete-Object coverage. Alarm CPU is the median per pass within each turn, then across repeats and Objects. Individual pass IDs, CPU, wall time and missing joins remain in the turn records.

| Seed / TTFT | State | Y submit | Y alarm/pass | Y awaitSettlement | pi fetch | tardie fetch | tardie alarm/pass |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50 / 0 | warm | 7 (3/3) | 64 (3/3) | 282 (3/3) | 259 (3/3) | 11 (3/3) | 539 (2/3) |
| 50 / 0 | cold | 55 (3/3) | 260 (3/3) | 331 (3/3) | 437 (3/3) | 223 (2/3) | 404 (2/3) |
| 50 / 400 | warm | 10 (3/3) | 10 (3/3) | 524 (3/3) | 242 (3/3) | 23 (2/3) | 187 (3/3) |
| 50 / 400 | cold | 90 (3/3) | 412 (3/3) | 558 (3/3) | 344 (3/3) | 448 (3/3) | 243 (3/3) |
| 250 / 0 | warm | 9 (3/3) | 166 (3/3) | 221 (3/3) | 262 (3/3) | 18 (3/3) | 35 (3/3) |
| 250 / 0 | cold | 60 (3/3) | 474 (3/3) | 324 (3/3) | 330 (3/3) | 543 (3/3) | 311 (3/3) |
| 250 / 400 | warm | 15 (3/3) | 78 (2/3) | 525 (2/3) | 408 (3/3) | 28 (3/3) | 335 (3/3) |
| 250 / 400 | cold | 92 (3/3) | 563 (2/3) | 581 (3/3) | 523 (3/3) | 741 (3/3) | 122 (3/3) |

Observed Thread alarm counts per accepted turn were 1 for Yielded and 1–6 for tardie. CPU is invocation accounting, not an additive critical-path partition: concurrent work may be charged to the waiter, and an alarm can outlive client completion. Missing or ambiguous joins are null, never zero. Tardie Actor native lookup/allocate CPU is observed-only and requires a unique Thread fetch/alarm trace; unscoped Actor alarms and Actor totals are not inferred. Across accepted turns, uniquely joined native Actor RPCs had pooled invocation CPU medians of 54 ms cold (9 observations) and 1 ms warm (28 observations); these are observed-only, not complete Object aggregates. Raw Actor invocation values and all captured outcomes are retained.

**Repeat spread.** Warm driver max−min within each Object: median range (largest range), ms. No latency outliers were trimmed. These repeats also lengthen the transcript, so the ranges combine workload growth with placement and time variation. Full Object ranges, drift and cold spreads are in the summary.

| Seed / TTFT | Yielded repeat range | pi repeat range | tardie repeat range |
| --- | --- | --- | --- |
| 50 / 0 | 122 (571) | 156 (171) | 313 (452) |
| 50 / 400 | 69 (126) | 175 (260) | 590 (910) |
| 250 / 0 | 287 (511) | 46 (190) | 615 (925) |
| 250 / 400 | 255 (274) | 98 (165) | 523 (542) |

All targets share one primary Worker bundle, avoiding separate-Worker deployment differences. The combined bundle can change isolate memory and JIT behavior relative to a single-framework deployment. Framework order is shuffled within each sample round and group order is shuffled. Distinct frameworks require distinct Object namespaces: three Objects per role are the statistical units. Three Objects and three warm repeats cannot establish an SLO or a portable ranking. This run does not isolate the individual effects of #825 or #826, and comparisons with earlier reports would mix deployments, Objects and observation scopes.

**Secondary laptop observation.** Same aggregation, ms. It includes ingress routing, clock probes and diagnostic reads outside the primary timer. CF-Ray colos across all recorded main responses (including the retired group): SJC (191); every ray is preserved per run. Provider receipt colos: DEN, DFW, LAX, SJC. A shared location hint is not proof of physical co-location.

| Seed / TTFT | State | Yielded laptop | pi laptop | tardie laptop |
| --- | --- | --- | --- | --- |
| 50 / 0 | warm | 829 [799–1,154] | 805 [731–809] | 2,113 [2,105–2,171] |
| 50 / 0 | cold | 1,348 [1,331–1,807] | 1,053 [972–1,140] | 2,760 [2,595–2,767] |
| 50 / 400 | warm | 5,060 [5,041–5,070] | 4,909 [4,854–4,996] | 6,162 [5,896–6,455] |
| 50 / 400 | cold | 5,612 [5,514–5,730] | 5,241 [5,079–5,426] | 6,530 [6,501–7,000] |
| 250 / 0 | warm | 1,141 [1,057–1,317] | 738 [725–845] | 2,344 [1,971–2,780] |
| 250 / 0 | cold | 1,620 [1,597–2,008] | 1,020 [1,013–1,088] | 2,880 [2,705–3,568] |
| 250 / 400 | warm | 5,148 [5,112–5,203] | 4,918 [4,913–4,965] | 5,863 [5,753–6,030] |
| 250 / 400 | cold | 5,826 [5,719–5,982] | 5,324 [5,238–5,463] | 7,011 [6,543–7,097] |

**Method and evidence limits.** Alchemy deployed the rebench-prefixed driver/target Worker, provider Worker and four Durable Object namespaces in **Danieljmerwe@gmail.com's Account**. Driver placement is aws:us-west-1; all Object bindings, including tardie's internal routing, use locationHint wnam. The uploaded measurement bundle is SHA-256 4616646c6ca990bf2df484cfb7e026439e6a7611c8698fb2195068619e372134. [source-contract.json](source-contract.json) verifies all 184 compiled framework sources against main. Exact seed/measurement bundles and source/input hashes are in build-identities; only observer code changed between seeding and measurement.

The harness reuses origin/dan/bench-production-path's stack.ts, run.mjs, build.mjs, deployed driver/mock and network Yielded target; tardie's native OpenAI-compatible adapter comes from origin/dan/cf-latency-breakdown. Yielded uses CloudflareThreadClient.submit → awaitSettlement through ThreadObject; client layer construction and definition hashing precede the driver timer. Processing stays in native alarm maintenance. Pi uses its native submit/wait/idle path. Tardie keeps the original pinned reference.methods.message → reference.wait bridge; it is not a new HTTP-route benchmark. Diagnostic metrics and Actor identity reads occur after the completion timestamp.

History is seeded in place through the original [1,1,0] lookup cycle. Fingerprints are exactly **50: b017b487524e44a4; 250: dcea9f30b0917245**. Every timed turn adds eight readonly lookups plus a final answer (nine model requests); the controller checks every request against the growing reference transcript. Compaction is off. Fingerprints cover role, text and lookup arguments, excluding system/developer framing and normalizing the Effect adapter's JSON-string tool result. Raw wire fingerprints and request sizes retain provider-envelope differences. The 400 ms TTFT setting also retains the reused mock's 10 ms inter-frame delays: **4,130 ms of programmed waiting per turn**, rather than 3,600 ms.

Cold uses acknowledged storage.sync and ctx.abort over seeded storage, followed by a new incarnation, first measured harness entry and no prior alarms. Both tardie Thread and Actor are reset. An Actor first-native-entry snapshot is taken only if lookup/allocate uses it; otherwise the post-timer diagnostic entry records the unused Actor without charging artificial hydration to the turn. This is Object reconstruction, not a guaranteed fresh isolate/JIT. Warm retains incarnation and Worker version from m0. Identity heartbeats touch only idle comparison Objects every three seconds. Canonical history is never rewound; settling and warm inputs grow it identically for all targets.

Primary elapsed/admission timings use one deployed driver's clock around I/O. Durable Object clocks can be frozen or stale, so storage settledAt and span durations are not used as exact notification or CPU timings. Provider-to-driver bounds intersect the two echo-probe offset intervals and require compatible colos; they assume an offset consistent with those probes. Provider-to-provider gaps require compatible receipt clocks and include transport plus durable processing, not just SQL. Negative bounds remain signed; unavailable evidence is not replaced with laptop timings.

CPU comes exclusively from deployed cf-worker-event telemetry. Submit, await and alarm START markers join to unique Worker-scoped invocation identities; contradictory END contexts do not replace the START join. Native fetches join by Object/version/query identity. Complete warm CPU aggregates require all three repeats for that Object. Platform sampling persisted despite headSamplingRate=1; largest reported sample intervals: primary 2.2, provider 1.6. No local timing is evidence.

**Failures and coverage.** Observed exceededCpu: **0**; exceededMemory: **2**. All captured non-OK invocation outcomes follow; individual events are in [failed-outcomes.json.gz](failed-outcomes.json.gz). These are observed counts under sampling, not an exhaustive platform census.

| Worker | Scope | Invocation | Outcome | Count |
| --- | --- | --- | --- | --- |
| primary | durableObject | alarm | canceled | 1078 |
| primary | durableObject | jsrpc | aborted | 44 |
| primary | durableObject | fetch | aborted | 130 |
| primary | durableObject | fetch | exceededMemory | 1 |
| primary | durableObject | alarm | exceededMemory | 1 |
| primary | stateless | fetch | canceled | 2 |
| primary | durableObject | alarm | aborted | 1 |
| primary | durableObject | jsrpc | canceled | 1 |

One initial tardie/250/zero-delay fixture failed during seed-220 with a memory reset. Its whole three-target group was retired before measurement, then replaced by fresh Objects seeded sequentially; no uncertain seed input was replayed. The reset appears in both a fetch and an alarm invocation. Eight initial driver-readiness requests returned 404 during deployment propagation. All 180 accepted main turns completed successfully. A controller process interruption left one request without a response after 116 recorded turns; seven completed groups were retained and the incomplete three-target group was replaced, without replaying the uncertain input. Its 11 successful but incomplete-group observations remain excluded. The repeated activation used identical module bytes and the existing Alchemy-owned resources; the two measurement epochs are a time-related confounder. The 9 failed HTTP requests across the full experiment, unreturned attempt and controller errors remain recorded. Plan/reference failures: 0/0; excluded retired-group responses: 11; join/observation issues retained: 161. The raw initial plan and retiredSeedGroups retain replacement provenance.

The append-only log/hash chain, producer fencing, claims/leases, Unknown handling, original-context recovery/historyDigest, canonical accounting and model-visible reference transcript are unchanged. No framework optimization or recovery shortcut was introduced.

**Artifacts and cleanup.** Reproduce reduction with vp exec node examples/durable-bench/results/rebench/analyze.mjs; inputs may be plain or gzip. requests/attempted JSONL retain controller evidence and CF-Ray values; turns JSONL retains individual timing/CPU joins; summary.json.gz and tables.md retain every cell, spread and limit. Raw telemetry, failures, plans, completion ledgers and exact build archives are included. The branch is dan/rebench-20261009; no PR is opened.

vp run ready passed (1187 tests passed, 10 skipped; 38/71 tasks reused cache). The final gate used the existing local Postgres override after initial overlapping attempts failed at the default test database and reported exit 137. Its log and task receipt are under validation; this is correctness evidence only.

Alchemy destroyed both Workers. At **2026-10-09T02:31:46.736Z**, API verification in **Danieljmerwe@gmail.com's Account** found **zero rebench Workers and zero rebench Durable Object namespaces**, with 404 for both recorded Workers. Final telemetry was collected, the exact account/API/benchmark credentials were scanned against raw and decompressed artifacts, and the private mode-700 Alchemy state was removed. [cleanup.json](cleanup.json) records the checks. No credentials or Alchemy state are committed.
