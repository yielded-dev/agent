# Cold-turn result: three deletions tested, no demonstrated latency saving

**No product change is retained and no latency-fix PR is opened.** Two independent constructor changes reduced cold-only native transactions from **5 to 3**; layout sharing reduced constructor SQL from **12 to 6**, and consistent-state reconciliation reduced it to **11**. The historical-fold change removed repeated traversals and ID validation. None beat the full repeated-baseline and candidate spread on driver latency, admission or joined CPU. No milliseconds saved are claimed. All three reviewed patches remain under [candidates/](candidates/); product files are restored to the baseline.

| Breakdown | Baseline | Layout candidate | Reconciliation candidate | Historical-fold candidate |
| --- | ---: | ---: | ---: | ---: |
| Cold constructor: native transactions / SQL / KV reads | 5 / 12 / 1 | 3 / 6 / 1 | 3 / 11 / 1 | 5 / 12 / 1 |
| Constructor write transactions / alarm calls in consistent idle fixture | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Post-construction transactions before first model | 11 | 11 | 11 | 11 |
| Post-construction transactions through alarm finish, of which writes | 34 / 22 | 34 / 22 | 34 / 22 | 34 / 22 |
| Warm constructor transactions | 0 | 0 | 0 | 0 |

The first two changes remove empty transactions whose native I/O-clock timestamps did not advance. They do not establish two fewer replication round trips. The historical change saves one repeated Tool Call ID decode per historical application call, two declaration traversals and one ordering-map construction per historical response. At the seed boundary, that is 34 calls / 84 responses for 50 turns and 167 / 417 for 250; later measured turns grow the history. These are source-derived work counts, not CPU savings.

## Driver results against repeat spread

Milliseconds, median within each Object, then median across two Objects. `B − C` is an observed point estimate: positive appears faster. The spread columns summarize each Object's **full max-minus-min repeat range**. Acceptance was fixed before editing code and checked in each Object individually; it was not tested against a smaller aggregate standard error. **Zero of 48 Object comparisons, cold and warm, cleared the threshold.**

| Candidate | Seed / provider delay | Cold baseline → candidate | B − C | Baseline / candidate repeat span |
| --- | --- | ---: | ---: | ---: |
| Layout | 50 / 0 | 1,318.5 → 1,449.5 | −131 | 358.5 / 239 |
| Layout | 250 / 0 | 1,561.5 → 1,473 | +88.5 | 397 / 576.5 |
| Layout | 50 / 400 | 5,662.5 → 5,479 | +183.5 | 438 / 282.5 |
| Layout | 250 / 400 | 5,346.5 → 5,333 | +13.5 | 528 / 929.5 |
| Reconciliation | 50 / 0 | 1,318.5 → 1,375 | −56.5 | 358.5 / 310 |
| Reconciliation | 250 / 0 | 1,561.5 → 1,443.5 | +118 | 397 / 585.5 |
| Reconciliation | 50 / 400 | 5,662.5 → 5,494 | +168.5 | 438 / 139 |
| Reconciliation | 250 / 400 | 5,346.5 → 5,384 | −37.5 | 528 / 165 |
| Historical fold | 50 / 0 | 1,636.5 → 1,650.5 | −14 | 374.5 / 462.5 |
| Historical fold | 250 / 0 | 1,645.75 → 1,567 | +78.75 | 881 / 426 |
| Historical fold | 50 / 400 | 5,515.75 → 5,481 | +34.75 | 786.5 / 1,187.5 |
| Historical fold | 250 / 400 | 5,473.75 → 5,581.5 | −107.75 | 548 / 275 |

The constructor candidates share comparison 1. The historical-fold candidate has its own comparison 2, with the constructor candidates disabled in every label. Do not add their point estimates. Exact Object values, baseline-label differences, repeat order and acceptance decisions are in [acceptance.json](acceptance.json).

Warm driver medians:

| Seed / delay | Comparison 1 baseline | Layout | Reconciliation | Comparison 2 baseline | Historical fold |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / 0 | 1,003.75 | 978.75 | 1,071.25 | 1,158.5 | 1,182.75 |
| 250 / 0 | 1,191.5 | 1,172.75 | 1,226 | 1,199.75 | 1,172 |
| 50 / 400 | 5,218.75 | 5,210 | 5,262.75 | 5,173.75 | 5,242.5 |
| 250 / 400 | 5,051.5 | 4,995.5 | 5,067.75 | 5,062 | 5,083.5 |

No warm difference exceeded its repeat range in either direction. This is no detected regression at this resolution, not proof of zero regression. All warm spreads, admission and CPU measurements appear in [measurements.md](measurements.md).

## Remaining gap to pi and the cold breakdown

There is **no demonstrated closing of the gap to pi**. In the latest cohort, the baseline and interleaved pi observations at matched history positions were:

| Seed / delay | Yielded baseline | Pi beside those baselines | Observed gap point estimate | Yielded baseline repeat span |
| --- | ---: | ---: | ---: | ---: |
| 50 / 0 | 1,636.5 | 1,167.5 | 469 | 374.5 |
| 250 / 0 | 1,645.75 | 1,307 | 338.75 | 881 |
| 50 / 400 | 5,515.75 | 5,185 | 330.75 | 786.5 |
| 250 / 400 | 5,473.75 | 5,378 | 95.75 | 548 |

Pi is a separate namespace, not a same-Object intervention. These are descriptive observations; sub-spread gaps remain unresolved. Pi's own per-Object spans and comparison 1's references are retained in acceptance.json. They do not replace the [combined rebench](https://github.com/yielded-dev/agent/blob/dan/rebench-20261009/examples/durable-bench/results/rebench/report.md), whose 325 / 637 ms instant-provider cold gaps motivated this work. That report was read at `4cbcad5f`; its resources were never used or changed.

Comparison 2 cold breakdown, baseline → historical-fold candidate, milliseconds:

| Seed / delay | Admission | Receipt → settlement | Submit CPU | Alarm-pass CPU | Await invocation CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / 0 | 601.75 → 640 | 1,032 → 993.5 | 53 → 57.5 | 174.5 → 185.5 | 344 → 356.5 |
| 250 / 0 | 703.5 → 640 | 917.5 → 941.5 | 49.25 → 39 | 282 → 266.5 | 355.75 → 334 |
| 50 / 400 | 517 → 556 | 4,926.75 → 5,015.5 | 53.5 → 57 | 187.75 → 164 | 391.25 → 353.5 |
| 250 / 400 | 670.75 → 729.5 | 4,810.5 → 4,852 | 34.75 → 28.5 | 172.5 → 198.5 | 249.5 → 257.5 |

Both Objects have complete pairs for these CPU columns. **Invocation CPU is not exclusive phase CPU**: Cloudflare can attribute concurrent alarm work to the await invocation, and whole invocations can extend beyond client return. The medians do not add to a critical-path decomposition. Missing or ambiguous joins stay missing; measurement tables disclose complete-pair counts, including unavailable warm CPU cells in comparison 1. No submit, alarm, await or joined CPU improvement cleared its repeat range.

Admission is substantially larger than submit CPU in these observations; the five constructor transactions alone do not explain it. History-sensitive first-pass work remains. Five optional deployed CPU profiles contain broad Effect evaluator, parser, stream and runtime frames, including idle samples. They do not isolate a repeatable semantic hot spot or justify a precise CPU attribution. Three captures were rate-limited. Profiles are excluded from latency cohorts.

## Evidence and experiment boundaries

Baseline is current main at task start, `b246f8aaa3a92d5f82934b1fc7a82356d1ad6664`, including #821, #823, #825 and #826. The [before map](map-before.md), [await inventory](await-inventory.md) and [scope](scope.md) were frozen and committed locally at `60b1c8e7` before any product edit. They extend the existing prod-admit and prod-turn maps. [The after map](map-after.md) records every changed stage and identifies the full ordered traces retained locally; local `map/reproduction.json` proves the baseline trace was reproduced byte-for-byte.

Cold means the **same persisted Object with existing completed history**, confirmed quiescent, its next variant durably selected, acknowledged `storage.sync()`, then `ctx.abort()`. Its measured first submit must have a changed incarnation, unchanged Object identity, no earlier harness request or alarm, and uncontradicted constructor evidence. Warm means a subsequent turn in that same verified incarnation. The definition is identical across candidates and baselines. Ordinary reset observations are not claimed as fresh isolates.

Each candidate is randomized with two identical baseline labels inside one Worker bundle, using three blocks per label per Object. A block contains one cold and two warm turns. Pi is interleaved alongside. There are two Objects per framework for every 50/250 seed × instant/400 ms cell. Comparison 1 contains **576 turns**; comparison 2 contains **432**. All **1,008** are eligible, including **336 verified cold** and **672 warm** turns. There are no failed comparison requests, excluded comparison cohorts or transcript mismatches. All **9,072** model streams have matching reference fingerprints and successful SSE completion; every Yielded model dispatch occurs in its production alarm context.

The immutable seed fingerprints are 50 `b017b487524e44a4` and 250 `dcea9f30b0917245`. Objects then accumulate measured history: 36 turns in comparison 1 and 27 in comparison 2, each with eight tool cycles and nine model calls. Randomization limits order bias but does not hold history size constant or remove platform noise. The 400 ms provider retains 10 ms chunk spacing. Repeats are within Objects, not extra independent Objects. No local elapsed time is performance evidence.

Alchemy deployed the provider and primary driver/DO Worker; the driver placement is `aws:us-west-1`, with Object hint `wnam`. Integer-millisecond driver clocks provide submit-to-receipt and submit-to-settlement durations. Native I/O clocks can freeze through SQL, parsing and Effect evaluation. Provider/driver residual bounds require compatible before/after echo offsets and are not exact alarm-dispatch or storage-notification measurements. Boundary beacons and full await traces run only in separate map diagnostics. Source, transformed source, bundle and controller archives identify the executed bytes; [README.md](README.md) gives offline reduction commands.

The post-change map has **32/32 eligible turns**, 16 verified cold, with all 288 streams matching. The frozen original map retains one contradictory constructor attribution at 50/400: it excludes that cold observation and its incomplete formal cohort, while preserving all 32 transcripts. No proof threshold was loosened.

## Fresh isolates, separately

Eight separate new Worker versions were attempted on existing comparison-2 Objects after their 27 completed measured turns. Each accepted observation additionally requires a changed isolate marker, the first Object in that isolate, zero earlier stateless Worker fetches in the same isolate, and matching new driver/Object versions. These are single diagnostic observations with no repeats or candidate comparison, not performance claims.

| Seed / delay | Yielded driver | Pi driver | Yielded admission | Yielded submit / alarm / await CPU | Pi fetch CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / 0 | Excluded | 1,777 | — | — | 571 |
| 250 / 0 | 2,138 | 1,231 | 1,014 | 107 / 406 / 413 | 452 |
| 50 / 400 | 6,451 | 5,880 | 1,229 | 220 / 444 / 624 | 744 |
| 250 / 400 | 6,120 | 6,043 | 1,092 | 132 / 464 / 420 | 882 |

Seven observations passed. The first Yielded 50/0 turn reached the **previous version and existing isolate** after `/driver` readiness had reached the uploaded version. Its actual transcript matched, but it fails fresh-isolate proof and was not replayed. The controller's error label `transcript=false` combined version and transcript validation; local `fresh/adjudication.json` distinguishes them. The 2,184 ms excluded observation remains in raw evidence and is not included above.

## Correctness, rejected alternatives and outcomes

The layout experiment shares acquisition only for the exact SQL client; independent opens still refuse newer or damaged layouts. The reconciliation experiment keeps mutation/reservation ownership, decoding and recovery-event hydration, taking the original repair path for inconsistent state. The historical fold retains canonical accounting, Unknown placeholders and late-result checks. No cache, new recovery mode, relaxed durability or `allowUnconfirmed` was introduced. Durable receipts, dispatch prerequisites, append-only records/hash chains, fencing, claims, leases, historyDigest and no automatic replay remain intact.

Existing storage checks passed 76 tests; platform checks passed 34; journal/recovery/history checks passed 29. A direct local Workerd workflow passed 16/16 layout-acquisition and refusal observations. It did not simulate commit failure or process interruption, and its distinct client objects used one local database. Full uncached `vp run ready` passed with the two constructor candidates, with all three candidates, and on the final restored product tree (**0/71 cache hits**). Logs and source hashes remain under local `validation/`. No new registered tests were added.

The normal fresh-Run path already uses narrow Prompt records and prepared metadata. Reusing already-full retained envelopes would save no reads for these uncompacted histories. Effect shares parser compilation by AST identity; there was no demonstrated large per-incarnation parser rebuild to remove. Upstream Prompt decoding still repeats some message validation, but replacing the canonical codec without equivalent proof was not justified. Required canonical validation and historyDigest recovery were retained. The pre-arm deadline, pass lifecycle and settlement tail were untouched; no `warm-floor` change is requested.

The final telemetry snapshot contains **41,328 events / 22,910 invocation records**. All **1,107 non-ok invocation outcomes** remain in the outcome files: **456 aborted Object fetches**, **647 canceled alarms**, and **4 canceled stateless fetches**. The stateless cancellations are two keepalives and two seeding requests, outside measured turns. Resets, cancellation and telemetry sampling are not silently relabeled successful invocations. No captured invocation reports a CPU- or memory-limit outcome.

Other non-ok results are preserved: the initial global-scope-randomness deployment failure; one pre-profile HTTP 500 rejected by the observer before admission, then retried only after proving it was unsubmitted; **15 HTTP 500s** from the optional version-metadata endpoint, with the existing metadata endpoint used successfully; **3 HTTP 429 profile captures**, never retried; and the **one rejected fresh-isolate proof**. Comparison 1 has 340 observation/join issues and comparison 2 has 249, principally END-context disagreements and missing telemetry joins; none is filled with invented CPU. Raw outcome lists, failed requests, errors and coverage are in each phase's `failed-outcomes.json.gz`, plus controller/API/profile records. [outcomes.md](outcomes.md) gives the complete inventory and local preparation/check failures.

## Account, cleanup and handoff

The account, verified through the checkout's direnv credentials, is **Danieljmerwe@gmail.com's Account**. Both `cold-turn-22562194-primary` and `cold-turn-22562194-provider` were destroyed through their task-owned Alchemy stages. The same account API returned 404 for both Workers, no owned namespaces and no Worker or namespace anywhere with the `cold-turn` prefix. The task-owned local PostgreSQL container was removed. [cleanup.json](cleanup.json) records verification and final private-state removal; [secret-scan.json](secret-scan.json) records exact credential/resource-ID scanning of raw and decompressed artifacts. Alchemy state stayed in a private mode-700 temporary directory outside the repository.

**PRs: none.** Evidence is on `dan/cold-turn-latency`; no merge or changes to `rebench` resources occurred. [measurements.md](measurements.md), [acceptance.json](acceptance.json), the two maps and the independent candidate patches make the negative result reviewable. Raw telemetry, harness scripts and source/bundle archives remain local; reproducing the reductions requires those retained local artifacts, as explained in README.md.
