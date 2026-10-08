**Production ThreadObject latency on deployed Cloudflare — prod-path**

A warm production user waited **1.93 s at 50 seeded turns and 2.24 s at 250 turns** with the zero-delay mock; with 400 ms to first token the waits were **5.23 s and 5.42 s**. These are submit sent → settlement observed in the deployed driver. Cold medians were **2.20/3.07 s** and **5.82/6.28 s**. Admission alone was **231–416 ms warm** and **537–950 ms cold** across the four conditions.

Production medians were lower than inline in all 28 warm same-Object pairs. The 400 ms conditions give the stronger separation: six of seven pairs at each history exceeded both variants' complete repeat ranges. **At 250 turns/zero delay, the 213 ms paired difference is below inline repeat noise; do not claim a resolved advantage.** At 50/zero, only two of seven pairs cleared both ranges. The evidence supports a lower observed production median, not a universal speedup or SLO.

The additional alarm and waiter boundaries do not impose a fixed 500 ms floor. Production already runs maintenance with deferred wakes: every accepted production turn identified one alarm, while inline's warm cohort medians were three to ten. Inline's extra maintenance and recovery work is a plausible explanation for its larger gaps between model calls, not a separately randomized causal estimate. The big observable budgets are admission and the eight inter-model gaps; in the 400 ms condition, the mock itself contains 4,130 ms of programmed waits. Exact alarm-entry, ownership and storage-notification costs have the clock and probe limits below.

**Primary turn latency.** Rounded milliseconds, median [Q1–Q3] of seven Object medians. Warm has four repeats per Yielded variant and eight for pi per Object; cold has one per Yielded variant and two for pi.

| Seed turns | TTFT | State | Production Yielded | Inline Yielded | pi |
| --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 1,927 [1,100–2,467] | 2,747 [1,545–3,263] | 567 [540–586] |
| 50 | 0 | cold | 2,202 [1,698–2,776] | 3,265 [2,114–3,847] | 944 [893–1,153] |
| 50 | 400 | warm | 5,230 [5,075–5,257] | 5,817 [5,731–6,291] | 4,698 [4,692–4,749] |
| 50 | 400 | cold | 5,818 [5,437–5,895] | 6,528 [6,187–6,997] | 5,026 [4,974–5,103] |
| 250 | 0 | warm | 2,242 [2,083–2,296] | 2,421 [2,324–2,741] | 867 [791–913] |
| 250 | 0 | cold | 3,074 [2,526–3,139] | 3,150 [3,034–3,537] | 1,266 [1,161–1,284] |
| 250 | 400 | warm | 5,421 [5,393–5,712] | 6,315 [6,181–6,655] | 4,818 [4,801–4,822] |
| 250 | 400 | cold | 6,275 [6,012–6,560] | 7,032 [6,848–7,439] | 5,130 [5,101–5,173] |

**Noise check.** Negative Δ favors production. Repeat range is the median within-Object range, with the largest such range in parentheses; no outliers were trimmed. Paired Δ is the median of paired differences, so it need not equal the difference between the preceding marginal medians.

| Seed / TTFT | Paired Δ [Q1–Q3], ms | Production repeat range | Inline repeat range | pi repeat range | Pairs with absolute Δ above both ranges |
| --- | --- | --- | --- | --- | --- |
| 50 / 0 | -733 [-839–-476] | 277 (1,265) | 535 (1,317) | 189 (780) | 2/7 |
| 50 / 400 | -773 [-1,034–-564] | 209 (3,150) | 400 (953) | 129 (185) | 6/7 |
| 250 / 0 | -213 [-383–-119] | 142 (263) | 814 (941) | 154 (378) | 0/7 |
| 250 / 400 | -867 [-964–-781] | 116 (238) | 116 (1,894) | 166 (310) | 6/7 |

The 50/400 production repeat range includes an 8,105 ms successful turn; it remains in the data. Full Object ranges, adjacent pairs and drift are in [repeat-comparison.json](repeat-comparison.json) and the main summary. The smaller differences inside these controls are unresolved.

**Production admission and model boundaries.** All values are ms. Admission and gap values cover all seven Objects. Bounds carry their own complete-Object coverage and are conditional on the provider/driver clock constraints. These columns are overlapping observations, not an additive waterfall.

| Seed / TTFT | State | Admission | Submit → first model | Receipt → first model | Median inter-model gap | Last model end → client |
| --- | --- | --- | --- | --- | --- | --- |
| 50 / 0 | warm | 231 | 248…260 (4/7) | -1…8 (4/7) | 188 | 145…157 (4/7) |
| 50 / 0 | cold | 550 | 685…695 (4/7) | 58…68 (4/7) | 189 | 144…155 (4/7) |
| 50 / 400 | warm | 240 | 251…260 (5/7) | 1…11 (5/7) | 78 | 89…97 (5/7) |
| 50 / 400 | cold | 537 | 594…605 (5/7) | 87…90 (5/7) | 89 | 117…126 (5/7) |
| 250 / 0 | warm | 416 | 380…390 (1/7) | -1…8 (1/7) | 200 | 179…189 (1/7) |
| 250 / 0 | cold | 950 | 874…884 (1/7) | -1…9 (1/7) | 201 | 172…182 (1/7) |
| 250 / 400 | warm | 332 | 380…387 (2/7) | 1…9 (2/7) | 106 | 111…119 (2/7) |
| 250 / 400 | cold | 929 | 1,061…1,065 (2/7) | 136…140 (2/7) | 119 | 164…168 (2/7) |

For covered warm Objects, the receipt→first-model median interval ends at about **8–11 ms**. Processing begins before that request reaches the provider and can overlap receipt travel. This is a combined dispatch/setup/transit residual, not an exact alarm-hop timestamp. At 250/zero, only one of seven Objects has compatible client/provider clock evidence, so its first-model interval must not be compared additively with the seven-Object admission median.

| Warm seed / TTFT | Production gap | Inline gap | pi gap | Production sum of eight gaps |
| --- | --- | --- | --- | --- |
| 50 / 0 | 188 | 281 | 54 | 1,510 |
| 50 / 400 | 78 | 156 | 56 | 651 |
| 250 / 0 | 200 | 230 | 82 | 1,611 |
| 250 / 400 | 106 | 199 | 66 | 854 |

**Production invocation CPU.** Milliseconds attributed by Cloudflare, median across Objects; parentheses show Objects with complete repeated CPU evidence. Every accepted production turn identifies one alarm, so the alarm sum and per-pass median/max coincide for that turn. Individual invocation values and identities remain in the turn artifact.

| Seed / TTFT | State | submit CPU | One alarm pass CPU | awaitSettlement CPU |
| --- | --- | --- | --- | --- |
| 50 / 0 | warm | 7 (5/7) | 113 (6/7) | 243 (6/7) |
| 50 / 0 | cold | 82 (6/7) | 171 (7/7) | 328 (7/7) |
| 50 / 400 | warm | 10 (6/7) | 132 (6/7) | 401 (6/7) |
| 50 / 400 | cold | 62 (6/7) | 182 (7/7) | 645 (7/7) |
| 250 / 0 | warm | 17 (6/7) | 305 (6/7) | 453 (7/7) |
| 250 / 0 | cold | 77 (7/7) | 348 (7/7) | 525 (7/7) |
| 250 / 400 | warm | 12 (7/7) | 166 (5/7) | 548 (6/7) |
| 250 / 400 | cold | 63 (7/7) | 351 (7/7) | 677 (7/7) |

**Warm comparison CPU.** Per-pass statistics first reduce the alarm passes within each turn, then the repeated turns within each Object. A sum of alarm CPU is not the same statistic as CPU per alarm. These invocations can extend beyond client completion, and concurrent invocation attribution is not a semantic phase decomposition.

| Seed / TTFT | Inline direct CPU | Inline alarms/turn | Inline alarm median CPU | Inline alarm max CPU | Inline alarm sum CPU | pi direct CPU |
| --- | --- | --- | --- | --- | --- | --- |
| 50 / 0 | 79 (7/7) | 9 | 31 (5/7) | 150 (5/7) | 401 (5/7) | 170 (5/7) |
| 50 / 400 | 76 (6/7) | 10 | 31 (4/7) | 154 (4/7) | 415 (4/7) | 200 (7/7) |
| 250 / 0 | 222 (7/7) | 3 | 146 (5/7) | 923 (5/7) | 1,150 (5/7) | 395 (7/7) |
| 250 / 400 | 164 (7/7) | 10 | 50 (3/7) | 499 (3/7) | 986 (3/7) | 334 (7/7) |

**Secondary laptop observation.** Milliseconds, same Object aggregation. These include routing, clock probes and diagnostic reads outside the primary timer. All 672 accepted main responses had **CF-Ray colo SJC**; each individual ray is retained in the raw requests and turn records. The earlier report's European ingress was not observed in this run. Provider receipts nevertheless span SJC, DFW, DEN, LAX and SEA; the shared location hint did not establish identical routing.

| Warm seed / TTFT | Production laptop | Inline laptop | pi laptop | CF-Ray colo |
| --- | --- | --- | --- | --- |
| 50 / 0 | 2,082 | 2,856 | 639 | SJC |
| 50 / 400 | 5,315 | 5,919 | 4,785 | SJC |
| 250 / 0 | 2,344 | 2,507 | 933 | SJC |
| 250 / 400 | 5,505 | 6,366 | 4,896 | SJC |

**Alarm hop and fixed-phase observations: separate deployed attribution run.** All 96 turns passed; 12 Objects, 48 probe-on turns and 432 valid markers. These are **beacon-arrival observations**, not isolated operation costs. Receipt→process is the requested processing-entry boundary; receipt→alarm separately marks the native callback. Client-relative intervals show their compatible-Object denominator; all other phase medians cover three Objects.

| Seed / TTFT | State | Receipt → alarm arrival | Receipt → process arrival | Ownership arrival separation | Post-claim setup separation | Control absolute median / largest turn |
| --- | --- | --- | --- | --- | --- | --- |
| 50 / 0 | cold | 30…37 (2/3) | 31.5…38.5 (2/3) | -1 | 9 | 3 / 8 |
| 50 / 0 | warm | 3.5…10 (2/3) | 4.5…11 (2/3) | 1.5 | 9.5 | 3 / 15 |
| 50 / 400 | cold | 48.5…57 (2/3) | 50.5…59 (2/3) | 1 | 6 | 1 / 3 |
| 50 / 400 | warm | -2.75…4 (2/3) | -2…4.75 (2/3) | -1 | 5.5 | 1 / 1 |
| 250 / 0 | cold | -1…4 (1/3) | 0…5 (1/3) | 2 | 2 | 1 / 2 |
| 250 / 0 | warm | 0.5…6 (1/3) | 0.5…6 (1/3) | 0 | 1.5 | 1 / 5 |
| 250 / 400 | cold | 69…77 (1/3) | 68…76 (1/3) | 2 | 4 | 1 / 70 |
| 250 / 400 | warm | -1.5…4.5 (1/3) | -1.5…4.5 (1/3) | 2.5 | 3 | 2 / 4 |

The warm receipt→process arrival intervals end at **4.5–11 ms** on covered Objects. Entry precedes its beacon arrival, so these provide conditional upper bounds, not precise dispatch durations. Cold intervals end at 5–76 ms across these small cohorts. Negative values remain visible because receipt delivery and alarm work can overlap. The paired control spans −70…15 ms across all probe-on turns; the −70 ms observation coincided with a +63 ms ownership separation. Millisecond ownership/setup values therefore do not resolve actual fixed costs.

| Seed / TTFT | State | Transaction-return arrival → client | Transaction-return → await-return arrivals | Main median upper envelope |
| --- | --- | --- | --- | --- |
| 50 / 0 | cold | -7.5…-0.5 (2/3) | 0 | ≤155 ms (4/7) |
| 50 / 0 | warm | -9.5…-3 (2/3) | -0.5 | ≤157 ms (4/7) |
| 50 / 400 | cold | -6…2.5 (2/3) | 1 | ≤126 ms (5/7) |
| 50 / 400 | warm | -11.75…-5 (2/3) | 0 | ≤97 ms (5/7) |
| 250 / 0 | cold | -2…3 (1/3) | 0 | ≤182 ms (1/7) |
| 250 / 0 | warm | -4.5…1 (1/3) | -0.5 | ≤189 ms (1/7) |
| 250 / 400 | cold | -6…2 (1/3) | 0 | ≤168 ms (2/7) |
| 250 / 400 | warm | -5…1 (1/3) | -0.5 | ≤119 ms (2/7) |

The rightmost column is the **entire last-model-end→client envelope in the unprobed main run**, conditional on its clock calibration. Final storage settlement occurs after the final model response, so this supplies a coarse upper envelope for the notification median too; it does not isolate notification from finalization and RPC return. These are limits on the median of Object medians, not per-turn maxima. The negative transaction-beacon residuals mean the beacon arrived too late to timestamp the storage boundary accurately. Zero or negative arrival separation cannot establish zero notification cost. No exact storage-settled→client point estimate is supported.

**Probe perturbation and repeat control.** Warm on−off differences are paired within each of three Objects. Parentheses show min…max across Objects; repeat-range parentheses show the largest Object range. Negative on−off values are retained. Cold on/off observations are distinct incarnations, with no within-mode repeats.

| Warm seed / TTFT | On − off, ms (min…max) | On repeat range (largest) | Off repeat range (largest) |
| --- | --- | --- | --- |
| 50 / 0 | 0.5 (-1,058…223.5) | 49 (323) | 22 (2,063) |
| 50 / 400 | 24.5 (20…98.5) | 101 (182) | 47 (70) |
| 250 / 0 | 66 (-11…83) | 31 (119) | 43 (61) |
| 250 / 400 | -8.5 (-36.5…195) | 130 (132) | 31 (64) |

The controls do not establish a universal probe cost, nor do they bound common output-gate/network delay. The supplement is never pooled into primary latency results. Full phase ranges, cold on/off differences, settling turns and signed observations are in [attribution/tables.md](attribution/tables.md).

**Which costs belong to the production path?** The extra public admission/receipt and settlement-wait boundaries, and alarm dispatch before processing, are production-path boundaries. Full runtime reopening is **not** repeated on every warm pass: the application ManagedRuntime is cached for the Object incarnation. Stable incarnation evidence plus the [ThreadObject contract](https://github.com/yielded-dev/agent/blob/8c05714de84d68961b14e5ab7a3b7d809599563f/packages/platform-cloudflare/src/ThreadObject.ts#L1071) supports zero additional full-runtime acquisitions per warm pass; this is a structural count, not a zero-duration timestamp. Cold reconstruction is included in admission and was not separately timed.

Each processing attempt still claims ownership and opens a Run session. [RunStorage.claim](https://github.com/yielded-dev/agent/blob/8c05714de84d68961b14e5ab7a3b7d809599563f/packages/effect-agent/src/durable/RunStorage.ts#L277) performs the ledger claim, materializes the thread and creates a writer. Inline pays these costs too. The probe's warm ownership arrival separations are 1.5/−1/0/2.5 ms, and post-claim session setup separations are 9.5/5.5/1.5/3 ms for 50/0, 50/400, 250/0 and 250/400 respectively. **These do not establish exact ownership/open costs or removable milliseconds**: they are close to control noise and can share deferred output delivery. There is no evidence here for a large, production-only reopening tax.

`awaitSettlement` [subscribes before its authoritative read](https://github.com/yielded-dev/agent/blob/8c05714de84d68961b14e5ab7a3b7d809599563f/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L11348) and races a hint with the 500 ms fallback; it re-reads after either. The main run's entire last-model→client envelope is about 90–190 ms warm on covered Objects, smaller than 500 ms, so a mandatory post-settlement 500 ms sleep is inconsistent with these observations under their clock assumptions. The notification-only delay remains unisolated. Negative supplemental beacon residuals are evidence of probe delivery lag, not negative or instantaneous storage notification. This benchmark waits for completion metadata; output-record download and UI rendering are outside the interval.

**Ranked levers for this path, with measured budgets rather than promised gains:**

1. **Model/provider latency dominates the slow-provider case:** the unchanged script deliberately waits 4,130 ms across nine calls. A faster provider is the largest application lever in that regime. Reducing the eight lookups, model calls or transcript would change this workload and is not credited as a framework optimization.
2. **Between-model work is the largest framework investigation budget:** production's eight gaps total about 1,510/651/1,611/854 ms in the four warm conditions. Profile serialization, prompt materialization, durable reads/writes and network transit before attributing that whole interval to SQL. Disposable projection caching and fewer redundant storage operations are candidates if canonical verification and recovery remain intact.
3. **Admission and cold initialization:** 231–416 ms warm, 537–950 ms cold. Investigate its storage gates, round trips and reconstruction work while retaining the mutation gate, pre-armed maintenance alarm and receipt durability. Do not interpret the whole cold-minus-warm difference as one constructor cost.
4. **Finalization and observation:** the covered warm last-provider→client envelopes end at roughly 97–189 ms. Improve proven work inside that envelope if further profiling identifies it; lowering the 500 ms fallback alone is not supported as a gain in these observed completed turns. Invocation CPU charged to `awaitSettlement` is not proof that the waiter itself consumes all of it.
5. **Alarm hop and repeated opening:** measured warm process-entry beacon residuals are small relative to admission, and a full runtime reopen does not occur each warm pass. These are lower-priority targets until better clock evidence resolves their costs. Production already coalesces immediate wakes during maintenance; applying the sibling inline fix is outside this measurement.

The durable-contract exclusions are listed with the method below. Neither the pi comparison nor these budgets authorize removing those contracts.


**Outcome and coverage inventory, including pilots, seeding, retired pairs, attribution and cleanup.** Captured telemetry contains 36,981 events and 20,980 invocations: 18,810 OK and 2,170 non-OK. **Observed `exceededCpu`: 0; `exceededMemory`: 0.** Platform sampling means these are observed counts, not a claim of exhaustive capture.

| Worker | Invocation | Outcome | Count |
| --- | --- | --- | --- |
| primary | stateless / fetch | ok | 3,167 |
| primary | durableObject / fetch | aborted | 302 |
| primary | durableObject / fetch | ok | 3,625 |
| primary | durableObject / alarm | canceled | 1,863 |
| primary | durableObject / alarm | ok | 2,327 |
| primary | stateless / fetch | canceled | 4 |
| primary | durableObject / jsrpc | ok | 535 |
| primary | durableObject / fetch | exception | 1 |
| provider | stateless / fetch | ok | 9,156 |

The 302 aborted fetches correspond to intentional cold/reset/release operations (150 cold resets, 82 seed releases, 70 final releases). Native logs identify 301; the remaining release has a controller receipt confirming the expected abort. **The causes of 1,863 canceled alarms remain unresolved:** 1,732 have no same-request logs and 131 have only inline completion logs. They are not silently reclassified as successful cleanup. Four canceled outer requests are seed requests, also without an established cause. The exceptional Object fetch is the unexpected pi cold disconnection described below. Provider telemetry contains 9,156 invocations, all observed OK.

At the application/controller boundary, two HTTP 500s remain: the initial pilot hit Cloudflare error 1042 before submission (fixed by the required public Worker-to-Worker fetch compatibility flag), and a main pi cold request lost its Object connection. Another pi response succeeded but failed warm-incarnation qualification. Host shutdown left one production turn request and a comparison keepalive without responses; the turn was not replayed. Three affected main Object pairs were retired and replaced. The raw inventory also includes two control-plane deployment/metadata gateway 500s; exact upload-byte and active-version checks succeeded through the recorded fallback. The sole version-integrity flag is on the excluded failed pi response, which had no version field. These overlapping log/controller inventories are not additional independent failed turns.

All 672 accepted main turns have valid nine-request transcript receipts, including 6,048 provider SSE receipts; all 96 attribution turns passed separately. Main CPU is available for 161/168 submits, 163/168 awaits and 159/168 production alarms; inline has 167/168 direct invocations and 1,262/1,298 alarms, and pi has 333/336 direct invocations. The reducer retains 552 join/observation issues, including 190 END-context disagreements. The largest reported sampling intervals are 3.571 for primary and 2.125 for provider. See [failed-outcomes.json.gz](failed-outcomes.json.gz), raw telemetry and the individual turn joins for every observed non-OK event and missing value.

**Method and public API.** The driver uses `CloudflareThreadClient.layerFromBinding`, `submit(agent, input, options)`, then `awaitSettlement(receipt)`. This follows the [Cloudflare guide](../../../../docs/src/content/docs/platforms/cloudflare.md) and the public endpoint comments. The timed interval starts immediately before `client.submit` and ends when the driver receives a completed settlement. Client Layer construction and definition hashing precede the timer. Admission ends when the receipt reaches that same driver. The native `ThreadObject.submitEncoded` and `awaitSettlementEncoded` endpoints, mutation gate, admission alarm, maintenance pass, ownership and durable finalization all execute normally. The observer subclasses these endpoints to record boundaries; it calls their original implementations. It never calls `processThreadResolved` for a production measurement.

The comparison invokes the original inline `submitRegistered` → `processThreadResolved` path on the **same Yielded Object**, interleaved with production turns. Pi uses its native durable completion path in a separate Object. All roles share one primary Worker bundle, avoiding a separate deployment per variant. A separate provider Worker serves the reused OpenAI-compatible SSE script. Every resource was created or updated through the task's Alchemy stacks, with the `prod-path` prefix. The driver has targeted placement `aws:us-west-1`; every seeded Object is requested with `locationHint: "wnam"`. These are placement requests, not proof that driver, Object and provider occupied the same physical colo. Request ingress and `CF-Ray` identify ingress, not necessarily the code's execution location. [Cloudflare placement](https://developers.cloudflare.com/workers/configuration/placement/)

The baseline is `8c05714de84d68961b14e5ab7a3b7d809599563f`, the fetched `origin/main` when this task began. The sibling wake-defer change is absent. Main advanced during the experiment with a toolchain change; no framework source from that later revision was incorporated. The harness was copied and adapted from `origin/dan/cf-latency-breakdown` at `52e3fd18e3977dcdeae78d2d1b23c7685ed0106e`: `stack.ts`, `run.mjs`, `build.mjs`, the networked provider, Yielded target and telemetry joins. The main bundle SHA-256 begins `7fd3fdb53317`; provider begins `bdd1c364a83b`. Exact compressed modules, build inputs and repository source snapshots are in `build-identities/`. `source-contract.json` verifies that all 184 compiled framework files match the baseline byte for byte. `resources.json` records active and replaced versions, settings and uploaded-byte checks. The later attribution build's checkout commit includes the target-only benchmark commit; the same source proof verifies its unchanged framework.

**Workload and transcript.** History is generated in place through the original durable benchmark's `[1, 1, 0]` lookup cycle, in at most ten-turn seed requests. No canonical payload is rewritten or imported from another Object. Each measured turn then performs eight sequential readonly `lookup` calls and a final answer, producing nine provider requests. Results are 256 bytes, with every 97th result expanded to 8 KiB. Compaction is off. The seed fingerprints are exactly **50: `b017b487524e44a4`; 250: `dcea9f30b0917245`**. The controller verifies all nine request fingerprints against the growing reference transcript for every successful measured turn, not just the seed.

The fingerprint is the durable-bench projection of role, text and lookup arguments. Both targets use the same explicit system instruction; the projection excludes system/developer and output-contract messages and decodes the Effect adapter's JSON-string tool result once. Framework-specific provider envelopes, tool-call identifiers and output-contract framing therefore need not have identical raw wire hashes. Raw request hashes and byte sizes remain evidence of that distinction. The **production and inline Yielded paths use the same agent, model adapter, definitions and transcript construction**; no transcript alteration was used to improve either path. Canonical log records, batch hashes, producer fences, claims, leases, accounting, `historyDigest` and original-context recovery are untouched. Unknown outcomes were not automatically replayed.

The zero-delay mock has no programmed waits. The 400 ms condition retains the earlier harness's 10 ms inter-frame delay as well as 400 ms to first token: nine streams contain **4,130 ms of programmed waiting per turn**, not just 3,600 ms. The report's residuals are not estimates of how a real model's compute would scale. The two delay conditions run on different Objects and at different times; subtracting them is not a controlled estimate of one isolated latency effect.

**Sampling and cold definition.** Each history/delay condition has seven Yielded Objects and seven pi Objects. Per Object, m0 and m1 are cold, m2 and m3 are settling turns, and m4–m11 are warm. The two cold Yielded variants are shuffled; each warm adjacent pair contains one production and one inline turn, in deterministic shuffled order. Pi supplies two cold and eight warm baseline turns. Framework order is shuffled too. Histories grow as these turns execute; variants trade earlier/later positions, and no state is rewound. The main plan contains 672 measured turns including settling turns. Statistics first take the median within each Object, then median, Q1/Q3 and range across Objects. Turns are not treated as independent Objects. Full per-Object results and repeated baseline ranges are retained.

Cold means the existing Object's storage was synced and `ctx.abort` acknowledged before the turn. The next measured entry must have a new incarnation, be the first harness request and have no prior alarm starts. It is **an Object reconstruction over seeded storage**, not a guaranteed fresh Worker isolate, JIT, process or deployment. Warm turns must keep the same Object, incarnation and version after m1. A read-only `/identity` heartbeat keeps the idle comparison Object alive while the other role executes; it never touches the active Object. This was added after an early pi warm turn was observed in a new incarnation. That complete Object pair was retired, and a fresh pair replaced it.

Two more interrupted pairs were replaced in full: one pi cold request returned “Durable Object instance is no longer active”; another pair lost its controller during a host restart while a production request was in flight. The latter request's outcome remains unknown. Completed groups were retained; no uncertain input was resubmitted. The controller reimported the same Alchemy-owned resources after its private temporary directory was lost, rotated the benchmark credential and verified identical deployed module bytes and unchanged namespace identity digests. `controller-recovery.json`, `main-failures.json`, attempted requests and retired successful responses retain the evidence. Replacement names preserve each original planned variant schedule. These interruptions and deployment epochs remain potential time-related confounders.

**Clock and phase precision.** Primary turn and admission latency use one deployed driver's clock around RPC I/O. Laptop elapsed time is secondary: it also includes ingress routing, diagnostic reads and before/after provider probes. Each raw response retains its `CF-Ray` and ingress colo. No laptop duration is substituted for a missing driver duration.

A Durable Object's `Date.now`, `performance.now` and Effect span times can stay fixed while code runs and can lag elapsed wall time. The apparent zero-duration claims and negative cross-clock intervals are retained as diagnostics, not treated as exact phase costs. In particular, subtracting persisted `settledAt` from driver time does **not** measure notification latency. [Cloudflare timing API](https://developers.cloudflare.com/workers/runtime-apis/performance/)

Provider SSE receipts identify arrivals, stream ends, transcript fingerprints, request bytes and colos independently of sampled logs. The two driver/provider echo probes give an offset interval each; the reducer intersects them and requires compatible nonempty colos. Reported submit→first-provider and last-provider→client bounds are conditional on a stable offset consistent with those probes. A common colo does not prove clock synchronization. Incompatible or missing evidence remains null. Provider-to-provider gaps use receipt timestamps with their own compatibility checks. Gap time includes response delivery, durable processing, serialization and the next request's transit; it is not pure SQL or CPU time.

The separate attribution run alternates external phase beacons on/off within each of three production Objects per condition. Beacons mark alarm entry, process entry, Run session opening, ownership claim entry/exit, session ready, native settlement transaction return and await return. An adjacent alarm-entry beacon measures differential delivery jitter. First-model marker-array position identifies the relevant pre-model claim without relying on frozen clocks; ambiguous claims remain unresolved. No probe changes a durable record. Beacon fetches retain output gates and invocation ownership, are not awaited in the turn, and drain in the later diagnostic request.

Beacon arrival differences include scheduling, output-gate and network delay. Receipt→alarm arrival is at best a conditional upper bound on the alarm's entry time relative to the receipt; a settlement-return beacon can arrive late and understate the return-to-client residual. Seven startup beacons can compete with the model request for the six connections awaiting headers. On/off comparisons and adjacent jitter expose observed perturbation, but do not bound common delivery delay or establish exact commit timing. Signed negative estimates remain visible. [Cloudflare connection limits](https://developers.cloudflare.com/workers/platform/limits/#simultaneous-open-connections)

**CPU and failure coverage.** CPU values come only from deployed `cf-worker-event` invocation telemetry. Generated submit, await and alarm IDs join their **START** console log to one Worker-scoped request ID and one invocation. An alarm's END log can appear in the waiter's Cloudflare context, so it does not override the START identity. Whole alarm CPU can include work after the client sees completion. Cloudflare's attribution to concurrent invocations is not a semantic allocation of CPU to admission, execution and waiting; an await invocation can be charged concurrent work.

The stack enables invocation logs and sets head sampling to one. Cloudflare can still apply platform sampling; captured `sampleInterval` values, missing joins and every returned non-ok outcome are preserved. CPU missing from telemetry is null, never zero. An absence of observed `exceededCpu` or `exceededMemory` is not proof that sampling captured every invocation. The main reducer requires a complete repeated metric before publishing that Object's corresponding CPU aggregate. [Cloudflare telemetry sampling fields](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/)

**Limits and optimization contracts.** These are unminified, instrumented benchmark applications using a deterministic network mock, bounded history, one active input per Object and a generous invocation CPU limit. Instrumentation overhead is retained, not estimated away. They do not establish a production percentile/SLO, congested admission behavior, cross-Object contention, real-model quality or recovery latency after unresolved external effects. The zero-delay and 400 ms cohorts are not a randomized intervention on the same Objects. Wide Object spread and within-Object repeat ranges prevent claims smaller than those controls. Deferred wakes already apply to production maintenance in this baseline; the sibling inline fix could narrow the measured separation and was not evaluated here.

Safe optimization candidates must preserve authoritative re-reads and durable recovery obligations. Eliminating the append-only log/hash chain, bypassing producer fencing or ownership claims/leases, returning final success before durable finalization, treating volatile notifications as the sole wake authority, silently replaying Unknown, rewriting canonical context or dropping `historyDigest`, changing canonical accounting, and reordering or unbounding tool execution are forbidden shortcuts. Compaction, fewer model round trips or a smaller model-visible transcript would change this benchmark's workload. Caching disposable projections and removing redundant reads may be candidates only if verification and original-context recovery still fail closed.

**Artifacts and publication.** The target-only [PR #821](https://github.com/yielded-dev/agent/pull/821) makes native submit/await the default `yielded` benchmark target and retains `yielded-inline` as an explicit comparison. Historical result rows without the production execution-path marker remain classified as inline. The deployed harness, optional beacons, controller recovery and evidence stay on `dan/bench-production-path`: they are considerably larger, task-specific operational machinery, and are not needed to run the reusable benchmark target. The PR is not merged.

`requests.jsonl.gz` and `attempted.jsonl.gz` retain the sanitized controller responses and attempts, including failures and excluded pairs; plans and completion ledgers identify accepted observations. `turns.jsonl.gz` retains the telemetry joins and individual alarm CPU records. `summary.json.gz`, `tables.md` and `failed-outcomes.json.gz` are reproducible with `vp node examples/durable-bench/results/prod-path/analyze.mjs`; the reducer reads plain or gzip inputs. `analyze-attribution.mjs` produces the separate `attribution/` reduction. Compressed raw telemetry, exact bundle/source archives and the artifact manifest make these calculations auditable after deployment deletion.

**Validation and cleanup.** `vp run ready` passed uncached: 71 tasks, 1,187 tests passed, 10 skipped, zero failures; CI `ready` and the Effect Agent review passed on PR head `ed48f9291ca3b129cb7eb38340c32c4b5fad52c1`. The final local gate used the documented PostgreSQL connection override to an existing local admin database after restart; no test or framework code changed, and no fixture databases remained. Earlier gate failures are retained separately. The reusable target's direct workflow proof checked the reference seed, eight readonly lookups, canonical completion and alarm execution; no local timing is used as production evidence. The deployed harness also passed scoped syntax, formatting and ordinary lint checks; generated-directory and third-party constraints excluded it from type-aware checking.

The account was **Danieljmerwe@gmail.com's Account**. Alchemy destroyed both `prod-path-1b0a1de4-primary` and `prod-path-1b0a1de4-provider`. At **2026-10-08 22:07:26 UTC**, API checks in that same account returned 404 for both Workers and found **zero `prod-path` Workers or Durable Object namespaces**. Final telemetry was collected, and the private mode-700 Alchemy state directory was removed. [cleanup.json](cleanup.json) is the verification receipt.

Account credentials and the current benchmark credential were checked against raw and decompressed artifacts before private-state removal. The pre-restart benchmark token was sanitized at capture but was lost with the temporary directory, so it was unavailable for a final exact-value comparison. No Alchemy state is included. The artifact manifest records file hashes and gzip round-trip hashes. The report, raw evidence, failure inventory and harness are committed on `dan/bench-production-path`; only the reusable target is in [PR #821](https://github.com/yielded-dev/agent/pull/821), which remains open and unmerged.
