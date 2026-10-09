# Cold admission and first-request bisection

The extra time outside the submit CPU counter is not all storage waiting. The alarm can already be doing the first turn's history work while the durable receipt is in flight. At 1,000 turns on a warm isolate, pinned-main admission is 752 ms, compared with 53 ms of submit invocation CPU; first provider arrival is 831 ms. The alarm's whole-invocation CPU median is 596 ms. Those invocation counters overlap and cannot be added as an exclusive partition. Invocation CPU and sampled profiles locate substantial CPU after admission. Fresh isolates also have a large gap before the first constructor timestamp; this investigation does not assign that interval to CPU, storage activation or routing.

No product fix or after-build latency claim is made. The largest measured prefix is after admission and is handed to history-cost. The remaining concrete open-path candidate, layout normalization, accounts for only about 2–3 nominal sample-ms per start. This does not explain the hundreds of milliseconds under investigation.

## Deployed timing

Instant provider; all durations below are milliseconds. Each cold cell uses 14–15 independently verified Objects. The primary zero-history comparison uses the native-RPC bare control and its same-run Yielded/pi cohorts. Y ÷ pi is a ratio of medians, not a paired causal estimate.

| Condition | Seeded turns | Objects Y / pi | Admission Y / pi, ms | Y ÷ pi | First request Y / pi, ms | Y ÷ pi | Complete turn Y ÷ pi |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold Object | 0 | 15 / 15 | 358 / 167 | 2.144× | 462 / 219 | 2.110× | 1.420× |
| Cold Object | 50 | 15 / 15 | 268 / 150 | 1.787× | 300 / 202 | 1.485× | 1.201× |
| Cold Object | 250 | 15 / 15 | 480 / 171 | 2.807× | 504 / 195 | 2.585× | 1.475× |
| Cold Object | 1,000 | 15 / 15 | 752 / 233 | 3.227× | 831 / 269 | 3.089× | 1.533× |
| Fresh isolate | 0 | 14 / 15 | 682 / 203 | 3.360× | 785.5 / 249 | 3.155× | 1.874× |
| Fresh isolate | 50 | 14 / 15 | 624 / 207 | 3.014× | 790 / 260 | 3.038× | 1.795× |
| Fresh isolate | 250 | 15 / 15 | 771 / 257 | 3.000× | 977 / 290 | 3.369× | 1.910× |
| Fresh isolate | 1,000 | 15 / 15 | 1,382 / 361 | 3.828× | 1,669 / 394 | 4.236× | 1.604× |

The native-RPC platform floor is 163 ms for a cold Object and 165 ms for a fresh isolate. Empty Yielded is 358 / 682 ms in those cohorts. The differences include initial Thread facts, admission and potentially overlapping first-turn work; they are not pure constructor CPU. Empty Objects have not executed seed model turns, while nonempty Objects have, so the lower 50-turn median does not show a benefit from adding history.

| Bare control | Admission, median [Q1–Q3] ms | First request, median [Q1–Q3] ms |
| --- | ---: | ---: |
| Cold Object, fetch | 152 [144.5–158.5] | 162 [151–192] |
| Cold Object, RPC | 163 [152.5–196] | 199 [192–230] |
| Fresh isolate, fetch | 173 [154–199] | 187 [170–274] |
| Fresh isolate, RPC | 165 [146.5–174] | 198 [168–277] |

Observed spread is substantial, especially for receipt delivery. [measurements.json](measurements.json) retains all targets’ quartiles, p10/p90, extrema, CPU coverage and warm repeat spread; [inputs.csv](inputs.csv) retains individual accepted numeric rows. Older zero-history fetch cohorts remain labelled as controls.

| Condition | Turns | Admission Y [Q1–Q3], ms | First request Y [Q1–Q3], ms |
| --- | ---: | ---: | ---: |
| Cold Object | 0 | 358 [290.5–413.5] | 462 [390–499] |
| Cold Object | 50 | 268 [234.5–320.5] | 300 [268.5–344.5] |
| Cold Object | 250 | 480 [348–494] | 504 [437.5–521] |
| Cold Object | 1,000 | 752 [228.5–937] | 831 [786–1,077] |
| Fresh isolate | 0 | 682 [579.8–737.8] | 785.5 [637.5–961.5] |
| Fresh isolate | 50 | 624 [535.8–817] | 790 [553–962.8] |
| Fresh isolate | 250 | 771 [668.5–896] | 977 [823–1,097] |
| Fresh isolate | 1,000 | 1,382 [925.5–1,567.5] | 1,669 [1,417–1,862] |

## Scope and method

The product is pinned to 07f0272e7ba49a494064b6b74c6318b55514ae19. All latency comes from deployed Cloudflare resources managed with Alchemy. The task-local harness extends [#828](https://github.com/yielded-dev/agent/pull/828) and the [cold-turn await inventory](https://github.com/yielded-dev/agent/blob/dan/cold-turn-latency/examples/durable-bench/results/cold-turn/await-inventory.md) in a separate worktree; no benchmark files belong to a product PR. Each target has its own Worker, and the bare and pi bundles contain no Effect code. Fifteen Objects per target/history cell run with an instant provider, one cold first turn, one discarded warmup and two warm repeats. Seeding and count checks finish before timing. The workload uses eight ordinary tools and nine model requests. The bare control writes one receipt row, awaits confirmed storage, and issues one direct provider probe. Its whole-turn duration is not an eight-tool comparison.

Empty means an initialized SQLite layout with zero canonical Thread records; its first admission materializes the Thread. The bare table is also initialized before timing. The initial bare control used fetch. It is retained as a transport control, not pooled with the primary RPC result. A later bare control uses native RPC, matching ThreadObject's entry transport; its same-run empty ThreadObject and pi cohorts repeat the zero-history comparison. The primary zero-history table uses that RPC control, selected before seeing its results. All earlier data stays in the evidence.

Cold Object means a changed incarnation on the same isolate. The zero-history isolate has previously initialized its storage but has not replayed a model turn; seeded isolates have executed their seed histories. Module and incarnation identity are verified, while identical JIT warmup across different history sizes is not claimed. Fresh means a changed isolate after a code upload, one observed Object construction, no earlier stateless fetch on that isolate, no earlier admission and no alarm before the input. Readiness probes use disposable Objects and do not touch measured fresh Objects. Warm turns require the same incarnation and isolate. Three completed first turns fail the constructor-count check and are excluded: one Yielded/50 and one pi/0 in the first fresh matrix, and one Yielded/0 in the second. This leaves 14 or 15 valid Objects per cold cell. Their later warm turns independently pass resident checks and remain in the warm aggregates.

The Object checks its own BUILD before admitting an input. Completed identity and every provider receipt must also match the expected build. Only acknowledged setup resets and read-only profile capture requests retry. Inputs do not retry. [The earlier cold-storage report](../cold-storage/report.md) discloses which older probes lacked these attestations. The latest shared benchmark head checked at publication (699d163a) adds the same Object-build protection and a Tardie directory distinction; the isolated harness already implements the relevant guards and does not run Tardie.

Admission uses the driver's submit-to-receipt clock. First request uses the provider receipt's arrival timestamp minus driver start, a cross-Worker interval whose clock offset is not independently calibrated here. It includes routing and any output-gate delay; it is not the Object's frozen dispatch timestamp. Latency tables use descriptive medians of Object medians, with Object spread and warm repeat spread retained. CPU counter tables use medians of the matched invocation observations. Each cold Object contributes one first turn, so it has no within-Object cold repeat estimate. No small latency gain is inferred from these unpaired cells.

At 1,000 turns, three of the earliest receipts arrive in 139–174 ms while those Objects' first model requests arrive in 810–1,279 ms. Three of the latest receipts arrive in 1,076–1,139 ms, almost together with their first requests. These are retrospective examples, not exclusions; every accepted Object remains in the aggregate. Identical open reads and the large receipt spread show why receipt time alone is not a storage-wait measurement.

## CPU and I/O boundaries

The I/O clock does not advance during synchronous work. All Yielded constructor and admission boundaries in the normal timing matrices share their respective frozen timestamps. The first SQL call also returns without a clock advance. For cold Objects at 50 and 250 turns, alarm entry to provider dispatch has a 0 ms median I/O-clock advance, despite a large driver-visible interval. [Cloudflare timer semantics](https://developers.cloudflare.com/workers/runtime-apis/performance/).

The bare RPC control records 2 / 5 ms of submit CPU, 80 / 72 ms of explicit storage-sync I/O-clock advance and 71 / 73 ms from driver start to constructor entry, for cold Object / fresh isolate. The earlier fetch control records 3 / 5 ms CPU and 75 / 85 ms sync advance. These separate medians are not an additive latency decomposition. The constructor’s trace does not expose a wait that grows with history size. Opening each tested Yielded Object reads 997 SQLite cursor rows in 12 statements: due-queue queries and three schema/layout inspections, with no canonical history read. These are cursor-work counts, not simply returned-row counts. Before the first provider, cumulative reads grow to 1,479 / 1,878 / 4,868 / 13,651 at 0 / 50 / 250 / 1,000 turns. Cursor row counts include SQLite's index work; they are not physical disk-page I/O.

An I/O-clock increment locates an asynchronous resumption; it does not by itself give an exclusive wait duration. CPU since the previous tick, scheduling and other invocations can overlap. Whole invocation CPU counters are therefore shown separately and never added as an exclusive partition of the receipt interval. Pi retains asynchronous turn work with `waitUntil`, so its submit invocation CPU can include later execution; it is not an isolated admission CPU measurement either. CPU telemetry is sampled or delayed; the aggregate retains each counter’s observed `n` and never infers missing CPU. The remaining pre-constructor and output-gate/transport intervals are explicit gaps, not automatically labelled CPU or storage. Cloudflare holds outgoing responses and fetches until required pending writes finish. [Output gates](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/).

| Condition | Turns | Driver to constructor Y / pi, ms | Submit invocation CPU Y / pi, ms | Y alarm invocation CPU, ms | Y admission to alarm I/O advance, ms | Y alarm to first dispatch I/O advance, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold Object | 0 | 60 / 69 | 92 / 41 | 155 | 50 | 1 |
| Cold Object | 50 | 68 / 64 | 42 / 66.5 | 93 | 50 | 0 |
| Cold Object | 250 | 62 / 70 | 47 / 90 | 290.5 | 50 | 0 |
| Cold Object | 1,000 | 61 / 66 | 53 / 109 | 596 | 50 | 1 |
| Fresh isolate | 0 | 323 / 132 | 138 / 63 | 204 | 64 | 1 |
| Fresh isolate | 50 | 326 / 134 | 135 / 65 | 279 | 58 | 1 |
| Fresh isolate | 250 | 326 / 146 | 128 / 96 | 472.5 | 64 | 1 |
| Fresh isolate | 1,000 | 365 / 138 | 166 / 174 | 930.5 | 64 | 1 |

The constructor and submit regions have zero I/O-clock advance in every accepted Yielded cold input; the first SQLite call does too. The much larger driver-visible prefix is consistent with synchronous CPU plus opaque transport/output-gate intervals, not a measured constructor storage wait. The 50–64 ms admission-to-alarm tick is an observed asynchronous boundary, not 50–64 ms of proven idle time. Caller-Worker submit CPU is generally 0–2 ms. The fresh pre-constructor gap remains unassigned.

## Deployed profiles

Production CPU captures cover ten accepted cold starts per mode at 250 seeded turns. All twenty captures pass their Object-build, provider-build, transcript and sampled-boundary checks. Named diagnostic CPU loops delimit constructor start, endpoint entry, framework admission return and the first prepared provider request. Samples after the first-provider boundary, sentinels and explicit idle are excluded. Profile turns are serialized and hold the first provider response for 20 seconds; their latency is never mixed with the normal timing matrix.

The production API requires a recently active, already-loaded isolate. For the cold-Object profile, capture starts before an acknowledged Object reset on the same isolate. For fresh initialization, a minimal native shell lets the profiler attach before constructing its first production ThreadObject. The shell's metadata and sentinel routes do not open storage or the Thread. This measures first framework initialization, not module evaluation or platform activation. The normal fresh-isolate timing rows use the ordinary production class. [Production profiling scope](https://developers.cloudflare.com/workers/observability/profiling-in-production/).

Exported pprof weights include elapsed gaps between samples, so they are not treated as CPU milliseconds. A deployed sentinel calibration had 8,351 ms of exported weights, 4,056 non-idle sample records at a nominal 1 ms period, and 4,328 ms of invocation CPU; eight long intervals alone contributed 4,016 ms to the weights. The summary counts original records before merging profiles and reports self-sample shares and nominal sample-ms estimates. Raw captures and exact source maps stay private; their hashes and the small aggregates are retained. One fresh diagnostic input has a 6.359-second I/O-clock interval before alarm entry; its CPU samples remain in the aggregate, while its raw elapsed weights are not charged as self CPU. The profiling shell can retain a stale pre-input clock tick. Long sampling gaps can also hide unsampled native work, so the sample-period estimates are approximate.

The post-admission region accounts for 83.8% and 75.7% of the respective sampled prefixes. These are estimates from original samples, not exact billed CPU or an additive decomposition of the timing-table medians.

| Profile phase at 250 turns | Cold Object, mean nominal sample-ms | Fresh initialization, mean nominal sample-ms |
| --- | ---: | ---: |
| Construction to endpoint | 26.9 | 108.9 |
| Endpoint to framework admission return | 36 | 45.9 |
| Admission return to first prepared model request | 324.7 | 482.6 |
| Total sampled prefix | 387.6 | 637.4 |

| Rank | Cold Object: top self samples | Fresh initialization: top self samples |
| ---: | --- | --- |
| 1 | Garbage collection, 5.73% | Garbage collection, 5.10% |
| 2 | Native/unresolved `next`, 4.49% | Effect generator thunk (`effect.ts:1254`), 4.27% |
| 3 | Native SQLite `exec`, 4.41% | `fnUntraced` thunk (`effect.ts:1267`), 3.94% |
| 4 | Effect generator thunk (`effect.ts:1254`), 3.25% | Union parser (`SchemaAST.ts:3668`), 3.45% |
| 5 | `fnUntraced` thunk (`effect.ts:1267`), 2.89% | Struct parser (`SchemaAST.ts:3034`), 3.11% |
| 6 | Union parser (`SchemaAST.ts:3668`), 2.55% | SQLite-call wrapper (`timeline.ts:135`), 3.00% |
| 7 | Effect `runLoop`, 2.45% | Effect `runLoop`, 2.62% |
| 8 | Struct parser (`SchemaAST.ts:3034`), 2.37% | SQL `runIterator`, 2.48% |

[profiles.json](profiles.json) retains per-capture sample counts, hashes, source-map hashes, all phase aggregates and the top twenty self functions per phase. These function shares identify sampled work; generic Effect frames do not by themselves justify flattening Effect composition. The wrapper at timeline.ts:135 directly invokes native SQLite exec; its samples include native SQL and must not all be called tracing overhead. Unresolved native/program frames and garbage collection remain visible.

## Ownership and next lever

The history-dependent prefix includes prompt hydration, journal projection and history-digest preparation. That work belongs to [history-cost / #830](https://github.com/yielded-dev/agent/pull/830) and is handed off there. These profiles measure the pinned baseline, not #830’s candidate. No prompt code is edited by this task. Layout normalization is 2.1 nominal sample-ms per cold start on a warm isolate and 3.0 during fresh initialization. The complete constructor averages 26.9 and 108.9 sample-ms. The sampled remaining layout work is small beside the first-turn prefix; sharing a validated layout across independently acquired adapters would introduce a new proof/lifecycle mechanism. No product change or latency gain is claimed from these observations. The history-dependent ownership lookup reads 1,002 cursor rows at 1,000 turns, while prompt-content and prompt-length queries read 10,690 combined; its row count alone does not establish a worthwhile latency fix.

No storage format, canonical record, hash chain, historyDigest recovery, lease, claim or mutation gate is changed. No unconfirmed writes or warm context cache is introduced.

## Outcomes, validation and cleanup

All setup failures, excluded turns, non-ok invocation categories and profile API responses are retained. A 20-Object initial attempt failed during an untimed 1000-turn seed after a provider transport error; it admitted no measured inputs. Failed seeds are never resumed. Subsequent controllers retain independent, fully verified cohorts if another seed fails. Earlier profile attempts returned no-active-isolate errors or completed without a valid capture; their inputs and outcomes are distinguished from accepted profiles.

The four normal timing runs completed all 1,440 inputs, including warmup. Every completed input passed transcript, Object-build and provider-build checks. Three completed first turns failed the freshness proof, leaving 1,077 accepted cold/fresh/warm numeric rows. All twenty primary profiles passed their independent boundary, build and transcript checks. The 50/250-turn fingerprints remain `b017b487524e44a4` / `dcea9f30b0917245`.

[outcomes.json](outcomes.json) retains all twelve controller runs, all setup-readiness failures, excluded/failed/skipped inputs, non-ok invocation aggregates and all 41 profile API attempts. One rate-limit response before a diagnostic input requested a 300-second delay, which was honored. Early failed preflights did not record their HTTP status; it remains unknown. A preflight-only controller row originally marked “unknown” is annotated as never dispatched, with the original retained. The three failed bare calibration inputs remain Unknown and were not replayed.

[validation.json](validation.json) records guard totals, source hashes, exact revisions and the successful `vp run ready` on the unchanged product snapshot, with no vendored `third-party/node_modules`. That gate included record-format, static/export checks, tests and package/documentation builds. The task changes evidence and an isolated harness only; no format baseline, changeset or product PR is needed. [storage-counts.json](storage-counts.json) contains logical SQL work, including the largest first-pass queries.

Alchemy teardown and an independent API inventory verified **zero `cold-bisect` Workers and Durable Object namespaces** at **2026-10-09 11:50:19 UTC**. Private Alchemy state was removed. [cleanup.json](cleanup.json) records the complete Worker inventory and paginated namespace verification. Accepted profile captures and source maps remain in a private mode-700 local audit directory, outside the repository; no raw archives or credentials are published.

## Warm reference

Two warm repeats per Object after a discarded warmup; all cohorts have 15 Objects per target. Warm histories include the preceding measured input and warmup. The two independently run cohorts are retained separately, and no small differences between them are presented as improvements.

| Warm cohort | Seeded turns | Admission Y / pi, ms | Y ÷ pi | First request Y / pi, ms | Y ÷ pi | Complete turn Y / pi, ms | Y ÷ pi |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| After cold Object | 0 | 139 / 67.5 | 2.059× | 165.5 / 64.5 | 2.566× | 711 / 528 | 1.347× |
| After cold Object | 50 | 155.5 / 73 | 2.130× | 179.5 / 69.5 | 2.583× | 725.5 / 545 | 1.331× |
| After cold Object | 250 | 275 / 65 | 4.231× | 325 / 70 | 4.643× | 901.5 / 655.5 | 1.375× |
| After cold Object | 1,000 | 486.5 / 96 | 5.068× | 633.5 / 84.5 | 7.497× | 1,266 / 795 | 1.592× |
| After fresh isolate | 0 | 148.5 / 59 | 2.517× | 175 / 63 | 2.778× | 736 / 512 | 1.438× |
| After fresh isolate | 50 | 170 / 96 | 1.771× | 196 / 78.5 | 2.497× | 759 / 570.5 | 1.330× |
| After fresh isolate | 250 | 279 / 67 | 4.164× | 311 / 75 | 4.147× | 927.5 / 686.5 | 1.351× |
| After fresh isolate | 1,000 | 694 / 119.5 | 5.808× | 763.5 / 123.5 | 6.182× | 1,543.5 / 958 | 1.611× |

## Reproduction

The [task harness](https://github.com/yielded-dev/agent/tree/dan/cold-bisect-bench) is on a separate branch. Pin `3108af42` for the first small-history matrices and cold-Object profiles, or `52db55ff` for the native-RPC controls, 1,000-turn matrices and fresh-initialization profiles. Yielded/pi executable source is identical across all four normal timing runs after normalizing generated source-path comments. Injected build identifiers differ deliberately and are checked at the Object boundary.

From a credential-enabled checkout, with the task harness checked out separately:

```sh
vp exec direnv exec . vp exec bun /path/to/bench/examples/durable-bench/deployed/main.ts \
  --isolate --framework 07f0272e7ba49a494064b6b74c6318b55514ae19 \
  --targets bare,yielded,pi --sizes 0,50,250 --ttft 0 \
  --objects 15 --repeats 2 --concurrency 16 --cold-mode object --cpu
```

Repeat with `--cold-mode fresh`, then repeat both modes with `--sizes 0,1000` on the later harness. Profiles use the same entry point with `--targets yielded --sizes 250 --objects 10 --repeats 1 --concurrency 16 --ttft 0 --profile --cpu`, once per cold mode. The controller serializes profile captures after parallel seeding. These are many-Object case comparisons of a constant product, not a baseline/candidate performance claim. Product fixes would require a guarded same-Object A/B.

The harness keeps Alchemy state in a mode-700 private temporary directory. Finish with the same entry point’s `--teardown`, then independently enumerate all Worker scripts and all pages of Durable Object namespaces for the `cold-bisect` prefix. Only prefixed resources belong to this investigation.
