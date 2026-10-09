No module-load candidate resolved a deployed latency improvement above repeat noise. Yielded still trails pi-durable on fresh turns. No performance PR was opened and nothing was merged. The direct-import experiment is preserved at [e63e577b](https://github.com/yielded-dev/agent/commit/e63e577b52f4038d37e5321359e81230a01c85b5); it removes 17.0% of the deployed bundle but only 1.46% of the counted Schema/AST work.

Measurements were taken on 2026-10-09 UTC against main [07f0272e](https://github.com/yielded-dev/agent/commit/07f0272e7ba49a494064b6b74c6318b55514ae19). The benchmark uses [78cb4eb7](https://github.com/yielded-dev/agent/commit/78cb4eb7589984c1cd194e0cb01a5a0055a6d3f3), the latest fetched head of PR #828 during this run, plus the task-local [harness.patch](harness.patch). All timing evidence comes from deployed Cloudflare. Local workerd runs supply deterministic counts only.

The controlled startup comparison uploaded the exact HTTP benchmark bundles in ten ABBA blocks, with an unchanged native pi bundle after each block. Every upload changes the source comment and BUILD marker; the same diagnostic Worker and native Durable Object binding are retained within each target. Alchemy's placeholder uploads are recorded but excluded.

| Cloudflare `startup_time_ms` | Baseline Yielded | Direct-import Yielded | Native pi |
| --- | ---: | ---: | ---: |
| Real uploads | 20 | 20 | 10 |
| Median, ms | 78 | 74 | 16 |
| Q1–Q3, ms | 73.75–93.75 | 69.75–97 | 15–16.75 |
| Range, ms | 69–149 | 64–148 | 13–25 |
| Yielded ÷ pi | 4.875× | 4.625× | 1× |

The median paired-block saving was 6.75 ms, with a range of −57 to +49 ms. Median absolute differences between repeated uploads of the same build were 24.5 ms for baseline and 13.5 ms for the candidate. The apparent four-millisecond shift is unresolved. [startup.json](startup.json) retains all block measurements and the calculation method.

The full run used 20 Objects per target and condition, with the same Objects carried through four upload epochs in ABBA order. A is main; B is the import experiment. Pi's source stays unchanged in A and B positions. Each epoch contains one fresh first turn, one warmup and two warm turns. Values below are medians of Object medians from complete four-pass cohorts; they are descriptive observations, not asserted speedups.

| Seeded turns / provider delay per model call | Fresh Yielded ms, A → B | Fresh pi ms, A → B | Fresh Yielded ÷ pi, A → B | Warm Yielded ÷ pi, A → B | Complete fresh Objects, Yielded / pi |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / 0 ms | 2,040 → 1,890 | 935.5 → 902.5 | 2.181× → 2.094× | 1.534× → 1.542× | 19 / 15 |
| 50 / 400 ms | 5,675 → 5,602 | 5,028 → 5,022.5 | 1.129× → 1.115× | 1.037× → 1.031× | 19 / 17 |
| 250 / 0 ms | 1,935.75 → 1,836.5 | 1,143.5 → 1,113.5 | 1.693× → 1.649× | 1.243× → 1.330× | 20 / 19 |
| 250 / 400 ms | 6,231.5 → 6,268.5 | 5,100.25 → 5,010.75 | 1.222× → 1.251× | 1.080× → 1.072× | 18 / 18 |

Pairing each Object with itself makes the uncertainty clearer. Repeat spread is the median absolute relative difference between the two passes of the same build, calculated as `100 * (max / min - 1)`.

| Seeded turns / delay | Paired fresh B ÷ A, Yielded | Paired fresh B ÷ A, unchanged pi | Yielded repeat spread A / B | Pi repeat spread A / B |
| --- | ---: | ---: | ---: | ---: |
| 50 / 0 ms | 0.934× | 0.931× | 15.5% / 11.5% | 15.2% / 10.0% |
| 50 / 400 ms | 0.978× | 0.995× | 2.5% / 1.4% | 2.5% / 1.0% |
| 250 / 0 ms | 1.000× | 0.961× | 6.4% / 9.4% | 8.6% / 13.9% |
| 250 / 400 ms | 0.998× | 0.992× | 1.8% / 2.0% | 2.1% / 1.5% |

The strongest apparent Yielded improvement coincides with an equally large improvement in unchanged pi. The other cells do not establish a consistent gain. Warm paired Yielded B/A medians range from 0.994× to 1.004×. Yielded's fresh submit-to-durable-receipt medians were 873 → 794, 846.5 → 690.5, 836.75 → 877 and 997.25 → 876 ms in table order. Their paired reductions were 4.2–15.1%, against 12.5–27.4% median repeat spread; pi has no comparable receipt API metric. Full per-Object values, warm latencies, spreads and exclusions are in [fresh-summary.json](fresh-summary.json).

These are driver-observed complete turns through durable settlement, not first-text latency or isolated module-evaluation time. Each turn makes eleven model calls and ten tool calls, so the 400 ms condition has approximately 4.4 seconds of provider delay. The production Durable Object's isolate is verified fresh; routing Workers are health-probed and warm. Histories grow through the balanced order, and the two frameworks occupy different physical Objects. Repeated-pass spreads are not confidence intervals.

Before each epoch, the old build acknowledges a cold abort. After upload, readiness probes touch only the stateless health path. A first turn is accepted as fresh only with the expected build, a new isolate and Object incarnation, first entry, one constructor, no previous stateless fetch and no previous alarm. Warm samples require identity continuity. Failed measured inputs are never retried. Initial seeding additionally waits for propagation and checks disposable Objects, which are separate from measured Objects.

All 2,560 planned turns reached a terminal result: 2,525 ok and 35 excluded, with zero failed or skipped measured turns. Exclusions comprise 26 fresh turns, three warmups and six warm turns. Of 640 first turns, 614 independently satisfied the freshness checks. Complete four-pass fresh cohorts contain 76 Yielded and 69 pi Objects out of 80 each; warm cohorts contain 80 and 77. Every retained controller evidence flag was rechecked, with no mismatches. The 32 transcript groups agree across frameworks; the seeded transcript fingerprints remain `b017b487524e44a4` at 50 and `dcea9f30b0917245` at 250. Individual provider receipts were validated by the controller; the compact artifact preserves its verification result, not those raw receipts.

Deterministic inspection explains why byte removal did not produce a clear startup win. The deployed candidate changes 409 Effect namespace bindings across 89 files to direct subpath imports, including nine explicit type-only imports. A syntax-tree comparison verifies identical non-import code and binding identities. [import-bindings.json](import-bindings.json) records the source hashes and result.

| Actual deployed bundle | Baseline Yielded | Direct-import Yielded | Native pi |
| --- | ---: | ---: | ---: |
| Raw bytes | 3,587,775 | 2,977,669 | 1,706,273 |
| Retained modules | 351 | 319 | 811 |
| Static class definitions | 766 | 736 | 224 |
| Static module-time call/new sites | 6,158 | 5,275 | 295 |
| Schema-family static sites | 3,917 | 3,742 | 0 |
| Context-family static sites | 320 | 303 | 0 |
| Layer-family static sites | 69 | 65 | 0 |
| Selected Schema/AST/equivalence function entries | 82,675 | 81,467 | 0 |
| `Schema.makeClass` entries | 507 | 507 | 0 |
| `Schema.Struct` entries | 924 | 920 | 0 |

| Package contribution, bytes | Baseline | Candidate |
| --- | ---: | ---: |
| Effect | 1,194,368 | 691,744 |
| effect-agent | 1,385,323 | 1,328,353 |
| storage-sql | 294,317 | 279,832 |
| storage-cloudflare | 303,282 | 285,183 |
| platform-cloudflare | 202,040 | 187,846 |

The large class-construction graph survives. Archives, import and lifecycle operations remain reachable from live runtime factories; metadata alone cannot remove them. The relevant packages already declare `sideEffects: []`. Effect decoder factories compile on use, so counting their declarations as eager parser compilation would be misleading.

[module-work.json](module-work.json) contains esbuild composition, dependency versions, source and metafile hashes, largest modules and counter definitions. [count.mjs](count.mjs) counts syntactic initializer, class heritage/static/computed-member and literal-IIFE call/new sites; it skips ordinary function and instance-initializer bodies and counts branch/loop sites once. [count-eval.mjs](count-eval.mjs) instruments copies of Schema, SchemaAST and internal/schema/toEquivalence functions and snapshots their entry counts at the end of global evaluation. These counts include nested calls, getters and constructors, but are neither allocation counts nor CPU attribution. Pi's zero selected entries means it contains none of those Effect functions, not that it performs no module work. It retains no Effect or Yielded runtime modules.

Both deployed Yielded builds use identical direct imports in the consumer harness, a native HTTP model client and the same mock provider. Pi uses its native HTTP SDK. The original scripted-provider entry with unchanged root Effect imports shrank only from 3,436,622 to 3,214,844 bytes (324 → 321 retained modules), because the consumer kept shared namespaces reachable. The old scripted pi bundle was 893,458 bytes; its earlier startup observations must not be compared with the 1,706,273-byte native HTTP pi bundle used in the final comparison. No minification experiment was repeated.

Other module-load candidates were investigated and rejected within this task's scope:

| Candidate | Evidence and disposition |
| --- | --- |
| Lazy record equivalence | Removed 4,589 selected function entries. Baseline uploads 108/83/87 ms; candidate 106/75/93/89/127 ms. No resolved gain. |
| Dynamic import of SQL thread import | Added about 530 KB and produced startup observations 138/119/127/153/134 ms. A retry after direct imports still added over 530 KB; generated initialization wrappers remained eager. Reverted. |
| Extract SQL storage-owner port | Removed about 46 KB and 27 static classes. Eight interleaved uploads per build shifted the median by 4.5 ms, below repeat spread. Reverted. |
| Pure kernel/function wrappers | One kernel annotation saved about 25 KB without reducing selected entries. A broader 142-wrapper probe added about 1.4 KB. Combining it with Schema annotations saved only 21 additional selected entries. Rejected. |
| Private Schema suspension | A 74-wrapper, 19-file sweep removed 8,371 selected entries, while leaving all 506 class factories in that scripted bundle. Most savings came from archive equivalence already probed; the remaining scoped savings did not justify the additional lazy machinery. Not shipped. |
| SQL journal row-schema extraction | About 26 KB, 124 selected entries and four classes removed. Below the observed measurement resolution. Not shipped. |
| Broad Schema purity annotations | Class-heritage annotations removed only 317 selected entries. A broader initializer sweep removed 6,149 entries but introduced many eager wrappers outside that counter and required unsafe blanket assumptions about arbitrary callbacks/getters. Rejected. |
| Two reviewed union purity annotations | Removed 2,067 selected entries without changing used public schemas. Measured separately on main; results below noise. Not shipped. |

The final, narrowly reviewed probe annotates only the `RunEvent` and `AgentError` union constructors. Its four ABBA blocks produced startup medians of 84.5 → 80 ms against pi's 17 ms (Yielded/pi 4.971× → 4.706×). The paired-block median saving was only 0.75 ms, against same-build repeat spreads of 21 and 16.5 ms. This did not justify another full-turn trial. [pure-unions.patch](pure-unions.patch), [pure-unions-startup.json](pure-unions-startup.json) and [reproduction.json](reproduction.json) preserve the exact probe. Further broad public-class laziness would require more machinery or move construction into the first turn. With the remaining scoped candidates below measurement noise, the search stopped.

The final import revision passed `vp run ready` using a task-owned PostgreSQL 18.6 instance, with no vendored `third-party/node_modules` in the product checkout. That database was stopped afterward. No tests were added. Runtime turn logic, canonical storage, pre-arming, receipts, fencing, recovery and Unknown handling were unchanged.

[outcomes.json](outcomes.json) records the pilot, successful full run and two interrupted setup attempts. The first failed setup returned five platform 404s; the second returned six 404s and three “Worker not found” 500s while seeding. Neither reached measured turns. Pending setup outcomes after interruption remain explicitly unknown; those Objects were deleted and no input retried. It also records one initial credential-loading configuration error, one rejected diagnostic binding migration, all readiness failures, non-ok CPU observations and prior verification environment failures. Intentional cold aborts and 490 unmatched CPU markers are retained; no aggregate CPU claim is made. [uploads.json](uploads.json) preserves all 198 observed upload responses, including the rejected upload and placeholders.

To reproduce, fetch the three public revisions above and create a separate benchmark worktree at `78cb4eb7`. Apply `harness.patch`, then run `vp install --frozen-lockfile` there and `vp -C examples/durable-bench/third-party install --frozen-lockfile` for the native pi dependencies. Keep those vendor dependencies out of the product checkout. The patch uses `/private/tmp/isolate-start-private` as its mode-700 Alchemy state directory; use it only after confirming that no other run owns that prefix or directory.

From a credential-bearing repository checkout, invoke the controller directly so a nested package-level direnv command cannot unload its credentials. Replace `/private/tmp/isolate-start-bench` with the benchmark worktree path:

```sh
direnv exec . vp -C /private/tmp/isolate-start-bench exec bun examples/durable-bench/deployed/main.ts \
  --rigorous --isolate --order ABBA \
  --baseline 07f0272e7ba49a494064b6b74c6318b55514ae19 \
  --candidate e63e577b52f4038d37e5321359e81230a01c85b5 \
  --targets yielded,pi --objects 20 --sizes 50,250 --ttft 0,400 --repeats 2 --concurrency 32
```

The private run directory retains the compiled baseline, candidate and pi bundles until teardown. `startup-compare.ts LABEL CANDIDATE_BUNDLE BASELINE_BUNDLE PI_BUNDLE 10 native` runs the upload-only comparison through the same `direnv exec . vp -C … exec bun` boundary. The upload observer records each API response without credentials. Counter usage is documented at the top of the two `.mjs` files; `count-eval.mjs --out <private-output-directory> <bundles...>` was rerun from the published copy and reproduced exactly 82,675 and 81,467 entries. Full raw results remain local; the compact artifacts retain hashes, paired Object observations and every known non-ok category without publishing account or Object identifiers.

Always finish with:

```sh
direnv exec . vp -C /private/tmp/isolate-start-bench exec bun examples/durable-bench/deployed/main.ts --teardown
```

Cleanup was API-verified at 2026-10-09 06:53:55 UTC: zero `isolate-start` Workers and zero matching Durable Object namespaces remain. All owned target and shared Alchemy stacks were destroyed; private state was removed only after checking both API inventories. The task-owned PostgreSQL server was stopped and its temporary data removed. [cleanup.json](cleanup.json) records the verification and method.
