# Cold storage

Adding 32 MiB of retained but unread storage did not produce a resolved cold-admission or first-turn penalty, including on verified fresh isolates. Forcing a fresh isolate to scan all those bytes raised CPU and showed a positive whole-turn signal; its admission change remained within the repeated-control spread. These tests do not establish storage-dependent waiting as the cause of the reported cold gap. No product format was changed or product PR opened. The fresh-isolate conditions address the subsequent [isolate-start signal](https://github.com/yielded-dev/agent/blob/dan/isolate-start-evidence/examples/durable-bench/results/isolate-start/report.md).

The framework revision is `07f0272e7ba49a494064b6b74c6318b55514ae19`. Both labels in each experiment run that exact revision. The Object-only padding probes retain a byte-identical Worker bundle; the fresh-isolate protocol below varies only the upload comment and build marker around unchanged compiled code. The production `ThreadObject` admission, mutation gate, pre-arm, durable receipt, claims, leases, canonical log, and settlement path remain intact.

## Controlled storage experiments

The first two experiments each use 10 Yielded and 10 pi Objects, 50 seeded turns, an instant network provider, four balanced same-Object passes, three measured warm turns per pass, and concurrency four. Both chose BAAB. B adds 8,192 rows of 4,096 deterministic ASCII bytes in a benchmark-owned table. A empties that table. The schemas and canonical history are unchanged; history grows through the same measured turns in both targets. All padding writes and `storage.sync()` finish before any timed turn in the pass, followed by two seconds of quiescence.

| Treatment | Paired cold admission delta | Paired cold turn delta | Cold turn repeat spread, A / B | Observed cold submit CPU, A → B | Paired warm turn delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| Extra 32 MiB, never read by the turn | −94 ms | −55 ms | 129 / 308 ms | 51 → 55.5 ms | −4 ms |
| Extra 32 MiB, scanned at cold startup | −56 ms | −14 ms | 245 / 51 ms | 44 → 126 ms | +16 ms |

Deltas are medians of within-Object B-minus-A differences. Repeat spread is the median within-Object range for the two cold observations of each label. CPU is the median of observed, attributed submit invocations; missing telemetry is not zero. These negative deltas are **not speedup claims**. No Object's padded admission observations all exceeded its unpadded observations in either experiment.

The scan treatment executes `SUM(length(payload))` over TEXT, returning one aggregate while reading all 33,554,432 payload bytes. SQLite reads each TEXT value to count its characters; the ASCII padding makes that equal the byte count. [SQLite function semantics](https://www.sqlite.org/lang_corefunc.html#length). Its cold startup reads increase by exactly 8,192 rows, from 1,190 to 9,382; subsequent warm turns do not repeat the scan. The CPU increase confirms that the read pressure is real. It does not resolve a storage-dependent waiting penalty of the size needed to explain the original 520–730 ms admission.

| Treatment | Cold Yielded ÷ pi, A → B | Warm Yielded ÷ pi, A → B |
| --- | ---: | ---: |
| Unread padding | 1.34× → 1.27× | 1.34× → 1.35× |
| Scanned padding | 1.29× → 0.97× | 1.42× → 1.40× |

These are diagnostic treatments applied to both frameworks, not before/after product builds. Each ratio divides the targets' medians of Object medians. The paired Yielded cold-turn ratios are 0.956× and 0.988×, respectively. Neither clears its repeated-control spread. Both experiments completed all 400 turns, verified every model-visible transcript, and verified deletion of their target stacks.

Cloudflare reports 4 KiB pages. Median active storage increased from about 4.20 to 41.99 MB for Yielded, and 0.40 to 38.18 MB for pi. `databaseSize` counts active pages, excluding the freelist; `page_count` and `freelist_count` queries were denied. Thus bytes ÷ 4,096 describes active pages, **not allocated file size or page faults**. The platform exposes SQL rows read, not a physical page-fault counter. [Storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/), [workerd implementation](https://github.com/cloudflare/workerd/blob/main/src/workerd/api/sql.c%2B%2B).

Deleting padding can leave free pages in an already enlarged file. A third experiment addresses that limitation: 20 fresh Objects per target start small for two A passes; ten then receive 32 MiB for two B passes, while ten remain unpadded. Both groups grow their canonical histories identically. The Worker bundle stays identical, with no redeployment between passes. Cohort order is shuffled, all preparation finishes before timing, and all 480 turns pass transcript and incarnation checks. There is one measured warm turn per pass, at concurrency six.

| Fresh allocation group | Paired cold admission delta | Paired cold turn delta | Cold turn repeat spread, A / B | Cold Yielded ÷ pi, A → B | Warm Yielded ÷ pi, A → B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Grows by 32 MiB | −166 ms | −188 ms | 171 / 71 ms | 1.46× → 1.27× | 1.45× → 1.58× |
| Never receives padding | +48 ms | −13 ms | 309 / 168 ms | 1.39× → 1.38× | 1.55× → 1.64× |

The difference between groups' paired median changes is −213 ms for Yielded admission and −174 ms for its cold turn; pi's cold-turn difference is −164 ms. Yielded's observed cold submit CPU is 44 → 43 ms in the growing group and 49 → 49 ms in controls. Active bytes grow from 3,330,048 to 41,981,952 in the treated Yielded group, versus 3,330,048 to 4,179,968 in controls. This also finds no positive storage penalty. AABB is intentionally directional, and large outliers remain in the evidence; the negative differences are not a padding speedup claim. No `VACUUM` experiment was performed.

Cold means a verified new Object incarnation after acknowledged sync and abort. It does not mean a cold isolate or cold disk; the harness cannot force eviction from the storage host or clear its page cache. This matches the [cold-turn experiment](https://github.com/yielded-dev/agent/blob/dan/cold-turn-latency/examples/durable-bench/results/cold-turn/report.md). Results bound this workload and perturbation; they do not establish that storage reads can never affect latency.

## Fresh-isolate extension

The user supplied the isolate-start finding after the Object probes: fresh admission remains much larger than measured module startup or submit CPU. The added storage test uses that task's verified-isolate protocol in [dan/cold-storage-isolate-bench](https://github.com/yielded-dev/agent/tree/dan/cold-storage-isolate-bench), with readiness hardened at `31c6e662486b3d7064dfcd1bf956606a9e3d95df`. The native pi bundle retains zero Effect modules. Both labels use the same framework revision and reuse the same compiled source; only the upload comment and build marker change to force a new version.

The experiment holds a 50-turn history constant at setup and varies retained bytes through the same 32 MiB treatment. Thirty Objects per target provide fifteen growing Objects and fifteen never-padded controls. AABB ensures every baseline database starts small. Padding is prepared and synced on the old build, then every Object acknowledges an abort before either new Worker is uploaded. Each measured upload has a 60-second untimed propagation interval and ten consecutive matching health rounds from both controller and deployed driver. Readiness touches only stateless routes. The first Object request is the measured turn.

A fresh sample must show the expected Worker and Object builds, a changed isolate and Object incarnation, first entry, exactly one constructor, zero prior stateless fetches and zero prior alarms. Warm samples require identity continuity. The analysis retains only complete four-pass cohorts and records all exclusions separately. As with Object aborts, a fresh isolate does not prove that the storage host's page cache was cold.

The unread-padding run completed all 720 turns without failures or exclusions. All 240 first turns passed independently rechecked freshness criteria, leaving fifteen complete Objects per target and group. Growing Yielded's active database from 3,637,248 to 42,334,208 bytes still produced no resolved cold penalty:

| Fresh-isolate unread group | Paired admission delta | Paired first-turn delta | First-turn repeat spread, A / B | Fresh Yielded ÷ pi, A → B | Warm Yielded ÷ pi, A → B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Grows by 32 MiB | −6 ms | +7 ms | 115 / 93 ms | 1.67× → 1.81× | 1.26× → 1.26× |
| Never receives padding | +24 ms | −61 ms | 209 / 117 ms | 1.92× → 1.67× | 1.31× → 1.27× |

Subtracting the groups' paired median changes gives −30 ms for admission and +68 ms for the first turn. Admission repeat spreads are 45/63 ms in growing Objects and 97/50 ms in controls. The turn change is below the repeated-control spread. Warm control-adjusted changes are +1 ms per turn and +34 ms through receipt return, also unresolved against controls. Observed fresh submit CPU is 145.5 → 133 ms in growing Objects and 162 → 152.5 ms in controls. Opening reads stay at 1,190 rows and touch zero padding bytes. [Complete-cohort calculations](fresh-unread.json) retain both targets' medians, paired changes and spreads.

The completed forced-read replication uses `f579db4f2e295df9110b8dadcaf186b985d3432b`. It also completed all 720 turns, with 240 verified fresh first turns, no failures or exclusions, and fifteen complete Objects per target and group. Padding stays in place between repeated labels; current size is measured after every turn. Median active bytes after fresh turns are 3,788,800 → 42,496,000 for growing Yielded Objects and 3,788,800 → 4,694,016 in controls.

| Fresh-isolate forced-read group | Paired admission delta | Paired first-turn delta | First-turn repeat spread, A / B | Fresh Yielded ÷ pi, A → B | Warm Yielded ÷ pi, A → B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Grows by 32 MiB and scans it | +128 ms | +158 ms | 88 / 106 ms | 1.81× → 1.82× | 1.34× → 1.42× |
| Never receives padding | −17 ms | −29 ms | 148 / 123 ms | 1.95× → 1.98× | 1.47× → 1.46× |

The control-adjusted changes are +144 ms for admission and +186 ms for the first turn. The whole-turn signal exceeds these median repeat ranges. Admission repeat spreads are 82/123 ms in growing Objects and 59/160 ms in controls, so a waiting penalty is not resolved by this admission comparison. Yielded submit CPU rises from 117 to 188 ms in growing Objects and stays at 144 ms in controls. Pi's observed run CPU rises from 274.5 to 379.5 ms in growing Objects, versus 379 to 385.5 ms in controls. These CPU and elapsed aggregates are not an exclusive partition; subtracting them would not identify a wait duration.

Every treated fresh request reads exactly 33,554,432 padding bytes; warm requests read none. Cold entry SQL reads, including the aggregate query, are 1,191 → 9,382 for Yielded and 30 → 8,221 for pi. The warm control-adjusted turn change is +59 ms, below the 67–82 ms control repeat spread; warm admission is −17 ms. [Forced-read calculations](fresh-scan.json) preserve all medians, cohort sizes and spreads.

Scanning extra data can therefore cost CPU and elapsed time. The measured production admission path has bounded schema and submission-lane reads, and does not scan these payloads or historical continuations. The unread-byte experiments address the proposed size-dependent database-availability wait; the forced scan does not identify a production read or format change that would remove that wait. The measured data does not justify a storage-format rewrite.

This extension uses the isolate task's native HTTP provider harness. Its within-experiment A/B comparisons hold that code constant; subtracting its absolute timings from the earlier Object matrix would also change the consumer harness and would not isolate module startup.

## History-size comparison

The deployed matrix uses 10 Objects per target/cell, four cold incarnations and one measured warm turn per incarnation. Both A/A labels use the same framework revision. Each history size runs separately; all three runs completed their 480 planned turns, with no failed measured turns and verified target cleanup.

Pooled results combine all four epochs within each Object before taking the median across Objects. Times are milliseconds, Yielded / pi.

| Seed turns / provider delay | Cold turn, Yielded / pi | Cold ratio | Warm turn, Yielded / pi | Warm ratio |
| --- | ---: | ---: | ---: | ---: |
| 50 / 0 ms | 1,360 / 844 | 1.61× | 849 / 607 | 1.40× |
| 50 / 400 ms | 5,322 / 4,937 | 1.08× | 4,916 / 4,710 | 1.04× |
| 250 / 0 ms | 1,647 / 963 | 1.71× | 1,095 / 680 | 1.61× |
| 250 / 400 ms | 5,718 / 5,153 | 1.11× | 5,195 / 4,833 | 1.07× |
| 1,000 / 0 ms | 2,455 / 1,796 | 1.37× | 1,478 / 1,392 | 1.06× |
| 1,000 / 400 ms | 6,547 / 5,400 | 1.21× | 5,734 / 5,097 | 1.12× |

The two unchanged-code labels expose the control drift:

| Seed turns / provider delay | Cold Yielded ÷ pi, A / B | Warm Yielded ÷ pi, A / B |
| --- | ---: | ---: |
| 50 / 0 ms | 1.70× / 1.55× | 1.30× / 1.43× |
| 50 / 400 ms | 1.13× / 1.06× | 1.03× / 1.04× |
| 250 / 0 ms | 1.80× / 1.70× | 1.59× / 1.66× |
| 250 / 400 ms | 1.13× / 1.13× | 1.07× / 1.07× |
| 1,000 / 0 ms | 1.44× / 1.39× | 1.21× / 1.11× |
| 1,000 / 400 ms | 1.19× / 1.20× | 1.13× / 1.13× |

These are unchanged-code control labels, not before/after fixes. The 50-turn run's paired Yielded cold-turn changes are −114 ms with an instant provider and −65 ms at 400 ms, versus repeated-control spreads of 209–239 ms and 110–122 ms. At 250 turns they are −87 ms and +8 ms, versus 95–125 ms and 190–351 ms. At 1,000 turns they are +279 ms and +111 ms, versus 233–387 ms and 160–340 ms; its largest within-label cold repeat range is 11,021 ms and is retained. A label difference alone is not evidence of an improvement.

The independently generated fixture fingerprints are `b017b487524e44a4` at 50 turns, `dcea9f30b0917245` at 250, and `ac520308146f2a8f` at 1,000. Compaction is disabled. Each measured turn makes eight sequential readonly tool calls and nine model requests. Yielded seeds through untimed public submit/await replay because canonical import omits historical attempt rows; no attempt, claim, or lease tables are copied around that importer.

Turn time is measured by the deployed driver, through a durable receipt and completed settlement for Yielded, or completed `/run` for pi. Admission is the driver's elapsed time through receipt return. Post-turn metrics collection and controller-to-driver transport are outside these clocks. These are complete-turn measurements, not first-text latency.

## What the cold path actually reads

Normal open reads schema declarations and singleton metadata, not canonical history. In the 50-turn diagnostic, three schema inspections account for 993 SQLite row reads; the benchmark's config-table discovery adds 186. Admission reads a 50-row submission lane; large lanes cap this discovery at 129 rows before indexed fallbacks. First context construction also reads historical prompt records during execution.

The unchanged-code matrix shows history sensitivity in the warm path as well as the cold path. With the instant provider, these are pooled Object medians; SQL row counts are cumulative through the named phase.

| Seed turns | Deployed seed bytes, Yielded / pi | Yielded admission, cold / warm | Observed submit CPU, cold / warm | Cold entry SQL rows | Cold SQL rows by first provider |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 | 3,092,480 / 241,664 | 481 / 184 ms | 64 / 6.5 ms | 1,181 | 2,247 |
| 250 | 12,484,608 / 753,664 | 667 / 357 ms | 68 / 12 ms | 1,181 | 5,239 |
| 1,000 | 47,898,624 / 2,654,208 | 1,074 / 629 ms | 65 / 11 ms | 1,181 | 14,021 |

The extra cold admission time over warm is about 297 ms at 50 turns, 311 ms at 250 and 445 ms at 1,000. Those are differences of medians, not isolated causal durations. Opening reads stay constant; subsequent reads and warm latency grow with actual history. The instant-provider warm turn grows from 849 to 1,478 ms in Yielded and 607 to 1,392 ms in pi. Yielded's observed execution-alarm CPU medians grow from 312 → 468 → 812 ms cold and 121 → 217 → 508 ms warm; these are 39–40 attributed invocations per condition, with missing rows excluded. This supports handing the historical-work investigation to `history-cost`, without attributing the cold wait to bulk retained bytes. Submit, alarm and await invocations can overlap, so their observed CPU values are not summed.

The suggested hot `batch_json` duplication has already been removed by [#810](https://github.com/yielded-dev/agent/pull/810). Hot batches retain a header and reconstruct canonical batch bytes from ordered `record_json` strings. Full duplicate batch payloads remain only in archives; all measured fixture archive tables are empty. [Reconstruction](https://github.com/yielded-dev/agent/blob/07f0272e7ba49a494064b6b74c6318b55514ae19/packages/storage-sql/src/SqlThreadArchiveRange.ts#L65).

The local fixture inventories reproduce the size disparity:

| Seed turns | Yielded / pi active bytes | Yielded canonical JSON | Continuation JSON | Hot batch headers |
| --- | ---: | ---: | ---: | ---: |
| 50 | 2,220,032 / 237,568 | 603,907 | 350,091 | 24,467 |
| 250 | 8,228,864 / 753,664 | 3,016,557 | 1,745,913 | 121,767 |
| 1,000 | 30,584,832 / 2,707,456 | 12,081,123 | 6,982,433 | 486,767 |

These are deterministic local byte counts, not latency evidence or deployed sizes. [The inventory](fixture-storage.json) includes SQLite page counts and the largest tables and indexes. Rewriting continuations or archived storage would add format risk without a demonstrated improvement to this cold path. The record-format baseline and product files remain unchanged.

The remaining source candidates include repeated 129-row lane probes and an ownership join over retained submissions above 128 turns. The latter is on the execution path. Neither has a measured latency benefit here; overlapping invocations and gate timing prevent assigning the driver's admission wait from code ordering alone. No speculative optimization is included. The earlier [await inventory](https://github.com/yielded-dev/agent/blob/dan/cold-turn-latency/examples/durable-bench/results/cold-turn/await-inventory.md) also separates synchronous SQL from transaction promises, KV/alarm operations, and transport. This investigation does not identify a particular Cloudflare scheduling, routing, or durability-gate wait as the cause; invocation CPU and Object/driver timestamps cannot uniquely separate those costs.

## Reproduction and outcomes

The isolated harness branch is [dan/cold-storage-bench](https://github.com/yielded-dev/agent/tree/dan/cold-storage-bench). The unread-padding run uses harness `ce0ce8dd`; the scan run uses `d0240c36`, both based on deployed-bench revision `9c63a3e8`. The final history runs use `41d51e71`, incorporating the subsequent deployed-bench fixes through `78cb4eb7`. The fresh allocation control uses `6364dcf6`. The harness changes are confined to that branch; no benchmark files enter a product PR. Use the recorded harness revision for each reproduction.

From the credential-owning project directory, with the isolated worktree at `/private/tmp/cold-storage-bench`:

```sh
direnv exec . vp -C /private/tmp/cold-storage-bench run -F @yielded/agent-example-durable-bench deployed -- --rigorous --baseline 07f0272e --candidate 07f0272e --targets yielded,pi --sizes 50 --ttft 0 --objects 10 --repeats 3 --concurrency 4 --storage-probe untouched
direnv exec . vp -C /private/tmp/cold-storage-bench run -F @yielded/agent-example-durable-bench deployed -- --rigorous --baseline 07f0272e --candidate 07f0272e --targets yielded,pi --sizes 50 --ttft 0 --objects 10 --repeats 3 --concurrency 4 --storage-probe touched
direnv exec . vp -C /private/tmp/cold-storage-bench run -F @yielded/agent-example-durable-bench deployed -- --rigorous --baseline 07f0272e --candidate 07f0272e --targets yielded,pi --sizes 50 --ttft 0 --objects 20 --repeats 1 --concurrency 6 --storage-probe allocated
for turns in 50 250 1000; do
  direnv exec . vp -C /private/tmp/cold-storage-bench run -F @yielded/agent-example-durable-bench deployed -- --rigorous --baseline 07f0272e --candidate 07f0272e --targets yielded,pi --sizes "$turns" --ttft 0,400 --objects 10 --repeats 1 --concurrency 6
done
```

The Object harness was fully torn down before starting the fresh-isolate harness, whose narrower resource prefix is `cold-storage-fresh`. With its separate worktree at `/private/tmp/cold-storage-isolate-bench`, the unread treatment is:

```sh
direnv exec . vp -C /private/tmp/cold-storage-isolate-bench run -F @yielded/agent-example-durable-bench deployed -- --rigorous --isolate --baseline 07f0272e --candidate 07f0272e --targets yielded,pi --sizes 50 --ttft 0 --objects 30 --repeats 1 --concurrency 6 --storage-probe untouched
```

Use `--storage-probe touched` for the forced-read condition. Both harnesses expose `--teardown`; final verification also inventories the complete `cold-storage` prefix, including both phases.

The first two storage probes observed SJC ingress for the driver and controller. Placement is a hint, not a guarantee of Object location. All runs use Alchemy, task-prefixed resources, a mode-700 temporary state directory outside the repository, and no paid model.

Preflight failures are retained in the evidence: an inner `direnv` load cleared inherited credentials in the temporary worktree; two local transfer attempts exposed an invalid native-state Proxy; a larger attempt encountered a route-propagation 404 while seeding. The initial history-matrix attempt (`63419790`) lost a 1,000-turn seed response before measurement. A second attempt (`c149ee6e`) lost a 250-turn seed response after 80 of 120 Objects had verified. Both outcomes remained unknown; each run was stopped and its target resources were deleted and API-verified. Neither showed an observed CPU/memory-limit outcome. None of these failed attempts produced timing samples or retried uncertain input.

The two-Object pilot completed, but is excluded from claims because its padding writes could overlap other Objects' timed turns and its repeat drift was large. A 30-second pre-input propagation interval and a separate padding-preparation phase address those observed problems. The final matrix seeds fresh Objects through acknowledged batches of 25 settled turns, at concurrency six. Complete counts and fingerprints are verified after all seed batches. The controller uses fresh HTTP connections and retains transport error names/codes; this changes no driver-observed timing boundary and does not establish the earlier failures' root cause.

The initial fresh-isolate run (`2b00c8a8`) passed both baseline epochs, then some pi requests reached the preceding Worker build after three matching health rounds. It ended with 652 ok, 12 failed, 53 skipped and three excluded turns. Failures include explicit HTTP 409 build mismatches and driver HTTP 502 responses carrying those mismatches; uncertain inputs were never retried. The run is excluded from performance claims and both target stacks were deleted and API-verified. The replacement uses new Objects and the longer stateless readiness protocol above.

The first fresh forced-read attempt completed three epochs and all 540 measured/warmup turns successfully, including the exact 32 MiB scan, then received HTTP 500 with an internal-error reference during untimed padding preparation for epoch four. The remaining 180 planned turns were not attempted. No CPU- or memory-limit outcome was observed, and the underlying cause is not established. Both targets were deleted and API-verified; no input was retried and the incomplete run is excluded from claims. The replacement controller, `f579db4f2e295df9110b8dadcaf186b985d3432b`, removes redundant padding rewrites between repeated labels. It retains the same Worker code and records current database size after every turn alongside the size captured at padding preparation.

That replacement finished all four epochs with 707 ok turns, four freshness exclusions, four failures and five skipped follow-ups. Four pi Objects returned HTTP 500 internal-error references, including one unpadded control; one error was wrapped as driver HTTP 502. No CPU- or memory-limit outcome was observed. The cause is unknown, no uncertain input was retried, and both target stacks were deleted and API-verified. This run is also excluded from performance claims. An unchanged replication on new Objects completed all 720 turns cleanly and supplies the forced-read results above.

Telemetry retains all non-ok outcomes. The unread probe observed 671 canceled alarms, 79 aborted fetches and three canceled fetches, with six unmatched markers; the scan probe observed 649, 79 and three, with 17 unmatched markers. Explicit cold aborts are intentional. Unattributed cancellations remain unattributed, rather than being relabeled as successful turns. Neither valid probe had a failed measured turn or an observed CPU/memory-limit outcome.

The 1,000-turn run also retains 18,772 canceled alarms, 155 aborted fetches, one canceled fetch and 308 unmatched markers. It had no failed measured turn or observed CPU/memory-limit outcome. Complete non-ok counts for every run are in `runs.json`.

The clean fresh forced-read run retains 1,720 canceled Yielded alarms, 116 Yielded and 118 pi intentional cold-abort outcomes, two canceled Yielded fetches, and 37 unmatched markers. All eight controlled experiments completed 4,160 successful turns, including 1,280 warmups; the compact tables retain the 2,880 measured turns. Setup failures, the overlapping pilot, and the three unsuccessful fresh-isolate attempts remain listed separately. The 62 observed fresh-harness upload responses all succeeded. [Upload and bundle checks](uploads.json) verify all ten compiled target bundles against the recorded source hashes and confirm that pi retains zero Effect modules.

`vp run ready` passed in the framework checkout with no `third-party/node_modules`. An initial attempt found no local PostgreSQL service; rerunning with a task-owned PostgreSQL 18.6 instance passed all 71 tasks, reusing 32 successful task results. The temporary server was stopped and its data removed. [Validation](validation.json) retains both attempts.

All task resources are destroyed. At `2026-10-09T08:52:28.287Z`, a final Cloudflare API inventory found no Worker or Durable Object namespace under the complete `cold-storage` prefix. Both private Alchemy state directories were removed. [cleanup.json](cleanup.json) records the combined verification and both phase teardowns.

[Run outcomes](runs.json) retain every observed non-ok invocation and setup failure. [Compact measurements](measurements.json) preserve per-Object timing values and SQL read counts without payload archives, account identifiers, or credentials.
