# sync-do-append

**Recommendation: stop; do not land this prototype.** The synchronous SQL core cuts append evaluations from **514.92 to 203.75 per append (−60.4%)**, but there is no repeatable deployed CPU improvement beyond the identical-code control's variation. The paired warm fresh CPU ratios are **0.947** at 10 seed records and **0.938** at 1,000; the control ratios' Q1–Q3 ranges are **0.784–1.447** and **0.897–1.883**. The experiment is incomplete: **611/720 planned CPU samples** passed validation.

The requested conversion of the complete append to one synchronous Effect boundary was **not achieved**. The SQL work runs synchronously, while atomic alarm enrollment, Crypto, optional lifecycle hooks and the surrounding typed APIs remain Effect workflows. Schema's synchronous decoders also evaluate Effects internally. The original stage's absolute share of turn CPU remains undetermined. The retained prototype adds duplicated domain rules and nested transactions without sufficient CPU evidence to justify landing it.

## Deterministic counts

`m0` is the first measured turn after reopening and waking a seeded Object: exactly 50 or 250 historical turns, nine scripted model calls and eight readonly tool calls. These are counts, not local timings.

| Metric | Main | Prototype | Change |
|---|---:|---:|---:|
| Append evaluations / turn, 50 and 250 | 6,179 | 2,445 | −60.4% |
| Append evaluations / append, mean of 12 | 514.92 | 203.75 | −311.17 |
| Append primitive allocations / turn | 6,348 | 2,968 | −53.2% |
| Append primitive allocations / append | 529.00 | 247.33 | −281.67 |
| Append explicit SQL statements / turn | 172 | 173 | +1 |
| Append explicit SQL statements / append | 14.33 | 14.42 | +0.08 |
| Append `transactionSync` calls / turn | 0 | 12 | +12 |
| Whole-turn evaluations, 50 | 37,431 | 33,697 | −10.0% |
| Whole-turn primitive allocations, 50 | 44,024 | 40,646 | −7.7% |
| Whole-turn explicit SQL statements, 50 | 205 | 206 | +1 |
| Whole-turn evaluations, 250 | 78,488 | 74,754 | −4.8% |
| Whole-turn primitive allocations, 250 | 103,868 | 100,488 | −3.3% |
| Whole-turn explicit SQL statements, 250 | 242 | 243 | +1 |

For the subsequent nine turns, append medians are **6,162 → 2,445 evaluations**, **6,325 → 2,957 primitive allocations**, and **171 → 171 explicit SQL statements**. The prototype still performs 12 nested synchronous transactions. The histories grow during these turns; they are not repeated samples at exactly the original history length.

The counts include synchronous Schema work: the plain core itself executes **199 evaluations and 828 primitive allocations across 12 calls** on `m0` (817 allocations subsequently). It does not contain an Effect workflow, but calling Schema's synchronous decoders is not equivalent to allocating or evaluating no Effects. The counter measures the copied harness's Effect primitive constructors, not every JavaScript allocation or allocated byte; SQL statement/fragment objects created with `Object.create` are outside that allocation counter.

The existing owner-transaction shell, facade and alarm enrollment account for 869 + 462 + 312 = **1,643 of the remaining 2,445 stage evaluations**. The new SQL core accounts for 199; reference preparation, hashing, owned reads and the append wrapper account for the rest.

**Statements removed: none.** The extra initial query checks whether the work-index tables exist in the new core's separate cache. It disappears after that cache warms. `RETURNING` validation and bounded reads remain. `SqlStorage.exec` does not expose workerd's implicit savepoint statements; the extra 12 `transactionSync` calls are counted separately, and their CPU cost is included in the deployed comparison.

## Deployed CPU

CPU figures below are from deployed Cloudflare invocation telemetry. Values are **median [Q1–Q3]**. The paired tables use 160 complete main/prototype/control triples; 131 additional validated samples remain in the raw files but cannot form complete triples. `n` counts matched operations per role. Fresh and compaction calls are both shown.

| Seed records | Warm operation | n | Main CPU ms | Prototype CPU ms | Identical-code control CPU ms |
|---:|---|---:|---:|---:|---:|
| 10 | Fresh | 17 | 185.0 [122.0–244.0] | 209.0 [131.0–231.0] | 196.0 [177.0–213.0] |
| 10 | Compaction | 15 | 348.0 [223.5–456.0] | 232.0 [203.5–357.0] | 400.0 [336.0–452.5] |
| 1,000 | Fresh | 29 | 147.0 [110.0–216.0] | 150.0 [120.0–188.0] | 222.0 [146.0–258.0] |
| 1,000 | Compaction | 18 | 306.5 [237.0–333.2] | 208.0 [184.5–319.2] | 323.5 [254.0–411.0] |

Paired ratios below are computed for each matching operation and then summarized; they are not ratios of the pooled medians above. Lower ratios indicate less CPU.

| Seed records | Warm operation | Prototype / main | Control / main | Meets all three blocks? |
|---:|---|---:|---:|---|
| 10 | Fresh | 0.947 [0.800–1.233] | 0.966 [0.784–1.447] | No |
| 10 | Compaction | 0.761 [0.669–0.957] | 1.179 [0.924–1.596] | No |
| 1,000 | Fresh | 0.938 [0.648–1.261] | 1.194 [0.897–1.883] | No |
| 1,000 | Compaction | 0.720 [0.577–1.076] | 0.984 [0.807–1.373] | No |

The block results show why the pooled apparent gains are insufficient. The control variation bound is the larger absolute distance of its Q1 or Q3 from 1. A result must save at least 10%, exceed that bound, and repeat in all three blocks. Only one of the 12 individual block/operation comparisons clears that bar.

| Seed records | Warm operation | Block | n | Prototype / main | Control / main | Control variation bound | Clears block bar? |
|---:|---|---:|---:|---:|---:|---:|---|
| 10 | Fresh | 1 | 8 | 0.903 | 0.781 [0.749–0.939] | 25.1% | No |
| 10 | Fresh | 2 | 2 | 1.515 | 1.182 [1.013–1.352] | 35.2% | No |
| 10 | Fresh | 3 | 7 | 0.832 | 1.391 [1.059–1.596] | 59.6% | No |
| 10 | Compaction | 1 | 7 | 0.828 | 0.893 [0.812–1.147] | 18.8% | No |
| 10 | Compaction | 2 | 1 | 1.174 | 2.146 [2.146–2.146] | 114.6% | No |
| 10 | Compaction | 3 | 7 | 0.738 | 1.276 [1.099–1.708] | 70.8% | No |
| 1,000 | Fresh | 1 | 12 | 0.880 | 0.911 [0.830–1.242] | 24.2% | No |
| 1,000 | Fresh | 2 | 8 | 0.914 | 1.874 [1.124–2.014] | 101.4% | No |
| 1,000 | Fresh | 3 | 9 | 0.995 | 1.342 [1.082–1.600] | 60.0% | No |
| 1,000 | Compaction | 1 | 6 | 0.924 | 0.857 [0.745–0.968] | 25.5% | No |
| 1,000 | Compaction | 2 | 6 | 0.733 | 1.557 [1.081–1.894] | 89.4% | No |
| 1,000 | Compaction | 3 | 6 | 0.635 | 0.978 [0.849–1.188] | 18.8% | Yes |

Client-observed wall time and DO invocation wall time use the same matched operations. The clock sampled inside the Object reads **0 [0–0] ms for every matched warm group, with maximum 0 ms**. That frozen clock is not evidence of zero execution time or a CPU measurement.

| Seed records | Warm operation | Role | Client wall ms | DO invocation wall ms |
|---:|---|---|---:|---:|
| 10 | Fresh | baseline | 508.3 [436.2–533.0] | 389.0 [365.0–438.0] |
| 10 | Fresh | candidate | 519.0 [458.5–661.3] | 445.0 [403.0–497.0] |
| 10 | Fresh | control | 502.1 [358.1–615.3] | 392.0 [317.0–497.0] |
| 10 | Compaction | baseline | 666.6 [559.5–715.4] | 558.0 [442.0–621.0] |
| 10 | Compaction | candidate | 624.7 [553.9–751.8] | 540.0 [453.5–592.0] |
| 10 | Compaction | control | 697.9 [597.4–834.2] | 661.0 [539.0–711.0] |
| 1,000 | Fresh | baseline | 518.2 [390.7–586.6] | 436.0 [347.0–460.0] |
| 1,000 | Fresh | candidate | 573.0 [470.2–673.8] | 448.0 [401.0–510.0] |
| 1,000 | Fresh | control | 546.5 [516.5–600.6] | 466.0 [409.0–512.0] |
| 1,000 | Compaction | baseline | 687.9 [555.3–770.2] | 567.0 [511.8–646.2] |
| 1,000 | Compaction | candidate | 661.2 [598.1–732.3] | 550.0 [487.5–595.0] |
| 1,000 | Compaction | control | 697.7 [654.9–812.2] | 583.0 [511.2–723.8] |

Across the primary run, 68 of 72 Objects completed their ten operations and final proof. Four request sequences hit transport errors: baseline small/large at phase 9, control small at phase 7, and candidate small while seeding. Their partial receipts remain retained. Among the 680 operations with final proofs, 46 DO CPU records and 23 matching ingress records were missing. Thus 611 samples qualified; the controller exited 1 at its strict completeness gate. Later exports after cleanup recovered no additional events for these Workers. The cause of the missing logs was not established.

Retained telemetry contains 818 successful stateless invocations, 1,517 successful DO invocations and 662 canceled alarm invocations. The 727 successful alarm invocations have CPU median 8 ms [6–11], maximum 92 ms, and are not assigned to foreground turns. No returned record reports `exceededCpu` or `exceededMemory`; maximum observed invocation CPU is 3,496 ms, below the 300,000 ms limit. All returned outcomes, failed requests and telemetry gaps remain in the raw evidence.

The CPU experiment uses the existing hosted replay workload, not the durable-bench workload above. It invokes the public Cloudflare `ThreadObject` runtime, whose processing and settlement publication reach this append implementation. It uses two scripted model calls and two tools per phase, 10 or 1,000 imported canonical seed records, and a retained approximately 400 KB context. Thus these CPU results must not be relabeled as durable-bench CPU at 50 or 250 turns.

Three deployment blocks each use a baseline, candidate and identical-code baseline control. Cohorts alternate baseline → candidate → control at each seed size. Each Object executes a five-phase initial cycle followed by a five-phase warm cycle in the same observed incarnation. Warm fresh phases are 7, 8 and 10; warm compaction phases are 6 and 9. Quartiles use linear interpolation. Paired results match block, cohort, seed size and phase; missing members never become zero-valued CPU samples. The standard harness report sums pairs of compaction phases; this task's analysis instead reports fresh and compaction invocations separately. Repeated phases within an Object are correlated, and the reported quartiles are descriptive spreads, not confidence intervals.

The comparison threshold is a reduction of at least 10% in every deployment block, exceeding the identical-code control variation. The task analysis also shows the control ratio's Q1–Q3 range, rather than relying only on its median shift.

### What can be said about the stage's CPU share

The measured removable-overhead signal for fresh warm invocations is a **5.3% / 6.2% paired median CPU reduction** at 10 / 1,000 seed records. Neither is distinguishable from the control variation, and neither repeats above the comparison bar in all blocks. The larger pooled compaction signals (23.9% / 28.0%) also fail the all-block criterion. These are descriptive observations from an incomplete sample set, not established CPU savings or the original append stage's CPU share.

This end-to-end A/B comparison seeks to estimate the CPU change caused by the conversion. It cannot identify the entire original append stage's CPU fraction: the prototype retains SQLite work, hashing, schema decoding and the asynchronous transaction shell. Dividing the turn's CPU change by the evaluation reduction would assume the very relationship this spike is testing. No such estimate is made. Cloudflare exposes invocation CPU on root spans; its custom-span clocks still stop between I/O events. See [root CPU attributes](https://developers.cloudflare.com/workers/observability/traces/spans-and-attributes/) and [the non-I/O timing limitation](https://developers.cloudflare.com/workers/observability/traces/known-limitations/).

## Append map and attribution

The count boundary matches the copied KOM-433 harness: 11 `DoThreadStore.append` calls plus the settlement's `journal.appendPrepared`. It does not include the settlement ledger's outer authority checks or its final due-queue flush. Those execute in the measured whole turn and deployed invocation. Changing attribution would not remove their cost.

```text
captured canonical batch + observed Thread tail
  → prepareCanonicalAppend: encode/hash the batch and chain tail
  → prepareAppend: capacity and record-ID bounds
  → owner transaction: fence → replay identity → expected tail → record identity
  → validate Run progress against prior canonical evidence
  → append/rotate journal range → insert batch → insert records and Run membership
  → advance Thread tail and owned views
  → canonical pointers + settlement intervals + work-index fold and header
  → optional lifecycle start-prefix retention
  → atomic maintenance enrollment: due lanes + KV state + alarm
  → due-queue SQL flush before commit
  → decode append receipt → continuation acceptance
```

The settlement takes the same append core through `DoSubmissionLedger.publish`, after validating the Submission, canonical settlement identity, claim/ownership token and producer epoch. Its authority checks remain inside the original transaction. `commitCaptured`/`publishCaptured` accept prepared progress only after append success; replay clears the cached progress as before.

This is an additive partition of the original 6,179 evaluations, using the innermost attributed call site. Inclusive function totals in the raw files overlap and must not be added.

| Call site or group | Evaluations / turn | Evaluations / append |
|---|---:|---:|
| `effect/sql/Statement.useSpan` (`withoutTransform` and related paths) | 1,068 | 89.00 |
| `effect/sql/Statement.evaluate` | 792 | 66.00 |
| `do-journal.append`, including owner-transaction shell | 869 | 72.42 |
| `do-journal.appendPrepared`, excluding attributed children | 780 | 65.00 |
| `DoThreadStore.append`, excluding attributed children | 462 | 38.50 |
| `SqlThreadWork` helpers and `workIndexChanges` | 1,007 | 83.92 |
| `RunContinuation.validateProgressAppend` and native validator wrapper | 348 | 29.00 |
| `Alarm.recordProgress` | 313 | 26.08 |
| Owned Thread reads | 238 | 19.83 |
| Archive range append and descriptor | 120 | 10.00 |
| Canonical preparation and batch digest | 143 | 11.92 |
| Settlement interval helpers | 39 | 3.25 |
| **Total** | **6,179** | **514.92** |

The due-queue `write`/`flush` functions already use plain synchronous SQL. Their directly attributed evaluation count is zero; their schema construction still allocates primitives. The 11 due-queue upserts inside this count boundary remain, and the settlement's upsert is outside it. Native reads include bounded prior-continuation/initial-evidence reads; work maintenance includes existing-entry reads and validated header/tail `RETURNING` results.

Individual `m0` append evaluations are 535, 458, eight × 542, 463, and 387 on main; the prototype is 192, 205, eight × 222, 210, and 62. The final entry is the narrower settlement append boundary. The raw files retain SQL templates, per-append counts, call-site partitions and source hashes.

## Prototype and contracts that resist conversion

The two new private files in `storage-cloudflare` implement the synchronous SQL core and copied pure progress/work rules. The core takes the real `SqlStorage` handle, returns a tagged `Result`, and maps it to typed `E` at one explicit Effect boundary. A private refusal marker unwinds `transactionSync` before becoming a failure value, so a refusal after a write rolls back. Unexpected defects remain defects. Public asynchronous APIs still return Effect with typed errors and service requirements.

The unchanged capacity preflight and the settlement ledger's claim/lease checks still return typed failures at their existing Effect boundaries. They were not folded into the new core; fencing, replay/digest conflicts, SQL refusal and progress validation inside that core use its single Result-to-Effect boundary. This is another reason not to describe the prototype as a complete one-boundary append.

The batch hash chain, replay identity, producer fencing, bounded progress evidence, work ownership, canonical accounting and persisted formats remain unchanged. The existing outer transaction invalidates owned caches on failure. Unknown tool state and original-context/history-digest recovery stay in their existing owners.

Several boundaries prevent the stronger claim that the entire stage is plain synchronous code:

- **Atomic alarm enrollment.** `SqlStorageProgress.committed` reaches `ThreadMutationGate.recordProgress`, which currently reads/writes maintenance KV and checks/sets an alarm asynchronously. Cloudflare has a synchronous `ctx.storage.kv` alternative, so KV itself is not an intrinsic blocker; the pinned `getAlarm`/`setAlarm` APIs still return Promises. Swapping the KV calls alone would not remove this boundary. Moving enrollment after commit changes the atomic wake contract. The SQL core therefore runs in a nested `transactionSync` inside the existing asynchronous source transaction. See [Cloudflare's synchronous KV, alarm and transaction APIs](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
- **Crypto.** Canonical batch hashing and work-evidence references use the existing Crypto service. Work references are prepared before the SQL core. This spike does not introduce a second SHA-256 implementation.
- **Schema.** Canonical persisted data still crosses Schema boundaries. Synchronous decoders can internally evaluate Effects; replacing them with unchecked casts would discard the validation contract.
- **Lifecycle retention and failpoints.** Optional lifecycle hooks may suspend. Existing failpoints can interrupt or evict at precise write cuts. When a custom failpoint handler is installed, the spike deliberately retains the original effectful append path. Existing failpoint tests therefore preserve their authority but do not prove every injected cut in the new core.
- **Shared adapters and observability.** The spike duplicates shared progress/work/range rules in the Cloudflare adapter and bypasses per-statement Effect tracing there. A landing would need shared pure reducers, an explicit synchronous failure-injection contract, equivalent transaction/cache proofs, and a decision about SQL tracing. It would not be a small adapter-only cleanup.

Removing the asynchronous owner transaction, enrolling alarms after commit, dropping canonical validation, or claiming stage CPU from evaluation counts were rejected because they would change the experiment's contracts or overstate its evidence. No record format or storage-layout migration was introduced.

## Correctness and measurement method

| Proof | Result |
|---|---|
| `vp -C packages/storage-cloudflare test` | 9 files, 111 tests passed |
| `vp -C packages/storage-sql test --passWithNoTests` | No runnable tests; package has type-contract tests |
| `vp run -F @yielded/agent-storage-sql check` | Passed |
| `vp -C packages/effect-agent test test/durable` | 10 files, 61 tests passed |
| Main and prototype transcript, 50 turns | `b017b487524e44a4` |
| Main and prototype transcript, 250 turns | `dcea9f30b0917245` |
| Additional ten eight-tool turns after 50 / 250 | Both revisions match `b73859cee894aca6` / `f6e15b16d798144d` |
| `vp run check` | Passed |
| `vp run build` | Passed |
| `PATH=/usr/bin:$PATH vp run ready` | Passed; uses macOS system Git for the local publication fixture |
| Deployed workload and final proofs | 68/72 Objects passed; four transport failures retained |
| Deployed CPU completeness gate | Failed: 611/720 validated samples; partial evidence only |

No new committed tests were added. The normal storage conformance cases use the new core. Existing lifecycle/progress failure coverage exercises rollback of core writes when a later transaction participant fails. The broader suites include recovery, fencing, accounting and ownership contracts. `vp env doctor` passed; its output and validation logs are retained under `checks/`. Earlier gate attempts hit local Git fixture timeouts, including failures reproduced on the untouched baseline. The final gate passed with the system Git first on `PATH`, without changing tests or their timeouts.

The original counting harness, report and discovery notes were copied into `source-harness/` before relying on the other worktree. `harness/counts.mjs` instruments copies of Effect constructors/interpreter and source bindings, plus actual `SqlStorage.exec` and `transactionSync` calls. Local workerd/Miniflare runs collect only deterministic counts and fingerprints. No local CPU profile or timing benchmark was run; timings in the preserved historical KOM-433 report are not evidence for this spike.

All timing evidence comes from deployed Alchemy resources. The upload uses `bundle: false`; measured bundles have no counting instrumentation. Every task resource is prefixed `sync-do-append`, every compared Object receives `locationHint: "wnam"`, the CPU limit is 300,000 ms, and invocation logging/head sampling are enabled at 1. Alchemy state and auth live in private mode-700 temporary directories outside the repository. The control uploads the exact baseline bytes again.

Client wall time covers request through response-body receipt. The in-Object wall timer covers operation entry through settlement, before the final operation-receipt persistence and response encoding. Workers Observability supplies invocation CPU and server wall time. Background alarms are retained in telemetry and reported separately; their CPU is not silently assigned to an individual foreground turn.

## Build identities and retained evidence

| Build | Source commit | Bundle bytes | Bundle SHA-256 |
|---|---|---:|---|
| Main and identical-code control | `8c05714de84d68961b14e5ab7a3b7d809599563f` | 1,645,601 | `a0ad376da1c509ed28baf074971e83c8b16f02e0698adf30bc8a4e7c1573f5c6` |
| Prototype | `f5a3f74f2783ce1ed6f4b09e3d41aa3adc822502` | 1,668,363 | `b6650059d385ecbdd1d26465205995f23c93f78508866a84b2f7fa5fb77e4b98` |

Both use Effect 4.0.0 with the repository patch and the same Effect build identity `0a5f6c215f0ed6be984037050462ee4fd45e295af6a20be71f8c59207b42b4cd` (SHA-256 of the ordered JSON array containing its package manifest and `dist/internal/core.js`/`effect.js`). The replay fixture SHA-256 is `65271b42f22ea6da49d879645cec3d9519c0446964420507223b009d8a7a1ab4`; the lockfile SHA-256 is `574fa0aff46d96f79a01c9f0125154c96266363804f47a0265c5cad564952abf`. [workers.json](workers.json) maps all 12 Workers and their namespace IDs to these exact builds and deployed version identities. Complete receipts are under `builds/` and each deployed role's directory.

The second run uses controller revision `452949cb57b417f3b481eb68afd9893d2bb7aa1b` to retain incomplete telemetry and continue all three blocks. It deploys the same immutable Worker bundles as the first attempt. The candidate adds 22,762 bundle bytes (1.38%), including the retained original path for custom failpoints.

Reproduce the counts with [harness/README.md](harness/README.md). The build/deploy commands are retained in the command logs and the existing [hosted harness guide](../../../../tooling/context-continuity-eval/README.md). Run `python3 harness/analyze.py .` from this directory to regenerate `analysis.json` from the retained raw files. Compressed files preserve the complete raw count reports, final Object proofs and build source maps. [compression.json](compression.json) records original and compressed hashes; Worker modules remain byte-for-byte unchanged. Earlier count-instrumentation discovery outputs are retained separately under `counts/exploratory/` and do not enter the report tables.

The first deployed attempt is retained in `deployed-attempt-1/`: one routing 404 and missing CPU invocation logs stopped the strict harness after its first block. Its three Workers/namespaces were destroyed and API-verified. The revised controller preserves missing telemetry explicitly and continues the remaining blocks, without replaying requests or fabricating CPU values; it still reports an incomplete experiment when fewer than 720 valid samples are available.

**Cleanup verified at 2026-10-08T16:25:11.064068+00:00.** Alchemy destroyed all 12 Workers across both attempts. An independent Cloudflare API check returned HTTP 404 for every Worker and found zero task-prefixed Workers or Durable Object namespaces. Both private Alchemy state/auth directories were removed. See [cleanup.json](cleanup.json), the per-run cleanup receipts, and [harness/verify-cleanup.py](harness/verify-cleanup.py).

The evidence does not establish a full synchronous append, a production-ready replacement for failpoint behavior, the absolute CPU share of append, or a CPU win at durable-bench's 50/250-turn workload. It does establish the deterministic evaluation reduction, matching transcripts, passing repository checks, and the absence of a repeatable CPU gain beyond the control variation in this hosted replay comparison. No change was merged into main, and no PR or Linear issue was created.
