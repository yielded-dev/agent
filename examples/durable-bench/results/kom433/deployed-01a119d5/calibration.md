# KOM-433: deployed bookkeeping calibration

**Inconclusive: `answered=false`, `proceed=false`. The 15% hypothesis is not refuted.** Six uninstrumented CPU samples were uniquely correlated, but the byte-identical control used **60.8–67.0% less CPU than baseline**. This overwhelms the intended 15% converted-stage signal. The frozen pilot gate stopped expansion; the planned 12 matched confirmation cohorts and append experiment were **not run**. All five owned Workers/namespaces are independently confirmed absent.

## Complete stage and fixed experiment

The [hosted map](hosted-map.md) was consulted first. It observed five `Usage.summarizeModelUsage` calls and 120 generator success-path yields in a warm operation. `sumRunTotals` received no contributions, so it was rejected as unrepresentative.

Calibration compares the **complete `summarizeModelUsage` stage** (`Usage.ts:371–571`): numeric/overflow checks, optional-seed validation, model/pricing grouping, coverage and Schema output construction. The baseline calls the unmodified production Effect implementation; the temporary synchronous core preserves that entire work. Five frozen profiles, derived from mapped canonical usage, have call cardinalities **[1,1,2,1,1]**. This is a bookkeeping calibration, not a partial append speedup claim.

The [plan](calibration/plan.json) and [freeze receipt](calibration/frozen.json) predate upload:

- Warm **both forms 2,000 times per Object**; each measured RPC executes **5,000 or 20,000 complete summaries**. Outputs feed a checksum; complete outputs, optional-seed behavior and unchanged inputs are verified outside measurement. Each form has **one root fiber per RPC**, not `runSync`/fiber creation per iteration.
- Control uploads the baseline bundle unchanged. Counted bundles are separate; CPU builds reject `__kom433` hooks.
- Pilot: one matched cohort, three roles, six CPU RPCs. Required all correlations, every 20,000-call RPC ≥20 ms, and baseline/control difference ≤`max(2 ms, 10% of baseline)` at each size.
- Conditional confirmation: **three deployment rounds × four matched cohorts**, rotated role/deployment ordering and alternating repetition order. The target was ≥15% improvement against both baseline/control in every round, exceeding declared control/resolution noise. A five-summary saving below **0.05 ms** was the scoped cheap-bookkeeping stop threshold. None of these thresholds was changed after observation.

## Actual invocation CPU and unresolved spread

These are Cloudflare **Durable Object RPC `cpuTimeMs`**, not client time, ingress CPU, or instrumented timing. Every pilot sample has a unique Object/method, correlated ingress trace, expected version, successful outcome and `truncated=false`; exports passed completeness and ABR checks.

| Repetitions | Effect baseline | Synchronous | Identical-code control | Control / baseline | Sync / control |
| ----------- | --------------: | ----------: | ---------------------: | -----------------: | -------------: |
| 5,000       |          361 ms |       99 ms |                 119 ms |              0.330 |          0.832 |
| 20,000      |        1,046 ms |      364 ms |                 410 ms |              0.392 |          0.888 |

The same-code spread is **242 / 636 ms**; the candidate-to-control gap is only **20 / 46 ms**. All samples comfortably exceed timer resolution, but normalized small/large CPU ratios are **1.380 baseline / 1.088 candidate / 1.161 control**. Even baseline repetition scaling is unsettled. One pilot cohort cannot establish a population spread or attribute these differences to placement, JIT state, GC or another host effect.

Consequently, neither a causal speedup nor “bookkeeping is cheap” is established. The remaining instrumentation gap is **uncontrolled cross-Worker CPU variability despite identical code and warming**. A better-controlled, newly predeclared lane is needed before testing the 15% target or using calibration to justify append work. [Raw samples](calibration/pilot/samples.json), [failed pilot gate](calibration/pilot-gate.json), and [analysis](calibration/analysis.json) retain every observation.

## Count denominator—not a transferable CPU price

Separate deployed bundles counted the **same repetition sizes**, including scheduler and nested Schema work. Exclusive partitions closed; all fibers completed; no attribution diagnostics occurred.

| Repetitions | Effect evaluations | Sync evaluations | Removed evaluations | Effect / sync selected allocations |
| ----------- | -----------------: | ---------------: | ------------------: | ---------------------------------: |
| 5,000       |             15,024 |                2 |              15,022 |                   200,010 / 65,002 |
| 20,000      |             60,090 |                2 |              60,088 |                  800,032 / 260,002 |

Naively dividing baseline-minus-candidate CPU by these denominators gives **17.44 / 11.35 μs per removed evaluation**; using the identical-code control gives **1.33 / 0.766 μs**. **Neither coefficient is accepted.** The disagreement is the control problem, and conversion also removes allocations/success-path work. Any valid coefficient would remain workload-specific, not a CPU share or an append forecast. [Raw denominators](calibration/denominators.json) and both complete count files are retained under `calibration/counts/`.

All five Objects passed full-output equality, consumed-checksum, unchanged-input and same-incarnation checks. Input/output digests matched across forms and deployments. The counted phase retained one missing candidate-large ingress log and exited 1; all four count/output observations succeeded. The uninstrumented pilot had **6/6** complete correlations and exited 0 before the separate control decision rejected expansion.

## Identity, limits and retirement

Baseline remained **`8c05714de84d68961b14e5ab7a3b7d809599563f`**; private fixture snapshot is **`67e2ae7417a3777a4b3be090c26782a981da0888`**. Protected `58359ea2` stayed unchanged. Exact source, fixture, lockfile and bundle receipts are under [calibration/source](calibration/source/):

- Uninstrumented Effect/control bundle: `4cee36edcebe452b1a6efd4535725dd4cc50dca3a44786c3af95981ffc429394`.
- Uninstrumented synchronous bundle: `4be2b0f2f2c3590c489ce4c8dbe0de93970fcb5b1cc071b6b24757013a618bb7`.
- Effect **4.0.0**, Alchemy **2.0.0-beta.80**, compatibility **2026-08-01 / nodejs_compat**; production exports. CPU builds contain no counting hooks.

| Worker suffix after `effect-agent-cpu-`   | Deployed version                       |
| ----------------------------------------- | -------------------------------------- |
| `01a119d543300005-b0-baseline` (counted)  | `fd7ba11b-5a39-4ffc-be3c-68999e582e87` |
| `01a119d543300005-b0-candidate` (counted) | `8a12d3ed-4b76-4381-a1e8-078256e8c989` |
| `01a119d543300006-b0-baseline`            | `2fc4ae85-dee1-46d4-ab3d-1baeb002431f` |
| `01a119d543300006-b0-candidate`           | `d7fa971e-0cb7-46d2-aab6-563cc6c731b7` |
| `01a119d543300006-b0-control`             | `79ec3b75-11e4-4968-bee1-fc15e0d33299` |

The configured account matched SHA-256 **`3e8f88a45f1480a607b6192f4f156c5630072d175f6d49cc61160be5384837bd`** through the required project `direnv` context. Logging settings were independently read back as enabled, persisted, invocation-enabled and sampled at 1.

Actual workload: **5 Objects / 10 counted-or-measured RPCs**, plus setup/audits; one active operation. Inner ceiling **20,000**, warmup **2,000/form**, input ceiling **16 KiB**, CPU guard **60 seconds/RPC**, dispatch deadline **10 minutes/phase**. Actual seed-to-last-operation spans were **61.043 / 6.118 seconds**. There were **zero SQL statements/application record writes and zero model API calls**; each disposable Object reported 4,096 database bytes. These workload limits are not a billing cap.

Ownership preceded uploads; no uncertain RPC was retried. [Counted](calibration/counts/cleanup.json) and [pilot](calibration/pilot/cleanup.json) retirement receipts record all targets complete and private state removed. The [independent check at 17:34:32Z](calibration/independent-cleanup.json) confirmed five Worker 404s and absence of all five exact namespace IDs/script associations across the complete two-page listing. **No live stack or background job remains.**

[Exact build, hosted-context and cleanup commands](calibration/commands/recorded-commands.txt), static-check logs, source patch and complete build archives are retained. No local runtime benchmark, test/`ready` suite, paid model call, PR, push, merge or tracker message occurred.
