# KOM-433 measurement result

## Recommendation: stop this performance prototype

The requested performance outcome is **not established**. Keep the work local; do not roll it out on these results.

- **Effect evaluations:** 37,431 → 34,736 on the first eight-tool turn after exactly 50 historical turns, a **7.20% reduction**, not 50%. The median matched-position ratio over all ten measured turns is **0.917496**: **8.25% fewer evaluations**. Primitive allocations fall **7.49%** at the median matched position.
- **Node user + system CPU:** seven alternating three-arm cohorts completed. The median paired candidate/baseline ratio is **1.007402**, or **0.74% more CPU observed**, not a demonstrated improvement. This is not evidence of a statistically established regression.
- **Control calibration:** identical baseline code has a median paired ratio of **1.005341**, but ranges from **0.845604 to 1.192382**. All slow/noisy cohorts are retained. The experiment does not support a small CPU effect, let alone a 15% saving.
- **Converted-stage CPU:** remains **unverified**. Separate stock Inspector profiles provide partial source-stack attribution, not complete per-stage process CPU. They cannot establish the 15% converted-stage target.

No production files were changed by this measurement role. No additional optimization campaign, commit, push, hosted service, model call, or deployment was performed.

## Identities and execution

Measured locally on 2026-10-08 UTC: **macOS 26.6.2 / Darwin 25.6.0**, **Apple M4 Max**, **16 logical CPUs**, **137,438,953,472 bytes RAM**.

| Identity | Value |
|---|---|
| Baseline commit | `ba5813ec33880a9063147be6e6cce94698b11725` |
| Candidate branch/base commit | Local dirty `dan/KOM-433`, same base commit |
| Candidate changed-source SHA-256 | `c1b557af834cfb57e30d74459876956089050fbcd23d8d0f31a52fe3d6eaabbf` |
| Candidate complete patch SHA-256 | `a25078d625d0d2c20d9eb15a888584224e782e115cc5cf95ac5555ed0db3f2e8` |
| Both lockfiles SHA-256 | `574fa0aff46d96f79a01c9f0125154c96266363804f47a0265c5cad564952abf` |
| Baseline production-build SHA-256 | `a3ad44235b4789408d94203bbfd8cff1295ecaa84410ee181a91694d75e678f2` |
| Candidate production-build SHA-256 | `718658c2959cd5bc8734882d6d149722543e86d2654cf3db5c4dc1b682507352` |
| Actual CPU worker | **Node `v24.16.0`**, `/Users/dan/.nvm/versions/node/v24.16.0/bin/node` |
| Effect / fixture transpiler | Effect **4.0.0**, esbuild **0.28.1** |

The already-successful builds were reused after byte verification, not rebuilt without invalidation. Both CPU inputs were freshly staged through the repository's `stageCheckout`, using each checkout's own installation. The 283 baseline and 284 candidate staged production modules match their frozen originals. Shared fixture bytes match. Framework imports resolve to published-style `dist` artifacts, not TypeScript source.

The CPU worker runtime was checked in every worker report and CPU capture, including executable-path equality across all arms. The shell shim's Node version and Bun's Node-compatibility `process.version` were **not** used as worker-runtime evidence.

See [premeasurement verification](premeasurement-verification.json), [postmeasurement verification](postmeasurement-verification.json), both CPU-stage `identity.json` files, and the unchanged [candidate freeze](../prototype-final/source-fingerprint.json).

## Deterministic work counts

The recovered corrected baseline was reused. The candidate used the **byte-identical corrected `count-probe.js`**, including nested Schema runtime evaluations. Only source selectors were adapted to renamed/plain Result helpers, preserving their logical labels. Constructor/evaluation hooks and the workload were unchanged. The candidate adaptation and site map are retained in [inputs](inputs/) and [candidate-counts-50](candidate-counts-50/).

The local workerd workload seeds 50 historical turns with the existing 1/1/0-tool cycle, closes/reopens the store, then executes ten turns with eight sequential lookup calls each. `modulesRoot: "/"` is used on both sides; a fresh store uses `/setup`, and `/wake` follows populated-store reopen. The first measured position has 50 historical turns; later positions have growing history. Compare matching positions, not the first turn with later warm positions.

### First measured position: exclusive counts

| Stage | Evaluations base → candidate | Primitive allocations base → candidate | Stage calls base → candidate |
|---|---:|---:|---:|
| Admission | 693 → 696 | 719 → 719 | 1 → 1 |
| Ownership acquisition | 537 → 540 | 501 → 503 | 2 → 2 |
| Context assembly | 9,976 → 7,975 | 12,412 → 10,414 | 2 → 2 |
| Model-response commit | 926 → 758 | 1,950 → 1,771 | 9 → 9 |
| Tool-settlement commit | 1,040 → 960 | 1,116 → 1,050 | 8 → 8 |
| Continuation preparation | 2,062 → 1,749 | 3,424 → 2,933 | 20 → 20 |
| Durable Object append | 6,179 → 6,121 | 6,348 → 6,290 | 12 → 12 |
| Settlement | 869 → 867 | 937 → 935 | 1 → 1 |
| Unattributed remainder | 15,149 → 15,070 | 16,619 → 16,379 | — |
| **Whole turn** | **37,431 → 34,736** | **44,026 → 40,994** | **430 → 384 selected-site entries** |

Whole-turn selected-site entries are not the sum of stage-boundary calls: they also count the retained helper inventory. The ownership count includes the later empty-lane claim attempt. Every measured turn retains **189 Async evaluations** on both sides. Inline successes are counted separately: first-position **2,303 → 2,294**.

Across ten matched positions, evaluation ratios range **0.909150–0.928001**; the median is **0.917496**. Excluding the first position, the nine-position median ratio is **0.916452**. Allocation median ratios are **0.925131** for ten positions and **0.924574** for the nine warm positions.

All exclusive evaluation/allocation/inline-success buckets close to whole-turn totals, every selected site closes, and there is no invalid `null` stage. Primitive allocation counts cover the documented Effect runtime constructors, not all heap objects, all Effect-like values, or GC bytes. Calls are the selected boundary/helper inventory, not every JavaScript call.

**Nesting:** context consists of initial history selection plus projection, not one continuous span. Model/tool commits enclose continuation preparation and append; readonly responses can prepare before a later combined response/results append. Settlement also nests preparation and append. Stage calls deduplicate nesting in the same stage. Inclusive tables and nesting edges remain in the raw reports; **do not add inclusive stages**. The exclusive table above is additive only for the three work counters. Approximately 40% of first-position baseline evaluations remain outside the selected stage boundaries.

Fingerprints match baseline/candidate at both observed boundaries:

- Historical 50-turn seed: `b017b487524e44a4`.
- After ten measured turns: `b73859cee894aca6`.

These are the existing normalized **last-provider-request** fingerprints, excluding system messages and the subsequently emitted final answer. They are not a full canonical archive digest or a fresh cross-target pi comparison.

Raw data: [counts-comparison.json](counts-comparison.json), [candidate report](candidate-counts-50/report.json), and [corrected baseline report](../baseline/counts-50-corrected-attribution/report.json). Instrumentation elapsed times are not production speed measurements.

## Resident Node process CPU

All **21 workers passed**. Each acquired a fresh resident file-backed SQLite host/database, completed **500 warmups**, then ran **1,000 checked operations** on fresh Threads/Submissions. Each measured worker recorded **5,000 model calls/finalizers**, **4,000 tool calls/finalizers**, and **1,000 canonical completions**, with the stock output, tool-order, and prior-result checks intact.

The native scripted provider uses `Stream.make`, four sequential immediate tools and five provider requests per operation, 32-byte inputs/results/output, and no inference/network or synthetic delay. This is the requested `sqlite-tool-rounds-4` workload, not the eight-tool long-history DO count workload.

`process.cpuUsage()` snapshots surround the stock continuous measured loop. Startup, imports, host acquisition, all warmups, report serialization/writes, and host disposal are excluded. Inline checks, canonical completion reads, run-owned finalizers, and same-process host background work inside the loop are included. The stock profiling-mode branch selects the loop without per-operation clocks; only its **fixture-side profiler adapter** is replaced with two process-CPU snapshots. No production stage/count hooks or Inspector run in these cohorts. User and system CPU are retained separately; wall time is not CPU. Process CPU may exceed wall time because it includes process threads.

### Raw cohort totals

CPU values below are **user + system seconds per 1,000 checked operations**, rounded for display. Exact microseconds, each component, warmup/operation reports, worker options, logs and host snapshots are retained in [cpu-cohorts-01](cpu-cohorts-01/). `P` is the prototype; `C` is identical baseline code in a separate fresh worker/database.

| Cohort | Order | Baseline CPU s | Prototype CPU s | Control CPU s | P/B | C/B |
|---:|---|---:|---:|---:|---:|---:|
| 1 | B P C | 20.087752 | 20.026287 | 17.183614 | 0.996940 | 0.855427 |
| 2 | C P B | 17.614884 | 18.215409 | 20.347057 | 1.034092 | 1.155106 |
| 3 | P C B | 20.562634 | 20.272317 | 17.387838 | 0.985881 | 0.845604 |
| 4 | B C P | 17.069512 | 19.766818 | 20.353372 | 1.158019 | 1.192382 |
| 5 | C B P | 17.081207 | 20.021289 | 20.362395 | 1.172124 | 1.192093 |
| 6 | P B C | 17.230495 | 17.195465 | 17.286683 | 0.997967 | 1.003261 |
| 7 | B P C | 17.098791 | 17.225362 | 17.190111 | 1.007402 | 1.005341 |

| Paired ratio | Median | Q1–Q3 | Min–max |
|---|---:|---:|---:|
| Prototype / baseline | **1.007402** | 0.997454–1.096055 | 0.985881–1.172124 |
| Identical control / baseline | **1.005341** | 0.929344–1.173600 | 0.845604–1.192382 |

The primary summary is the **median of within-cohort ratios**, not the ratio of medians. Raw per-arm CPU medians are 17.230495 s baseline, 19.766818 s prototype, and 17.387838 s control; their markedly different shapes are visible in the full table. Quartiles use inclusive interpolation and are descriptive, not confidence intervals. No cohorts were discarded, corrected using the control, or selected after inspection. There were no failed or partial workers in this comparison.

All builds, staging and count runs finished before CPU capture. Workers and cohorts were serial and supervised to completion. A 15-second quiet interval preceded the lane. No other session build/test/benchmark ran during it. Other users' work was not stopped: snapshots show a VM, terminals, background agents and system services. Whole-host busy fractions during capture were **10.60%–18.15%**, including the benchmark. Pre-lane load averages were 2.68/5.32/6.44. The bimodal CPU/control spread has **no established cause**. This host was not an isolated CI runner.

See [cpu-summary.json](cpu-summary.json), [raw cohorts](cpu-cohorts-01/cohorts.json), and [host-before.txt](host-before.txt).

## Separate diagnostic profiles: partial attribution only

The unmodified stock Inspector adapter ran afterward, once per frozen original production stage, with the same 500/1,000 resident workload and all checks passing. Actual runtime remained Node `v24.16.0`; requested sampling interval was 1 ms. Baseline retained 12,420 samples and candidate 12,711.

The analyzer maps generated-code function regions and observed synchronous stack ancestry. It assigns the closest recognized primary region exclusively, keeps shared/combined work separate, and retains an inclusive table without summing it. Ownership uses function headers only, avoiding false attribution of returned session methods to acquisition.

The following are **sample-weighted observed residence milliseconds**, not per-stage user+system CPU and not comparable production timings:

| Source-stack region | Baseline sampled ms | Candidate sampled ms |
|---|---:|---:|
| Admission | 13.500 | 32.793 |
| Ownership acquisition | 16.377 | 16.373 |
| Context assembly | 31.126 | 15.042 |
| Model-response commit | 181.868 | 320.459 |
| Tool-settlement commit | 38.667 | 35.752 |
| Continuation preparation | 760.508 | 933.903 |
| Settlement | 56.333 | 54.790 |
| Durable Object append | **Not exercised** | **Not exercised** |
| Shared/combined commit | 63.249 | 79.794 |
| Node SQLite append, not DO | 40.082 | 50.708 |
| SQLite driver without stage ancestry | 6,750.043 | 6,997.419 |
| Otherwise unattributed | 9,111.916 | 9,112.621 |

Shared usage/capture/JSON, GC, idle and Inspector overhead are separately retained in [profile-attribution.json](profile-attribution.json). All exclusive sample counts and delta weights close to the raw profile totals. The native `run`/`all` hot frames were inspected against the SQLite driver's `StatementSync.run/all` call sites; they are not all charged to append.

**Limits:** 83.48%/82.77% of observed profile deltas are otherwise unattributed or driver work lacking stage ancestry, before the additional shared/GC buckets. Async ancestry is absent, some phase samples are sparse, and plain-helper conversion changes stack visibility. There is only one diagnostic capture per revision. Main-thread samples do not apportion all process threads or user/system CPU. These profiles cannot establish an exact converted-stage CPU reduction. In particular, do not scale workerd count shares into Node CPU or interpret the sample table as a stage speedup/regression.

Raw captures, worker checks, options and logs are in [profiles-01](profiles-01/); generated source-region hashes and the analysis script are retained.

## Reproduction and retained inputs

The existing task helper root is `/tmp/kom433-spike-01a119d5-baseline.bcZT4R`. The preserved baseline checkout, its own installation and build remain under `baseline-checkout`. All commands below must run serially; use fresh output paths, and keep all other session builds/tests/benchmarks idle during CPU work. Helpers refuse existing output directories. The retained stages contain external dependency links to their respective checkouts; restoring elsewhere requires the source archive/patch, own-lockfile installations and production builds described by the baseline receipt.

Set paths once:

```sh
TASK=/tmp/kom433-spike-01a119d5-baseline.bcZT4R
ROOT=/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433
EV="$ROOT/examples/durable-bench/results/kom433/spike-01a119d5"
OUT="$EV/measurement-01a11a40"
cd "$TASK"
```

The missing candidate counts were obtained with `vp run counts -- "$ROOT" "$TASK/measurement-01a11a40/candidate-counts-50" counted`. The already-correct baseline was not repeated. Repeating counts requires a new output path; the current runner chooses only the documented selector adaptations when the private usage implementation exists.

The production CPU stages were prepared before timing with:

```sh
vp run stage -- "$TASK/baseline-checkout" "$OUT/base-cpu-stage" "$TASK/baseline-checkout" cpu base
vp run stage -- "$ROOT" "$OUT/candidate-cpu-stage" "$TASK/baseline-checkout" cpu head
```

Those destinations now exist: reuse them for an exact artifact comparison rather than overwriting them. The completed seven-cohort command was:

```sh
vp run --no-cache cpu-cohorts -- "$OUT/base-cpu-stage" "$OUT/candidate-cpu-stage" "$OUT/cpu-cohorts-01"
```

To repeat, use a new final directory, for example `$OUT/cpu-cohorts-repeat-01`. The controller checks actual worker version/executable equality, fixture equality and every required operation count. A different Node version creates a new environment, not a direct reproduction of these `v24.16.0` results.

The stock diagnostic commands were:

```sh
vp run --no-cache cpu-worker -- "$EV/baseline/production-stage" "$OUT/profiles-01/base.options.json"
vp run --no-cache cpu-worker -- "$EV/prototype-final/production-stage" "$OUT/profiles-01/candidate.options.json"
vp run --no-cache profile-report -- "$EV/baseline/production-stage" "$EV/prototype-final/production-stage" "$OUT/profiles-01" "$OUT/profile-attribution.json"
```

Copy the two options files and change their output/profile paths before repeating, so existing captures are not replaced. For analysis elsewhere, `inputs/package-profiles.json` contains the helper tasks including `profile-report`; `inputs/package.json` is the exact pre-CPU task manifest. The CPU/count/adapter/selector scripts and their input hashes are retained in `inputs/` and `premeasurement-verification.json`. Baseline startup failures and the discarded invalid-attribution attempt remain in the earlier evidence bundle; no failed attempt was turned into a passing cohort.

## Remaining requirements and handoff

- The **50% per-step evaluation target is not met** on this workload. The supported count improvement is limited to the reported matched positions.
- The **15% converted-stage Node CPU target is unverified**; there is no demonstrated whole-operation CPU gain. Exact stage CPU, Node-side deterministic stage counts, and DO CPU attribution are not established by these profiles.
- Fresh exact pi transcript equality at **50, 250, 1,000 and 3,500 turns**, both **`long-thread-aging-256-131328`** and **`long-thread-store-size`**, and informational 50/250-turn pi-durable 1.0.4/tardie 0.44.0 comparisons were **not run by this role** and remain required for any overall acceptance claim.
- Append caller-mutation/laziness and schema-valid false-continuation-accounting negative proof remain unverified. General host fairness and high-cardinality public-summary performance remain unmeasured.
- The successful **`VITEST_MAX_WORKERS=1 vp run ready`** on this exact frozen candidate is reused. Its source fingerprint and retained log hash were rechecked after measurement; no production/dependency/configuration change invalidated it. Tests, timeouts and production source were not changed, and the gate was not rerun during CPU capture.

All owned measurement commands completed, all 21 CPU and two profile workers exited successfully, post-measurement source/build verification passed, `git diff --check` passed, and no owned worker processes remained. The performance hypothesis did not justify rollout; stopping is the bounded recommendation, not a merge or a claim that all durability acceptance is complete.
