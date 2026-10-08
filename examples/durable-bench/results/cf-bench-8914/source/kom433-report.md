# KOM-433: durable pipeline flattening spike

## Recommendation

**Stop this broad rewrite as a performance rollout. Retain the prototype and evidence for inspection.**

The prototype reduced Effect evaluations by **8.25%** and counted primitive allocations by **7.49%** at median matched positions. The requested **50% evaluation reduction was not reached**. Seven alternating baseline/prototype/identical-control cohorts showed **no demonstrated whole-operation CPU improvement**. Exact converted-stage user-plus-system CPU remains **unverified**, so the 15% converted-stage target is unresolved.

All requested correctness checks passed, including fresh pi transcript fingerprints at four sizes, both long-thread diagnostics, and the full repository handoff gate. These correctness results support the experimental implementation; the performance evidence supplies no basis for adopting the broad refactor.

## What was prototyped

The local patch changes 11 files, with 2,281 additions and 1,739 deletions, including the move of usage implementation behind its existing public module.

```text
Public Effect / Stream
    → bounded synchronous record, usage, projection and continuation work
    → tagged Result failures translated at Effect boundaries
    → existing Crypto, storage, ownership, Scope and commit operations
```

- Usage arithmetic and aggregation use private synchronous helpers. Public APIs retain typed Effects and yield every 64 inputs.
- Journal construction and projection share plain reducers. Projection processes at most eight records per Effect portion.
- Continuation preparation and canonical verification share the fact reducer. Capacity checks and byte convergence run as plain code; evidence acquisition and hashing retain Effect boundaries.
- Record/append capture returns expected refusals as values and retains owned, frozen canonical bytes. Canonical JSON traversal was already synchronous on the baseline.
- Durable Object append preflight combines synchronous checks. Transactions, fencing, claims, leases, failpoints, post-commit acceptance and conflict re-preparation remain intact.
- A one-line benchmark configuration repair supplies Miniflare's explicit module root for absolute module paths.

Public exports, canonical record formats, storage layouts and the dependency lockfile remain unchanged. The patch adds no data cache, framework package, committed test, or reporting infrastructure.

### Code-shape cost and constraints

| Area | Cost or boundary that remains |
|---|---|
| Usage | Explicit failure guards and carried aggregates replace compact per-field Effect composition. Public unrestricted inputs require cooperative batching. |
| Journal projection | A shared mutable reducer and bounded portions preserve streaming behavior, ordering and compaction semantics. |
| Continuations | Acquisition/orchestration and pure reduction are separate. Recalculation, capacity convergence and post-commit acceptance remain necessary. |
| Capture / canonical JSON | Result plumbing makes expected failures explicit; serialization and owned capture were already synchronous. |
| Crypto / storage / ownership | Service requirements, real suspension, transactional authority and resource lifetime retain their existing Effect boundaries. |
| Schema codecs | `decodeResult` and `encodeResult` retain internal Effect parsing and synchronous-runner paths. Their work remains in the counters. |
| Observation | Existing enclosing spans and failpoints remain. Per-stage CPU attribution across asynchronous work remains incomplete. |

The review exposed an unrestricted public aggregation loop. A failure-first task-local probe showed 4,096 inputs consumed before interruption. The corrected source and frozen production build stop at 64 reads, accept interruption, and join finalization. Accounting, seed immutability and typed overflow results were preserved. See [review dispositions](prototype-final/review-dispositions.json).

## Matched work counts

The local Durable Object fixture seeds 50 historical turns, reopens the store, and executes ten eight-tool turns. Later measured positions contain growing history; every comparison uses matching positions. All selected sites close, and exclusive work counters sum to the whole-turn totals.

First measured position, after exactly 50 historical turns:

| Stage | Evaluations: base → prototype | Primitive allocations: base → prototype | Stage entries: base → prototype |
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
| **Whole turn** | **37,431 → 34,736** | **44,026 → 40,994** | **430 → 384 selected helper/boundary entries** |

The first-position evaluation reduction is **7.20%**. Across all ten positions, the median paired evaluation ratio is **0.917496**, and allocation ratio is **0.925131**. Across the nine later warm positions, they are **0.916452** and **0.924574**. Both versions retain 189 Async evaluations per turn. Counts describe selected Effect constructors and call sites; general heap allocation and every JavaScript call are outside this inventory.

Context assembly supplies **2,001 of the 2,695** removed first-position evaluations, about **74%**. The resident Node CPU workload starts fresh Threads, so it exercises a different history-cost mix.

Exact process CPU for each of the eight stages is unverified. The separate Inspector capture provides partial source-stack residence data, with roughly 83% of observed deltas in unattributed or driver work without stage ancestry. Node SQLite does not execute Durable Object append. Full tables, inclusive/exclusive nesting and attribution limits are in the [measurement report](measurement-01a11a40/report.md).

## Node process CPU

Measured on an Apple M4 Max, macOS/Darwin arm64, Node **24.16.0**, Effect **4.0.0**. Each of 21 separate workers performed **500 warmups** and **1,000 checked `sqlite-tool-rounds-4` operations**. Production builds and identical fixture bytes were staged with `stageCheckout`; the timing lane used two `process.cpuUsage()` snapshots around the resident measured loop. Builds, counter instrumentation and Inspector captures were outside that lane.

All workers passed their 5,000 model-call/finalizer, 4,000 tool-call/finalizer, and 1,000 canonical-completion checks. All samples were retained.

| Within-cohort ratio | Median | Q1–Q3 | Minimum–maximum |
|---|---:|---:|---:|
| Prototype / baseline | **1.007402** | 0.997454–1.096055 | 0.985881–1.172124 |
| Identical control / baseline | **1.005341** | 0.929344–1.173600 | 0.845604–1.192382 |

The observed prototype difference is **+0.74%**. The control's broad spread prevents a small-effect conclusion or an established regression claim. Host busy fractions were 10.60%–18.15%, including the benchmark; the source of the bimodality remains unknown. These data establish no CPU saving. The partial profiles also leave the requested **15% converted-stage CPU** criterion unverified.

The parent independently rechecked every worker, all paired ratios, counter closure, fixture fingerprints and source identities: [measurement verification](parent-checks/measurement-verification.json).

## Correctness and informational comparison

| Required check | Final result |
|---|---|
| Fresh pi fingerprint equality: 50 / 250 / 1,000 / 3,500 turns | **Passed** at all four sizes. |
| `long-thread-aging-256-131328` | **Passed**, 10 measured samples per revision, plus warmups. |
| `long-thread-store-size` | **Passed**, 10 measured samples per revision, plus warmups. |
| Full handoff gate | **Passed:** `VITEST_MAX_WORKERS=1 vp run ready`; unchanged tests and timeouts, with ordinary task-cache reuse. |

The exact normalized last-provider-request fingerprints are `b017b487524e44a4`, `dcea9f30b0917245`, `ac520308146f2a8f`, and `0a8c8e4b0d9a0794`. Their scope excludes system messages and the final answer emitted afterward. See [fresh fingerprint proof](verification-20261008/fingerprint-proof.json).

The original diagnostic attempt stopped at the clean-checkout guard. The parent created a separate local validation commit from the byte-matched frozen patch and reran the unchanged diagnostic command. Both checkouts were genuinely clean; the user worktree and its branch were untouched. All four worker batches and all **56 samples including warmups** passed. Every recovery shape used eight selected requests returning 15 records / 13,100 bytes; the largest page contained five records / 3,166 bytes. Original mutations remained one, changed-handler calls remained zero, and whole-Thread reads/exports/observations remained zero.

The successful [long-thread report](long-thread-clean/diagnostics/report.md), [raw data](long-thread-clean/diagnostics/report.json), and [independent verification](long-thread-clean/verification.json) supersede the clean-checkout blocker in the earlier [verification receipt](verification.md).

Informational local Miniflare wall-time medians, milliseconds:

| Target | 50 cold | 50 warm | 250 cold | 250 warm |
|---|---:|---:|---:|---:|
| pi-durable 1.0.4 | 56.9 | 23.6 | 63.1 | 41.5 |
| tardie 0.44.0 | 89.7 | 33.7 | 137.6 | 37.8 |
| Prototype | 103.7 | 45.4 | 133.3 | 68.9 |

All 18 samples / 180 turns completed. Thirteen invocations exceeded the harness's soft idle threshold. These elapsed values support neither a CPU claim nor a statistically established ranking. [Raw values and spread](verification-20261008/durable-summary.json) retain slow samples and host observations.

## Decision and possible follow-up

**Adopt no additional synchronous-stage conversions from this spike.** The measured reduction and code-shape cost do not justify this broad patch as a performance change.

If performance work resumes, use this order:

1. Resolve the identical-code CPU variability and obtain stage attribution capable of following asynchronous ownership. Establish sensitivity before another rewrite.
2. Investigate the SQLite work visible in native driver frames, with a concrete statement/work budget and the existing fencing and recovery checks preserved.
3. Measure context projection with representative retained history in the CPU workload; this is where most counted savings occurred.
4. Consider a narrowly scoped conversion only after a demonstrated CPU benefit. Keep its public interruption, typed-error and Scope behavior explicit.

The prototype keeps Crypto, storage, authority and resource operations in Effect. Result-codec substitutions preserve internal parser work. New caches and record/layout changes were outside the experiment.

## Limits and retained state

- Exact per-stage user-plus-system CPU remains unverified. The 15% converted-stage target is unresolved; the 50% evaluation target was missed.
- General host fairness and high-cardinality public-summary performance were not measured. Append caller-mutation and schema-valid false-continuation-accounting negative probes were not added; existing contract, recovery, fingerprint and long-thread proof passed.
- The working branch remains **`dan/KOM-433`**, with an uncommitted experimental patch. No source publication, merge, deployment, paid model call, or tracker update occurred.
- The clean validation commit **`4b4931979b689ea318987b3fc77eae24a86ab470`** exists only as a disposable-checkout snapshot and the retained [Git bundle](long-thread-clean/prototype-validation.bundle), based on **`ba5813ec33880a9063147be6e6cce94698b11725`**.
- The final patch SHA-256 is **`a25078d625d0d2c20d9eb15a888584224e782e115cc5cf95ac5555ed0db3f2e8`**; changed-source fingerprint is **`c1b557af834cfb57e30d74459876956089050fbcd23d8d0f31a52fe3d6eaabbf`**. See the [patch](prototype-final/prototype-final.patch) and [source manifest](prototype-final/source-fingerprint.json).

The workflow completed implementation, reviews, measurement and most verification. Session transport failures interrupted its report writer; the parent completed this report and the clean-checkout diagnostics from preserved evidence. Replay and temporary-resource retirement are recorded in [cleanup.json](cleanup.json). Retained production stages preserve measured code; replay requires fresh own-lockfile installations and staging as documented in the measurement and verification receipts.
