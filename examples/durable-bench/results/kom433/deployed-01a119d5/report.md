# Deployed Cloudflare spike: findings and remaining question

**The performance question remains open.** Deployed counting is now sufficiently attributed to choose a target. The two deployed usage calibrations failed their measurement-quality gates. A complete append candidate was not implemented under the bounded small-adapter scope, so append CPU performance remains unmeasured.

This report replaces the earlier local experiment's broad stop inference. The current evidence supports neither a general rejection of flattening nor a rollout recommendation.

## What was learned

### The cost map is substantially complete

Thirty operations ran on real Cloudflare through Alchemy-managed deployments. Warm count triples reproduced exactly across three Objects. Source-backed attribution explains **33,915 of 33,930** formerly unattributed warm evaluations: **99.956%**. Three host-prelude evaluations per operation remain unresolved.

| Ownership of the former remainder | Evaluations |
|---|---:|
| Interpreter/model/tool control | 10,084 |
| Attempt preparation/progress/control | 6,790 |
| Ownership/ledger operations | 6,169 |
| Nested Schema runtime | 4,147 |
| Storage/cache/SQL | 4,057 |
| Commit/continuation/context pre/post-body work | 943 |
| Native AI stream/tool machinery | 625 |
| Fixture work | 510 |
| Scope/fiber/host machinery | 445 |
| Digests | 145 |
| Unresolved host prelude | 15 |

The strongest narrow target is complete public `DoThreadStore.append`. Its optimistic whole-RPC **evaluation-removal ceiling is 14–15%**, retaining observed Schema, digest, SQL/transaction and nested-read work. That bound describes counted work; it is not a CPU forecast. No selected narrow stage supports the earlier 50% whole-operation evaluation target.

All count partitions closed, counted fibers completed, retained output/context checks passed, and the three Objects produced matching normalized final prompts. Telemetry correlation was incomplete for the instrumented mapping runs, so their CPU values supply no timing evidence. [Full map and raw inputs](hosted-map.md).

### The first deployed calibration failed its control

The complete Effect and synchronous `summarizeModelUsage` implementations used identical frozen inputs, complete output checks and consumed checksums. Separate counting bundles retained the exact work denominators.

| Summaries per RPC | Effect baseline CPU | Synchronous CPU | Identical-code control CPU |
|---|---:|---:|---:|
| 5,000 | 361 ms | 99 ms | 119 ms |
| 20,000 | 1,046 ms | 364 ms | 410 ms |

All six uninstrumented DO invocation CPU observations were strictly correlated. The identical-code control differed from baseline by **60.8–67.0%**, overwhelming the proposed 15% signal. Source inspection found matching implementations, inputs and declared configuration. Different ingress locations do not establish Object placement or explain the cause. Setup also omitted warming the actual measured batch wrappers.

A universal CPU price per Effect evaluation cannot be derived from this result. [Calibration](calibration.md), [source audit](control-source-audit.md).

### The within-Object crossover also failed stability

One identical dual-implementation bundle served two Workers, one Object each. Both exact batch paths were warmed twice at 20,000 repetitions per form per Object. Eight serial measured RPCs retained complete outputs, checksum, input immutability and the same observed incarnation.

| Object / fixed order | Cloudflare DO CPU at 20,000 summaries |
|---|---|
| A: Effect / Sync / Sync / Effect | 1,040 / 894 / 772 / 727 ms |
| B: Sync / Effect / Effect / Sync | 444 / 463 / 682* / 442 ms |

The 682 ms event lacks its ingress trace; it remains an unqualified raw observation. Seven of eight samples met strict correlation. Object A's same-form repeat gaps were **35.43% for Effect** and **14.65% for Sync**; both exceeded the frozen 10%/2 ms stability criterion. Object B's Sync pair was stable, while its raw Effect pair was unstable.

Exact-path warming and within-Object pairing did not establish a reliable signal in this small pilot. Placement, JIT and GC causes remain unproven. Two Objects and two repeats per form do not establish population performance. [Crossover report and raw evidence](crossover-pilot/report.md).

## Why append remains unmeasured

The feasibility inspection followed the full public append through five representative batches recovered from the deployed fixture: admitted input; run start/context; model response/tool declarations; tool settlements; final response/completion. They contain two or three records each, including a continuation.

A faithful candidate must preserve continuation reads/validation, archive updates, Run membership, work-index publication, cached-view rollback and native progress enrollment. The installed owner uses asynchronous `storage.transaction`; native progress also includes real KV/alarm waits. Individual SQL statements are synchronous.

The append investigation stopped at the agreed small-hook gate: implementing the complete native path would span several shared owners and their rollback contracts. This is a **scope assessment**, not a proof that a complete conversion is impossible or unhelpful. No raw-SQL-only substitute was measured, and no partial candidate was presented as complete. New append deployments, CPU observations and count deltas are all **zero**. [Exact source boundary and failure inventory](append-pilot/report.md).

A further complete-stage experiment would need explicit scope for native synchronous kernels across those shared helpers while retaining the real transaction/progress waits. Performance would still require a control design sensitive enough to distinguish the proposed effect. The usage microbenchmarks cannot answer that append question.

## Status and retained state

| Outcome | Status |
|---|---|
| Deployed cost attribution | Completed, with the small explicit host-prelude remainder. |
| Real deployed CPU data | Collected for both calibration attempts; all failed/slow/missing observations retained. |
| Reliable 15% stage-speedup conclusion | Unresolved; control/repeat gates failed. |
| Complete synchronous append candidate and comparison | Not implemented; stopped at the documented scope boundary. |
| Cleanup | All nine historical Workers and exact namespace IDs independently confirmed absent. |

There were no local runtime tests/benchmarks, paid model calls, PRs, pushes, merges or tracker updates in this deployed continuation. Local work was inspection, builds, static checks and retained-data analysis. Protected worktree commit **`58359ea25a78727d18f74847c0aa7a6a4873bc61`** remains clean; framework baseline was **`8c05714de84d68961b14e5ab7a3b7d809599563f`**. Private fixture identities and complete build archives are retained beside each attempt.

The append inspection's [cleanup receipt](append-pilot/cleanup.json) covers the final GET-only recheck of all nine retired Workers/namespaces. [Local retirement](local-cleanup.json) records task-owned scratch removal. Replay requires fresh own-lockfile checkouts and the retained source/build inputs; old temporary paths are historical execution records.
