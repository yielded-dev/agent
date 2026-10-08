# KOM-433 hosted preflight

**Ready for a bounded deployed pilot, not a passed workload.** Configured-account, Workers, Durable Objects and empty log-query access succeeded. This role created **zero cloud resources**, dispatched **zero workload RPCs**, and ran **no local tests or runtime benchmarks**. Deployment/deletion authorization and actual CPU-log delivery remain deployment-dependent.

## Target and read-only evidence

- Context: `/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433`, loaded after `cd` using `/usr/bin/env direnv exec .`.
- Account SHA-256: `3e8f88a45f1480a607b6192f4f156c5630072d175f6d49cc61160be5384837bd`. The account API returned exactly the configured account ID. Do not substitute another account.
- Cloudflare account/token were present and usable. The Workers subdomain environment variable was absent, but the harness's subdomain API read succeeded.
- No Infisical login/export was needed. If needed later: configured project `e27bd4e8-050b-4b7b-ab72-045abb9463cc`, environment `dev`, path `/`, domain `https://app.infisical.com/api`, project machine identity only. The project `.envrc` has the Yielded override; its Universal Auth pair was present. Unexported backing variables prevented independently comparing identity values.

Checks used `cd /Users/dan/dev/effect-agent/.worktrees/dan-KOM-433 && /usr/bin/env direnv exec . python3 -`, in-memory authentication, 30-second timeouts and a 2,000,000-byte response bound. Account root below means `https://api.cloudflare.com/client/v4/accounts/{configured-account}`. No customer Object contents or existing workload logs were inspected.

| Check start, UTC     | Request                                                                            | Sanitized raw observation                                                                         |
| -------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| 2026-10-08T15:44:39Z | `GET {account-root}`                                                               | HTTP 200; `success=true`; configured ID matched.                                                  |
| Same check           | `GET {account-root}/workers/subdomain`                                             | HTTP 200; `success=true`; valid nonempty subdomain.                                               |
| Same check           | `GET {account-root}/workers/scripts`                                               | HTTP 200; `success=true`; 54 metadata entries; proposed pilot absent. No script contents fetched. |
| 2026-10-08T15:45:38Z | `GET {account-root}/workers/durable_objects/namespaces?page=1&per_page=1`          | HTTP 200; `success=true`; one entry. Access proof, not exhaustive absence proof.                  |
| Same check           | `GET {account-root}/workers/scripts/effect-agent-cpu-01a119d543300001-b0-baseline` | HTTP 404; `success=false`; code `10007`.                                                          |
| Same check           | `POST {account-root}/workers/observability/telemetry/query`                        | HTTP 200; `success=true`; `events.count=0`; zero returned events.                                 |

Exact read-only telemetry query: `queryId="effect-agent-cpu-01a119d543300001-b0-baseline"`, `dry=true`, `view="events"`, `limit=1`, timeframe `{from:1791474279536,to:1791474339536}`; `and` filters: string `$workers.scriptName eq "effect-agent-cpu-01a119d543300001-b0-baseline"`, number `$workers.cpuTimeMs exists`. The absent-script restriction excluded unrelated logs.

The first helper stopped on API `errors:null` after the three successful initial checks. Null-list parsing was corrected; only the unreported DO check and subsequent absence/log checks were repeated. No mutation or workload was retried.

**Still unverified:** deployment/destruction permissions, invocation delivery/completeness, numeric CPU/wall-time fields, Object/method/trace/version correlation and log delay. Empty-query success does not establish these.

## Harness and repair findings

Source baseline: `/tmp/kom433-cf-01a119d5/baseline` at `8c05714de84d68961b14e5ab7a3b7d809599563f`. Protected prototype remains `58359ea25a78727d18f74847c0aa7a6a4873bc61`. The baseline includes repair `ba5813ec33880a9063147be6e6cce94698b11725` (#817). Installed manifests confirm Effect `4.0.0`, Alchemy `2.0.0-beta.80` and Vite+ `0.3.3`. **No new source commit, bundle or deployment identity exists from this role.**

- [Builder](/tmp/kom433-cf-01a119d5/baseline/tooling/context-continuity-eval/src/replay-cpu-build.ts): clean exact source before/after; fixture, lockfile, versions, bundle and metafile receipts; framework public imports resolve to production `dist`. Use one fixture-builder checkout for both source roots. Control uploads baseline bundle bytes unchanged.
- [Stack](/tmp/kom433-cf-01a119d5/baseline/tooling/context-continuity-eval/src/replay-cpu.stack.ts): `effect-agent-replay-cpu`; resource `Benchmark`; class `ReplayCpuThread`; binding `REPLAY_CPU_THREADS`; logical DO `ReplayCpuThreads`. Compatibility is `2026-08-01` / `nodejs_compat`; no rebundling; invocation logs sampled at 1; no model keys/custom domains.
- [Ownership](/tmp/kom433-cf-01a119d5/baseline/tooling/context-continuity-eval/src/replay-cpu-deployment.ts): account-bound private state; target receipt saved **before upload**; Alchemy destroy followed by independent Worker 404 and fully paged namespace absence. Private state survives failed cleanup. Installed Alchemy uses forced Worker deletion, not the separate live-model harness's tombstone path.
- [Telemetry](/tmp/kom433-cf-01a119d5/baseline/tooling/context-continuity-eval/src/replay-cpu-results.ts) / [runner](/tmp/kom433-cf-01a119d5/baseline/tooling/context-continuity-eval/src/replay-cpu-main.ts): exactly one DO invocation per Object/method and one correlated ingress trace; expected deployment/name, successful outcomes and explicit `truncated=false`. CPU is **DO RPC `cpuTimeMs`**, not ingress CPU, elapsed time, or alarms/evidence. Duplicate IDs, count/length mismatch and exports reaching 2,000 events fail closed; log polling is bounded to 13 attempts, ten seconds apart.

**#817 verified by source/diff inspection:** protocol `replay-cpu-v2`; corrected production resolution; complete `streamExport`; post-seed batch-chain recomputation; Schema-encoded `RunContinuation`; removal of read/append observers from measured operations. Preserve prefix/tail/sequence checks, finite model/tool/finalizer counts, settlements and normalized prompt equality. Encoding/hashing/export stays after measured cycles. Initial is not guaranteed cold; warmed means same observed incarnation, with accumulated state—not isolated JIT warm-up.

Two small retention gaps matter for a temporary adapter: stock request receipts are written after responses, and invalid telemetry can be rejected before export is saved. Persist an attempt marker before dispatch and retain allowlisted failed-export metadata before validation. Do not relax sample validity or add reporting infrastructure.

## Minimal hosted path

Reuse `prepareReplayCpuResources`, `openReplayCpuDeployment`, stack/build/export code. The stock CLI has **no pilot-size flags**: its fixed workload is 72 Objects/720 RPCs. A small task-local schedule/probe adapter is necessary; do not launch the full default as an access check. Keep edits and private state under `/tmp/kom433-cf-01a119d5`; set `TMPDIR` there.

1. **Counters before optimization.** Add counting around the stock whole-operation boundary, returning counts with final evidence. Use its two archive sizes and 400 KB retained same-Thread context, rather than the old fresh-Thread Node lane. Reuse corrected `globalThis.Boolean` and nested-Fiber inheritance for Schema runtime work; adapt source selectors. Exclusive totals must close, with formerly unattributed work named; retain inclusive/nesting views separately. Counting/SQL observations use separate bundles. **Evaluation shares are not CPU shares.**
2. **Whole-stage calibration.** Only after the map selects a stage, compare its complete Effect and alternate implementations, with normal services and identical inputs. Add three distinct RPC methods for exactly 1, 4 and 16 executions. Distinct methods preserve the existing Object/method telemetry join. Baseline/control are byte-identical; candidate differs only in the selected implementation. Calibration determines a resolvable repetition count, not an instrumented CPU claim.
3. **Append-only path, conditional on selection.** Two unique RPCs, initial and warmed, each execute the complete existing `ThreadStore.append` stage at fixed `R≤16`: capture/validation, encoding/digests, fencing, transaction, canonical/index/work/progress writes and result validation. Preserve [normal append authority and atomicity](/tmp/kom433-cf-01a119d5/baseline/packages/storage-cloudflare/src/internal/do-journal.ts#L456); no raw-SQL bypass or nested transaction. Use fresh canonical identities and real returned tails; verify complete exports afterward. This is append-stage evidence, not whole-agent speedup. Do not reuse the replay fixture's model assertions or compaction-specific summary unchanged.

## Proposed resources, order and limits

Names are **proposals, not reservations**. Recheck immediately before upload. Stages: `<run>-b<0..2>-<baseline|candidate|control>`; Workers: `effect-agent-cpu-<stage>`. Object names remain `small-0` / `large-0`, with existing internal prefix `issue692-<stage>-`.

| Phase                          | Run / stages                               | Objects | Counted/measured RPCs | Inner work                                     | Dispatch limit |
| ------------------------------ | ------------------------------------------ | ------: | --------------------: | ---------------------------------------------- | -------------- |
| Counter pilot                  | `01a119d543300001`, `b0-baseline`          |       2 |                    20 | Stock ten-operation sequence/Object            | 10 min         |
| Stage calibration              | `01a119d543300002`, `b0` × three roles     |       6 |                    18 | 1+4+16 executions/Object = 126                 | 15 min         |
| Append comparison, conditional | `01a119d543300003`, `b0..b2` × three roles |      18 |                    36 | Two RPCs/Object × `R≤16` = at most 576 appends | 20 min         |

Total: **26 Objects, 74 diagnostic/measured RPCs**, plus 26 seeds and at most 26 final audits. Native alarms/ingress/audits remain separately retained and billable. No provider API calls. Every failed/uncertain attempt consumes budget. User ceilings remain 100 Objects, 1,000 measured RPCs, one active operation and 45 minutes dispatch/phase—not a billing cap.

- Counters: small then large, all ten operations consecutive; audit afterward.
- Calibration: deploy baseline/control/candidate; small samples in that order, large reversed; repetitions 1,4,16 per Object.
- Append rounds: deployment and small-sample orders `baseline,candidate,control`; `candidate,control,baseline`; `control,baseline,candidate`. Large samples reverse each order. Initial/warmed are consecutive, without intermediate evidence calls. Export and destroy each round before the next; save the schedule before upload.
- Separate counting/CPU identities; identical CPU fixture bytes across sources; unchanged baseline bytes for control. Private clean snapshot commits are allowed only under the task directory and never pushed.
- Primary metric: warmed DO `cpuTimeMs/R`, with raw values, initial results, ranges and paired candidate/control ratios by round. Proposed directional criterion: ≥10% reduction in all three rounds exceeding each control shift. One cohort/size/round is diagnostic, not statistical confidence. If resolution/control noise is inadequate at 16 repetitions, report inconclusive.

### Predeclared SQL/storage envelope

All phases seed **13,130 records in 117 batches** (128-record chunks); counters add 20 whole operations. A storage-stage recipe must be representative and frozen after mapping, capped at **eight records / 64 KiB encoded per batch**. The 702 maximum stage executions then add at most **5,616 records / 43.875 MiB logical payload**, before archive/index duplication. Stop if the selected complete stage cannot fit; do not substitute a tiny helper.

Planning estimates, not measurements: per stage execution or seed batch, allow 100 statements/10,000 rows read; writes allow 1,000 rows/execution or 4,000/seed batch. Per counted whole operation allow 1,000 statements/100,000 reads/10,000 writes; initialization plus final audit allow 500 statements/50,000 reads/10,000 writes per Object. Combined estimate: **114,900 statements, 11.49 million rows read, 1.63 million rows written**. Confirm with the diagnostic pilot before scaling; revise the declared plan rather than silently exceeding it. No SQL observers belong in CPU bundles.

Retain 32 MiB/Object admission, with a conservative 16 MiB pilot stop threshold. At most six Objects coexist per three-role round (192 MiB at admission thresholds). Admission is not an exact storage/billing cap. Use final evidence and the pilot to bound later work; never weaken transaction atomicity to enforce reporting limits.

## Commands and closeout

Existing build command, **not run here**, from the task-owned fixture checkout:

`vp run perf:cloudflare:cpu:build --source-root /tmp/kom433-cf-01a119d5/baseline --output-dir /tmp/kom433-cf-01a119d5/build-baseline`

Build the alternate clean checkout through the same entrypoint into a different directory; use `vp install --frozen-lockfile` only if needed. Existing `vp run perf:cloudflare:cpu --baseline-dir <baseline-build> --candidate-dir <candidate-build> --output-dir <new-output> --dry-run` prints the **full default**, not this pilot. The temporary adapter needs its bounded plan reviewed before dispatch; no nonexistent pilot flag is implied.

Exact recovery command for the proposed counter output:

`cd /Users/dan/dev/effect-agent/.worktrees/dan-KOM-433 && /usr/bin/env direnv exec . sh -eu -c 'export TMPDIR=/tmp/kom433-cf-01a119d5; cd /tmp/kom433-cf-01a119d5/baseline; exec vp run perf:cloudflare:cpu --cleanup --output-dir /tmp/kom433-cf-01a119d5/hosted-counts'`

Use the same command with `hosted-calibration` or `hosted-append`. It reopens recorded private state, calls `vp exec alchemy destroy <replay-cpu.stack.ts> --stage <recorded-stage> --yes`, then verifies Worker and namespace absence. Preserve the state/pointer if cleanup fails; never manually erase it to obtain a green receipt.

Stop dispatch on account/identity/fixture mismatch, name collision, missing ownership, ambiguous upload, incarnation change, exceeded limits, failed canonical/authority checks, uncertain RPCs or incomplete/invalid telemetry. Inspect the recorded Worker's deployment/settings and matching namespace before any ambiguous-deployment retry. **Never retry uncertain seed/workload/audit RPCs**; stock evidence clears captures even though its transport is GET. Only bounded identity readiness (≤15 probes) and read-only log queries are retryable. A different account or materially exceeding the user's ceilings requires renewed approval, not an automatic larger phase.

Retain every slow/failed/missing observation and scheduled denominator. Supervise commands through completion; no simultaneous measurement lanes, builds or count instrumentation during CPU collection. Retain only sanitized evidence here, never credentials, private Alchemy/auth state or capability URLs.

Close only when `cleanup.json` has `complete=true`, `secretRemoved=true`, every target `cleanupComplete=true`, and independent Worker/fully paged namespace absence checks passed. **Preflight cleanup: not applicable—nothing was created or deployed; no owned background command remains.** No PR, push, merge or tracker action occurred.
