# Wake deferral measurement

This is a disposable Yielded-only reuse of the cf-latency report, stack, native provider, network host, counters, and telemetry joins at `origin/dan/cf-latency-breakdown` (`52e3fd18e3977dcdeae78d2d1b23c7685ed0106e`). The original runtime baseline is `8c05714de84d68961b14e5ab7a3b7d809599563f`. No package script additions or product switches are required.

Read the [measurement report](report.md) for the result and limitations, [cleanup receipt](cleanup.json) for resource verification, and [raw archive manifest](raw-manifest.json) for byte hashes. The offline reduction commands below reproduce the retained summary without deploying anything.

The esbuild transform decorates the Cloudflare WakeScheduler layer at assembly. Its optional `withProcessing(threadId, body)` returns `body` for baseline and delegates both arguments to the real method for candidate, retaining the final `scheduleLocal`. Nothing wraps the harness execute operation. Both arms use the candidate's interruption-safe maintenance counter and the same observer code. Observer cost varies with work and has not been subtracted; driver timing includes the measurement receipt. The A/B baseline is distinct from the archived original runtime.

## Run

From the worktree root, the build is local:

```sh
vp exec node examples/durable-bench/results/wake-defer/build.mjs ab
```

Run each operational action in order using:

```sh
vp exec direnv exec . vp exec node examples/durable-bench/results/wake-defer/run.mjs <action>
```

Actions: `init` → `deploy-ab` → `seed` → `activate` → `measure` → `telemetry` → `secret-scan` → `cleanup`. Initialization is once per retained run. Deployment and destruction use Alchemy only. Keep bundled sources and archived bytes frozen through measurement.

Re-running `deploy-ab` verifies each recorded role's archived bytes and configuration without redeploying it, then deploys missing roles. A recorded 404 or mismatch fails without replacing ownership. Multipart verification accepts both string and File parts and requires exactly one exact module match.

On termination or failure, retain the evidence and run `cleanup`; it attempts every recorded stack, verifies Worker 404s plus complete namespace/prefix listings, collects the telemetry tail, and removes private state only after verification. An incomplete cleanup retains state for a retry.

`seed` resumes only contiguous acknowledged chunks. `measure` skips fully completed Objects and refuses an interrupted Object with an existing measurement attempt; it never replays a canonical input. A partial Object stays visible in the evidence but cannot enter paired summaries. Main may explicitly retire a failed setup Object and record a fresh alias before measurement, retaining all old attempts. Measured observations are never silently replaced.

Offline reduction is safe during or after collection:

```sh
vp exec node examples/durable-bench/results/wake-defer/analyze.mjs
```

It writes only `analysis.json` and `analysis.md`; it does not change the report or raw evidence. For the report tables, run `vp exec node examples/durable-bench/results/wake-defer/render-summary.mjs examples/durable-bench/results/wake-defer`. That pure offline formatter writes `measurement-summary.json` and `measurement-tables.md`, using only the common set of Objects with both arm medians known for each metric. A partial run is labelled explicitly. Re-run after final telemetry and cleanup. No credentials or remote calls are needed for reduction.

`WAKE_DEFER_EVIDENCE_DIR` optionally selects another evidence input/output directory; source imports and dependency resolution stay with the checked-out script. The root files contain the primary fresh `r2` run. `interrupted-run/` retains the surviving earlier snapshot and must not be pooled into primary results. Missing temporary data from the interrupted session is recorded there, not reconstructed or treated as zero.

The same command replays retained proof from compressed raw evidence, without extraction or deployment. Every input is read from its plain filename when present, otherwise from that filename plus `.gz`: for example, `requests.jsonl.gz` and `telemetry-network.json.gz`. Plain files take precedence if both forms exist; a corrupt archive fails reduction instead of being treated as missing evidence.

Keep controller inputs plain during active collection. Main archives raw evidence after its final controller writes, verifies that decompression reproduces the original raw-byte SHA-256, then removes the plain copy. Retain `analysis.md` as a readable summary; the larger `analysis.json` may also be compressed. Compression changes storage only; the reducer uses the same receipts, joins, fingerprints, and admission rules for either input form.

## Design and interpretation

The primary plan uses **28 fresh Objects and 224 measured turns**: seven Objects per 50/250 seed-history and 0/400 ms TTFT cell. Each Object supplies two baseline and two candidate repeats at each temperature. Temperature/repeat blocks, variant order, and Object order are seeded and shuffled. The independent unit is the Object; nine model calls are one turn. Expected counts and completeness come from the active `network-plan.json` cohorts.

The preliminary run suffered a checkout-access failure during warmup, then a session interruption that lost the private temporary state and later receipts. Its surviving 32 measured receipts on four complete Objects remain in `interrupted-run/`; they are excluded from the primary matrix. The `recover-state` action is limited to missing private state after archival. It restores the owned stacks through Alchemy with exact archived modules and a new disposable token, then requires fresh `r2` Objects. No uncertain input is replayed.

Initial seed histories are 50 and 250 turns. Eight settling turns for seed 50 and four for seed 250 move the fixture's large lookup #97/#194 outside comparison. The eight comparison turns therefore begin with **58–65 and 254–261 historical turns**, respectively, and each adds eight 256-byte tool results. Both arms run on the same Object with growing canonical history; the comparison does not hold history fixed at 50/250. `historyBeforeTurn` records the planned prior turn count for each sample. Seed fingerprints remain `b017b487524e44a4` / `dcea9f30b0917245`, and every model request is checked against the full growing reference transcript. TTFT 400 uses 10 ms chunk spacing (4,130 ms total programmed delay per nine-call turn); TTFT 0 uses no programmed delay.

Cold requires an acknowledged explicit abort, the same Object hash, a new incarnation hash, first harness entry, and zero prior/active alarms. Contradictory constructor telemetry vetoes admission. Warm means the incarnation from the preceding completed turn persists; it may be the first turn following a completed cold measurement. The 250 ms drain observes background work without invoking maintenance manually. It does not prove completion: a long alarm can remain active across the next meter reset and turn.

The driver targets `aws:us-west-1`; every Object uses `locationHint: "wnam"`. Pinned Alchemy `2.0.0-beta.80` derives placement from Distilled's `PutScriptRequest.metadata.placement` and passes it through; pinned Distilled explicitly supports `{ mode: "targeted", region }`. The controller verifies deployed settings. The driver's **response** `cf-placement`, received by the laptop as `row.cfPlacement`, is an observed placement receipt. It is retained separately from the driver's incoming request header, the target response placement, ingress `cf.colo`, and CF-Ray. A region hint alone does not prove physical co-location with the Object. See [Cloudflare placement](https://developers.cloudflare.com/workers/configuration/placement/#cf-placement-header).

Driver and laptop durations use their own clocks. First provider arrival and model gaps compare Worker wall clocks and can include skew; DO `Date.now()` advances at I/O and is not a CPU stopwatch. The driver's response `cf-placement` supplies `driverExecutionColo`: `local-SJC` yields `SJC`. `driverIncomingPlacement` and `driverIncomingPlacementColo` describe the separate incoming header, which can be null while the response receipt is present. CF-Ray suffixes identify request-edge colos, not physical Object placement.

| Output                                                                                                     | Meaning                                                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `records[].driverMs`, `laptopMs`                                                                           | Whole target fetch as seen by the deployed driver; whole request as seen by the laptop. Raw CF-Ray and placement fields remain separate.                                                                  |
| `driverResponsePlacement`, `driverExecutionColo`, `driverIncomingPlacement`, `driverIncomingPlacementColo` | Driver response placement and its parsed colo, separately from the incoming request header and its parsed colo.                                                                                           |
| `coverage.*.driverPlacementReceipts`, `driverIncomingPlacementReceipts`                                    | Nonempty driver response headers versus nonempty incoming headers. Zero incoming receipts does not imply missing response placement evidence.                                                             |
| `providerMs`, `*MinusScriptedMs`, `*MinusProviderMs`                                                       | Sum of actual per-provider durations; client duration minus programmed or observed provider time. These are residuals, not CPU measurements.                                                              |
| `firstModelRequest`, `firstModelArrivalFrom*Ms`, `modelGap1Ms` … `modelGap8Ms`                             | First provider request identity/arrival and eight inter-model gaps; also retained as an array, median, and total.                                                                                         |
| `alarmOverlapObservedCount`, `alarmStartedInsideCount`, `alarmEndedInsideCount`                            | Alarms touching the active turn window versus alarms whose start/end occurs inside it. Missing or inconsistent window receipts give null counts.                                                          |
| `objectModelGapsIoMs`, `objectModelGapMedianIoMs`, `objectModelGapTotalIoMs` | Eight same-Object I/O-clock gaps from stream wrapper finalization to next fetch dispatch. All eight finite values and a valid transcript are required. A zero does not exclude CPU work between I/O events. |
| `alarmJoinedCount`, `alarmCpuMs`, `alarmOverlapFullCpuMs`                                                  | Unique native invocation joins; their full CPU when exclusive to this turn; non-additive full overlap CPU retained diagnostically even when shared.                                                       |
| `containedAlarmCpuMs`, `boundaryAlarmCpuMs`, `attributedCpuMs`                                             | Full CPU of contained/boundary alarms and fetch CPU plus contained alarm CPU. Shared costs become null in affected paired metrics. These are not wall-time components to add.                             |
| `alarmJoins`, `alarmJoinCoverage`, `alarmAccounting`                                                       | Stable alarm/invocation IDs, window edges, log timestamps, coverage, alarms shared across turns/arms, and native CPU deduplicated across returned turns.                                                  |
| `paired[].units[].metrics`                                                                                 | Per-Object medians for baseline/candidate, `saved = baseline - candidate`, repeat-pair savings, and both arms' signed/absolute repeat differences.                                                        |
| `paired[].metrics`, `coverage`, `fingerprintCoverage`                                                      | Object-level distributions with known-value N, receipt/join coverage, expected/observed hashes, and every missing or mismatched step.                                                                     |
| `errors`, `outcomes`                                                                                       | All phases and Workers, independently of admission: request/provider/controller failures, unanswered attempts, exceptions, every non-ok invocation, and explicit `exceededCpu` / `exceededMemory` counts. |

Alarm membership comes from each `/run` receipt's active edges and start/end snapshots, with its `runStartedMs`/`runEndedMs` window. The query/sample attached to a long alarm can change at a meter reset and is not used to join it. CPU attribution requires both logged alarm edges, matching Object/incarnation/version, one native invocation per alarm ID/request/trace, and the reverse uniqueness check. Log timestamps and the platform invocation timestamp/wall duration remain join evidence; the reducer does not infer a missing end from the drain duration.

Every alarm CPU value is the **whole native invocation cost, never prorated** to the turn window. An alarm overlapping multiple returned turns is flagged, including when it spans baseline and candidate. Its diagnostic `alarmOverlapFullCpuMs` must not be summed across turns; affected paired CPU metrics become null. `alarmAccounting` sums unique native invocation costs once and exposes missing coverage. Contained CPU remains usable when only a boundary alarm is shared. Platform events without a usable ID remain in failure accounting but cannot establish a CPU join. Non-invocation outcome records are retained separately to avoid counting a console record as another invocation.

Each complete Object has one vote per metric. Both repeats in each arm must have known values for a paired delta. Every metric retains `baselineRepeatDifference` and `candidateRepeatDifference` (planned repeat 1 minus repeat 0), plus `baselineRepeatAbsoluteDifference` and `candidateRepeatAbsoluteDifference`. Cell summaries retain both repeat-spread distributions and the per-Object effect range (`saved.min`/`saved.max`). These are descriptive noise references, not confidence intervals or proof that an effect exceeds noise. `stats.n` exposes missing CPU evidence; no missing value is filled with zero. Coverage totals count overlap memberships, with distinct alarm/invocation counts alongside them. A zero platform-error count describes the captured events only.

`errors.nonOkInvocations` includes captured invocation records with a missing outcome as well as explicit non-ok outcomes and expected cold aborts. Inspect the `outcomes` groups and retained query-sampling information before treating that count as failed workloads. The reducer does not apply sampling weights or extrapolate captured counts to all executions.

Mechanism counts use the active request window, with constructor counts and after-return events separate. Missing or truncated mechanism receipts give null count metrics. `scheduleNowCalls` counts all calls; `suppressedScheduleNow` counts deferred calls; `nondeferredScheduleNow` counts calls that attempt `armNow`. `armNowExecutions` and `nativeSetAlarmCalls` are separate: a nondeferred call does not prove that a native deadline changed. This distinction matters in the local mechanism proof, where total calls stayed 23→23/25/25 while nondeferred calls fell 14→2. Receipts also retain notify kind/appendBatch tags, settlement, claims, recovery, maintenance reports, and generation checkpoints. `instrumentedMaintenanceScans` covers the two explicit entry/checkpoint sites, not every SQL nonterminal scan.

## Provenance and retained evidence

`build-identities/` archives exact modules, original/transformed source snapshots, input hashes, dirty status, lock/fixture hashes, and tool versions. Deployment verifies exact uploaded bytes and active version/build binding. The native OpenAI-compatible adapter resolves from the already-installed catalog-pinned dependency in `examples/browser-speed`; there is no vendor installation.

The selected module hashes are in [build-identities/all.json](build-identities/all.json). The independently captured deterministic mechanism proof is described by [local-counts/provenance.json](local-counts/provenance.json), alongside `captured-script.mjs.gz`, `before.mjs.gz`, and `after.mjs.gz`. The raw counts and their capture sources are retained unchanged.

Account/token/Alchemy state stays in a mode-700 directory under `/private/tmp`; opaque Cloudflare Object, incarnation, alarm, resource, version, and telemetry identities are hashed. The account name is allowed. Credential scanning includes decompressed archives and known opaque upload IDs. Never retain raw state or print environment values.

The product change passed `vp run ready`; see `validation/summary.json`. The bulky sanitized evidence is published on a dedicated branch and linked from the product PR. This results directory is gitignored, so publication explicitly adds the intended files. Private state is never included.
