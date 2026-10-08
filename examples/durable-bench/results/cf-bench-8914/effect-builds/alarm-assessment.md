# Tardie alarm CPU accounting and cold-state assessment

Historical review of earlier harness revisions. The final harness adds the constructor marker and Object-scoped collection discussed below; its current method, observed failures and coverage are documented in [report.md](../report.md). References to the “current” collector and pending diagnostics below describe the review-time state.

Read-only review for cf-bench-8914. No code, tests, timing runs, seed workloads, or resource mutations were performed. This file records the review result only. Source links refer to the installed dependency and task files inspected in the cf-bench worktree; those files can subsequently change.

## Finding

**RPC-only CPU can undercount Tardie's lifecycle work. The current same-trace DO total also excludes relevant alarm activity.** Waiting for a turn to finish does not establish that its work was charged to the initiating RPC.

The benchmark calls `reference.methods.message`, then `reference.wait` ([third-party/src/tardie.ts](../../../third-party/src/tardie.ts), lines 84–87). The installed runtime has an independent alarm path:

- [`ThreadObject.alarm()`](../../../third-party/node_modules/tardie/src/platform/cloudflare/objects.ts), line 332, runs watchdog restoration and the scheduler. Its scheduler can resume effect-owned wakes directly, or invoke the watchdog (line 236).
- The same file's `recover()` opens the actor, calls `thread.resume`, then `thread.recover` (lines 296–301). Its watchdog launcher uses `Effect.runFork` (lines 242–245).
- [`createWatchdog`](../../../third-party/node_modules/tardie/src/core/services/watchdog.ts), lines 105–148, probes state and can launch recovery; journal admission schedules pending work immediately through `admit` (lines 60–67).
- [`execution.ts`](../../../third-party/node_modules/tardie/src/core/runtime/execution.ts), lines 652–659, runs a background processing loop that drains queued work. `resume` schedules processing (line 714); `wait` observes scheduled processing and background fibers (lines 773–780).
- [`actors.ts`](../../../third-party/node_modules/tardie/src/core/runtime/actors.ts), lines 202–205 and 239–243, implements method invocation/result waiting and the reference's wait operation.

These paths allow actor/model/tool processing to be initiated or resumed through alarms while an RPC waits. They do not identify which individual model calls ran under each observed invocation.

## Observed evidence and limits

The saved [seed telemetry preview](../run/seed-telemetry-preview-tardie.json) explicitly has `limitedPreviewOnly: true` and a query limit of 20. Its 20 records contain **16 alarm invocations totaling 1,738 ms of reported CPU**, plus four supporting DO RPCs. The alarms use distinct trace IDs; the supporting RPCs share particular alarm traces. The 16 alarm records include canceled outcomes.

This proves material alarm activity in the preview, not complete seed cost or per-turn attribution. It does not establish which model calls ran in those alarms or explain the hanging seed request. The reported socket closure and absence of CPU/memory-limit outcomes do not establish a root cause. Seeding recovery remained with the coordinating agent.

The collector in [main.ts](../harness/main.ts), lines 112–132, considers an expected RPC and matching stateless ingress sufficient for collection completeness. It selects supporting DO invocations only when they share the RPC's trace ID. Separate alarm traces are therefore omitted, and the completeness check does not wait for alarm telemetry.

## Recommended accounting

Keep RPC CPU as a diagnostic. For the primary comparison, collect a complete, object-scoped cohort account using the unchanged benchmark bundle and existing durable behavior:

1. Collect all invocation records for the cohort's Thread DO and its dedicated Actor DO across trace IDs, including alarms, retries, and canceled outcomes. Deduplicate by invocation ID. Retain ingress separately. Do not aggregate unrelated objects merely because they share a Worker script.
2. Use the same declared accounting boundaries across implementations: workload completion plus a predefined alarm tail. Report pre-cold recovery separately from measured turns. If background work remains active at the boundary, report incomplete lifecycle accounting rather than treating returned RPCs as proof of quiescence.
3. Require complete exports, sufficient delayed-log collection, and explicit handling of missing, truncated, or sampled data. A limit-sized preview cannot establish completeness.
4. Do not force alarms into individual cold/warm buckets when they span phases or their causality is ambiguous. Retain those costs at cohort level; do not prorate CPU by wall time.
5. Label the result **sum of observed invocation CPU**, not exclusive model CPU. This review does not establish Cloudflare's CPU accounting semantics for overlapping executions, so summation alone does not prove exclusive physical CPU attribution.

Do not suppress, cancel, or retime Tardie's alarms to make the measurement easier. Those operations would change the durable behavior being compared. Cloudflare documents object-ID filtering for invocation logs in its [metrics and analytics guidance](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/).

## Cold-state definition and verification

The [wrapper](../harness/wrapper.ts), lines 69–79, creates module/runtime identity markers in the observed Thread DO constructor. Its `cold` method calls `super.wake()` and `super.turn()` (lines 88–93). The [controller](../harness/main.ts), lines 167–172, requires cold's module/runtime/version IDs to differ from seed and warm calls to retain cold's IDs.

Those checks establish a different incarnation/version from seed. **They do not prove cold was the first workload of that incarnation.** An alarm can construct and warm it first. Cloudflare explicitly states that an incoming alarm can trigger the constructor before its handler: [Durable Object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/).

The cheapest adequate verification is:

1. First use existing telemetry: query the relevant objects from **each role's deployment start** through cold entry, across all traces and event types. The current controller opens its telemetry window only after deploying every role (main.ts, lines 149–156), leaving a pre-cold gap. Absence in incomplete telemetry, or telemetry missing still-pending invocations, is insufficient evidence of no prior activity.
2. If that cannot establish the trigger, add a minimal wrapper-only constructor log containing the existing object/runtime/module/version IDs, and require that constructor log's invocation identity to match the cold RPC. This positively establishes construction by cold for that incarnation, without another RPC or durable storage mutation. No such marker was implemented during this review.
3. Describe that verified case as **first activation of a fresh DO incarnation over seeded storage**. Proving no earlier alarm anywhere under the new version still requires complete version-wide activity history: a constructor marker or per-instance alarm counter cannot rule out an earlier, subsequently evicted incarnation. The claim is not cold underlying storage or platform caches.

Stable warm identities likewise establish residency, not absence of intervening alarm work. Preserve that distinction when reporting warm CPU.

## Follow-up: namespace isolation and conditional watchdog blocking

The subsequent read-only source review found **no namespace-remapping or coordinate-cache collision in the inspected path**. The updated [wrapper](../harness/wrapper.ts) remaps ingress and both Thread/Actor subclass constructors from the original suffixed bindings. Tardie's [Worker factory](../../../third-party/node_modules/tardie/src/platform/cloudflare/index.ts), lines 46–52, reads the supplied environment; internal routing in [objects.ts](../../../third-party/node_modules/tardie/src/platform/cloudflare/objects.ts) uses each instance's `this.env`. Repeated names/coordinates therefore do not themselves select another fixture's namespace.

Coordinate-keyed thread/journal/subscription maps belong to each `createActorExecution` invocation ([actors.ts](../../../third-party/node_modules/tardie/src/core/runtime/actors.ts), lines 131–146); supervisor maps are similarly local to `createSupervisor` ([supervisor.ts](../../../third-party/node_modules/tardie/src/core/services/supervisor.ts), lines 30–61). Each store creates its own atom registry ([store.ts](../../../third-party/node_modules/tardie/src/core/atoms/store.ts), line 9), and runtime services build through `Layer.fresh` ([execution.ts](../../../third-party/node_modules/tardie/src/core/runtime/execution.ts), line 302). The inspected shared tool-promise cache stores journal-scoped atom definitions, not a shared pending-request table.

One fixture observation hazard remains: [tardie.ts](../../../third-party/src/tardie.ts), lines 33–45 and 90–92, holds `seen` at module scope and fingerprints it after awaited turns. Another object sharing that module could overwrite the observation. Each scripted model call, however, replaces `seen` and computes `next(seen)` synchronously. This is non-object-scoped fingerprint ownership, **not a demonstrated cause of stalled processing**.

The watchdog has a concrete conditional failure path:

- Defaults are 20 recovery attempts and five no-progress attempts. Admission preserves an existing blocked state; progress does not automatically reset total attempts ([watchdog.ts](../../../third-party/node_modules/tardie/src/core/services/watchdog.ts), lines 5, 60–89).
- A blocked watchdog makes `canDrive` return false ([objects.ts](../../../third-party/node_modules/tardie/src/platform/cloudflare/objects.ts), line 291), causing `drain()` to return ([execution.ts](../../../third-party/node_modules/tardie/src/core/runtime/execution.ts), lines 386–388).
- Method-result waiting awaits a Deferred without checking watchdog status ([actors.ts](../../../third-party/node_modules/tardie/src/core/runtime/actors.ts), lines 207–240). Thus processing can stop while the method RPC remains pending.
- Watchdog records are persisted under `tardie:watchdog:` ([storage adapter](../../../third-party/node_modules/tardie/src/platform/cloudflare/watchdog.ts), lines 7–19). Wrapper abort/reconstruction does not clear those records.

These are source-level conditions, **not evidence that the stalled object reached blocked state**. At this update, the coordinating agent reports that a read-only wrapper diagnostic using `storage.list({ prefix: "tardie:watchdog:" })` and `getAlarm()` has been deployed after the second seed client's failure; its result remains pending and was not inspected by this worker. Relevant discriminators are `status`, `reason`, `attempts`, `consecutiveNoProgress`, `generation`, and scheduled alarm state.

The initial `250_0` name is a routing label. Its initial history inputs are the same as the 50-turn fixtures ([plan.ts](../../../src/plan.ts), lines 30–35). The reported stall during turns 10–20 occurred before that object's scheduled 50-turn restart. **It does not establish a 250-turn history limit.** No cause attribution, target modification, new workload, or diagnostic collection was performed by this worker.
