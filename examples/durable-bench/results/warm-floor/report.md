# Warm-floor: no demonstrated fixed-cost saving

**None of the three candidates established a warm-turn latency saving above the
repeated-baseline spread. No performance PR was opened; product code was restored
to main.** The immediate pre-arm often moved time into receipt delivery instead of
advancing the first model request. Atomic settlement removed one transaction, but
did not establish a tail-latency saving. Skipping unchanged row views removed repeated
work without a resolved latency or CPU benefit.

Baseline: `b246f8aaa3a92d5f82934b1fc7a82356d1ad6664`, including #821, #823, #825 and
#826. Account: **Danieljmerwe@gmail.com's Account**. All timing below came from the
deployed production `CloudflareThreadClient` → `ThreadObject` → alarm →
`awaitSettlement` path. Local runs supplied correctness evidence only.

| Change | Before → candidate | Established milliseconds saved per turn |
| --- | --- | --- |
| Immediate mutation pre-arm | `max(due, now + 50)` → `max(due, now)`; 34 transactions / 22 with writes unchanged | None |
| Ordinary-root atomic settlement | 34 / 22 → 33 / 21; terminal tail 7 / 5 → 6 / 4; same 12 canonical batches | None |
| Leave unchanged row views intact | Warm view reconstructions about 169–187 → 29–33; serialization about 111–125 KB less per turn | None |

Driver medians below are the median of the two Object medians. Baseline pools the
two labels that execute identical main code. “Repeat spread” is the median of the
**full baseline ranges within each Object**, a conservative noise check; the raw
per-Object ranges and differences between baseline-label medians are also retained.
Candidate values are observations, not claimed improvements or regressions.

| Seed / provider | Main | Immediate pre-arm | Atomic settlement | Both | Baseline repeat spread | pi alongside |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 / instant | 968.5 | 914 | 1,035 | 886.75 | 382 | 773.25 |
| 50 / 400 ms | 4,913.25 | 4,905.75 | 4,926 | 4,889.75 | 219.5 | 4,794.5 |
| 250 / instant | 905.25 | 948.25 | 937.5 | 888.25 | 338 | 804.75 |
| 250 / 400 ms | 5,040.75 | 4,992.5 | 5,052 | 5,007 | 291 | 4,718.25 |

For example, the largest favorable median shift, 81.75 ms for both changes at
50/instant, is below its 382 ms repeat spread. No individual matched Object clears
the full-range check for a warm driver saving. The [comparison tables](candidates/tables.md)
and compressed `candidates/comparisons.json.gz` contain the complete paired results.

The row-view experiment used fresh cohorts, with main/repeat/candidate interleaved
on each physical Object. Its absolute times must not be compared with the first
experiment to infer a gain.

| Seed / provider | Main | Row-view candidate | Main − candidate | Baseline repeat spread | pi alongside |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / instant | 625.5 | 650.5 | −25 | 239.5 | 779.5 |
| 50 / 400 ms | 4,885.5 | 4,887.25 | −1.75 | 215.5 | 4,675.25 |
| 250 / instant | 1,079.25 | 1,079.5 | −0.25 | 393.5 | 695.5 |
| 250 / 400 ms | 5,001.5 | 5,050.5 | −49 | 235.5 | 4,874 |

**The remaining gap to pi is not resolved as a fixed library cost.** The observed
main-minus-pi medians are 195.25 / 100.5 ms at 50 / 250 instant in the first cohort,
and −154 / 383.75 ms in the second. At 400 ms they are 118.75 / 322.5 ms and
210.25 / 127.5 ms respectively. These are alongside comparisons with placement
variation, not causal estimates. No change was retained, so this work establishes
no reduction in that gap. It does not add effects from earlier PR measurements.

## What remains at the two ends

The [map captured before production edits](baseline-map.md) extends the existing
[admission inventory](https://github.com/yielded-dev/agent/blob/dan/prod-admission-latency/examples/durable-bench/results/prod-admit/await-inventory.md)
and [turn map](https://github.com/yielded-dev/agent/blob/dan/prod-turn-latency/examples/durable-bench/results/prod-turn/baseline-map.md).
The final [diagnostic timing table](diagnostic-timing.md) retains before/after
receipt-to-pass, receipt-to-first-model and last-model-to-client observations and
their clock limits.

The 50 ms pre-arm was a bounded recovery fallback: the original implementation
used the earliest jittered retry as a deadline, with a separate best-effort immediate
wake for promptness. It was not a transaction isolation mechanism. Generation
fencing, active-mutation tracking and storage gates protect unfinished admission.
After #825 the fallback is also the normal admission alarm. Nevertheless, it is
chosen **before** admission's remaining durable work, not 50 ms after the caller
receives its receipt. The initial map observed pass beacons before or within a few
milliseconds of receipt delivery; it did not find a universal 50 ms post-receipt wait.

Immediate pre-arming can overlap current-turn processing with receipt delivery.
On one matched 250/instant Object, admission rose from 57.5 to 233.5 ms, while both
total-turn medians were 762.5 ms. Receipt-to-first-provider brackets moved from
149–296 ms across baseline samples to 3–20 ms for the candidate. Model dispatch
relative to submission did not show a corresponding advance. The
[gate map](gates-and-retirement.md) separates the observed boundary shift from the
inference that native output gating and concurrent current-turn work explain it.

The candidate changed only the mutation pre-arm deadline in `Alarm.ts`. It retained
wake deferral, due-queue retry floors and the pass fallback. The pinned public
[workerd scheduler](https://github.com/cloudflare/workerd/blob/baeb40cf80e31cd8588037bda77a20869387a4b9/src/workerd/io/worker.c%2B%2B#L4098-L4160)
distinguishes cancellation of scheduled deliveries from a handler already marked
running; this is not proof of Cloudflare's deployed private revision. Canceled
deliveries remain in the outcome inventory. A deadline change is not a reason to
remove the existing deferral rules.

Starting another in-process execution immediately after durable admission was not
implemented. It would introduce a second processing trigger alongside the required
pre-armed alarm, with scope and ownership coordination to preserve. The simpler
deadline experiment already allowed processing to overlap receipt delivery without
a resolved driver benefit. This result does not rule out a different dispatch
design; it does rule out treating the configured 50 ms as 50 ms of recoverable
post-receipt idle time.

The settlement tail consists of:

1. A canonical append already combining the final model response, `RunCompleted`
   and its continuation. There was no separate completion append to remove.
2. The `SubmissionSettled` append and continuation, followed on main by ledger
   finalization and ownership release. The atomic candidate combined those writes
   for ordinary roots only, preserving notification ordering for joined, child,
   worker and message submissions and enrolling host lanes before publication.
3. Two keyed terminal-authority reads by the waiter, including finalization replay
   and materialization of canonical disposition/usage. These are logical reads,
   not two demonstrated replication barriers. They are not full-history scans.
4. Gated RPC delivery and maintenance retirement. Boundary snapshots show one
   read transaction and two write transactions after the endpoint body's logical
   return. An endpoint-body return is **not** client-visible settlement.

Maintenance's generation acknowledgement and final alarm disposition still carry
durable work. The pass permit is not an admission lock; the owned-state reader
gate, SQL connection permit, native input gate and synchronous JavaScript can block
admission. The source audit found no ordinary-root full-history scan after the
settlement hint. See [gates and retirement](gates-and-retirement.md) for the exact
ownership boundaries and remaining joins, notifications and scope cleanup.

The additional re-benchmark signal did not justify assuming that the next measured
submit waited for the previous alarm. Its 60 accepted turns, our 32-turn initial
map, and the new admission snapshots had no active previous alarm at admission
entry. Both controllers collect metrics between turns. This does not bound a native
input-gate wait or prove the same behavior for a client immediately submitting its
next input without diagnostics.

## CPU, cold behavior and proof

The [CPU table](cpu.md) reports submit, await and alarm invocation CPU with matched
coverage and repeat spread for every candidate. For example, at 50/instant in the
row-view cohort, submit was 5.5 → 6.75 ms, await 158.5 → 170 ms and alarm 81 →
95.75 ms. No CPU improvement cleared the full repeated-baseline range. Cloudflare
can attribute concurrent alarm work to an awaiting RPC; these columns are not an
additive decomposition of the turn.

[Cold driver observations](cold.md) are retained for both history sizes and provider
delays. Each requires an acknowledged eviction and changed Object incarnation, but
not a fresh isolate. One cold sample per variant per Object is too sparse to prove
absence of regression; no candidate is being shipped on that basis.
The clearest adverse observation is immediate pre-arming at 250/instant: the cold
median rose from 1,223.5 to 1,918.5 ms, a 695 ms increase against a 236 ms cold
repeat spread. That remains a regression signal, not a claimed improvement hidden
by the warm aggregate.

The primary comparisons completed **704/704 planned turns** (432 first experiment,
272 row-view experiment), with no transcript, version, seed, cold-incarnation or
production-alarm placement failure in accepted cohorts. The 32-turn map, 128-turn
diagnostic pass and 60-turn clock-calibration continuation supply another 220
accepted attribution turns. All 48 warm continuation turns have valid conditional
provider/driver bounds; the original diagnostic warm bounds remain unresolved.
Each measured turn makes nine
model requests and eight read-only tool calls. The seed fingerprints are
`b017b487524e44a4` and `dcea9f30b0917245`; the reducer independently recomputes each
subsequent transcript. The append-only log, batch hash chain, authority fences,
leases, Unknown handling, recovery digest and accounting remain intact. No
`allowUnconfirmed` or relaxed durability was used.

The [validation record](validation/results.json) includes the full `vp run ready`
gate for all candidates together, for the row-view change alone and for the final
evidence-only diff. Existing atomicity/recovery assertions were adapted and first
failed against the old boundary before implementing the atomic candidate; those
test edits were discarded with it. No new test suite or production mode remains.

## Evidence and limits

Two task-prefixed Workers and task-prefixed namespaces were deployed through
[Alchemy](stack.ts). The driver used `aws:us-west-1`, Objects used the `wnam` hint,
and compatibility date was `2026-08-18`. Each comparison puts baseline, repeat and
candidate functions in **one Worker bundle and the same Object**; variant order
and framework order are randomized from retained seeds. Four warm repetitions per
variant follow cold and settling positions. Canonical history grows across those
turns in the same scheduled order. The 400 ms provider also has 10 ms stream-frame
spacing, totaling 4,130 ms programmed wait across the nine model calls.

Driver `Date.now()` intervals have millisecond resolution. Fractional medians are
arithmetic, not sub-millisecond precision. Workers clocks advance at I/O and can
lag other invocations; raw Object-clock differences are not wall-time components.
Provider/driver bounds require consistent before/after echo intervals and matching
colos. Missing or inconsistent brackets remain missing. A phase beacon's arrival
is an upper bound on its source event, with output-gate and network delay. Beacon
turns are excluded from primary latency claims.
The final timing continuation routes warm echoes through the existing Object to
match provider colos; its wider intervals and extra I/O are disclosed separately.
[Cloudflare clock behavior](https://developers.cloudflare.com/workers/runtime-apis/performance/),
[storage gates](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options-1).

The [outcome inventory](outcomes.md) retains every captured non-ok invocation, failed request,
controller error, missing join and excluded cohort. Two incomplete Object pairs
were retired, never replayed: an old mock-provider schema rejected the new pre-arm
label, and a later settled pi request changed incarnation during a warm series.
Their 50 attempted turns remain excluded evidence. The diagnostic read after the
provider failure returned 409. An undeployed bundle with a formatting-sensitive
probe anchor was rejected before upload; subsequent comparison builds require all
variant selectors. A successful 10,458 ms turn remains in the data without trimming.
Local gate failures and their resolutions are recorded separately in validation.

`resources.json` records exact deployed bundle hashes, version/namespace continuity
proof and account name. `build-identities/` retains exact compressed module bytes,
input hashes and source snapshots. `experiments/` retains the rejected patches;
`requests.jsonl.gz`, `attempted.jsonl.gz` and telemetry preserve the observations.
Large JSON/JSONL/log artifacts use `.gz`; [the reducer](analyze.mjs) reads either form.

To reproduce, check out this evidence branch, apply the three implementation
patches in `experiments/` to the recorded baseline, install with `vp install`, and
run the bench's `vendor` task. Use `vp exec node --experimental-transform-types`
for `build.mjs`, `run.mjs` and `analyze.mjs`; Cloudflare actions run through
`vp exec direnv exec .`. Preserve the per-phase plans, never replay an attempted
input, keep state outside the repository, and finish with the controller's cleanup
action. A new run needs a fresh results directory/resource identity, not the
recorded resources.

**Cleanup was verified in the same personal account.** Both Worker GETs returned
404, both namespaces were absent, and the account-wide prefix inventory contained
zero Workers and zero namespaces. [cleanup.json](cleanup.json) records those API
checks and removal of the private Alchemy state. The final credential scan passed
for raw and decompressed artifacts before private state removal. No `rebench` or
`cold-turn` resource or construction code was changed.
