# Cloudflare crossover pilot

**Rejected: stability failed and strict telemetry was incomplete (7/8).** All eight measured RPCs succeeded, but `bEffect3` lacked its ingress trace after bounded polling. Its raw DO CPU is retained, not accepted as a qualifying sample. Object A independently fails stability for both forms.

## Actual CPU

Each RPC performed **20,000 complete summaries**. These are Cloudflare DO `cpuTimeMs`, not ingress CPU or client time, in frozen dispatch order.

| Object | RPC slot | Form | CPU, ms |
| --- | --- | --- | ---: |
| A | `aEffect1` | Effect | 1040 |
| A | `aSync2` | Sync | 894 |
| A | `aSync3` | Sync | 772 |
| A | `aEffect4` | Effect | 727 |
| B | `bSync1` | Sync | 444 |
| B | `bEffect2` | Effect | 463 |
| B | `bEffect3` | Effect | 682* |
| B | `bSync4` | Sync | 442 |

\* Successful, untruncated, unique DO event with expected Object/method/version, but missing ingress correlation. All raw CPUs exceed 20 ms and remain below 60 seconds. Exports passed completeness checks with `COMPLETED` status and ABR 1. [Raw observations and export pointers](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/crossover-pilot/raw-cpu.json) retain every result.

## Descriptive comparisons

| Object | Effect mean, ms | Sync mean, ms | Effect / Sync | Sync / Effect |
| --- | ---: | ---: | ---: | ---: |
| A | 883.5 | 833.0 | 1.0606 | 0.9428 |
| B* | 572.5 | 443.0 | 1.2923 | 0.7738 |

B's Effect mean and associated ratios are **provisional raw arithmetic**, including the unqualified observation.

The frozen repeat limit was `max(2 ms, 10% of pair mean)`:

| Object / form | Gap, ms | Gap / mean | Allowed, ms | Verdict |
| --- | ---: | ---: | ---: | --- |
| A / Effect | 313 | 35.43% | 88.35 | Fail |
| A / Sync | 122 | 14.65% | 83.30 | Fail |
| B / Effect* | 219 | 38.25% | 57.25 | Raw fail; unqualified |
| B / Sync | 2 | 0.45% | 44.30 | Pass |

Cross-Worker same-form **mean spread** was **311 ms / 42.72% for Effect*** and **390 ms / 61.13% for Sync**. Percentages use the average of the two Object means; B/A was 0.6480 and 0.5318. Effect's spread is provisional for the same correlation reason.

## Execution and verification

The [plan](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/crossover-pilot/plan.json) was frozen at **18:45:23 UTC**, before deployment. One identical dual bundle served two Alchemy Workers, one Object each. Both complete implementations and fixed profiles were unchanged. Each exact batch path was warmed twice at 20,000 repetitions per Object, before measurement, through the same dispatch/wrappers with one root Effect execution per batch. No counting hooks were present.

All eight warmup and eight measured batches passed full five-profile output, checksum (**486355968**), input-immutability and observed module/runtime-incarnation checks. Optional-seed equality passed during setup. Calls were serial; the last batch finished **70.528 seconds** after the deadline clock started, within ten minutes. Application SQL writes and model/provider API calls were **zero**. No workload retry, replacement, attribution rerun, append/appendix experiment or additional cohort occurred.

This diagnostic establishes neither a 15% gain nor a universal per-Effect cost. Exact warming did not establish stability. Two Objects and two repeats per form cannot establish population performance. Incarnation tokens do not prove placement, JIT tier or GC state; ingress location is not DO placement. RPC CPU includes the shared checksum/output guards.

## Cleanup and retained evidence

**Cleanup complete.** The command exited 1 for missing correlation after Alchemy destroyed both stages. Independent GET-only verification at **18:50:28 UTC** found both Workers 404 and both exact namespace IDs absent across the complete listing. The approved account digest matched. Private recovery state was removed; no experiment process remains. [Cleanup proof](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/crossover-pilot/deployed/independent-cleanup.json) and [final state](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/crossover-pilot/final-state.json) are retained.

Framework baseline was `8c05714d`; private fixture was `1e9bac87`, derived from `67e2ae74`. Exact identities, the four-file patch, single bundle archive, frozen plan, commands, all receipts/exports, [analysis](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/crossover-pilot/analysis.json) and [failure records](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/deployed-01a119d5/crossover-pilot/failure-records.json) are retained here. Static `vp check` and the build passed; no local runtime tests, benchmarks or `ready` suite ran. **Tracked worktree files and commit `58359ea2` remain untouched.** No PR, push, merge or tracker update occurred.
