# Warm-floor: map before changing production code

Baseline: `b246f8aaa3a92d5f82934b1fc7a82356d1ad6664` (current main, including #821,
#823, #825 and #826). Captured before applying either candidate. Account:
**Danieljmerwe@gmail.com's Account**. The deployed network bundle is
`9c92c09e823666c38c1206fd7a43c68ffeb9207432e18fa2d2d98e99ec34450a`.

This extends [prod-admit's await inventory](https://github.com/yielded-dev/agent/blob/dan/prod-admission-latency/examples/durable-bench/results/prod-admit/await-inventory.md)
and [prod-turn's map](https://github.com/yielded-dev/agent/blob/dan/prod-turn-latency/examples/durable-bench/results/prod-turn/baseline-map.md).
It does not compare different Objects with the older reports.

## Counts and ownership

All 32 mapped turns succeeded with the expected transcript, nine provider requests,
eight read-only tools and one observed entered alarm. All model dispatches occurred
in the alarm invocation. Eight cold turns verified an acknowledged eviction and a
new Object incarnation. Seed fingerprints are `b017b487524e44a4` (50) and
`dcea9f30b0917245` (250).

The warm path has **34 native transactions, 22 containing writes**, zero explicit
sync calls, three alarm writes, 12 canonical batches and 34 canonical records.
Counts include maintenance after the client can finish; transactions are not
equivalent to independently confirmed replication barriers.

| Region | Transactions | With writes | Work |
| --- | ---: | ---: | --- |
| Through first model dispatch | 11 | 9 | Three admission transactions, pass acquisition, recovery/ownership, input and Run-start appends |
| Eight model-to-model intervals | 16 | 8 | One read transaction and one canonical append per read-only tool round |
| After last model stream ends | 7 | 5 | One read transaction; final response/completion append; settlement publication; ledger finalization; maintenance read and two maintenance writes |

Admission still prearms before its first durable mutation. Its three transactions
are prearm, admitted receipt and readiness publication. The receipt must be
durable before returning. #825 removed the later immediate alarm pull-forward.

The terminal canonical append **already contains `ModelResponseRecorded`,
`RunCompleted` and `RunContinuation`**. The next append contains
`SubmissionSettled` and its continuation. Ledger finalization then updates the
settled projection and releases ownership in a separate transaction. The awakened
waiter checks receipt identity, replays finalization against canonical evidence,
then reads the canonical envelope again for disposition and usage. Those are the
two terminal authority reads. RPC delivery remains behind Cloudflare's output gate.
Joined settlement handling and maintenance acknowledgement can follow the hint.

## Deployed timing

Driver medians below are four warm observations per Object. Each condition has
one Object in this mapping phase; alternate turns carry asynchronous external
phase beacons. This is attribution evidence, not the later unprobed A/B result.

| Seeded turns / provider | Driver median | Full warm range | Submit CPU | Await CPU | Alarm CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / instant | 548.5 ms | 49 ms | 7.5 ms | 187 ms | 73.5 ms |
| 50 / 400 ms | 4,877.5 ms | 35 ms | 7.5 ms | 254.5 ms | 82 ms |
| 250 / instant | 921.5 ms | 32 ms | 10 ms | 226.5 ms | 158 ms |
| 250 / 400 ms | 5,233 ms | 255 ms | 18.5 ms | 417 ms | 231.5 ms |

CPU is Cloudflare invocation attribution, not additive semantic phase cost. Await
CPU includes work performed while the alarm is running. See `map/turns.jsonl`,
`map/counts.json`, `map/summary.json` and `requests.jsonl` for the retained timestamps,
counts, identities and transcript checks.

The following external receipts have consistent before/after echo offset brackets.
All values are milliseconds relative to receipt delivery or final provider output,
as indicated. A pass beacon is sent at `beginPass` entry. Its arrival is an **upper
bound on entry**, with output-gate and network delay; it is not an exact entry time.
Negative values are retained: a pass can begin before the client receives its receipt.

| Object / sample | Receipt → pass beacon arrival | Receipt → first model request | Last provider end → client settlement |
| --- | ---: | ---: | ---: |
| 50 / instant / m7 | −1…2 | 5…8 | 49…52 |
| 50 / 400 ms / m5 | −3…1 | 28…32 | 78…82 |
| 50 / 400 ms / m7 | −1…5 | 24…30 | 78…84 |
| 250 / instant / m5 | 4…7 | 25…28 | 83…86 |
| 250 / instant / m7 | −10…−5 | 23…28 | 90…95 |

The other three warm beacon observations lack a valid bracket: one has inconsistent
probe intervals and two route model requests to LAX while driver echo probes use
SJC. They remain in the raw evidence and are not silently treated as synchronized.
Thus this run does **not** establish a universal 50 ms wait after receipt. Raw DO
timestamps lag the driver by hundreds of milliseconds and cannot decompose wall
latency. Workers clocks advance at I/O, not during synchronous CPU work.
[Cloudflare clock documentation](https://developers.cloudflare.com/workers/runtime-apis/performance/).

## Bounded experiments selected from this map

1. Change only the mutation prearm deadline from `max(deadline, now + minimumDelay)`
   to `max(deadline, now)`. The original half-backoff was the minimum jittered retry
   deadline, allowing later retries to move the alarm later. An admission safety
   interval is not its durability mechanism: generation fencing and active mutation
   tracking already preserve work if a pass overlaps unfinished admission. Preserve
   due-queue floors, pass fallback, source-progress scheduling and wake deferral.
   Earlier scheduled alarm replacement can still alter contention and canceled
   deliveries; measure those outcomes rather than assuming safety from elapsed time.
2. Reuse the existing finalization body inside the publication transaction for
   ordinary root submissions. Exclude joined, child, worker and message submissions
   so their notification/delivery ordering remains unchanged. Enroll host lanes
   before publication so the same source transaction retains delivery obligations.
   This removes one sequential write transaction; preserve canonical replay checks
   and the later waiter reads in this first experiment.

History: [original prearm policy](https://github.com/yielded-dev/agent/blob/38ac06eea0956d7bef4576c5e527c6053f5a86f0/packages/platform-cloudflare/src/alarm.ts#L358-L360).
Public workerd distinguishes a queued, cancelable scheduled delivery from an already
running, non-cancelable handler; that source is not proof of Cloudflare's private
deployed revision. Existing deferral remains unchanged.
[workerd scheduling states](https://github.com/cloudflare/workerd/blob/baeb40cf80e31cd8588037bda77a20869387a4b9/src/workerd/io/worker.c%2B%2B#L3653-L3666).

## Non-ok inventory at mapping completion

No mapped turn, model response, transaction or transcript check failed. Telemetry
also contains 16 intentionally aborted Object fetches, 68 canceled alarms and two
canceled stateless fetches across seed/setup/map phases. These are retained in
`map/failed-outcomes.json`; cancellation is not equated with successful work.
All 32 model-running alarm/submit/await START joins are available. Their alarm END
logs carry the await RPC context in 32 cases; CPU uses the unambiguous START join
and inventories that disagreement. One controller initialization failed inside the
network sandbox before deployment; its sanitized error is retained. API fallback
and other operational outcomes are recorded separately in `api-non-ok.jsonl`.

The candidate comparison uses repeated identical baseline labels and randomized,
interleaved variants within each Object. No saving below the baseline repeat
spread will be claimed. Cold construction and first-pass code are outside this task.
