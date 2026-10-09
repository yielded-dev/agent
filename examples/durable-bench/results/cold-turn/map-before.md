# Cold construction before changes

The deployed baseline adds **5 native transactions, 12 SQL statements and 1 KV read** when an existing Thread Object wakes. None of those five transactions wrote in this settled-history fixture. The first turn then pays **34 further transactions**, exactly as warm turns do. Cold initialization also repeats three full layout inspections and their schema normalization. These are candidates for removal, not five demonstrated storage round trips.

This map was frozen before any product edit, against `b246f8aaa3a92d5f82934b1fc7a82356d1ad6664` (main including #823, #825 and #826). It extends [prod-admit's await inventory](https://github.com/yielded-dev/agent/blob/dan/prod-admission-latency/examples/durable-bench/results/prod-admit/await-inventory.md) and [prod-turn's per-step map](https://github.com/yielded-dev/agent/blob/dan/prod-turn-latency/examples/durable-bench/results/prod-turn/baseline-map.md). The baseline bundle is `895bf46bc1ce74e64b132f826fe565e69c13630c0689d21baf4e87d56ac9a3bc`; its original and effective source archives are in `build-identities/`.

Cold is a confirmed `storage.sync()` followed by `ctx.abort()` of the same persisted Object, then its first submit in a different incarnation, with no previous harness request or alarm handler. The telemetry constructor event must not contradict that proof. Isolate identity is recorded separately: **all mapped forced resets retained their module isolate**. This experiment therefore establishes cold Objects, not fresh isolates. The harness's one synchronous KV read to select the experiment variant precedes framework construction in every variant and is excluded from framework counts.

## Deployed observations

Driver submit-to-settlement / receipt medians, milliseconds; diagnostic samples include boundary beacons and are not the later optimization comparison:

| Seed turns / provider delay | Cold driver / receipt | Warm driver / receipt | Cold submit CPU | Warm submit CPU |
| --- | ---: | ---: | ---: | ---: |
| 50 / 0 ms | 1,601.5 / 785 | 821.5 / 161.5 | 47.5 | 7 |
| 50 / 400 ms | 6,282 / 528 | 5,183.5 / 264 | 141 | 12.5 |
| 250 / 0 ms | 2,072.5 / 934.5 | 1,044 / 270 | 41 | 8.5 |
| 250 / 400 ms | 6,491.5 / 1,029.5 | 5,297 / 425 | 120.5 | 19.5 |

There are two cold and four warm samples per condition, except one cold sample conservatively excluded at 50 / 400 ms. Submit CPU joins are present for 7/7 included cold and 15/16 warm samples; these are invocation CPU, not construction-only CPU. The 50 / 400 ms Object is excluded as an incomplete cohort by the strict comparison reducer; its remaining individually valid points appear here only as map diagnostics.

Every turn produced nine matching provider receipts: 32/32 transcripts, 288/288 streams, seed fingerprints 50 `b017b487524e44a4`, 250 `dcea9f30b0917245`. The provider's 400 ms first-token case retains 10 ms chunk spacing. Thirty-one requests have individually valid entry proofs; seven of eight intended cold requests have uncontradicted cold proof. For `cold-turn-map-h50-d400-o0/m1`, application entry says no earlier alarm, but the constructor and submit logs attach to an alarm invocation. The evidence cannot distinguish platform log attribution from an alarm-triggered wake, so that point remains excluded; no proof rule was relaxed.

The account is **Danieljmerwe@gmail.com's Account**. Mapping used only `cold-turn` resources. Native and driver clocks represent integer milliseconds; native clocks freeze through synchronous work and most constructor promises. They cannot time SQL execution, parser compilation or individual replication waits. Provider beacons locate output-gated boundaries but add network activity and can arrive out of order.

## Ordered native transactions

The order below is the observed 50 / 0 ms cold `m1` trace. Detailed SQL, every recorded await edge, counts, calls, driver timestamps and provider beacons are in [`map/ordered-awaits.json`](map/ordered-awaits.json). [`await-inventory.md`](await-inventory.md) ties synchronous Effect work and uninstrumented crypto awaits to the same order. No transaction callback overlap was observed.

| Cold transaction number | Operation | Warm pays it? | Writes in this fixture? |
| --- | --- | --- | --- |
| 1–3 | Three journal ports independently inspect the same layout: sqlite_master, legacy version row, current header | No | No |
| 4 | Register native maintenance lanes; all lanes already present | No | No |
| 5 | Constructor `ensureAlarm`: decode maintenance KV, inspect retained due queue | No | No |
| 6 | Pre-arm maintenance before admission's first durable mutation | Yes | Yes |
| 7 | Admit Submission and persist receipt, enroll source progress | Yes | Yes |
| 8 | Mark Submission ready, enroll source progress | Yes | Yes |
| 9 | Begin alarm pass and protect its deadline | Yes | Yes |
| 10 | Checkpoint work recovery discovery cursor | Yes | Yes |
| 11 | Select native lane / read work recovery cursor | Yes | No |
| 12 | Claim Submission, advance producer fence, persist attempt and lease | Yes | Yes |
| 13 | Append input fact and continuation; canonical accounting and hash chain | Yes | Yes |
| 14 | Mark input applied in ledger | Yes | Yes |
| 15 | Append RunStarted / RunContextInitialized and continuation | Yes | Yes |
| 16 | Initial join drain | Yes | No |
| — | First model request | Yes | — |
| 17–32 | Eight readonly tool cycles: response/tool/continuation canonical commit, then join drain, then next model request | Yes | Eight writes, eight empty joins |
| 33 | Final join drain | Yes | No |
| 34 | Final response / RunCompleted canonical commit | Yes | Yes |
| 35 | Append SubmissionSettled with canonical proof | Yes | Yes |
| 36 | Finalize ledger settlement, release ownership, signal waiter | Yes | Yes |
| 37 | Read source generation after native work | Yes | No |
| 38 | Checkpoint native lane / maintenance generation | Yes | Yes |
| 39 | Finish maintenance event and remove its alarm | Yes | Yes |

Transactions 37–39 and final metrics can overlap or follow the client's settlement return. Total turn counts therefore describe the observed invocation work, not an additive critical path. Twelve canonical batches contain 34 records. Before the first model request, cold has 16 transactions including construction; warm has 11. Construction has 12 SQL statements; post-construction cold has 227 / 262 at 50 / 250 turns, versus warm 220 / 257. First-use caches account for the additional reads; their validation is required until proven redundant.

## Timestamp example

For the same cold 50 / 0 ms `m1` sample, the driver records submit `1791512945862`, receipt `1791512946669`, and settlement `1791512947306`. All constructor await edges have the native I/O timestamp `1791512946401`; this is **not** evidence of zero CPU or zero storage cost.

Provider-side beacon arrivals are layer start `1791512946440`, runtime services ready `1791512946453`, layer end `1791512946450`, gate ready `1791512946452`, submit entry `1791512946453`, and alarm entry `1791512946669`. The first model reaches the provider at `1791512946697`; the final provider stream ends at `1791512947234`. Settlement-transaction-return and waiter-return beacons arrive at `1791512947307` and `1791512947306`. Cross-clock differences are not component durations. Use the reducer's before/after provider echo bounds for valid driver/provider brackets.

## Outcomes and proposed deletions

The initial telemetry snapshot has 941 invocation records, including **16 aborted fetches** from explicit resets and **61 canceled alarms** across seeding/reset activity. No measured controller request failed. All non-ok records remain in `map/failed-outcomes.json`; cancellations are observations, not silently relabeled successful turns. Forty-nine observation/join issues comprise 30 END-log context disagreements, 2 missing/ambiguous submit joins, 2 await joins, 5 alarm joins, and 10 provider log joins. Provider SSE receipts cover all model calls. The initial deploy's global-scope randomness error, sandbox/network preparation failures, and optional metadata API failures are retained separately in controller/preparation/Alchemy logs.

First candidates: share the existing layout acquisition within one host Layer graph (3 checks → 1), and skip empty lane-registration / already-idle reconciliation transactions without skipping decoding or recovery-event hydration. Inspect an existing due-queue schema before issuing no-op CREATE. These changes must retain independent-open refusal, exact SQL-client ownership, the mutation gate, pre-arm semantics, pending recovery repairs, registry changes, corrupt-state refusal, canonical recovery and Unknown behavior. Runtime registry hashing, parser factories and repeated recovery reads remain mapped; no speculative cache is introduced. Pre-arm deadline and settlement-tail changes belong to `warm-floor` and are excluded.
