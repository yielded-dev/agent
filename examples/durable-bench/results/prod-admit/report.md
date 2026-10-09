# Cloudflare admission: prod-admit

Measured on 8 October 2026, from `origin/main` at
`d52ee32a51c7bf93a226272578f13cebcc66f2e1`.

Admission spends most of its elapsed time inside the Object invocation while CPU
remains small. The useful distinction is **local transaction completion versus
the output gate's durability confirmation**. The default seeded path awaits four
native transactions and writes the alarm twice: first for `now + 50 ms`, then for
`now`. These are not four measured replication round trips. Cold acquisition adds
five native transactions, three independent storage-layout checks, and runtime
construction before the endpoint runs.

The kept fix suppresses the optional alarm reschedule during `runtime.submit`,
inside the existing mutation gate. It reduces admission from **four transactions
and two alarm writes to three and one**, preserving the confirmed receipt. It
cleared the predeclared noise threshold on **3/8 warm Objects and 2/8 cold
Objects**. The other eleven Object/state comparisons are inconclusive, including
small negative median differences; none exceeds the baseline range as a regression.

| State / seed history / delay / Object | Baseline receipt (ms) | Kept (ms) | Saved (ms) | Baseline / candidate full ranges (ms) | Submit CPU before → after (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| warm / 50 / 0 / o0 | 253.5 | 56.5 | 197 | 114 / 23 | 13.5 → 10 |
| warm / 50 / 400 / o1 | 245.5 | 54.5 | 191 | 182 / 29 | 13 → 11 |
| warm / 250 / 0 / o0 | 439.5 | 69 | 370.5 | 319 / 22 | 19 → 19 |
| cold / 50 / 0 / o0 | 600 | 405 | 195 | 134 / 8 | 69 → 47.5 |
| cold / 50 / 400 / o1 | 593.5 | 401.5 | 192 | 58 / 31 | 60.5 → 52.5 |

Warm has eight baseline and four kept-fix requests per Object; cold has four and
two. All five gains exceed both baseline and candidate repeat ranges. These are
conditional receipt-latency results, not a universal savings claim.
[All sixteen kept-fix comparisons, including inconclusive results](prototype-comparison.md#prearm-only).
The pre-arm delay remains configurable: 50 ms by default; large custom
`alarmBackoffBase` values can delay healthy processing.

**No end-to-end turn improvement is established.** The contemporary paired
warm gaps to pi on Object 0 remain about **552, 318 and 686 ms** for 50/0,
50/400 and 250/0. The 250/400 pi comparison is inconclusive because its repeat
range is 2,352 ms. Admission intervals overlap other work; savings cannot simply
be subtracted from the earlier production report's ~530 ms total gap.

| Seed history / delay | Yielded baseline turn median | Kept turn median | Pi median on matching kept samples | Paired kept-minus-pi median | Pi / kept full ranges |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 / 0 | 1216.0 | 1081.0 | 561.5 | 551.5 | 303 / 430 |
| 50 / 400 | 5239.5 | 5217.5 | 4920.0 | 317.5 | 193 / 104 |
| 250 / 0 | 1379.5 | 1391.0 | 696.5 | 685.5 | 323 / 126 |
| 250 / 400 | 5107.5 | 5163.0 | 5670.0 | -547.5 | 2352 / 166 |

All values are milliseconds. The paired gap uses the same sample index in the
randomized framework order, so it need not equal a subtraction of column medians.
Pi and Yielded are necessarily different physical Objects; only admission variants
share an Object. The negative 250/400 point estimate is not a claim that Yielded
is faster. Product changes are proposed in an unmerged PR; evidence remains on
its separate branch.

## Before and after, in execution order

The initial map was frozen in [map-before.md](map-before.md), before product edits.
[await-inventory.md](await-inventory.md) lists every Effect/JavaScript await edge,
SQL/read/decode dependency, source link, and non-default branch. The table below
describes new input on an already seeded Thread, with default publication.

| Step | Baseline work before the next step | Kept change (`prearm-only`) |
| --- | --- | --- |
| Driver | Encode input/request; await native `submitEncoded` RPC | Unchanged |
| Cold Object only | Constructor/layer acquisition behind `blockConcurrencyWhile`; three layout transactions, native-lane registration, alarm reconciliation | Unchanged: five extra transactions |
| Endpoint and limits | Decode, placement validation, retained-submission lookup, input-byte bound, nonterminal control scan, database-size read | Unchanged |
| Mutation gate / T1 | Generation permit; maintenance KV get/put; dirty due queue; get/set alarm; finish transaction before invoking mutation | Unchanged, including its configured deadline and retained retry floors |
| Input | Schema encode/decode, canonical JSON and WebCrypto input digest, admission-request validation | Unchanged |
| Admission / T2 | Replay/authority/fence checks; queue allocation; append admission row with `RETURNING`; source-atomic progress KV/due-queue writes and alarm check | Unchanged |
| Materialize / journal | Inspect materialization metadata and canonical tail; seeded Thread already exists | Unchanged; no canonical batch append in this seeded branch |
| Readiness / T3 | `UPDATE … RETURNING`; retain required publication intent when configured; source-atomic progress and alarm check | Unchanged |
| Wake / T4 | Notify local listeners; `scheduleNow` opens an async transaction, reads alarm, moves it from `now + 50` to `now` | Local notifications still run; existing wake deferral skips the optional native scheduling hint while admission is covered by its pre-arm |
| Publication | Default `publishCommitted` has empty invalidate/drain Effects | Unchanged |
| Return | Release mutation bookkeeping; encode response; native RPC/output gate; client decode and receipt extraction | Unchanged confirmed-write boundary |

The wake is inside `runtime.submit`, **before** default `publishCommitted`, rather
than caused by default publication. `ensureScheduledBy` has its own asynchronous
`getAlarm`/`setAlarm` transaction, but the measured submit branch does not call it.

Canonical admission is persisted in the submission ledger. A previously empty
Thread additionally materializes, appends `ThreadCreated`, and wakes during
initialization. Input application and the turn's canonical event batches happen
after admission. The seeded measurements must not be represented as timing a
nonexistent canonical history append. The batch hash chain, producer fencing,
claims, leases, original-context `historyDigest`, and accounting are untouched.

Baseline native calls per seeded admission are **4 async transactions, 0
transactionSync, 3 KV gets, 3 KV puts, 4 getAlarm and 2 setAlarm**. Warm SQL counts
are 5 at seed history 50 and 11 at 250. Cold endpoint SQL counts are 10 and 14,
plus 12 constructor SQL statements and one constructor KV get. The kept change
consistently has **3 async transactions, 0 transactionSync, 3 KV gets, 3 KV puts,
3 getAlarm and 1 setAlarm**. SQL counts are unchanged. Including construction,
cold admission goes from nine native transaction calls to eight.

## What the deployed clocks can establish

The unmodified map completed 48/48 turns. Warm admission medians at 50/0, 50/400,
250/0 and 250/400 were **218, 212.5, 317 and 521 ms**; corresponding cold medians
were **683, 505, 736.5 and 1010.5 ms**. Warm submit CPU was 13.5, 8 and 16 ms in
the first three conditions; the last condition had an incomplete CPU join. These
reproduce the large elapsed/CPU gap, not the exact original Object's latency.

The repeated unmodified baselines in the separate diagnostic give the strongest
location evidence. Values below are medians in milliseconds; the last column is
computed per request, rather than by subtracting medians.

| History / delay | State | Driver admission | Submit invocation wall | Submit CPU | Driver minus invocation |
| --- | --- | ---: | ---: | ---: | ---: |
| 50 / 0 | Warm | 212 | 206 | 7 | 6.5 |
| 50 / 400 | Warm | 350.5 | 342.5 | 14 | 8 |
| 250 / 0 | Warm | 367 | 361.5 | 10 | 6 |
| 250 / 400 | Warm | 521.5 | 510.5 | 16 | 9.5 |
| 50 / 0 | Cold | 444 | 431.5 | 23.5 | 12.5 |
| 50 / 400 | Cold | 700 | 650 | 73 | 16 |
| 250 / 0 | Cold | 656 | 647.5 | 30.5 | 8 |
| 250 / 400 | Cold | 800 | 790.5 | 49 | 9 |

Warm has eight baseline requests per condition and cold has four. The small
driver-minus-invocation gap rules out ordinary driver/RPC transit as the dominant
warm cost in these samples. The remaining interval is inside the invocation;
constructor work and output-gate confirmation are not separately exposed by the
ordinary trace.

All in-Object timestamps in each initial trace were frozen at one I/O-clock value,
sometimes older than the driver's call start. For example, 50/0 warm m4 recorded
driver start `1791500603292`, endpoint trace `1791500602450`, and receipt
`1791500603499`. Assigning milliseconds to its individual transaction awaits by
subtraction would fabricate a waterfall. Integer milliseconds describe timestamp
representation, not useful precision while the Worker clock is frozen.

A separate randomized diagnostic compared unchanged baseline labels against an
entry-provider echo followed by the normal submit body and an explicit final
`storage.sync()`. It completed 80/80 turns and preserved all 720 model request
fingerprints. Warm final-sync I/O intervals were **92.5, 173.5, 218 and 216.5 ms**
at the same four conditions. Diagnostic latency changes were smaller than the
full baseline repeat ranges; this is an observation aid, not an optimization.
See [diagnostic-admission.json](diagnostic-admission.json).

The sync-start clock may still equal the entry-echo clock, so that interval also
contains intervening body execution; it is not a CPU-exclusive stopwatch around
the native `sync()` call. Nor is it an additive measure of the normal response hold:
`sync()` yields, allowing alarm work to start before RPC return. In the subsequent
candidate matrix's sixteen `clock-sync` probes, snapshots can grow from four to
eighteen native transactions and from zero to one provider call across that wait
([probe rows](candidates-admission-turns.jsonl)). The earlier 80-turn diagnostic
has no such snapshot details. Normal body-return snapshots are retained so
admission counts do not accidentally include later processing. The final paired
diagnostics add output-gated beacon receipts at actual phase boundaries and
adjacent control beacons to expose delivery/order noise. Those final diagnostic
pairs in the candidate and combined-shortcut matrices run in fixed order after
the randomized primary samples. A final, separate sixteen-turn diagnostic shuffles
baseline/kept pairs in each cold/warm state. Neither supplies optimization evidence.

The checked workerd implementation releases a local SQLite savepoint and connects
confirmation to the output gate. Alarm scheduling can precede database persistence;
moving an in-flight alarm earlier can require another scheduling operation, and
pending commits may merge. Thus removing redundant scheduling has a plausible
durability-path effect, but the JavaScript trace does not count backend RPCs.
[Explicit commit and alarm synchronization](https://github.com/cloudflare/workerd/blob/baeb40cf80e31cd8588037bda77a20869387a4b9/src/workerd/io/actor-sqlite.c%2B%2B#L178-L248).

The output gate waits on the locks that existed when the outgoing operation began
waiting. Later alarm writes do not automatically extend that same wait. There are
**zero explicit `storage.sync` calls and one externally enforced response
durability boundary** in normal admission, before and after. Four baseline
transaction promises must finish in source order; no evidence establishes four
sequential remote durability confirmations.
[Output-gate implementation](https://github.com/cloudflare/workerd/blob/baeb40cf80e31cd8588037bda77a20869387a4b9/src/workerd/io/io-gate.c%2B%2B#L344-L360),
[transaction promise and sync implementation](https://github.com/cloudflare/workerd/blob/baeb40cf80e31cd8588037bda77a20869387a4b9/src/workerd/api/actor-state.c%2B%2B#L608-L736).
This is public runtime source inspected on the measurement date, not a claim to
know Cloudflare's deployed private build or replication topology.

The final diagnostic completed sixteen turns with 224 successful admission beacon
receipts. Below is one warm 50/0 pair, in **provider arrival milliseconds after
each request's entry beacon**. These are not step durations and cannot be summed.

| Boundary that emitted the beacon | Baseline arrival | Kept arrival |
| --- | ---: | ---: |
| Entry | 0 | 0 |
| Limits complete | 11 | 12 |
| Pre-arm complete | 158 | 133 |
| Input digest complete | 163 | 131 |
| Admission row complete | 157 | 134 |
| Materialization / existing Thread check complete | 162 / 157 | 134 / 134 |
| Ready row complete | 159 | 132 |
| Wake complete | 163 | 135 |
| Publication complete | 163 | 138 |
| Response encoding complete | 164 | 138 |
| Native submit body returns | 165 | 138 |
| Diagnostic final sync returns | 174 | 147 |

The post-write beacons cluster after a long output-gated interval, despite frozen
local timestamps and some reversed arrival order. This exposes the durability/
transport hold without attributing it to individual transaction promises. Adjacent
entry-control beacons differ by −2 to +2 ms across all sixteen samples; that is
observed delivery noise, not a general accuracy guarantee. Driver admission in
this diagnostic pair is 218/206 ms and final-sync I/O intervals are 92/68 ms;
these perturbing, single-sample observations are **not savings evidence**.
[All eight cold/warm condition pairs, epochs and limits](phase-timestamps.md),
[normalized timestamp data](phase-timestamps.json).

## Prototype decisions

The first candidate matrix isolates two mechanisms in the same Worker bundle:

- **`prearm-only`** suppresses the final wake transaction using existing wake
  deferral, while leaving the first alarm at the configured half-backoff delay.
  It removes one transaction and one alarm write. This is the kept, simpler fix:
  the omitted wake is explicitly optional, and the existing durable deadline and
  generation checks preserve liveness. Its configurable promptness tradeoff is
  documented below.
- **`prearm-now`** moves the first alarm immediately but retains the final
  transaction. This leaves four transactions and one alarm write. Its measured
  gains are less consistent; the unchanged transaction remains redundant.
- **`prearm-fast`** combines immediate prearming with a checked shortcut around
  the final transaction. It adds no cache, public mode, unconfirmed write, or
  object-wide admission wake deferral. Its fallback retains transactional
  scheduling when needed. Its full comparison found no warm improvement beyond
  the repeat range and highly variable cold results; it is rejected.

The first matrix completed **336/336 turns**, including 96 verified cold
incarnations, with all 3,024 provider fingerprints matching. Wake suppression
exceeded the full baseline range in five of sixteen Object/state comparisons;
immediate prearming alone did so in two, both cold, but those two differences
were smaller than the candidate's own range and establish no gain. Neither
showed a median regression larger than the baseline range. The clearest warm suppression results were
253.5 → 56.5 ms (197 saved versus 114 spread), 245.5 → 54.5 ms (191 versus 182),
and 439.5 → 69 ms (370.5 versus 319). These are specific Objects, not a general
per-turn savings claim. [All prototype comparisons](prototype-comparison.md).

The combined shortcut completed **264/264 turns**, 72 verified cold incarnations,
and all 2,376 fingerprints. None of its eight warm differences clears the baseline
range. At 50/400 its two Objects have cold regression point estimates of
359.5 and 285.5 ms, exceeding baseline ranges of 94 and 144 ms but smaller than
candidate ranges of 812 and 593 ms. One cold 50/0 Object's 247.5 ms improvement
exceeds its 188 ms baseline range but not the candidate's 280 ms range. None
establishes a change beyond both observed spreads; this does not justify keeping
the extra mechanism. [All combined-shortcut comparisons and CPU](fast-comparison.md).

Wrapping the whole admission in a native transaction was rejected: the gate
explicitly requires prearming outside source SQL transactions, and its generation
and shared-connection lock order would need redesign. Four promise completions
alone are not evidence that this redesign would remove four replication waits.
`transactionSync`/`ctx.storage.kv` cannot simply substitute for callbacks that
await the alarm API and compose source Effects. No such unmeasured performance
claim is made. Combining source writes is a possible separate architecture task,
not a reason to weaken the prearm or receipt contract.

The ledger already updates owned row views from `RETURNING`; the readiness path
does not reread a just-written row from SQL. Above 128 retained submissions,
admission can repeat an overflow lane scan. Removing one scan might reduce CPU,
but cannot be reported as removing a durability barrier. Cold graph acquisition
also repeats layout inspection through three services. Those storage-owner
refactors were not mixed into the alarm fix. Optional lifecycle publication can
flush a canonical suffix; it is absent from this benchmark and is not covered by
the default four-transaction count.

The kept behavior change is confined to `ThreadObject.submit`: run
`runtime.submit` with existing wake deferral **inside** the mutation gate, then
run publication **outside** deferral and still inside the gate. Other runtime
changes in the product diff are absent; alarm/config edits are API comments.
Maintenance-pass execution, model/tool processing, settlement waiting, and the
sibling `prod-turn` code were not edited. The existing comment attributing
all earlier-alarm writes to cancellation of a *running* handler overstates the
public runtime evidence: queued deliveries may be canceled/replaced. Canceled
delivery telemetry alone does not prove interrupted Attempts.

The deferral counter is Object-wide: an overlapping mutation can temporarily lose
its optional `scheduleNow` acceleration too, while retaining its own pre-arm.
Local subscriber/PubSub notifications are unaffected. The pre-arm minimum is
`max(1, ceil(alarmBackoffBase / 2))`, **50 ms by default**. `alarmBackoffCap` does
not clamp it, and large custom base values can delay healthy work. Existing earlier
alarms and retry floors still govern scheduling; this is a configured deadline,
not an elapsed-time delivery SLA. A caller's settlement timeout can expire while
accepted work remains durable. Custom hosts invoking the exported
`ThreadObject.submit` Effect must also provide their `DurableAlarmService`; the
native ThreadObject layer already does. These consumer-facing changes are in the
guide, API comments, and one-sentence changeset.

## Evidence, comparability and limits

All timing comes from deployed Cloudflare Workers created with the copied
Alchemy stacks. Account: **Danieljmerwe@gmail.com's Account**, loaded from this
checkout's `direnv exec .`. Driver placement is **`aws:us-west-1`**; Objects use a
`wnam` location hint. Workers are `prod-admit-1c518729-primary` and
`prod-admit-1c518729-provider`; their stacks and namespace names use the same
task prefix. Account/namespace identifiers, tokens and Alchemy state are omitted
from public evidence. State stayed in a private mode-700 temporary directory and was removed after
verified cleanup.

The observer subclasses real `ThreadObject` endpoints. The driver calls
`CloudflareThreadClient.submit` and stops admission timing when its receipt is
returned. It separately waits for settlement. The timer read is placed after
definition hashing, but the clock remains anchored at the preceding probe's I/O
completion: intervening hashing and client setup can be included in the reported
interval. This is an I/O-clock interval through receipt, not a finer stopwatch
around the RPC alone. Clock-probe network time and later diagnostic payload reads
are outside that interval. There is
no local wall-time evidence. The public production target from #821 is retained
on the evidence branch; no benchmark changes enter the product PR.

Baseline A and B execute identical code. Candidate assignments are randomized
within each physical Object, with repeated cold resets and warm interleaving in
one verified bundle. Cold means `storage.sync` plus acknowledged `ctx.abort`, a
new Object incarnation, its first harness request, and no prior alarm start;
it does **not** guarantee a new isolate or storage machine. Seed history is 50 or
250 and measured turns append in order; comparisons therefore interleave on the
same growing history rather than restoring it between samples.

A positive speed claim must exceed the **entire pooled baseline repeat range**
for that Object and state. Claimed gains also exceed the candidate's own observed
repeat range; the independent A/B median spread is recorded too.
This conservative noise rule was set before the comparisons; it is not a
statistical significance test. Outliers remain. CPU has explicit sample counts
and missing joins, and is Cloudflare invocation attribution rather than semantic
phase accounting. Object-to-Object latency differences are not averaged away.

Seed fingerprints are **50 `b017b487524e44a4`** and **250 `dcea9f30b0917245`**.
Every measured turn's nine model-visible transcripts are independently hashed.
Variant selection travels only in the opaque idempotency key, leaving the model
input unchanged. An explicit Effect context owns selection because ambient
AsyncLocalStorage is insufficient across Effect resumption. The original encoded
argument is registered before native dispatch and checked at the endpoint;
every measured candidate must report its selected variant correctly.

Exact uploaded bundle bytes, source identities, transformed product sources,
and transform scripts are archived under [build-identities](build-identities/).
The current build recipe is [build.mjs](build.mjs); its final comparison inputs
and dependency locks are archived separately. Earlier bundles do not all have an
independently archived copy of that build driver.
Baselines and candidates use identical observers. The initial map and diagnostic
precede all product edits; the product fix lives in a separate worktree based on
main, with no evidence in its diff.

Raw records: [attempted requests](attempted.jsonl), [responses](requests.jsonl),
[primary telemetry](telemetry-primary.json), [provider telemetry](telemetry-provider.json),
[resource metadata](resources.json), and the phase plans/completion files.
`analyze.mjs` performs telemetry joins and transcript/cold checks;
`reduce-admission.mjs` computes per-Object comparisons. A request is recorded
before dispatch. An uncertain outcome is not silently retried under the same
input identity.

Across all five phases, **744/744 turns qualified**, including **208 verified cold
incarnations**, with **6,696/6,696 matching model request fingerprints**. There are
no missing planned turns, uncertain/unreturned attempts, application failures,
controller failures, integrity failures, or unverified warm cohorts. These counts
include diagnostic and settling turns, not just primary comparisons.

The final telemetry contains 29,467 events and 16,533 invocations. Every non-ok
invocation is retained in [the full outcome ledger](prearm-probe/failed-outcomes.json):

| Non-ok outcome | Count | Interpretation |
| --- | ---: | --- |
| Durable Object fetch aborted | 276 | Explicit `/cold` resets/releases requested by the harness |
| Alarm delivery canceled | 762 | Retained as observed; does not prove a running Attempt was interrupted |
| Stateless fetch canceled | 4 | Three seeds and one pi identity keepalive; controller receipts succeeded, but cancellation attribution remains unresolved |
| Provider readiness HTTP 404 | 2 | Initial propagation checks; readiness subsequently succeeded |
| Recorded version-metadata API HTTP 500 | 4 | Fallback verified active version and exact uploaded bytes; older occurrences were not individually logged, so their exact count is unavailable |

Join limitations are preserved rather than converted to zero CPU or missing
work. Per-phase join-failure counts are 100, 196, 507, 397 and 52; many are optional
publication-end markers or end-log context disagreement. Nine Yielded submit CPU
joins are missing across 544 Yielded turns; the 200 pi turns have no submit
invocation by design. All five claimed gain rows have complete CPU coverage.
[Outcome inventory, phase totals and missing CPU rows](outcomes.json).

**Cleanup is API-verified** in the recorded personal account: both Workers return
404, no task namespaces remain, and an account-wide prefix scan returns no
`prod-admit` Workers or namespaces. Private Alchemy state was removed. The exact
account/namespace/token scan passed, including decompressed archives. Later
summaries derive only from those sanitized records. See [cleanup.json](cleanup.json)
and [secret-scan.json](secret-scan.json). The task-owned local validation database
was also removed; its check is in [validation.json](validation.json).

The product change is isolated at
[commit `16406112`](https://github.com/yielded-dev/agent/commit/1640611291eff448dc293797cb503c0e0ed287d1),
branch `dan/prod-admit-prearmed-wake`, against `main`. No evidence files enter that diff.

The full product gate is recorded in [validation.json](validation.json):
`vp run ready` passed, using Vite+'s managed Node 24 and access to the repository's
task-owned local PostgreSQL service. Earlier attempts exposed the system Node 26
flag mismatch, sandbox loopback denial, and a shared PostgreSQL service disappearing.
The final gate passed with 71 tasks (56 cache hits); the temporary database was then
removed and its absence verified. No tests or unrelated code were changed.
Existing Cloudflare eviction, rollback, generation, retry, publication and
acknowledgement recovery checks were reused. No tests or test infrastructure were
added for the timing investigation.
