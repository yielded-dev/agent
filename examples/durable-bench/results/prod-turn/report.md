# Production turn latency — prod-turn

**Keep the single-Thread maintenance dispatch fix.** It saves **902 ms per warm
turn at history 50 and 797 ms at 250 with the instant provider**, against full
baseline repeat spreads of **217 and 225 ms**. Warm 400 ms-provider driver
improvements are **not established above noise**. The measured gaps do shrink in
both provider conditions, and the instant-provider settlement tail roughly halves.

Baseline: `origin/main` at `d52ee32a51c7bf93a226272578f13cebcc66f2e1`.
Account: **Danieljmerwe@gmail.com's Account**. Measurements ran 8–9 October 2026 UTC.
The [product commit](https://github.com/yielded-dev/agent/commit/83ac097d23201d7c5ebc8a872f14d34d9dd60fa7)
contains only the dispatch fix, its guide/API comments and one-sentence changeset.
The evidence branch contains experimental source and rejected prototypes; it is
not a merge target. No admission or mutation-gate implementation was changed.

| Warm history / first-token delay | Driver baseline → candidate | Paired saving | Baseline repeat spread | pi driver | Candidate minus pi |
| -- | --: | --: | --: | --: | --: |
| 50 / 0 ms | 1,939 → 1,037 ms | **902 ms** | 217 ms | 776 ms | 261 ms |
| 250 / 0 ms | 1,838 → 1,041 ms | **797 ms** | 225 ms | 771 ms | 270 ms |
| 50 / 400 ms | 5,013 → 4,797 ms | 216 ms, below noise | 285 ms | 4,818 ms | −21 ms, below noise |
| 250 / 400 ms | 5,503 → 5,286 ms | 218 ms, below noise | 401 ms | 5,079 ms | 207 ms |

Driver latency is primary. These are rounded medians of Object medians, with two
Objects per condition, eight warm baseline and four candidate turns per Object.
pi is an adjacent, interleaved reference in its own Objects, not a causal
same-Object comparison; the signed remaining gap is descriptive, not a claim
that Yielded beats pi. Absolute medians differ from the original report's Objects.

| Warm history / delay | Provider gap baseline → candidate | Gap repeat spread | pi gap | Native transactions per turn baseline → candidate |
| -- | --: | --: | --: | --: |
| 50 / 0 | 187.1 → 77.3 ms | 7 ms | 74.3 ms | 126 → 35 |
| 250 / 0 | 165.3 → 73.5 ms | 3.8 ms | 74 ms | 123 → 35 |
| 50 / 400 | 74.1 → 48.4 ms | 11.3 ms | 69.8 ms | 258 → 35 |
| 250 / 400 | 103.5 → 65.5 ms | 12.5 ms | 91.5 ms | 270 → 35 |

The answer to the original 78/188 ms gap is not separate model-response and tool
commits: the eight read-only steps already co-commit their response, result and
continuation. One canonical write transaction remains between calls. Maintenance
also tries to fill a second native slot while this Object's only Thread is active.
It performs nine write transactions during the nine provider calls and two more
in the tail. The retained fix removes those eleven writes (**34 → 23 total**) and
their surrounding scans/evaluations. Instant responses expose that work and its
storage gating; a 400 ms provider wait hides much of it. Narrow-gap appends,
prompt construction and continuation work are unchanged. See the I/O-clock map
below for the parts which cannot honestly be assigned separate milliseconds.

`awaitSettlement` already listens for settlement hints, not ordinary progress.
Its 500 ms fallback performs a cached single-submission lookup. Removing all
fallback reads in a diagnostic leaves hundreds of milliseconds of CPU charged
to the RPC, with no repeatable driver improvement. That charge is not a profile
of the waiter's own algorithm. The last-response tail contains final canonical
commits, ledger finalization, two terminal authority reads, and gated RPC delivery.

| Cold history / delay | Driver baseline → candidate | Paired saving / repeat spread |
| -- | --: | --: |
| 50 / 0 | 2,310 → 1,512 ms | 798 / 126 ms |
| 250 / 0 | 2,321 → 1,729 ms | 592 / 205 ms; mixed Objects |
| 50 / 400 | 5,474 → 5,078 ms | 396 / 110 ms |
| 250 / 400 | 6,050 → 6,051 ms | −1 / 247 ms; no resolved gain |

Each Object has one cold candidate and two cold baselines. At 250/0 the individual
driver differences are −30 and +1,214.5 ms; the first is inside its 134 ms repeat
spread. Cold admission remains variable and is owned by `prod-admit`. Its code
was not changed here. The [full dispatch comparison](dispatch/comparisons.json.gz)
includes per-Object values, post-receipt differences, all cold results and missing
CPU coverage, rather than implying uniform cold improvement.

CPU comparisons below use only Objects with every required invocation joined
for that metric; parentheses give that count out of two. Different columns can
therefore cover different Objects. Totals sum matched submit/waiter/alarm charges
before taking medians, so the displayed component medians need not add up.

| Warm history / delay | Waiter CPU baseline → candidate | Alarm CPU baseline → candidate | Joined CPU baseline → candidate | Joined CPU saving / repeat range |
| -- | --: | --: | --: | --: |
| 50 / 0 | 407.5 → 383 ms (1) | 182.5 → 161 ms (1) | 591.5 → 576.5 ms (1) | 15 / 313 ms |
| 250 / 0 | 353.25 → 245.75 ms (2) | 212 → 200.75 ms (2) | 581 → 480.75 ms (2) | 100.25 / 193.5 ms |
| 50 / 400 | 387.5 → 219.5 ms (2) | 77.5 → 82 ms (1) | 476.5 → 325 ms (1) | 151.5 / 155 ms |
| 250 / 400 | 708.5 → 453 ms (1) | 250.5 → 302 ms (1) | 1,002 → 766 ms (1) | 236 / 121 ms |

Only the last joined-CPU estimate clears its repeat spread, and it covers one
Object. The waiter CPU reduction with unchanged waiter code, alongside the
hints-only diagnostic below, supports concurrent maintenance accounting rather
than attributing the full RPC charge to status polling.

**What one gap contains.** The [baseline map](baseline-map.md) was recorded
before any prototype. In `prod-turn-map-h50-d0-o0/m6`, the provider finished one
response at `1791500516781` and received the next request at `1791500516901`:
120 ms on its I/O clock. All Object timestamps within the narrow processing
interval were `1791500515651`; the clock did not advance through synchronous work.

| Order | Work | Count before the next fetch |
| -- | -- | -- |
| 1 | Decode streamed Effect AI response; prepare its continuation | One response; first continuation preparation |
| 2 | Offer the model-response commit | Deferred for this read-only batch; no write |
| 3 | Execute the scoped lookup tool | One tool; no external I/O in this fixture |
| 4 | Prepare the settled continuation; validate, encode and hash the batch | Second continuation preparation |
| 5 | Append response, tool result and continuation together | One awaited native write transaction; 15 SQL statements at history 50, 17 at 250; one KV put, getAlarm and setAlarm |
| 6 | Publish progress and check joined inputs | One progress notification; one empty joining transaction |
| 7 | Materialize the next prompt and construct the provider request | 280 messages in this example; 281 on the provider wire |
| 8 | Call fetch | Normal Cloudflare output gate remains in force |

The example's 15 SQL statements include 10 mutations: canonical records and
batch chain, run references, thread tail, tool declaration and work/due indexes.
One work-index entry is inserted and removed in the same transaction. No separate
immediate wake is written by the deferred alarm scheduler.
This co-commit applies to this read-only tool workload. Required intent before an
ordinary external tool dispatch remains a separate durability requirement.

Whole-turn deterministic evidence: 17 engine commit evaluations, 20 continuation
preparations, nine prompt materializations, eight tool batches, and one final
output decode. The canonical turn adds 12 batches / 34 records: input +
continuation; RunStarted + RunContextRecorded + continuation; eight response +
tool result + continuation batches; final response + RunCompleted + continuation;
SubmissionSettled + continuation. Local #821 target counts also show 22 record-run
rows, eight tool declarations, and one Submission/Attempt. No local elapsed time
is used. See [local counts](local-counts.json).
All 12 canonical appends in the mapped production turn occur in the alarm;
the local before/after count also covers admission of the receipt in the ledger.

One awaited write transaction is not a count of Cloudflare's physical replication
rounds. There is no explicit `storage.sync` in this warm turn. The next external
dispatch remains subject to an output gate, including any other pending writes
on the Object. Cloudflare may buffer/coalesce writes, and input gates can delay
incoming fetch responses while storage work is outstanding.
[Storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/),
[gate rules](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/).

**What runs concurrently.** Native progress advances the due-row revision even
while the Attempt remains active. The two-slot scheduler then claims/scans for
another Thread, only to exclude the one it already owns. In the mapped first
call, this background claim writes at Object I/O-clock +68 ms after fetch starts,
an empty transaction follows at +78 ms, the waiter starts at +109 ms, and response
headers/end arrive at +116 ms. Every baseline workload in the first comparison
has nine write transactions overlapping the nine provider calls. This is separate
from the eight narrow-gap canonical appends. The 500 ms abort check and waiter
fallback also run; the default ownership renewal is 10 seconds.

The provider conditions differ in two ways: the reused 400 ms condition spaces
SSE chunks by 10 ms, whereas the instant condition has no chunk delay. The
`burst` diagnostic retains the 400 ms first-token delay and removes chunk spacing.
It changes the provider only and isolates the effect of stream pacing.
The completed diagnostic has 34 successful turns on two Objects. Warm gaps are
96.5 → 100.25 ms at history 50 and 125 → 128.5 ms at 250, against baseline gap
repeat ranges of 44.5 and 19.5 ms. It does not reproduce a roughly 100 ms gap
increase merely by removing chunk spacing. The extra first-token time, during
which background maintenance can finish, is the stronger explanation.

The scripted provider service drops from 4,130 to about 3,600 ms per turn.
Observed warm driver savings are only 202 / 443 ms against 1,397 / 815 ms full
repeat ranges (histories 50 / 250); those are not reported as wins. Cold results
are mixed. See [the diagnostic comparisons](streaming/comparisons.json.gz).
The reducer initially assumed all 400 ms variants used 10 ms chunk spacing;
its corrected workload check accepts the planned zero-spacing variant while
still independently checking every fingerprint. The false rejection and its
cause are retained in [validation](validation/streaming-reducer-correction.json);
no turn was replayed or discarded.

**What the waiter reads.** The endpoint decodes the receipt, authorizes, checks
placement and performs one submission lookup. The runtime authorizes once, then
opens a scope and subscribes **before** each authoritative read. While pending it
races the hint against the 500 ms fallback and repeats. Its terminal read also
has a subscription established first, preventing a read/parking race.

Each pending read validates receipt identity against one submission. The existing
owned-row cache and row-identity decode cache avoid SQL and admission decoding
when that row is unchanged. A terminal read validates finalization replay and
reads the canonical settlement: two canonical SELECTs/JSON decodes, with
receipt/outcome checks. It does not decode the Thread's full history.

In the first comparison, the usual final settlement gives one hint. Warm baseline
medians are five status reads with an instant provider and 11–12 with the delayed
provider. The hints-only diagnostic has exactly two runtime reads, zero fallback
wakes, one hint and one snapshot decode. It preserves subscription-before-read
and terminal authority checks; removing the fallback is diagnostic-only because
hints are not a durable delivery contract.
The mapped turn sends 14 notifications: 12 progress notifications for canonical
appends, one broad notification from submit and one broad settlement notification.
Only the final broad notification normally reaches the already-started waiter.

All fibers share the Object's thread, so waiter work can compete with execution.
The experiment does not resolve that marginal cost beyond baseline noise. It does
refute interpreting the RPC's entire CPU charge as status-read cost. Alarm end
logs often carry the waiter's request context; joins use the alarm's start event
and retain every disagreement. Missing joins remain null, rather than being
assigned another invocation's CPU.

| Warm condition | Baseline reads → hints-only | CPU charged to waiter, baseline → hints-only | Paired CPU saving / repeat range |
| -- | --: | --: | --: |
| 50 / 0 | 5 → 2 | 321.5 → 354 ms | −6.5 / 154 ms; 2 complete pairs |
| 50 / 400 | 11 → 2 | 675 → 615.5 ms | 39.5 / 214 ms; 3 pairs |
| 250 / 0 | 5 → 2 | 267 → 284 ms | −8 / 151 ms; 3 pairs |
| 250 / 400 | 12 → 2 | 385.5 → 404 ms | 3 / 90 ms; 3 pairs |

These are medians of Object medians; a paired saving need not equal the difference
between aggregate medians. CPU is Cloudflare invocation accounting, not an
isolated cost measurement. The missing 50/0 baseline Object is explicitly null.

Four separate 10-second deployed [CPU captures](cpu-profiles/captures.json)
succeeded, with two verified turns per capture after a warmup. In the 50/400
capture, Effect `evaluate` accounts for 71.39% of cumulative sample weight.
The compiled maintenance dispatch generator and begin-pass callback are visible
at `worker.mjs:71558` and `:71062`; the waiter generator and read are at `:67351`
and `:67276`. See [all sampled locations](cpu-profiles/h50-d400-all.txt) and the
matching archived `d137bf32…` bundle. Async Effect continuations share evaluator
frames, so these profiles do not provide an exact per-RPC CPU split. Sample
weights include idle/program categories and are not billed CPU or latency.
Profiling ended before either following latency comparison began.

**Where settlement delivery waits.** After the last streamed response, the runtime
decodes the final output and commits response + RunCompleted + continuation,
publishes SubmissionSettled + continuation in another canonical transaction,
then finalizes the ledger and releases ownership in a third write transaction.
It publishes the settlement hint as finalization returns. The waiter performs
the two terminal authority reads and returns through RPC/output gating. There is
no intentional 500 ms delay after a successful settlement hint. Maintenance and
joined-settlement cleanup can overlap this interval.

The baseline map's compatible provider/driver clock bounds put the warm tail at
103.5–112.5 ms for 50/0 and 134.5–143.5 ms for 250/400. Other mapping conditions
have inconsistent offset bounds and receive no invented millisecond split.
Combining ordinary settlement publication and ledger finalization removes one
write transaction, but its measured driver benefit does not repeat above noise.

The dispatch fix does shorten the tail by preventing more native refill work.
On `prod-turn-dispatch-h50-d0-o0`, warm tail bounds change from 187.5–195 to
107.5–115 ms; on `o1`, from 186–194.5 to 73.5–85 ms. Conservative paired savings
are 72.5–87.5 and 101–121 ms, above the corresponding 45–51 and 34–35 ms repeat
spreads. The tail write count through the post-return diagnostic falls from
seven to five. That count includes claim release and maintenance checkpoint work
which can overlap delivery; it is not five mandatory sequential barriers before
the RPC returns. The three settlement-path writes and both terminal authority
reads are unchanged.

**Prototype decisions.** `maintenance` removes empty/refill discovery
transactions; `settlement` co-finalizes eligible ordinary root settlements in
their publication transaction; `hints` removes fallback polling only in the
experimental build; `dispatch` reserves one native slot for the exact runtime in
a known single-Thread Object. All variant switches live in the results harness,
not the proposed product change. Shared and custom replacement runtimes retain
two native slots. Auxiliary delivery, claims, fencing, leases, pre-arming,
canonical accounting, Unknown handling and original-context recovery are retained.

The first three prototypes were compared across three Objects per condition,
648 total turns including pi. This table gives paired driver savings; negative
means slower. The final column is the median within-Object full baseline repeat
range. No warm result exceeds it.

| History / delay | State | Empty maintenance | Co-finalize settlement | Hints only | Repeat range |
| -- | -- | --: | --: | --: | --: |
| 50 / 0 | Warm | −33.5 ms | 5.5 ms | −37.5 ms | 182 ms |
| 50 / 400 | Warm | −79.5 ms | 32 ms | 45.5 ms | 483 ms |
| 250 / 0 | Warm | 25.5 ms | 25.5 ms | 36 ms | 275 ms |
| 250 / 400 | Warm | −9.5 ms | −41 ms | 10 ms | 152 ms |
| 50 / 0 | Cold | −33.5 ms | 45.5 ms | 111.5 ms | 155 ms |
| 50 / 400 | Cold | −386.5 ms | −57.5 ms | −18.5 ms | 232 ms |
| 250 / 0 | Cold | 316 ms | 269 ms | 337 ms | 220 ms |
| 250 / 400 | Cold | 92 ms | 147.5 ms | 81 ms | 213 ms |

Cold estimates are mixed. For example, the 250/0 maintenance and hints-only
post-receipt savings are only 38.5 and 43.5 ms against a 218 ms repeat range;
most of their apparent total improvement is admission, which these candidates
do not change. The co-finalization cold result does not repeat as a warm gain.
No one of these three prototypes is retained. Co-finalization changes whole-turn
write transactions from 34 to 33; all three leave the nine writes during model
calls intact. The full [comparison data](candidates/comparisons.json.gz) also report
individual Objects, baseline-label differences, CPU and tail bounds.

The first ambient-placement version of `dispatch` failed two existing
concurrency/recovery regressions: a replacement runtime could inherit the
placement. It was reverted, then replaced by exact runtime binding. The two
unchanged tests and all 56 focused checks pass. That rejected approach and its
red/green evidence are archived under [validation](validation/).

**Measurement and reproduction.** Timing comes only from deployed Cloudflare
Workers provisioned with Alchemy. The driver targets `aws:us-west-1`; the mock
provider and ThreadObject observer are adapted from
`origin/dan/bench-production-path` at
`2e78a1a5a07250fc90fd332ddb51df21bf5032b1`.
The local production target comes from #821 at
`ed48f9291ca3b129cb7eb38340c32c4b5fad52c1`.

Every comparison randomizes/interleaves variants and two identical baseline
labels inside the same physical Object and bundle. Analysis first takes medians
within each Object; comparisons subtract the candidate from its pooled baselines.
Positive is saving. The noise check reports the full baseline max–min repeat
range as well as the difference between baseline-label medians. Do not subtract
unrelated Object medians or compare absolute timings across deployment phases.

Cold means acknowledged storage sync followed by explicit Object abort and a
verified new incarnation; it does not prove a fresh isolate. Histories start at
50 or 250 completed turns, then grow during randomized measurements. Every one
of the nine model requests is checked against the corresponding reference
transcript. Seed fingerprints remain 50 `b017b487524e44a4`,
250 `dcea9f30b0917245`. A measured turn is never retried after uncertain execution.

I/O-clock observations have integer-millisecond precision and can remain frozen
through CPU work. Provider-to-provider gaps use provider receipts; cross-service
tail estimates use intersected before/after offset bounds only when compatible.
They are not additive phase timings. Alternating mapping turns use asynchronous
beacons; candidate comparisons do not. Counter/span instrumentation is identical
within comparisons. Post-return diagnostic counters can include later maintenance.
[Workers clock behavior](https://developers.cloudflare.com/workers/runtime-apis/performance/).

The combined prototypes and isolated dispatch PR each pass `vp run ready`.
The isolated gate covers 71 tasks, including checks, tests and builds. Its first
attempt failed when restart subprocesses resolved Homebrew Node 26, which rejects
`--experimental-transform-types`; `vp env doctor` identified PATH precedence.
Running with Vite+'s managed Node 24.21.0 first on the process PATH passes without
changing tests or repository configuration. PostgreSQL verification used a
task-owned disposable fixture; its removal is verified in
[local-postgres.json](validation/local-postgres.json).

Raw requests, telemetry, plans, reducer outputs, exact bundle/source hashes,
validation failures, and cleanup evidence are retained here. Alchemy state and
credentials were held in a private mode-700 temporary directory outside the
repository and removed after cleanup.

There were **986/986 valid mapping/comparison turns**, **8,874 provider SSE
receipts**, and **182 verified forced-cold incarnations**: map 32 turns;
first prototypes 648; stream pacing 34; dispatch 272. All measured model calls
were in the alarm for the production target, all reference transcripts matched,
and no measured request failed or was retried. The 12 additional profile turns
also passed their transcript/alarm checks and are excluded from latency results.
Each reducer reports zero plan and integrity failures.

The full [non-ok inventory](dispatch/failed-outcomes.json.gz) covers the entire run,
not just this final comparison: **916 non-ok invocation outcomes**. These are
274 explicit `/cold` aborts, 638 canceled alarm invocations (637 with zero charged
CPU, one with 1 ms), and four canceled driver invocations (three seed requests,
one identity heartbeat). Cancellations remain visible; they are not silently
classified as successful turns or all assigned an inferred cause. The controller
received the required seed/heartbeat acknowledgements, and all measured turns
completed. There are no application failures or unreturned attempted turns.

One controller command selected a nonexistent telemetry role; the corrected
collection succeeded. The optional beta Worker-version metadata endpoint returned
HTTP 500; the legacy version endpoint and exact uploaded-byte checks supplied
the version proof. Preparation failures (sandbox install permissions, missing
compat dependency, and two local-count path errors) are recorded in
[validation-preparation.json](validation-preparation.json). Validation also retains
the rejected ambient-runtime regression, missing-PostgreSQL/workerd setup attempts,
the Node-resolution failure, and the reducer correction described above. None
was erased or turned into a successful timing sample.

Telemetry is not complete enough to assign every CPU charge: the dispatch phase
has 196 join diagnostics, including 133 alarm end-context disagreements, 45
missing provider-log joins (the SSE receipts still independently verify every
call), and 18 other missing invocation joins. Missing metrics remain null.
The other phases retain their own complete join inventories. This limits CPU
claims but does not discard or substitute driver timings.

To re-run the analysis without redeploying, use:

```sh
vp node --experimental-transform-types examples/durable-bench/results/prod-turn/analyze.mjs --phase dispatch --out-dir /private/tmp/prod-turn-reanalysis
```

`run.mjs`, `build.mjs`, `stack.ts`, `probes.mjs` and `network/` retain the deployed
harness. Plans record the shuffled order and transcript references; build
identities archive exact modules and their source inputs. A new deployment needs
a fresh results directory and private state; the controller deliberately refuses
to replay an already-attempted measured turn.

**Cleanup verified at 2026-10-09 00:19:21 UTC.** Alchemy destroyed
`prod-turn-9c3e79ef-primary` and `prod-turn-9c3e79ef-provider`. The API returned
404 for both Workers and an empty inventory of all `prod-turn` Workers and
namespaces in **Danieljmerwe@gmail.com's Account**. The private state directory
was removed. See [cleanup.json](cleanup.json), the destroy logs, and the
[credential scan](secret-scan.json), which checked raw and decompressed artifacts.

Large raw and derived JSON/JSONL artifacts are losslessly gzip-compressed; the reducer reads them directly. [archive-manifest.json](archive-manifest.json) records their uncompressed hashes.
