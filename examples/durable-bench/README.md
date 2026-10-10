# Durable thread benchmark

Compare Yielded's production `ThreadObject` path with pi-durable 1.1.0 and tardie 0.45.2
on deployed Cloudflare. A driver Worker measures a complete user turn against a networked,
scripted OpenAI-compatible provider. No model API key or paid model is involved.

## Run

Install workspace dependencies with `vp install`. The command uses this checkout's
`direnv exec .` credentials (`CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`); use the
personal benchmark account. Its name appears only in local terminal output. The token
needs Workers/SQLite Durable Object deployment and account-read access, plus Workers
Observability read access if `--cpu` is selected. Dependencies for pi and tardie are installed
in the isolated `third-party` workspace automatically on first use. Repository checks do not
require that optional install.

From the repository root:

```sh
vp run -F @yielded/agent-example-durable-bench deployed -- --targets yielded,pi,tardie --sizes 50,250
```

Or run `vp run deployed` in this directory. Quick mode defaults to seven Objects per
target/cell, four sequential warm turns per Object after an excluded warmup, 0 and 400 ms
time to first token, and six concurrent Objects. Fixtures are imported in bulk when the
native import reproduces their table counts; Yielded currently needs the replay fallback
described below. All seeding finishes before timing. Increase `--concurrency` to run more Objects
at once; changing it can also change contention and the numbers being compared.

```sh
vp run deployed -- --ttft 0 --objects 7 --repeats 4 --concurrency 6
vp run deployed -- --text-streaming --ttft 400 --objects 2 --repeats 2
vp run deployed -- --cold --cpu
vp run deployed -- --profile cpu,memory
```

The output has one table per cell: **first text (final answer)** in default mode or
**first text** with `--text-streaming`, beside turn completion. Both use median [Q1–Q3]
of **Object medians**, with Yielded ÷ pi for both. It also shows
Object entry to the first outgoing model request, the completion medians' range and each Object's
completion repeat range (median / maximum).
The ratio includes the observed min(Yielded)/max(pi)–max(Yielded)/min(pi) range of
Object medians. This unpaired spread is descriptive, not a confidence interval.
JSON and the printed tables go to gitignored `results/durable-bench-*.{json,md}`. JSON also
records subscription setup, the first text fragment, admission, model-call gaps, the last response-to-client interval, ingress colos,
fingerprint/build checks, cold setup attempts, failures and cleanup. No timing evidence is committed.

## Build history on deployed Objects

`--build-history` measures the four cards in
[Mario Zechner's pi-durable comparison](https://github.com/badlogic/durable-bench/tree/main/pi-vs-tardigrade):
building history, cold turn, warm turn and SQLite storage. It skips local fixtures entirely.

```sh
vp run deployed -- --build-history
```

Defaults are 50, 250, 1,000 and 3,500 historical turns, three independent Objects per
target and size, concurrency one, zero provider TTFT, one cold turn and nine warm turns. `--sizes`,
`--objects`, `--repeats`, `--ttft` and `--concurrency` remain configurable. Each Object
starts empty and runs real historical turns inside its owning runtime in batches of 50. The history provider always has zero delays. Sizes use separate Objects, so measured
turns never enter another size's history. The driver restarts each Object between
batches over its retained storage, matching the original seeder's disposal of each
runtime. The adapters close pi's Harness and tardie's Thread and Actor runtimes before
aborting; pi's extension registry belongs to each Harness so isolate globals cannot
retain an old runtime. These restarts count toward build time. Serial builds avoid
overlapping histories in Cloudflare's shared isolate memory budget.

The nearby driver times the complete build across all batches, including network and
storage acknowledgement. Deployment and the initial readiness/abort check are setup outside
this timer; constructors may initialize empty database schemas there, but no history is built.
Every batch records its final model-visible fingerprint;
every measured model request must match the independent reference transcript. Failed
or uncertain batches are never resubmitted. Failure invalidates that Object while the
remaining Objects continue; partial build progress and errors stay in the result JSON.

After an acknowledged Object abort, an untimed request constructs the new Object
without opening pi's Harness or tardie's native reference. Cold adds the Object-local
constructor-and-clock-probe interval to the driver's native open-and-first-turn interval.
This includes Yielded's eager constructor initialization while excluding isolate startup
and priming transport. JSON retains both intervals and the primed identity; the timed
turn must use that same instance. A real request to the existing provider's health endpoint
advances [Cloudflare's otherwise frozen clock](https://developers.cloudflare.com/workers/runtime-apis/performance/).
Its roundtrip remains in cold time; the harness does not estimate or subtract it.
This is an observable interval, not an isolated constructor CPU measurement.
No text observer opens an agent before timing.

There is no extra warmup: warm pools the next nine turns from each complete Object,
each with eight tool calls. Build and cold values are medians across Objects. Storage
is the median SQLite size after the last measured turn in decimal MB, including tardie's
Actor directory. Tables include Yielded ÷ pi and tardie ÷ pi for all four cards.
The mode excludes text observation and A/B redeployment; use the original mode for those
workflows. Deployed absolute times include network hops and cannot be
compared with Miniflare timings; compare ratios within a matched deployment.

`--profile cpu|memory` accepts repeats or a comma-separated list. It captures the first
Object of each selected target in every history/TTFT cell and build pass (tardie targets
its Thread Object). Captures start after warmup, alongside the warm turns, for an estimated batch duration
(5–50 seconds). Cloudflare profiles the running isolate containing that Object, which may
also contain other Objects. Only already-running isolates can be captured; the bench supplies
traffic. Short batches may finish before capture does. Memory profiles contain allocations
made during the window, not retained memory or a heap snapshot.

For history-build diagnostics, add `--profile-after <turns>` to start a ten-second capture
after a verified checkpoint while subsequent batches continue. This requires one target,
size, TTFT, Object and concurrency one. The checkpoint must be a multiple of 50 before
the final history turn. JSON records the trigger and the latest acknowledged checkpoint
at capture completion; exclude this diagnostic run from timing comparisons.

```sh
vp run deployed -- --build-history --targets pi --sizes 3500 --objects 1 --profile memory --profile-after 1950
```

Profile requests, downloads and file writes are outside the turn timers, but profiling adds
runtime overhead and can extend the run. Use unprofiled runs for timing comparisons.
Profiled Object batches are serialized and paced to five captures per five minutes;
quota waits happen before warmup. Other account activity can still cause HTTP 429; wait
for the reported `Retry-After` before rerunning.
The token needs Workers Scripts Read permission and the account must support profiling.
Saved `.pprof` paths are printed at completion; filenames and result JSON identify
the cell, target, build and Object. Unminified bundles and linked source maps are uploaded;
copies remain beside the profiles in `results/` after cleanup. Inspect with
`go tool pprof -http=:8080 results/<run>/<profile>.pprof`, or use Cloudflare's Observability →
Flamegraph view while the Worker is retained with `--keep`.

## Infrastructure and cleanup

Alchemy owns two stacks: the named `durable-bench-infrastructure` stage keeps
`durable-bench-shared-<installation>-driver` and `durable-bench-shared-<installation>-provider`
deployed between runs. Fresh installations use fresh names to avoid recently deleted endpoints. A unique
`durable-bench-*` target stage contains the run's Worker and four SQLite Durable Object
namespaces. Each normal run destroys its target stage and verifies deletion through the
Cloudflare API, including after failure. `--keep` retains it for inspection. Interruption
attempts cleanup too; if the process is killed before it finishes, run `--teardown`.

Private ownership data and Alchemy state live under `~/.local/state/durable-bench` (mode
700), outside the checkout. Keep that directory until cleanup. The command refuses to
reuse it with another account, or adopt existing resources without ownership state.
Run only one benchmark/teardown command at a time. Within a run, Objects are concurrent.
Set `DURABLE_BENCH_PREFIX=bench-profile` to isolate resource names and private state under
that prefix; use the same environment value for `--teardown`.

```sh
vp run deployed -- --teardown
```

This destroys retained target stacks and the shared infrastructure, verifies that no
`durable-bench` Workers or namespaces remain in the same account, writes `results/cleanup.json`,
and removes private state. After an interrupted cleanup, rerun `--teardown`.
Shared code changes redeploy the infrastructure on the next run; otherwise only the target
Worker is deployed. Account IDs, tokens, namespace IDs and Alchemy output are not results.

## Workload and measurement

Historical turns repeat a one-tool, one-tool, zero-tool cycle. Each measured turn makes
eight sequential readonly `lookup` calls, hence nine model requests. Tool results are
256 bytes, except every 97th result is 8 KiB. Compaction is disabled for every target.
The standard 400 ms provider also spaces SSE chunks by 10 ms, matching the deployed rebench harness.
`--text-streaming` retains the eight lookups and nine model requests, but starts the first
response with "I will look up the requested records, then summarize what I find." before
its tool call. This preamble and a longer final reply stream in word-sized fragments every
25 ms in the 400 ms cell (about 40 fragments/s, roughly one second for the final reply).
Tool-only responses retain their original pacing; the 0 ms cell emits the same text without
delays. Every target receives the same preamble and reply, and subsequent requests are
checked against the extended reference transcript. Before forwarding streaming requests, the
common bridge splits mixed assistant text/tool messages to match the pinned Effect provider's layout.
The flag keeps the original completion workload available; compare tables with the same workload label.
All Objects request `locationHint: "wnam"`; the driver and target Worker request
`aws:us-west-1` placement. Placement is a hint, not a guarantee.

For Yielded, the driver times public `submit` admission through public `awaitSettlement`:
the production mutation gate, pre-armed alarm, maintenance processing and notification are
all included. The Worker caches its client runtime and definition digest before timing.
The waiter uses production wake hints with a 500 ms polling fallback. pi and tardie use
their native turn APIs and storage. Driver elapsed time includes the HTTP/RPC boundary;
laptop time and its `CF-Ray` colo are secondary fields, not the headline latency.

Before submitting, the driver attaches to Yielded's public `watchText(threadId)` or
pi-durable's public `watchEvents` inside its Object, forwarded over HTTP as text frames.
It waits for the subscription acknowledgement, then starts both timers at submit. First text
ends when nonempty assistant text reaches the driver; it includes framework publication and
transport, rather than the provider's first byte. Yielded drafts are matched to the new
Receipt. Instant-provider Yielded uses the settled-record fallback when drafts are too short-lived to observe.
The public `awaitSettlementRecord` read follows completion; first text includes that extra read.
JSON's `firstTextSource` distinguishes preview delivery from this canonical fallback.
Default mode emits text only after the eight tool calls, so **first text (final answer)**
measures the final reply. Streaming mode's **first text** measures submit to the first visible
preamble token, including framework publication and transport. Tardie shows `n/a`:
its existing adapter exposes completed method calls, and connecting its separate execution
stream is outside this small harness change. Missing first text or observation failure before
it invalidates the sample. `Object → model` measures submission-handler entry to the first
outgoing model request on the Object's clock. It excludes driver transport and routing before
that Object; short intervals may resolve to zero. Object/driver clock offsets are not request latency.

`--cold` includes the first turn after acknowledged `storage.sync()` + `ctx.abort()`;
tardie's Actor directory and Thread are both restarted. It verifies a new instance, no
earlier alarm entry, and a first request. This is a cold Object over retained storage,
not a cold isolate, cold disk, or newly seeded conversation. Warm samples require the
same live instance throughout the Object's sequential repeat batch.
Tardie's native reference can skip the Actor directory. `directoryUsed` records whether
the Thread has called it since reset; an unused directory's identity observes residency only.

Worker and Object [code updates propagate separately](https://developers.cloudflare.com/durable-objects/platform/known-issues/#code-updates).
Before each pass, cold setup waits for the Object's own `BUILD` (including tardie's Actor
directory), retrying acknowledged resets for up to three minutes. Each attempt is recorded;
setup errors stop the run. The final reset is followed by observer attachment, then the timed input.
Each Thread checks its build before admission, and completed metrics and all provider receipts
must match the expected build. A mismatch invalidates the sample; inputs are never retried.

For Yielded and pi, pre-subscription opens the new Object before a cold turn's submit timer;
`observationMs` records that excluded setup. Cold cells therefore describe the first turn
after restart with observation prepared, and are not comparable to earlier unobserved cold
completion timings. Tardie's cold timer still includes opening its native reference.

The gap is measured between outgoing model requests and consumed responses on one Object
clock. Last-response-to-client crosses Object/driver clocks and is approximate; it is
not an exact settlement-notification decomposition. CPU is opt-in. Invocation markers join
Cloudflare invocation events; missing/sampled rows stay missing, never zero. All observed
outcomes, including `exceededCpu`, `exceededMemory` and intentional cold-abort exceptions,
are retained. CPU tables describe observed invocations, not billing totals.

## Fixtures

Missing fixtures are generated once by the existing local `seed` command. To regenerate:

```sh
vp run seed -- yielded 50 250
vp run seed -- pi 50 250
vp run seed -- tardie 50 250
```

Miniflare is used only for deterministic seeding, canonical transfer and table counts;
its timings are not performance evidence. Local timing/report/chart commands have been
removed. Fixtures and results are disposable and gitignored.

Yielded uses the canonical storage-sql thread import when it reproduces the local fixture's
table counts. That public transfer contract currently omits historical attempt rows, so
Yielded alone falls back to untimed public submit/await replay with the instant provider.
The fallback reason is recorded in JSON. Attempt, claim and lease rows are never copied
around the canonical importer. pi and tardie bulk-insert their complete SQLite table/index
dumps in synchronous transactions, including tardie's Actor directory.

Each deployed Object must match the local fixture's complete table counts and transcript
fingerprint before timing. The 50/250 fingerprints are `b017b487524e44a4` and
`dcea9f30b0917245`. Every subsequent model request is also checked against an independent
reference transcript. A failed or uncertain input is recorded and never resubmitted.

## Comparing a change

Quick mode answers where the targets stand, with a small sample. Different Objects and
deployments can vary substantially; do not claim a small optimization from that table.

```sh
vp run deployed -- --rigorous --baseline origin/main --candidate HEAD
```

Rigorous mode enables cold turns, CPU and six warm repeats per pass. It randomly chooses
ABBA or BAAB, redeploying the **same Worker and namespaces** with the two Yielded builds.
Each pass restarts the same stored Objects; history grows identically for all targets.
Repeated baselines reveal deployment/time/history drift. The output includes paired
candidate ÷ baseline ratios and baseline repeat spread. Do not claim gains smaller than
the control or within-Object repeat spread; inspect all Objects, not just the median.

Refs supply `packages/` sources; both builds use this harness and the checkout's installed
catalog dependencies. They must support the current fixture and each other's persisted
state. Import, recovery, transcript, build or residency failures invalidate the affected Object;
the command does not reset history or bypass durability checks to force a comparison.
