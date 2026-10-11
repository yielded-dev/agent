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

From the repository root, screen a candidate against `origin/main`:

```sh
DURABLE_BENCH_PREFIX=bench-change vp run -F @yielded/agent-example-durable-bench deployed -- --preset quick --candidate HEAD
```

Quick defaults to Yielded, 1,000 historical turns, TTFT 0 and 12 Objects. Each build
runs one excluded preparation turn, resets the isolate, then measures a cold turn,
four sequential warm turns and a second cold turn after another reset. Preparation
awaits native post-settlement writers so revision-specific derived state exists. Both builds
measure the same Objects. `--baseline <ref>` selects another baseline. The report
includes Object medians, paired differences and ratios with Object-level bootstrap 95% CIs,
and setup, measurement and teardown wall times. Screening does not establish a small
optimization when the interval includes zero.

Quick runs retain their per-Object Workers and uploaded versions. Every run clears and restores
its Objects, then measures both revisions on those same Objects; earlier samples are never reused.
Bundle hashes select cached Worker versions, so subsequent passes activate them without uploading
again. Full, profiling and tardie runs use fresh Workers. Remove all retained resources when done:

```sh
DURABLE_BENCH_PREFIX=bench-change vp run -F @yielded/agent-example-durable-bench deployed -- --teardown
```

Use `--preset full` (or `--rigorous`) for 30 Objects, six warm repeats, invocation CPU
and the balanced ABBA/BAAB sequence. Both presets preserve the driver timer, cold
restart acknowledgement, warm residency and transcript checks. Each cohort has a
separate Worker; isolate sharing rejects its measurements. Independent cells run
concurrently, with `--concurrency` limiting measured Objects and `--build-concurrency`
limiting restore work. Turns within an Object remain sequential.

```sh
vp run deployed -- --preset full --baseline origin/main --candidate HEAD
vp run deployed -- --sizes 1000,10000 --ttft 0,400
vp run deployed -- --targets yielded,pi --text-streaming --ttft 400
vp run deployed -- --profile cpu
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
vp run deployed -- --build-history --build-concurrency 36 --concurrency 3
```

Defaults are 50, 250, 1,000 and 3,500 historical turns, three independent Objects per
target and size, zero provider TTFT, one cold turn and nine warm turns. All selected histories
build concurrently by default; `--build-concurrency` limits that phase. Measurements default
to six concurrent Objects, controlled separately by `--concurrency`. `--sizes`, `--objects`,
`--repeats` and `--ttft` remain configurable.

Each logical sample Object (target, size and replica) has a dedicated Worker so concurrent
samples do not share a Worker isolate's CPU budget. Results record a per-isolate token.
If separate sample Objects in the concurrent build cohort report the same token, the
cohort's timings are rejected.

Each Object starts empty and runs real historical turns inside its owning runtime in batches
of 50. The history provider always has zero delays.
Sizes use separate Objects, so measured turns never enter another size's history.
The driver restarts each Object between
batches over its retained storage, matching the original seeder's disposal of each
runtime. The adapters close pi's Harness and tardie's Thread and Actor runtimes before
aborting; pi's extension registry belongs to each Harness so isolate globals cannot
retain an old runtime. These restarts count toward build time.

The nearby driver handles one resumable request per 50-turn batch. End-to-end build time
spans the first batch through the final acknowledgement, including network, storage,
controller gaps and downtime if resumed. Deployment and the initial readiness/abort check
are setup outside this timer; constructors may initialize empty database schemas there,
but no history is built. Every batch records its completed count and final model-visible
fingerprint; every measured model request must match the independent reference transcript.
Failed or uncertain batches are never resubmitted. Failure invalidates that Object while
the remaining Objects continue; partial progress and errors stay in the result JSON.

A graceful interruption waits for each current 50-turn batch and preserves the run for
`--resume <run-name>`. Resume uses the saved options and requires identical compiled Worker
bundles. It continues only from verified batch boundaries whose count and fingerprint match
the Object's state. Unknown in-flight batches and partially measured Objects are retired.
Use the same `DURABLE_BENCH_PREFIX` when resuming.

```sh
vp run deployed -- --resume run-name
```

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
Object of each selected target in every history/TTFT cell on the first build pass (tardie targets
its Thread Object). Captures start after warmup, alongside the warm turns, for an estimated batch duration
(5–50 seconds). Cloudflare profiles the running isolate containing that Object, which may
also contain other Objects. Only already-running isolates can be captured; the bench supplies
traffic. Short batches may finish before capture does. Memory profiles contain allocations
made during the window, not retained memory or a heap snapshot.

For history-build diagnostics, add `--profile-after <turns>` to start a ten-second capture
after a verified checkpoint while subsequent batches continue. This requires one target,
size, TTFT and Object, with both concurrency limits set to one. The checkpoint must be a
multiple of 50 before the final history turn. JSON records the trigger and the latest acknowledged checkpoint
at capture completion; exclude this diagnostic run from timing comparisons.

```sh
vp run deployed -- --build-history --targets pi --sizes 3500 --objects 1 --concurrency 1 --profile memory --profile-after 1950
```

Profile requests, downloads and file writes are outside the turn timers, but profiling adds
runtime overhead and can extend the run. Use unprofiled runs for timing comparisons.
Profiled Object batches are serialized and paced to five captures per five minutes;
quota waits happen before warmup. Other account activity can still cause HTTP 429; wait
for the reported `Retry-After` before rerunning.
The token needs Workers Scripts Read permission and the account must support profiling.
Saved `.pprof` paths are printed at completion; filenames and result JSON identify
the cell, target, build and Object. Bundles and linked source maps are uploaded;
copies remain beside the profiles in `results/` after cleanup. Inspect with
`go tool pprof -http=:8080 results/<run>/<profile>.pprof`, or use Cloudflare's Observability →
Flamegraph view while the Worker is retained with `--keep`.

## Infrastructure and cleanup

The shared `durable-bench-infrastructure` Alchemy stage keeps
`durable-bench-shared-<installation>-driver` and `durable-bench-shared-<installation>-provider`
deployed between runs. Fresh installations use fresh names to avoid recently deleted endpoints.
Run-specific `durable-bench-*` target stages contain the run's Workers and SQLite Durable Object
namespaces. Every cohort uses a dedicated Worker in both measurement presets and history builds. Full and profiling runs destroy their target stages and verify deletion through the
Cloudflare API, including after failure. `--keep` retains it for inspection. Interrupted
history-build runs retain their resources for `--resume`; other interruptions attempt cleanup.
Use `--teardown` to discard retained runs or finish interrupted cleanup.

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
Workers are deployed. Account IDs, API tokens, namespace IDs and Alchemy output are not results.

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
All Objects request `locationHint: "wnam"`; the driver and target Workers request
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

The default fixture directory is `~/.cache/yielded-durable-bench/fixtures/<format-key>/`;
`--fixture-directory <path>` selects an existing directory. The format key is the SHA-256
prefix of the Cloudflare adapter migrations and canonical record schema files.
Historical `main-<revision>` caches are adopted only when those files match exactly.
For 1,000-turn Yielded and pi cells, both presets use `<format-key>-natural-id50-v1/`:
the deployed history protocol generates this cache locally with 50-character Thread IDs,
the networked mock provider and the same historical tool cycle. Historical turns have no
text preamble or delay, matching deployed history construction; `--text-streaming` controls
measured turns. Larger capacity fixtures retain their original workload and identities.
Missing fixtures are generated locally and cloned into the cache; saved histories are
never deleted or overwritten. Miniflare is used for deterministic fixture generation,
never performance evidence.

Every ordinary cell restores the original idle SQLite snapshot, including historical
attempt rows, indexes and triggers. Cohorts use separate namespaces so each can retain
the original Thread identity and stored bytes. Bounded uploads run concurrently
across Objects. Yielded restores through a seed-only Worker before deploying the measured
runtime onto the same Objects. No canonical import normalization, history replay or identity rewriting
runs during setup. Each Object's complete table counts and transcript fingerprint must
match the saved source before timing. Every subsequent model request is checked against
an independent reference transcript. Failed or uncertain mutations are never replayed.

Use `--build-history` only to measure real deployed history construction, as described
below. This explicit mode retains its native submission/settlement path and checkpoints.

## Comparing a change

Quick mode screens a candidate with a paired comparison. Use full mode to check repeat
drift before claiming a small optimization.

```sh
vp run deployed -- --rigorous --baseline origin/main --candidate HEAD
```

Rigorous mode enables cold turns, CPU and six warm repeats per pass. It randomly chooses
ABBA or BAAB, redeploying the **same Worker and namespaces** with the two Yielded builds.
Each pass restarts the same stored Objects; history grows identically for all targets.
Repeated baselines reveal deployment/time/history drift. The output includes paired differences and confidence intervals. Do not claim gains smaller than
the control or within-Object repeat spread; inspect all Objects, not just the median.

Refs supply `packages/` sources; both builds use this harness and the checkout's installed
catalog dependencies. They must support the current fixture and each other's persisted
state. Import, recovery, transcript, build or residency failures invalidate the affected Object;
the command does not reset history or bypass durability checks to force a comparison.
