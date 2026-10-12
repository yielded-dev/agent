# Durable thread benchmark

Compare Yielded's production `ThreadObject` path with pi-durable 1.1.0 and tardie 0.45.2
on deployed Cloudflare. A driver Worker measures a complete user turn against a networked,
scripted OpenAI-compatible provider. No model API key or paid model is involved.

## Run

Install workspace dependencies with `vp install`. The command uses this checkout's
`direnv exec .` credentials (`CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`); use the
configured benchmark account. The token needs Workers/SQLite Durable Object deployment
and account-read access, plus R2 bucket read/write access if `--cpu` is selected.
Dependencies for pi and tardie are installed
in the isolated `third-party` workspace automatically on first use. Repository checks do not
require that optional install.

From the repository root, screen a candidate against `origin/main`:

```sh
DURABLE_BENCH_PREFIX=bench-change vp run -F @yielded/agent-example-durable-bench deployed -- --preset quick --candidate HEAD
```

Quick defaults to Yielded, 1,000 historical turns, TTFT 0 and 12 Objects. Each build
runs one excluded preparation turn, resets the isolate, then measures a cold turn,
twelve sequential warm turns and a second cold turn after another reset. Preparation
awaits native post-settlement writers so revision-specific derived state exists. Both builds
measure the same Objects. Within each target/history/TTFT cell, quick randomly assigns half
the Objects to baseline then candidate (AB), and half to candidate then baseline (BA),
swapping revisions in the second pass; an odd Object count differs by one between orders.
`--baseline <ref>` selects another baseline. The report
includes Object medians, paired differences and ratios with Object-level bootstrap 95% CIs,
the first-pass minus second-pass order effect, and setup, measurement and teardown wall times. Screening does not establish a small
optimization when the interval includes zero.

Quick runs retain one per-Object Worker pool per prefix. Changing pool options destroys
the previous pool before allocating its replacement. Each Worker declares only its target's
namespaces (one for Yielded or pi, two for Tardie). A pre-deploy account inventory rejects
runs that would leave fewer than 100 of the account's 500 namespace slots free.
Every run clears and restores
its Objects. Histories through 1,000 turns restore before every pass, so both revisions start
from identical saved state on those same Objects; earlier samples are never reused.
Each pass uploads a fresh version and rejects Worker-isolate reuse across passes. Activating an
already uploaded version can retain the previous isolate, including in an A/A control, so version
activation is not used. Full, profiling and tardie runs use fresh Workers. Remove retained resources when done:

```sh
DURABLE_BENCH_PREFIX=bench-change vp run -F @yielded/agent-example-durable-bench deployed -- --teardown
```

Use `--preset full` (or `--rigorous`) for 30 Objects, six warm repeats
and the balanced ABBA/BAAB sequence. Add `--cpu` for invocation CPU in either preset. Both presets preserve the driver timer, cold
restart acknowledgement, warm residency and transcript checks. Each cohort has a
separate Worker; isolate sharing rejects its measurements. Independent cells run
concurrently, with `--concurrency` limiting measured Objects and `--build-concurrency`
limiting restore work. Quick and full default to 12 concurrent Objects.
Turns within an Object remain sequential.

```sh
vp run deployed -- --preset full --baseline origin/main --candidate HEAD
vp run deployed -- --sizes 1000,10000 --ttft 0,400
vp run deployed -- --targets yielded,pi --text-streaming --ttft 400
vp run deployed -- --profile cpu
vp run deployed -- --targets yielded,effect-ai-floor --text-streaming --cpu
```

The output has one table per cell: **first text (final answer)** in default mode or
**first text** with `--text-streaming`, beside turn completion. The main table uses
**submit → first text/completion**, after observer acknowledgement. A separate cold table
uses **observer-open → first text/completion**, including attachment and runtime opening.
Both use median [Q1–Q3] of **Object medians**, with Yielded ÷ pi for both. It also shows
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

`--profile cpu|memory` selects one profile type per run. It captures the first
Object of each selected target in every history/TTFT cell on the first build pass (tardie targets
its Thread Object). Captures start after warmup, alongside the warm turns, for an estimated batch duration
(1–50 seconds). Cloudflare profiles the running isolate containing that Object, which may
also contain other Objects. Only already-running isolates can be captured; the bench supplies
traffic. Captures are spaced at least five minutes apart, and the final cold reset waits for capture to finish. A capture that outlives its warm workload is incomplete. Memory profiles contain allocations
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
`durable-bench` Workers, namespaces or Tail CPU buckets remain in the same account, writes `results/cleanup.json`,
and removes private state. After an interrupted cleanup, rerun `--teardown`.
Shared code changes redeploy the infrastructure on the next run; otherwise only the target
Workers are deployed. Account IDs, API tokens, namespace IDs and Alchemy output are not results.

## Workload and measurement

Historical turns repeat a one-tool, one-tool, zero-tool cycle. Each measured turn makes
eight sequential pure `lookup` calls, hence nine model requests. Yielded classifies the
tool as `readonly`; pi registers it as replay-safe. Tool results are
256 bytes, except every 97th result is 8 KiB. Compaction is disabled for every target.
The standard 400 ms provider also spaces SSE chunks by 10 ms, matching the deployed rebench harness.
`--text-streaming` retains the eight lookups and nine model requests, but starts the first
response with "I will look up the requested records, then summarize what I find." before
its tool call. This preamble and a longer final reply stream in word-sized fragments every
25 ms in the 400 ms cell (about 40 fragments/s, roughly one second for the final reply).
Tool-only responses retain their original pacing; the 0 ms cell emits the same text without
delays. Every target receives the same preamble and reply, and subsequent requests are
checked against the extended reference transcript. Before forwarding streaming requests, the
provider Worker splits mixed assistant text/tool messages to match the pinned Effect provider's layout.
The flag keeps the original completion workload available; compare tables with the same workload label.
The reference fingerprint compares normalized user, assistant and tool messages. It
excludes system/developer instructions, offered tool schemas, IDs and provider wire layout;
it does not prove byte-identical requests. Pi and the Effect AI floor receive the same
ordinary-text output directive that Yielded appends, and pi's parameter uses the same safe
integer bounds as Yielded's `Schema.Int`. Pi's replay-safe lookup still records native tool
intents; Yielded's readonly policy and pi's recovery contract remain different. Each target
uses its production API and provider encoding, so those costs remain in the comparison.

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
The **submit → X** boundary starts after subscription acknowledgement. The independent
**observer-open → X** boundary starts before the `/text` request, capturing attachment and
any runtime opening it triggers. Both stop at the same driver-observed endpoint; the cold
tables label them separately. JSON records `observerToFirstTextMs` and
`observerToCompletionMs` alongside `firstTextMs` and `driverMs`. First text
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
The driver collects metrics after its timer endpoints, before closing the text observer.
A confirmed lost incarnation invalidates the Object's entire pass; the result retains those
records. Snapshot-backed passes restore and verify the same Object, repeat preparation,
and remeasure once with fresh input IDs. A second loss, sample-key mismatch, wrong build,
or ambiguous turn failure stops the run. Real-history builds and passes that carry history
into or out of another pass cannot recover this way. `--verify-incarnation-recovery` exercises this path by
aborting one settled Object per target before metrics; its interrupted passes are classified
and excluded, so use this diagnostic separately from a latency comparison.
Tardie's native reference can skip the Actor directory. `directoryUsed` records whether
the Thread has called it since reset; an unused directory's identity observes residency only.

Worker readiness requires five seconds of continuously successful build probes from both the
controller and deployed driver; a failed probe restarts that stability window.

Worker and Object [code updates propagate separately](https://developers.cloudflare.com/durable-objects/platform/known-issues/#code-updates).
Before the final cold reset, setup probes `/measure` and every target route it uses
(`/text`, `/submit`, `/await`, `/result`, `/run`, `/prime`, `/metrics`, as applicable)
through the deployed driver. Router-only GET probes must report the expected driver and
target builds continuously for five seconds; they do not dispatch inputs or attach observers.
Route status, build, errors and retries are saved in `readiness`. These checks share the
three-minute setup bound with the physical Object identity check and cannot guarantee
later propagation stability: a wrong build during observation or timing still fails the run.

Cold setup then probes the physical Object's identity (including Tardie's Actor directory),
retrying only a missing placeholder fetch handler or a wrong `BUILD` within a three-minute
setup bound. An acknowledged cold reset retires a responding old incarnation before
probing its replacement; placeholders are never reset. Every identity attempt and reset
is recorded in `readiness`; other errors stop the run. Identity probes never import rows or submit turns. The final
reset is followed by observer attachment, then the timed input; timing never retries.
Each Thread checks its build before admission, and completed metrics and all provider receipts
must match the expected build. A mismatch invalidates the sample; inputs are never retried.

For Yielded and pi, pre-subscription opens the new Object before a cold turn's submit timer;
`observationMs` records that excluded setup. Cold cells therefore describe the first turn
after restart with observation prepared, and are not comparable to earlier unobserved cold
completion timings. Tardie's cold timer still includes opening its native reference.

Cold completion, first text and admission receipt share the logical start `driver submit start − constructorAndProbeMs`; the constructor/probe interval is also reported separately, while priming transport and observer attachment remain excluded.

Request decoding, transcript normalization and fingerprinting run in the provider Worker,
not the measured Object. The Object records response chunks and timestamps; it validates
SSE and receipts only when post-turn metrics are collected. Every provider fingerprint
must still match the independent expected transcript.
Deployment uploads contain executable modules; profiling artifacts retain the source maps locally.

Object-clock markers record provider first token received and settlement relative to entry;
receipt latency remains the driver-clock `admissionMs`. A forwarding timestamp is available
where the adapter already handles decoded text (pi and the floor). Yielded's native frames
pass unchanged to the public client; the benchmark does not decode them again inside the
Object. First-visible text is measured at the driver for every streaming target.
Raw provider calls and available markers are included in result JSON and the quick table.

The gap is measured between outgoing model requests and consumed responses on one Object
clock. Last-response-to-client crosses Object/driver clocks and is approximate; it is
not an exact settlement-notification decomposition. CPU is opt-in. `--cpu` attaches a Tail
Worker that stores marked native Object invocations in a temporary R2 bucket. It sums each
invocation's CPU once per turn, including marked submission, settlement wait, alarm, text
observation and fallback record reads; driver, provider, metrics collection and deployment CPU are outside that
total. Multiple markers in one native invocation do not multiply its CPU. After the final
quiescent metrics, the harness closes each Object so native text observers finish before
Tail collection. This happens outside the timers. Coverage requires the controller-planned
Yielded submit/await/alarm or native run roles, a text invocation for every target with an
observer, and any fallback record read issued by the driver. The requirements do not depend
on emitted markers or which text source wins a race; an unknown read plan is incomplete.
Every acknowledged marker must also be present.
Missing, truncated or ambiguous records invalidate the Object pair; missing CPU is never
zero. The paired report compares whole-Object medians with Object-level bootstrap 95% CIs.
These are observed invocation totals, not billing totals or sampled CPU profiles (`--profile cpu`).

`effect-ai-floor` is an opt-in warm comparison using native Effect AI streaming, tools and
an in-memory Prompt. It synthesizes the same historical transcript and verifies every model
request against the shared reference. It has no durable receipts, storage or recovery and
is not a drop-in durable engine. Each pass starts from the original history. It runs the
same preceding turns as Yielded, but excludes the cold slots from its results and stays
resident; Yielded and pi retain their normal cold measurements. Only compare their warm
cells. The floor requires `--restore-each-pass` (the default through 1,000 turns), rejects
history-build/recovery diagnostics, and uses the shared bundle, so it does not measure
isolated Effect startup or an additive cost of durability.

## Fixtures

The default fixture directory is `~/.cache/yielded-durable-bench/fixtures/<format-key>/`;
`--fixture-directory <path>` selects an existing directory. The format key is the SHA-256
prefix of the Cloudflare adapter migrations and canonical record schema files at each build ref.
Historical `main-<revision>` caches are adopted only when those files match exactly.
For 1,000-turn Yielded and pi cells, both presets use `<format-key>-natural-id50-v1/`:
the deployed history protocol generates this cache locally with 50-character Thread IDs,
the networked mock provider and the same historical tool cycle. Historical turns have no
text preamble or delay, matching deployed history construction; `--text-streaming` controls
measured turns. Larger capacity fixtures retain their original workload and identities.
Missing natural fixtures are generated with their build revision and copied into the cache (clone-copy on macOS); saved histories are
never deleted or overwritten. Miniflare is used for deterministic fixture generation,
never performance evidence. Cached idle histories may retain earlier definition entries; the
excluded preparation turn installs the current instructions and tool declarations before
measurement. Historical storage still reflects the original fixture.

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

Rigorous mode enables cold turns and six warm repeats per pass; CPU requires `--cpu`. It randomly chooses
ABBA or BAAB, redeploying the **same Worker and namespaces** with the two Yielded builds.
Each pass restarts the same physical Objects. Different storage-format keys automatically
restore each revision's own fixture before every pass. Matching formats also restore by
default when all selected histories have at most 1,000 turns. For larger matching-format
capacity runs, `--restore-each-pass` opts in; otherwise history grows across passes. All restores preserve
original SQLite rows and table counts, then run an excluded preparation turn.
Repeated baselines reveal deployment/time/history drift. The output includes paired differences and confidence intervals. Do not claim gains smaller than
the control or within-Object repeat spread; inspect all Objects, not just the median.

Refs supply `packages/` sources and their own `bun.lock` dependencies. Different lockfiles
receive isolated frozen installations; identical locks reuse an installation. Both builds
use this harness, while pi and tardie retain their isolated vendor dependencies. Each
revision must support its selected fixture. Import, recovery,
transcript, build or residency failures invalidate the affected Object; the command never
bypasses durability checks to force a comparison.
