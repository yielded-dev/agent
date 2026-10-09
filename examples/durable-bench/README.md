# Durable thread benchmark

Compare Yielded's production `ThreadObject` path with pi-durable 1.0.4 and tardie 0.44.0
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
vp run deployed -- --cold --cpu
```

The output has one table per cell: median [Q1–Q3] of **Object medians**, the range of
those medians, each Object's repeat range (median / maximum), and Yielded ÷ pi.
The ratio includes the observed min(Yielded)/max(pi)–max(Yielded)/min(pi) range of
Object medians. This unpaired spread is descriptive, not a confidence interval.
JSON and the printed tables go to gitignored `results/durable-bench-*.{json,md}`. JSON also
records admission, model-call gaps, the last response-to-client interval, ingress colos,
fingerprint/build checks, cold setup attempts, failures and cleanup. No timing evidence is committed.

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
The 400 ms provider also spaces SSE chunks by 10 ms, matching the deployed rebench harness.
All Objects request `locationHint: "wnam"`; the driver and target Worker request
`aws:us-west-1` placement. Placement is a hint, not a guarantee.

For Yielded, the driver times public `submit` admission through public `awaitSettlement`:
the production mutation gate, pre-armed alarm, maintenance processing and notification are
all included. The Worker caches its client runtime and definition digest before timing.
The waiter uses production wake hints with a 500 ms polling fallback. pi and tardie use
their native turn APIs and storage. Driver elapsed time includes the HTTP/RPC boundary;
laptop time and its `CF-Ray` colo are secondary fields, not the headline latency.

`--cold` includes the first turn after acknowledged `storage.sync()` + `ctx.abort()`;
tardie's Actor directory and Thread are both restarted. It verifies a new instance, no
earlier alarm entry, and a first request. This is a cold Object over retained storage,
not a cold isolate, cold disk, or newly seeded conversation. Warm samples require the
same live instance throughout the Object's sequential repeat batch.

Worker and Object [code updates propagate separately](https://developers.cloudflare.com/durable-objects/platform/known-issues/#code-updates).
Before each pass, cold setup waits for the Object's own `BUILD` (including tardie's Actor
directory), retrying acknowledged resets for up to three minutes. Each attempt is recorded;
setup errors stop the run. The final reset is followed directly by the timed input.
Each Thread checks its build before admission, and completed metrics and all provider receipts
must match the expected build. A mismatch invalidates the sample; inputs are never retried.

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
