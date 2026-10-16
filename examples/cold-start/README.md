# Cloudflare cold-start reproduction

This example reproduces extra first-request latency in Yielded Agent compared with
pi-durable 1.1.0. Both run in SQLite-backed Durable Objects and call the same scripted
Chat Completions provider, which immediately replies “hello”. No model key is needed.

| Layout            | Yielded first text (ms) | pi first text (ms) | Paired gap (ms) |
| ----------------- | ----------------------: | -----------------: | --------------: |
| Single script     |    1,306 [1,121, 1,501] |     670 [590, 766] | +636 [457, 821] |
| Thin front + host |          854 [772, 946] |     557 [506, 625] | +297 [236, 374] |

Means and 95% confidence intervals; **12 pairs per layout across two independent
deployment cohorts**, six pairs in each. All samples, including slow ones, are retained.

Measured 2026-10-16 against Yielded commit
[`481ab30`](https://github.com/yielded-dev/agent/tree/481ab30de77b9dad7137b84a2b1c6d4b2168ec4f),
pi-durable **1.1.0**, Effect **4.0.5**, and effect-cf **0.56.7**. Bundles use esbuild
**0.28.1**, whitespace minification only, and compatibility date **2026-08-18** with
`nodejs_compat` and `global_fetch_strictly_public`. Driver and Thread Objects request
the western North America (`wnam`) location hint; placement is advisory.

Intervals are paired, pointwise 95% bootstrap intervals: 20,000 whole-block resamples
within each deployment. They describe these deployments, not future regions or
traffic. Timers run inside the Cloudflare driver and include transport, scheduling,
and buffering. First-use markers establish logical fresh isolates, not an empty
physical-host code cache.

The measured Thread is **brand new**, with no prior requests or stored history. It
reproduces the gap without fixtures or idle-state preparation. Each sample
has newly uploaded code, a separate namespace, and first-use Worker/Object identities.

## What runs

- `src/yielded-entry.ts`: one Worker exports the Thread Object and forwards requests.
- `src/front.ts`: the thin front, bound to that same Object in a separate host script
  through `script_name`. It imports no framework code.
- `third-party/src/entry.ts`: the corresponding pi-durable implementation.
- `src/identity.ts`: an empty Object exported by each complete framework bundle. It
  returns a marker without constructing a framework host or submitting a turn.
- `src/driver.ts`: a Cloudflare driver Object measures arrival times in one clock.

The thin front avoids evaluating the framework in the forwarding Worker. The Object
still evaluates it and runs the first turn. The single and split layouts use identical
Object bundles. Here it reduces Yielded’s first-text latency by **452 [264, 666] ms**.
This example does not prewarm targets or measure subsequent turns.

## What the measurements attribute

Local module evaluation is dominated by Effect Schema construction: **488 Class
definitions, 888 Struct calls, and 5,213 schema wrappers**. Three independent coverage
captures agree. Seven CPU profiles attribute 56% of the bundle’s self-samples to
Schema/AST helpers; runtime, profiler, and GC samples are separate.

Twelve fresh Node 24.22.0 / V8 13.6 processes per bundle on an Apple M4 Max measure
**28.3 ms** mean evaluation CPU for Yielded and **2.7 ms** for pi. Parsing/linking is
outside that timer (29.1 and 12.6 ms respectively). These are local diagnostics, not
Cloudflare CPU measurements.

On Cloudflare, the single-script **Worker-marker gap is 190 [135, 246] ms**, before
the Object marker. That is about **7.4×** the local evaluation gap of 25.6 ms, or
**4.5×** the local parsing/linking-plus-evaluation gap of 42.1 ms. This compares
elapsed startup with local CPU; transport, scheduling, parsing and compilation are
not separated, so it is not a measured CPU slowdown factor.

The empty Object exposes a startup gap without a framework host or turn. For the
time between Worker and Object markers, the **Yielded minus pi paired gap** has a
mean of **953 [130, 2,494] ms** and a median of **240 ms**. A full agent turn is not
necessary to observe the extra delay.

One Yielded empty-Object marker arrived after **9.6 seconds** (9,654 ms from request
start; 9,597 ms after its Worker marker). This sample remains in the data and widens
the mean's interval.

In the thin-front layout, the time from the Object's first marker to first text is
level: the Yielded minus pi gap is **0 [−26, 24] ms**. Marker delivery waits behind
synchronous work, so this cannot split Object startup from first-turn work. The
**297 ms total gap remains unattributed**; it cannot all be assigned to Schema
construction.

## Run

Use a Workers Paid account with `workers.dev` enabled, Node compatible with the
repository, and Vite+.
The API token needs **Account / Workers Scripts / Edit** for the selected account,
including script/version reads, namespace metadata, and profiling. Set `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` in your environment.
Keep output outside the checkout: it contains deployment identifiers and source maps.

From a fresh clone:

```sh
vp install
vp run -F @yielded/agent-example-cold-start vendor
vp run -F @yielded/agent-example-cold-start bundle
vp run -F @yielded/agent-example-cold-start measure run --out /tmp/cold-start-1
vp run -F @yielded/agent-example-cold-start measure run --out /tmp/cold-start-2
vp run -F @yielded/agent-example-cold-start analyze /tmp/cold-start-1 /tmp/cold-start-2 --out /tmp/cold-start-analysis.json
vp run -F @yielded/agent-example-cold-start local --out /tmp/cold-start-local
```

With direnv-managed credentials, prefix the two `measure` commands and the `profile`
command below with `direnv exec .`. Pi is installed in its own nested workspace and
is not required by repository checks. Its [MIT license](https://github.com/earendil-works/pi/blob/v1.1.0/LICENSE)
permits this use; no third-party implementation is vendored here.

Each run uses a fresh `press-cs-` prefix and owns at most 37 namespaces. Before every
upload it checks for at least 100 spare namespace slots and at most 40 namespaces
across that prefix family, using the standard [500-class account limit](https://developers.cloudflare.com/durable-objects/platform/limits/). It leaves
all targets untouched for at least 180 seconds after the final upload, confirms the
active versions through the management API, then sends one request per target.
Only the provider and driver receive readiness requests.

Every run releases its scripts on completion or interruption and checks the API for remaining scripts,
namespaces, and uploaded versions. After a hard interruption, finish cleanup with the
same output directory and account:

```sh
vp run -F @yielded/agent-example-cold-start measure cleanup --out /tmp/cold-start-1
```

Cleanup is restricted to the exact names recorded in that run. Never reuse an output
directory for new samples. Failed or slow captures remain in the evidence; the analyzer
rejects incomplete or contaminated blocks instead of retrying measured requests.

## Profiling

```sh
vp run -F @yielded/agent-example-cold-start profile --out /tmp/cold-start-profile
```

This separate run requests a five-second CPU profile before any target request, then
resolves the native Object ID without visiting it and requests an Object profile.
Finally, eight no-op Object touches make it recently active, followed by a profile
request issued 300 ms before its first turn. There is no second turn. Source maps and exact
version IDs accompany the uploads; responses and any profile bytes remain in the
output directory, which supports the same cleanup command.

All six attempts (three per framework) returned HTTP 404 / code **10393**:
“No recent executions were found for this Worker.” Both active-Object attempts returned
before the first turn started, despite eight successful no-op touches with matching
Object identity and version. Both first turns completed with one provider call. No
CPU samples were obtained; these errors do not identify the latency’s cause.

Cloudflare’s [on-demand profiler](https://developers.cloudflare.com/workers/observability/profiling-in-production/)
selects an already loaded, recently active isolate. Request issuance is not an
attachment acknowledgement. We need a way to arm recording before module evaluation,
identify the selected isolate, and acknowledge attachment before dispatching the first
turn, plus help diagnosing 10393 after successful traffic to the exact Object.
