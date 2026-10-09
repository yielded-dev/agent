# Deployed Durable Object RPC overhead

Effect RPC over HTTP added about **3–4 ms** to the matched-Object median in this fixture and substantially more recorded CPU than native RPC. An established hibernatable Effect RPC WebSocket added about **1 ms**, with uncertainty overlapping zero. The native Schema envelope and cached `effect-cf` runtime did not produce a resolved additional latency penalty. These results do not explain the slower tardie turn benchmark: its measured path uses native Durable Object RPC, despite defining its contracts with Effect RPC.

The branch contains the runnable [fixture](../../deployed/rpc-overhead/README.md), the bounded resumable `watch` [prototype](../../deployed/rpc-overhead/websocket.ts), sanitized deployed observations, and [recomputable statistics](summary.json). No production framework change or optimization PR was made; no removable per-call client→Object cost was established.

All numbers below are **milliseconds**, measured on deployed Cloudflare on 2026-10-09. Latency pairs are **median / p90**. CPU values are Cloudflare's **integer-millisecond invocation observations**: a recorded zero is below the reporting resolution, not proof of zero work. A mean of those observations is not a microsecond CPU measurement.

| Path | Application bytes each way | RTT p50 / p90 | Driver CPU mean / call¹ | Driver CPU mean / invocation¹ | Driver CPU p50 / p90 / invocation¹ | Object CPU mean / invocation | Object CPU p50 / p90 / invocation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Native RPC | 200 | 12 / 48 | 0.370 | 66.645 | 53 / 103 | 0.003 | 0 / 0 |
| Native RPC | 20000 | 14 / 58 | 0.398 | 71.677 | 56 / 118 | 0.023 | 0 / 0 |
| Native fetch + JSON | 200 | 14 / 46 | 0.442 | 79.484 | 60 / 124 | 0.014 | 0 / 0 |
| Native fetch + JSON | 20000 | 15 / 57 | 0.533 | 95.867 | 76.5 / 138 | 0.102 | 0 / 0 |
| Native + Schema, synchronous | 200 | 13 / 48 | 0.442 | 79.562 | 76.5 / 121 | 0.005 | 0 / 0 |
| Native + Schema, synchronous | 20000 | 15 / 57 | 0.495 | 89.032 | 84 / 128 | 0.019 | 0 / 0 |
| Native + Schema + cached runtime | 200 | 14 / 45 | 0.458 | 82.406 | 88.5 / 133 | 0.075 | 0 / 0 |
| Native + Schema + cached runtime | 20000 | 15 / 56 | 0.461 | 83.000 | 63 / 140 | 0.118 | 0 / 0 |
| Effect RPC / HTTP JSON | 200 | 16 / 50 | 1.386 | 249.484 | 194 / 397 | 0.810 | 1 / 1 |
| Effect RPC / HTTP JSON | 20000 | 17 / 60 | 1.734 | 312.172 | 293 / 523 | 0.899 | 1 / 2 |
| Effect RPC / HTTP NDJSON | 200 | 16 / 49 | 1.782 | 320.733 | 279 / 460 | 0.803 | 1 / 1 |
| Effect RPC / HTTP NDJSON | 20000 | 18 / 59 | 2.168 | 390.290 | 316 / 629 | 0.936 | 1 / 2 |
| Effect RPC / hibernatable WebSocket | 200 | 14 / 48 | 0.887 | 159.679 | 148 / 249 | 0.618 | 0 / 1 |
| Effect RPC / hibernatable WebSocket | 20000 | 16 / 56 | 0.973 | 175.226 | 180 / 246 | 0.707 | 1 / 1 |

¹ Each unary driver invocation owns one batch of 160 measured calls plus 20 warmups. Its actual CPU includes setup, guards, warmup, validation and result serialization. The per-call column divides that whole invocation CPU by 180; it is an amortized estimate, not an isolated single-call CPU observation. Object CPU rows are individual measured method/fetch/message invocations. Connection setup is excluded from RTT and remains in driver batch CPU. Instrumentation, including attribution logs, is present in every variant.

The application payload is a shallow versioned envelope containing an ASCII string, exactly 200 or 20,000 bytes as JSON in each direction. Build and attribution metadata add request bytes beyond that payload. A large flat string does not represent a 20 KB tree of many Schema-validated fields. Native RPC and fetch return precomputed constants; no storage/model work or echo computation is timed. The fetch floor includes JSON serialization, but no Schema validation. Both Schema variants validate/encode on both ends; only the runtime variant enters the Object's cached `effect-cf` runtime. HTTP JSON and NDJSON use one request per call, with the client built once per batch and the Object server cached. WebSocket calls use one established connection per batch.

The unary comparison uses the same 16 named Objects, sequential calls inside each Object, four Objects in flight, rotating/reversing variant order, and a second round with the supplied variant list reversed. There are 71,680 timed calls (5,120 per path/size), plus excluded warmups. All 448 batches passed pre/post build and constructor-identity guards, with no timed-call retry or failure. The second unary round used a later build with stream controls and the completed Thread fixture; the measured unary paths were unchanged. Object names are reused across rounds; identity is guaranteed within a batch, not across deployments.

The Worker requested `aws:us-west-1` placement and Objects requested `wnam`. Every second-round response recorded `cf-placement: remote-SJC`; the first round predates per-response header capture. A setup health check also observed remote-SJC. Ingress colo is retained only as ingress metadata and is not interpreted as driver execution placement. These are measurements of one configured placement, not a fleet-wide latency claim. Millisecond clocks, temporal variation and the network limit resolution of small differences.

Within-Object differences are more useful than subtracting pooled medians. For each Object, take each round's median difference, average those two differences, then take the median across the 16 Objects. Brackets below are pointwise 95% Object-cluster bootstrap intervals, conditional on these two runs: 100,000 resamples, seed 829. Calls are not treated as independent replicates.

| Matched comparison | 200 B delta [95% interval] | 20 KB delta [95% interval] |
| --- | --- | --- |
| Native fetch + JSON minus Native RPC | 1 [-0.5, 1.625] | 1.25 [0, 2.25] |
| Native + Schema, synchronous minus Native RPC | 1 [-0.5, 1.75] | 0.5 [0, 1.5] |
| Native + Schema + cached runtime minus Native RPC | 0.125 [-0.75, 2] | 0.25 [-0.25, 1.5] |
| Effect RPC / HTTP JSON minus Native RPC | 3.25 [1.5, 4.625] | 3.25 [2.5, 3.75] |
| Effect RPC / HTTP NDJSON minus Native RPC | 3.25 [1.25, 4.5] | 4 [3.25, 5.5] |
| Effect RPC / hibernatable WebSocket minus Native RPC | 1.25 [0, 2] | 1.125 [-0.5, 2.5] |
| Native + Schema + cached runtime minus Native + Schema, synchronous | -0.25 [-1.25, 0.5] | 0 [-0.5, 0.75] |

The cached runtime versus synchronous Schema comparison is unresolved around zero. The client and Object already cache the runtime, application layers and Schema parsers. Class-result guards after decoding do not establish a second removable deep Schema traversal. Small per-call Context/guard allocations exist, but neither this experiment nor source inspection establishes a worthwhile production optimization. Removing validation or runtime semantics would be an unsupported tradeoff.

For completeness, the two temporal rounds independently yielded:

| Path | Application bytes | Round A RTT p50 / p90 | Round B RTT p50 / p90 |
| --- | --- | --- | --- |
| Native RPC | 200 | 13 / 50 | 12 / 47 |
| Native RPC | 20000 | 14 / 58 | 14 / 57 |
| Native fetch + JSON | 200 | 14 / 47 | 14 / 45 |
| Native fetch + JSON | 20000 | 16 / 59 | 15 / 56 |
| Native + Schema, synchronous | 200 | 14 / 49 | 13 / 48 |
| Native + Schema, synchronous | 20000 | 15 / 60 | 14 / 56 |
| Native + Schema + cached runtime | 200 | 14 / 45 | 13 / 45 |
| Native + Schema + cached runtime | 20000 | 14 / 55 | 15 / 57 |
| Effect RPC / HTTP JSON | 200 | 18 / 54 | 15 / 49 |
| Effect RPC / HTTP JSON | 20000 | 19 / 62 | 16 / 59 |
| Effect RPC / HTTP NDJSON | 200 | 16 / 54 | 16 / 48 |
| Effect RPC / HTTP NDJSON | 20000 | 17 / 58 | 19 / 61 |
| Effect RPC / hibernatable WebSocket | 200 | 15 / 46 | 14 / 48 |
| Effect RPC / hibernatable WebSocket | 20000 | 16 / 58 | 16 / 55 |

WebSocket setup includes upgrade plus a successful hello/build check, before warmup; hello carries control data independent of the following payload size:

| Following application payload | Connections | Setup p50 / p90 |
| --- | --- | --- |
| 200 | 32 | 35 / 111 |
| 20000 | 32 | 39.5 / 92 |

The real endpoint fixture subclasses the actual `ThreadObject`, calls through `CloudflareThreadClient`, and seeds eight completed turns using a scripted model. Status reads a settled seed receipt; progress is already available after sequence zero; submit admits a fresh 200-byte input. These are real endpoint envelopes, not fixed-size 200-byte responses. Admission excludes subsequent settlement draining from latency.

| Real endpoint | RTT p50 / p90 | Driver CPU mean / invocation | Driver CPU p50 / p90 | Object CPU mean / invocation | Object CPU p50 / p90 |
| --- | --- | --- | --- | --- | --- |
| Native floor on Thread Object | 13 / 51 | 1.278 | 1 / 2 | 0.000 | 0 / 0 |
| submissionStatus | 17 / 53 | 2.193 | 2 / 4 | 3.793 | 3 / 7 |
| awaitProgress, already available | 14 / 51 | 1.878 | 2 / 3 | 1.512 | 1 / 3 |
| submit, seeded Thread | 203 / 273 | 1.936 | 2 / 3 | 13.108 | 10 / 22 |

RTT above is 1,600 calls per endpoint across 16 Objects, with ten excluded warmups per batch. CPU is from a separate cohort of 256 calls per endpoint, **one endpoint call per driver invocation**. Preparation, probes and settlement draining occur in different requests; client-cold requests are marked and excluded from warm driver CPU. Whole invocation CPU still includes validation, dispatch and response encoding. Thread history had grown by this later cohort; do not treat these CPU values and the larger cohort's RTT as same-request samples, or subtract this driver CPU from the amortized synthetic driver CPU. The batch cohort's raw CPU is also retained separately.

Warm driver and Object CPU observations in the isolated Thread cohort:

| Endpoint | Warm driver observations | Object observations / 256 |
| --- | --- | --- |
| Native floor on Thread Object | 234 | 246 |
| submissionStatus | 228 | 241 |
| awaitProgress, already available | 237 | 244 |
| submit, seeded Thread | 234 | 240 |

There were 1,010 warm-client requests among 1,024 total; 933 warm driver CPU observations were joinable, and 971 Object observations were joinable. Already-available progress was near the native floor. Status adds actual storage/settlement work. Seeded admission's 203 ms median is far larger than any resolved synthetic envelope/runtime cost; this experiment does not decompose its storage and engine stages.

The server-push comparison uses a common native publisher and a bounded durable frame source. Four small frames (`{sequence, text: "frame-N"}`, about 31–33 application JSON bytes) are pushed per burst. Timing starts on the deployed driver immediately before a publish or notification and ends when that same driver receives a frame. This is **trigger-to-delivery latency**, not a cross-machine one-way timestamp subtraction. Frames in a burst are correlated. Two bursts are separated by 45 seconds of driver-side waiting; there is no Object polling timer. A third control appends and flushes frames without waking the parked consumer, then times only notification and delivery. An early frame invalidates the control.

| Controlled push experiment | Native ReadableStream p50 / p90 | Hibernatable Effect RPC p50 / p90 |
| --- | --- | --- |
| Initial stream transport setup | 14 / 53 | 32 / 107 |
| Publish (including source write) → frame | 14 / 53 | 44 / 68 |
| Notify (source already committed) → frame | 14 / 53 | 12.5 / 51 |
| Wake + validated replay on original socket | — | 20.5 / 406 |
| New WebSocket upgrade + hello | — | 28 / 104 |
| New native stream / WS reconnect → first resumed frame | 20.5 / 55 | 38 / 155 |

Each setup/wake/reconnect row has eight Objects; write-and-delivery has 64 frames and precommitted delivery has 32. The p90 of an eight-Object row is its maximum. The **406 ms wake/replay outlier is retained**. Native and WebSocket delivery are comparable in the precommitted control; this is not evidence that WebSockets are faster. The approximately 30 ms WebSocket gap when source writes are timed disappears under that control. This supports a source-write/output-gating explanation rather than an inherent established-connection penalty; it does not identify an internal platform mechanism conclusively. Cloudflare documents network output gating around durable writes. [Storage API](https://developers.cloudflare.com/durable-objects/api/legacy-kv-storage-api/)

All **8/8** controlled Objects naturally recreated their constructor across idle while retaining the same WebSocket attachment and a single upgrade. The client had acknowledged frame 3, deliberately left frame 4 unacknowledged, then sent a hello to wake the Object. Rebuild replayed exactly frame 4 with correct content; checkpointing it allowed frames 5–8, then 9–12. Explicit reconnect resumed frame 12 from checkpoint 11, on a new connection. No transparent retry masked a lost connection. The earlier eight-Object push run also passed natural hibernation/resume on 8/8; it had no third-burst control (native write/delivery 16/52, WebSocket 46/71). This tests client-triggered wake and replay plus subsequent server push, not spontaneous server wake without an incoming event.

The effect-cf 0.53.0 transport uses JSON, the default `auto-response` heartbeat policy, `DurableObjectWebSocket` hibernation callbacks, and an explicit `resumableStream` descriptor/checkpoint. Connection setup uses the official Effect socket client with automatic reconnection disabled for measurement. Hibernation preserved the subscription descriptor and acknowledged cursor; the durable source supplied replayable frame contents. It did not preserve an arbitrary running JavaScript stream or its in-memory data. Non-resumable pending Effect RPC requests do not acquire this guarantee by changing transport. [effect-cf WebSocket RPC](https://github.com/danieljvdm/effect-cf/blob/main/docs/websocket-rpc.md)

Native stream cleanup exposed a separate limitation: explicit reader cancellation and RPC disposal completed and unlocked the client reader, but the Object still retained two producer observers. Eight extra diagnostic writes did not release them. Every controlled run therefore recorded a cleanup restart **before opening the WebSocket**, and verified zero remaining observers. This restart is excluded from all hibernation proof; the subsequent constructor change occurs naturally with the original WebSocket still connected. The cause of the native producer lifetime is unresolved, and the fixture is not proof of the same bug in draft #829. Producer cancellation should be verified there before shipping. The original pending-reader release-lock failure and subsequent cancellation diagnostics remain in the pilot artifacts.

For draft #829's active live text, keep the native `ReadableStream` approach for now. The controlled comparison gives no latency reason to migrate. Hibernation's main value is leaving a subscription attached while the Object is otherwise idle; an active model/provider connection already keeps the Object awake. Draft text is provisional, future-only and held in memory. Reliable replay would require a bounded durable text buffer (with writes/retention) or a deliberate reset/snapshot policy, plus acknowledgement, deduplication, backpressure and reconnect semantics. The branch prototype demonstrates these mechanics against a durable source; it is not a drop-in implementation of the draft's text semantics.

For long idle progress or settlement waits, a hibernatable **resumable subscription** is a promising next design. Progress already has a durable sequence, and settlement has a durable receipt/result, so correctness need not depend on retaining a fiber. A change from native unary RPC to an ordinary pending Effect RPC unary call is insufficient. An event-driven stream must rebuild from durable state, recheck authorization, checkpoint consumption, handle replay/reset and reconnect, and park without Object timers or unfinished event I/O. Its initial connection and measured reconnect costs are real, but can be amortized across many waits. This is more application protocol work than today's one pending call; the prototype establishes feasibility, not a production implementation. [Lifecycle rules](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)

Idle duration savings are a documented property, **not a measured dollar saving** here. Cloudflare does not charge duration while an Object is idle and eligible for hibernation, even before actual eviction. Duration is shared across all concurrent work on the Object, so removing a waiter saves nothing while other work already keeps it active. Incoming WebSocket messages receive a 20:1 request billing ratio; outgoing messages and platform auto-response heartbeats have separate favorable rules. Count checkpoint/ack/heartbeat messages and buffer writes when modelling a production protocol. Invocation wall time in these logs is not summed as a billing estimate. [Pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

The tardie interpretation also needs correction. The deployed adapter invokes `reference.methods.message`; the installed tardie 0.44.0 Cloudflare adapter dispatches through `THREADS.getByName(...).receive(...)`, a native DO RPC method. It defines Effect `Rpc.make` contracts but the measured turn is not traversing either HTTP Effect RPC transport tested here. The existing end-to-end ranking cannot be attributed to Effect RPC serialization. See the [deployed adapter](../../third-party/src/deployed/tardie.ts) and the installed package's `src/platform/cloudflare/objects.ts` receive implementation, pinned by the third-party lockfile.

CPU attribution joins application markers to platform invocation records, then removes provider identifiers. Query windows are bisected until untruncated; duplicate telemetry event IDs are removed before redaction. Missing or ambiguous joins are never zero-filled. Pooled unary coverage is **428/448 driver invocations** and **63020/71,680 measured Object invocations**. Coverage is incomplete even with full configured log sampling, so CPU comparisons can have missing-data bias:

| Path | Application bytes | Driver CPU observed / expected | Object CPU observed / expected |
| --- | --- | --- | --- |
| Native RPC | 200 | 31/32 | 4562/5120 |
| Native RPC | 20000 | 31/32 | 4699/5120 |
| Native fetch + JSON | 200 | 31/32 | 4521/5120 |
| Native fetch + JSON | 20000 | 30/32 | 4559/5120 |
| Native + Schema, synchronous | 200 | 32/32 | 4364/5120 |
| Native + Schema, synchronous | 20000 | 31/32 | 4626/5120 |
| Native + Schema + cached runtime | 200 | 32/32 | 4416/5120 |
| Native + Schema + cached runtime | 20000 | 30/32 | 4564/5120 |
| Effect RPC / HTTP JSON | 200 | 31/32 | 4316/5120 |
| Effect RPC / HTTP JSON | 20000 | 29/32 | 4493/5120 |
| Effect RPC / HTTP NDJSON | 200 | 30/32 | 4461/5120 |
| Effect RPC / HTTP NDJSON | 20000 | 31/32 | 4377/5120 |
| Effect RPC / hibernatable WebSocket | 200 | 28/32 | 4523/5120 |
| Effect RPC / hibernatable WebSocket | 20000 | 31/32 | 4539/5120 |

| Telemetry cohort | Retrieved events | Unmatched markers | Retried transient read errors |
| --- | --- | --- | --- |
| warm-unary-a | 74561 | 1139 | not recorded |
| warm-unary-b | 74232 | 1334 | 2 |
| warm-thread-a | 21448 | 65 | not recorded |
| warm-thread-cpu | 6752 | 32 | not recorded |
| warm-push | 2242 | 0 | not recorded |
| warm-push-controlled | 2274 | 0 | not recorded |

CPU exports also retain unmarked protocol/setup/control invocations. The stream driver does cleanup/probes and the native stream remains open; these mixed invocations do not provide an isolated frame CPU estimate. They are retained as raw evidence, not reported as per-frame cost or duration billing. Telemetry export encountered HTTP 500, 503 and 403 failures; measured requests were never replayed. Bounded retries covered 429/5xx only. A subsequent account/telemetry probe after 403 succeeded, and successful query pages were privately cached during the final read-only export so another API failure would not discard progress. Raw cache and private ownership state were removed with cleanup.

The final deployed source is commit `b55102f04f06bcd95586d8afc0d97f64feac38ee`; the later telemetry-only change is `40e94837`. Effect is 4.0.0 (the current stable import is `effect/rpc`, formerly `effect/unstable/rpc`), effect-cf is 0.53.0, Bun is 1.4.2, Vite+ is 1.1.0, and the Worker compatibility date is 2026-08-18. Early runs record the base revision before the fixture was committed; exact deployed bytes for unary A and final B are preserved in [compressed bundles](bundles/manifest.json), identified by SHA-256. The paired unary methods are unchanged between those bundles. The early Thread cohorts retain their distinct build hashes, but their exact compiled bundles were not archived; their completed source was committed in `47ff94d0`. Initial push and diagnostic pilot revisions are recorded in their own results. This limits exact reconstruction of those intermediate deployments; see [source provenance](provenance.json).

To recompute every summary table and Object bootstrap from the saved observations, run:

```sh
vp exec python3 examples/durable-bench/results/rpc-overhead/analyze.py
```

The script reads plain result JSON and compressed CPU JSON, uses the arithmetic mean of the middle two values for an even-sample median and nearest-rank p90 (`ceil(0.9*n)-1`), and performs no network requests. [summary.json](summary.json) preserves per-Object paired deltas and per-round statistics. `warm-*.json` are the completed headline cohorts; `*-cpu.json.gz` are sanitized invocation exports. `pilot-*` retain debugging failures and partial outcomes. One two-Object push pilot lost a driver response: its remote outcome remains unknown and was not retried. All headline unary, Thread and eight-Object push cohorts completed without call failures.

`vp run ready` passed after the final benchmark CLI change, as did the focused fixture type check. Earlier validation encountered a local-socket sandbox denial and an unrelated Postgres conformance timeout; the focused existing case and the subsequent full gate passed without changing that test. [Validation](validation.json)

Alchemy destroyed the owned resources through the personal account. Independent API enumeration verified **zero `rpc-overhead` Workers and zero Object namespaces**, then removed private state. [Cleanup](cleanup.json) The privacy audit found no credentials, private Cloudflare account names, deployed resource names, email addresses, or provider IDs in the publishable artifacts. Constructor/socket identity values were reduced to checked booleans before saving results. No PR was opened and nothing was merged.
