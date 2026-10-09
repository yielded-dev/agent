# Durable Object RPC overhead

This deployed fixture separates native RPC, Schema processing, the cached
`effect-cf` runtime, and Effect RPC. Every synthetic variant uses the same
`MicroDO` instances. The real endpoint fixture subclasses `ThreadObject` and
calls it through `CloudflareThreadClient`.

| Variant          | Request path                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------- |
| `native`         | Native DO RPC, constant reply                                                               |
| `fetch`          | DO `fetch`, JSON request and constant JSON reply                                            |
| `schema-sync`    | Native RPC with synchronous Schema encoding/decoding on both sides                          |
| `schema-runtime` | Native RPC with Effect Schema encoding/decoding and the Object's cached `effect-cf` runtime |
| `effect-json`    | Effect RPC over DO `fetch`, JSON serialization                                              |
| `effect-ndjson`  | Effect RPC over DO `fetch`, NDJSON serialization                                            |
| `effect-ws`      | Effect RPC over an established hibernatable DO WebSocket                                    |

The pinned Effect version exposes these APIs under `effect/rpc`. Each payload
is a shallow, versioned string envelope whose JSON encoding is exactly 200 or
20,000 bytes, in both directions. Request attribution and build metadata add
bytes beyond that application payload. Replies are precomputed constants;
there is no storage, model, or echo work in the synthetic method.

## Run and clean up

The Vite task uses `direnv exec .`. Select the personal Cloudflare account in
that environment before deploying. Alchemy owns one `rpc-overhead-*` Worker
and its two SQLite Object namespaces. Private ownership state, credentials,
and the bundled Worker live outside the repository in
`/private/tmp/rpc-overhead-private`. Keep that directory until cleanup succeeds.

```sh
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action deploy
vp run -F @yielded/agent-example-durable-bench rpc-overhead --round unary-a --objects 16 --calls 160 --warmup 20 --concurrency 4 --variants native,fetch,schema-sync,schema-runtime,effect-json,effect-ndjson,effect-ws
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action cpu --round unary-a
vp run -F @yielded/agent-example-durable-bench rpc-overhead --round unary-b --objects 16 --calls 160 --warmup 20 --concurrency 4 --variants effect-ws,effect-ndjson,effect-json,schema-runtime,schema-sync,fetch,native
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action cpu --round unary-b
vp run -F @yielded/agent-example-durable-bench rpc-overhead --round thread --objects 16 --calls 100 --warmup 10 --concurrency 4 --variants thread-native,thread-status,thread-progress,thread-submit
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action cpu --round thread
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action thread-cpu --round thread-cpu --objects 16 --calls 16 --concurrency 4 --variants thread-native,thread-status,thread-progress,thread-submit
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action cpu --round thread-cpu
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action push --round push --objects 8 --concurrency 4
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action cpu --round push
vp run -F @yielded/agent-example-durable-bench rpc-overhead --action destroy
```

Use a fresh round name for each run. Do not deploy while measurements are
running, or overlap runs on the same Objects. `destroy` checks the account
against ownership state, destroys through Alchemy, verifies that no
`rpc-overhead` Workers or namespaces remain, and writes `cleanup.json` before
removing private state. Rerun it after interrupted cleanup.

## Measurement boundaries

The driver requests `aws:us-west-1` placement; Objects request `wnam`.
`cf-placement` records driver execution placement when supplied.
`request.cf.colo` is ingress and must not be interpreted as execution location.
Only clocks inside the deployed driver supply headline timings.

Calls within an Object are sequential. Variant order rotates and reverses
across Objects; the second unary command reverses the supplied order again.
Every batch checks the Object's build and constructor identity before and
after measured calls. Setup can reset an old Object build; measured calls
are never retried. Warmup and client/connection setup are outside call RTT.
Warm comparisons remain sensitive to network and temporal variation, so
compare within-Object medians as well as pooled median/p90.

The driver runtime and protocols are scoped to a batch. Thread clients cache
their runtime and definition digest. Each Thread has eight completed seed
submissions. Status queries the seed receipt, progress reads already available
progress, and admission submits a fresh 200-byte input. Seed execution and
settlement draining are outside admission RTT. The separate `thread-cpu`
action puts exactly one endpoint call in each measured driver invocation;
preparation and settlement draining use other requests. It records and
excludes cold client initialization from the warm CPU cohort.

CPU comes from Cloudflare invocation logs joined to per-call markers. Queries
split until the API result is untruncated. Missing or ambiguous telemetry
stays missing, with explicit coverage. Object CPU is per platform invocation.
Unary driver CPU is per whole batch, including setup, guards, and warmup;
dividing by measured plus warmup calls is an amortized estimate. Integer
millisecond CPU observations cannot resolve a submillisecond Object cost.
These observations are not billing totals.

## Hibernation and stream resume

The `watch` prototype uses `DurableObjectRpcWebSocket.resumableStream` and the
hibernatable `DurableObjectWebSocket` API. A bounded durable frame source,
resume descriptor, subscription key, and acknowledged cursor make replay
verifiable. Native `ReadableStream` and WebSocket consumers read the same
source implementation. A common native control publishes each four-frame
burst. Delivery latency runs from the driver initiating publication to that
driver receiving a frame, so it includes the common trigger and storage work
without subtracting clocks on different machines.

Two bursts are separated by 45 seconds of driver-side waiting. The Object has
no timer or periodic storage poll. The WebSocket leaves the last frame
unacknowledged over idle, then checks for a new constructor with the same
socket attachment and no additional upgrade. Only that combination proves
natural hibernation in this experiment. Replay must contain the expected
sequence and content; checkpointing then allows the next burst. If there is
no recreation, a second idle window is tried and the absence is reported.
A fresh connection also resumes from the saved cursor, with upgrade/hello
and upgrade-through-first-frame costs recorded separately.

Native cancellation is checked at the Object before starting WebSocket idle.
If cancellation leaves a producer parked, an additional eight-frame diagnostic
write tests whether further writes release it. Those frames are excluded from
delivery measurements. If producers remain, an explicit cleanup restart of
the same Object precedes opening the WebSocket; the result records that fact
and verifies zero remaining observers. This cleanup never counts as
hibernation evidence: the socket must survive a later, natural recreation.

This prototype persists its text frames. It does not establish replay for
provisional text that exists only in an Object's memory. A production stream
also needs authorization on rebuild, retention limits, cursor/deduplication
semantics, cancellation ownership, and a reconnect/reset policy.
