# First visible text on durable Cloudflare Threads

Durable Yielded Threads have a material first-text delay. A provisional-text prototype reduces warm first visible text from about 2 seconds to 0.7–0.8 seconds. The baseline, concurrent control/prototype comparison, and targeted same-Object observation check are complete. All timing comes from deployed Cloudflare Workers and Durable Objects created through private Alchemy stacks. No local timing is evidence.

## Part 1

The baseline contains 60 successful turns and 540 model calls across 12 Objects, including the settling turns excluded from this table.

| Phase    | Target / seed / state | Objects | First visible (ms) | Visibility lag (ms) | Last token → complete text (ms) |     Settlement (ms) |
| -------- | --------------------- | ------: | -----------------: | ------------------: | ------------------------------: | ------------------: |
| baseline | pi/250/cold           |       3 |      800 [789–815] |       127 [122–133] |                      1 [-14–10] | 16350 [16347–16410] |
| baseline | pi/250/warm           |       3 |      580 [574–595] |       128 [123–133] |                      -2 [-6–-1] | 16157 [16107–16161] |
| baseline | yielded/50/cold       |       3 |   2641 [2609–2810] |    1479 [1450–1479] |                   229 [204–230] | 17140 [16947–17231] |
| baseline | yielded/250/cold      |       3 |   2728 [2611–3885] |    1437 [1424–1452] |                   194 [179–205] | 17104 [16900–18389] |
| baseline | yielded/50/warm       |       3 |   2055 [2036–2079] |    1431 [1421–1449] |                   181 [172–200] | 16334 [16286–16340] |
| baseline | yielded/250/warm      |       3 |   2100 [2082–2180] |    1442 [1426–1452] |                   194 [178–204] | 16364 [16279–16543] |
| baseline | pi/50/cold            |       3 |      810 [797–977] |       119 [117–125] |                       -1 [-4–3] | 16461 [16430–16594] |
| baseline | pi/50/warm            |       3 |      611 [595–620] |       117 [115–124] |                      14 [10–18] | 16105 [16094–16142] |

Yielded adds roughly 1.3 seconds of visibility lag per call compared with pi. Warm first-text repeat ranges within one Object were 12–100 ms for Yielded and 2–72 ms for pi. Cross-clock calibration envelopes were at most 106 ms wide; the gap is much larger. Settlement differs by hundreds of milliseconds, while first visible text differs by about 1.4–1.5 seconds on warm Objects. This satisfies the condition to build Part 2.

## Part 2

Fresh control and prototype cohorts ran concurrently: three Objects at each seed size, with the same cold/settling/three-warm-turn sequence. The control uses the exact frozen main bundle from Part 1. Both cohorts use the same provider bundle and match Part 1's 90 exact model-visible request fingerprints. All 60 turns and 540 model calls passed; all 270 prototype responses first appeared provisionally and reconciled to their canonical records.

| Seed / state | Control first text (ms) | Prototype first text (ms) | Control settlement (ms) | Prototype settlement (ms) |
| ------------ | ----------------------: | ------------------------: | ----------------------: | ------------------------: |
| 50 / cold    |        2351 [2334–2559] |          1174 [1090–1175] |     16867 [16783–17051] |       16875 [16837–16910] |
| 50 / warm    |        2003 [2001–2047] |             681 [672–690] |     16409 [16337–16430] |       16377 [16336–16412] |
| 250 / cold   |        2706 [2662–3075] |          1502 [1479–1640] |     17054 [16926–17575] |       17719 [17619–17773] |
| 250 / warm   |        2178 [2099–2211] |             837 [811–841] |     16349 [16244–16505] |       16683 [16647–16690] |

Warm visibility lag falls to **28 [23–30] ms at 50** and **26 [25–26] ms at 250**, from 1408 [1403–1414] and 1395 [1391–1412] ms in the concurrent control. Last-token → complete-text medians become −50 [−56–−48] and −55 [−57–−55] ms: the text finishes before the later tool-argument tokens. [All metrics](tables.md), [per-call aggregates](summary.json), and [signed calibration bounds](visibility-bounds.json) retain the complete comparison. Calibration envelopes were at most 247 ms wide for the control and 59 ms for the prototype.

Settlement is −0.2% at 50 and +2.0% at 250 in the warm comparison; cold differences are +0.05% and +3.9%. Do not treat the unpaired comparison as proof of zero completion overhead. The three 250-turn prototype Objects had median provider-header latencies of 47–54 ms versus 22–53 ms in the control. Same-Object warm settlement ranges were 34–921 ms in the control and 98–283 ms in the prototype. The report retains these differences without adjusting them away.

To isolate observation cost, the same three 250-seeded prototype Objects then ran one warmup and four alternating live/canonical-only turns, reversing the order in one Object. Each Object retained one incarnation; every sample's exact model-visible requests matched across observation modes and Objects. The paired settlement difference was **−14.5 [−107.8–−3.3] ms**, or −0.09%, including opening a fresh preview subscription. Individual Object differences were −201, −14.5, and +8 ms, against repeat ranges of 75–342 ms. This check detects no completion regression from subscribing; it does not establish a universal bound on prototype overhead or cold startup. [Receipts](observer-noise.jsonl), [paired results](observer-noise-summary.json), and [request parity](observer-noise-parity.json) are retained.

The prototype bundle is `179dd9d6827e78ad1f9b2f0cedc30866d0ae880f1f217db2ed0d1acced5c684c`; the concurrent provider is `4ee1482b8dbcd9d4d631d17f1ce7b5e5892bf891c330aefca6cc54af83e3d351`. [Build identities](build-identities/all.json), [source hashes](build-identities/network-sources.json), and the [source comparison](prototype-source-proof.json) identify the measured inputs and verify they match the submitted implementation. The provider's additional wide-payload branch is used only by the separate overflow proof; ordinary measured responses are unchanged.

## Prototype contract and deployed proof

`ProvisionalText.Publisher` is a platform-neutral, no-op-by-default port. Durable execution binds trusted Thread/Submission/Attempt identity and supplies upstream Effect text parts after validation. Cloudflare supplies synchronous bounded queue offers; producer publication performs no I/O or waits. The new `CloudflareThreadClient.watchText` returns a typed Stream over native Durable Object byte-stream RPC. It authorizes through the same `observe` decision as `readPage` and `awaitProgress`.

An initial `Reset` establishes readiness. Text carries Run/Turn and model-generation identity; `Discard` and `AttemptEnded` invalidate drafts. Consumers reconcile to committed responses and permanently retire matching drafts. Subscription sequences detect dropped frames. Each physical Object has at most eight subscribers and 32 queued frames per subscriber; oversized frames and slow consumers fail closed at the client. Every subscription belongs to an incarnation-owned Scope. The canonical log, continuation schemas, hash chain, claims, fencing, leases, recovery, external-dispatch durability, Unknown handling, and accounting paths are unchanged.

[Deployed receipts](proofs.jsonl) record five separate checks:

| Check                   | Observed outcome                                                                                                                                                                                                        |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Denied observation      | All three public observation APIs returned `OperationDenied`; no Reset/text escaped.                                                                                                                                    |
| Abort after first text  | Matching `Discard` and `AttemptEnded`; canonical `aborted`, zero committed model responses and zero lookups. The in-flight provider stream was cancelled as expected.                                                   |
| Subscriber cancellation | Turn completed with nine model calls/eight lookups; ten sequential reopen/close cycles obtained Reset without exhausting the eight-subscriber capacity.                                                                 |
| Consuming wide text     | Public decoder received all 432 deltas / 1,769,472 text characters and reconciled nine model responses.                                                                                                                 |
| Stalled reader          | Turn completed with nine model calls/eight lookups before reading resumed. Native RPC prefetch plus the bounded queue retained a prefix; resumed sequence jumped from expected 220 to 420, exposing 200 dropped frames. |

Wide proofs pad the same 48 fragments to 4,096 ASCII characters, preserving cadence. They are excluded from performance cohorts. The stalled-reader proof inspects the raw RPC sequence gap; the public decoder rejects discontinuities with `HostProtocolError`. It does not silently deliver a truncated prefix as a complete response.

The prototype is prospective: no current-draft snapshot, replay, or resume. Disconnects and Object replacement require clearing uncommitted drafts and rereading canonical records. Only ordinary assistant text is exposed; reasoning, tool arguments, structured final-tool output, and provider metadata are excluded. Native RPC streams are the initial transport; hibernating WebSockets are not implemented. See the updated [runtime model](../../../../docs/src/content/docs/concepts/runtime-model.md) and [Cloudflare guide](../../../../docs/src/content/docs/platforms/cloudflare.md).

Design-review questions: whether the prospective API is sufficient; whether the eight/32/size limits should be configurable; whether RPC streaming or a hibernating transport should be the long-term interface; and whether the observed cold/250-turn completion differences warrant a larger placement-controlled study. This work is for a draft PR, not merging.

## Workload and comparison

The baseline is `c53cd159208c16c794739da2e35c82df17e6366c` on `main`. The harness adapts the [rebench experiment](https://github.com/yielded-dev/agent/tree/4cbcad5fd1e6544edfbb70e47728d7e10678f0d3/examples/durable-bench/results/rebench): native Yielded admission and alarms, pi-durable 1.0.4, a driver Worker, and a separate OpenAI-compatible provider Worker. [Source proof](baseline-source-proof.json) verifies that every bundled framework source matches that baseline.

Each measured turn requests eight sequential `lookup` calls followed by an answer: nine model calls. Every response emits 48 deterministic word fragments, with 400 ms to the first text fragment and 25 ms between fragments (40 simulated tokens/s). Tool-call declaration and argument fragments follow the text. Text generation lasts 1,175 ms; the eight tool-producing responses have another 75 ms of argument tokens. These are simulated tokens, not tokenizer counts. Finish/usage/SSE termination follows at the same cadence.

Seed histories contain 50 or 250 turns using the original repeating one-tool, one-tool, no-tool pattern and original tool payload sizes. Three independent Objects per target and seed size each execute a cold turn, one settling turn, and three warm repeats. Cold means explicit Object abort and verified reconstruction, not a fresh Worker isolate. Warm means the same verified Object incarnation. Every turn appends history, so this is not a controlled estimate of the isolated effect of warming.

Part 1 ran two Objects concurrently. Part 2 ran two per cohort concurrently (four across control/prototype). The same-Object follow-up ran its three Objects together; failure/overflow proofs ran separately. All accepted performance turns settled successfully, with nine complete native streams and nine verified complete texts.

Yielded's driver uses `CloudflareThreadClient.submit`, `readPage` + `awaitProgress`, and `awaitSettlement`. Pi attaches its public `watchEvents` in its Object before submitting, forwards the snapshot and event batches over a streaming response, and consumes them in the driver. Pi's native partial-generation commits remain enabled. Tardie is omitted: its execution SSE would require a separate observation-readiness/replay integration to make the cold comparison valid.

The provider checks a deterministic semantic transcript fingerprint at every call. An additional SHA-256 covers the actual model-visible messages, system framing, tools, tool choice, model, sampling, and token limit, with only JSON object-key order normalized. For the same seed size, repeat index, and model-call index, it must match across targets and Objects. Pi's provider projection matches Effect's assistant message boundaries and output instruction; the harness does not change either framework's canonical records. Every complete response must match the reference text byte for byte.

## Clocks and aggregation

`submit → first visible text` and settlement use driver Worker timestamps at real I/O completions. Provider receipts record first text, last text, last token (including tool arguments), and SSE end after timer/fetch awaits. Millisecond `Date.now()` is an I/O clock in Workers; it does not measure synchronous CPU time. Laptop timing is excluded.

The prototype's timer starts at submit intent, before opening `watchText` and awaiting Reset. Thus cold startup and subscription readiness are included rather than moved outside the timer. `observationSetupMs` is retained separately. Each benchmark turn opens a fresh subscription; a UI may keep one open across turns.

Cross-Worker visibility lags are calibrated estimates. Echo receipts before and after each turn, including two probes routed through that Object after the primary timer, bound observed provider-minus-driver offsets. We retain the full same-colo envelope plus 1 ms quantization. The assumption is that the offset during the turn stays inside that observed envelope; this is not a guarantee of globally synchronized clocks. Signed bounds are retained. Missing calibration is never replaced with zero.

Completion means the last actual text change that yields the verified complete response. Message-finalization time is also retained. The requested last-token metric can be negative because all assistant text may arrive before the provider finishes its later tool arguments; last-assistant-text-token comparisons are retained separately.

Tables report medians [Q1–Q3] across the three Object medians, using linear-interpolated quartiles. Within an Object, warm values use the median of its three repeats; cold has one sample. Visibility and completion aggregate the nine per-call lags within each turn. Per-call results and same-Object repeat ranges are in [summary.json](summary.json); the settling turn is excluded from headline aggregates.

## Reproduce

Use fresh checkouts and archive previously generated JSON/JSONL artifacts, retaining the harness sources. The baseline harness commit is `af0137c9b965bb91b9fae52cc9675909b999dc73`; its framework is main `c53cd159`. Install the pinned dependencies with `vp install --frozen-lockfile` and `vp run -F @yielded/agent-example-durable-bench vendor`. Credentials come from `direnv exec .` as `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`. Controller commands use `direnv exec . vp exec node examples/durable-bench/results/first-text/run.mjs <action> <phase>`:

1. In the baseline checkout: `init`; build with `vp exec node examples/durable-bench/results/first-text/build.mjs`; then `deploy-seed baseline`, `seed baseline`, `deploy-measure baseline`, `measure baseline`.
2. Transfer the generated evidence and ignored `.private-path` pointer to the PR checkout, retaining the private directory. **Before rebuilding**, run the PR controller's `freeze-baseline baseline`. It checks the captured framework hashes against the original revision and preserves the immutable control bundle.
3. Build the PR harness, then `deploy-provider prototype`. Run `deploy-seed control`, `seed control`, `deploy-measure control`; then `deploy-seed prototype`, `seed prototype`, `deploy-measure prototype`.
4. Start `measure control` and `measure prototype` in separate terminals. After both finish, run `observer-noise prototype-noise`, then `prove proof-prototype`.
5. Recompute tables and paired results with `vp exec node examples/durable-bench/results/first-text/analyze.mjs`.
6. `cleanup` destroys the Alchemy stacks and verifies that both Worker and Durable Object namespace listings contain no `first-text` prefix.

The controller refuses to replay a recorded measured input after an uncertain outcome. Alchemy state and large raw receipts live in a private mode-700 temporary directory outside the repository and are removed after cleanup. Committed evidence retains compact receipts, source fingerprints, tables, and outcomes.

## Outcomes and cleanup

Pilot 1 established the observation paths but normalized away native framing differences. Pilot 2's exact-input check rejected one Yielded comparison because pi tool results were JSON-quoted; the native turns themselves settled. That cohort was retired without replay. Pilot 3 corrected the fixture and passed exact request parity and complete-text checks. Pilot results are excluded from the full baseline table.

Readiness polling received five HTTP 404 responses before workers.dev routes became available. Seven recorded calls to the optional beta `workers/workers` metadata endpoint returned HTTP 500; deployment verification now uses the stable version endpoint. Earlier optional lookups encountered the same error before individual recording was enabled, so their count is unavailable. These failures preceded measurement and did not replace any accepted turn receipt.

Captured platform telemetry contains 382 interruption outcomes: 30 Object fetch aborts from forced reconstruction, 306 cancelled alarms (all zero CPU), 44 cancelled candidate RPC invocations, one cancelled provider fetch during the deliberate abort proof, and one cancelled baseline Worker fetch during control seeding. That last cancellation's cause is unclassified; every controller seed request returned HTTP 200. RPC/alarm cancellation counts are retained without inferring a failure of accepted work. All accepted performance turns, the same-Object follow-up, and all five deployed proofs completed successfully.

The full repository gate passed: [validation.json](validation.json) records `vp run ready`, including the earlier fixed worker-types diagnostic and local test-environment failures. No unit tests were added. [Controller outcomes](non-ok.jsonl), [control-plane outcomes](control-non-ok.jsonl), and [captured platform outcomes](platform-non-ok.json) preserve failures and expected interruption events. Cloudflare invocation telemetry can be sampled; successful per-turn receipts are checked independently.

Cleanup was verified through the Cloudflare API on 2026-10-09 at 06:00 UTC: all three Workers return 404, their Durable Object namespaces are gone, and account-wide Worker/namespace listings contain no `first-text` prefix. The private mode-700 Alchemy directory was removed after its final telemetry collection and credential scan. The isolated local Postgres server used by the repository gate was stopped and its temporary directory removed. [cleanup.json](cleanup.json) records the authoritative checks.
