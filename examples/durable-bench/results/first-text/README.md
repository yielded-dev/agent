# First visible text on durable Cloudflare Threads

Part 1 completed: 60 turns and 540 model calls passed transcript, exact-request, complete-text, and Object-incarnation checks. The provisional-text prototype is in progress. All timing below comes from deployed Cloudflare Workers and Durable Objects created through private Alchemy stacks. No local timing is evidence.

## Part 1

| Phase | Target / seed / state | Objects | First visible (ms) | Visibility lag (ms) | Last token → complete text (ms) | Settlement (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| baseline | pi/250/cold | 3 | 800 [789–815] | 127 [122–133] | 1 [-14–10] | 16350 [16347–16410] |
| baseline | pi/250/warm | 3 | 580 [574–595] | 128 [123–133] | -2 [-6–-1] | 16157 [16107–16161] |
| baseline | yielded/50/cold | 3 | 2641 [2609–2810] | 1479 [1450–1479] | 229 [204–230] | 17140 [16947–17231] |
| baseline | yielded/250/cold | 3 | 2728 [2611–3885] | 1437 [1424–1452] | 194 [179–205] | 17104 [16900–18389] |
| baseline | yielded/50/warm | 3 | 2055 [2036–2079] | 1431 [1421–1449] | 181 [172–200] | 16334 [16286–16340] |
| baseline | yielded/250/warm | 3 | 2100 [2082–2180] | 1442 [1426–1452] | 194 [178–204] | 16364 [16279–16543] |
| baseline | pi/50/cold | 3 | 810 [797–977] | 119 [117–125] | -1 [-4–3] | 16461 [16430–16594] |
| baseline | pi/50/warm | 3 | 611 [595–620] | 117 [115–124] | 14 [10–18] | 16105 [16094–16142] |

Yielded adds roughly 1.3 seconds of visibility lag per call compared with pi. Warm first-text repeat ranges within one Object were 12–100 ms for Yielded and 2–72 ms for pi. Cross-clock calibration envelopes were at most 106 ms wide; the gap is much larger. Settlement differs by hundreds of milliseconds, while first visible text differs by about 1.4–1.5 seconds on warm Objects. This satisfies the condition to build Part 2.

## Workload and comparison

The baseline is `c53cd159208c16c794739da2e35c82df17e6366c` on `main`. The harness adapts the [rebench experiment](https://github.com/yielded-dev/agent/tree/4cbcad5fd1e6544edfbb70e47728d7e10678f0d3/examples/durable-bench/results/rebench): native Yielded admission and alarms, pi-durable 1.0.4, a driver Worker, and a separate OpenAI-compatible provider Worker. [Source proof](baseline-source-proof.json) verifies that every bundled framework source matches that baseline.

Each measured turn requests eight sequential `lookup` calls followed by an answer: nine model calls. Every response emits 48 deterministic word fragments, with 400 ms to the first text fragment and 25 ms between fragments (40 simulated tokens/s). Tool-call declaration and argument fragments follow the text. Text generation lasts 1,175 ms; the eight tool-producing responses have another 75 ms of argument tokens. These are simulated tokens, not tokenizer counts. Finish/usage/SSE termination follows at the same cadence.

Seed histories contain 50 or 250 turns using the original repeating one-tool, one-tool, no-tool pattern and original tool payload sizes. Three independent Objects per target and seed size each execute a cold turn, one settling turn, and three warm repeats. Cold means explicit Object abort and verified reconstruction, not a fresh Worker isolate. Warm means the same verified Object incarnation. Every turn appends history, so this is not a controlled estimate of the isolated effect of warming.

Yielded's driver uses `CloudflareThreadClient.submit`, `readPage` + `awaitProgress`, and `awaitSettlement`. Pi attaches its public `watchEvents` in its Object before submitting, forwards the snapshot and event batches over a streaming response, and consumes them in the driver. Pi's native partial-generation commits remain enabled. Tardie is omitted: its execution SSE would require a separate observation-readiness/replay integration to make the cold comparison valid.

The provider checks a deterministic semantic transcript fingerprint at every call. An additional SHA-256 covers the actual model-visible messages, system framing, tools, tool choice, model, sampling, and token limit, with only JSON object-key order normalized. It must match across targets, Objects, and repeats. Pi's provider projection matches Effect's assistant message boundaries and output instruction; the harness does not change either framework's canonical records. Every complete response must match the reference text byte for byte.

## Clocks and aggregation

`submit → first visible text` and settlement use driver Worker timestamps at real I/O completions. Provider receipts record first text, last text, last token (including tool arguments), and SSE end after timer/fetch awaits. Millisecond `Date.now()` is an I/O clock in Workers; it does not measure synchronous CPU time. Laptop timing is excluded.

Cross-Worker visibility lags are calibrated estimates. Echo receipts before and after each turn, including two probes routed through that Object after the primary timer, bound observed provider-minus-driver offsets. We retain the full same-colo envelope plus 1 ms quantization. The assumption is that the offset during the turn stays inside that observed envelope; this is not a guarantee of globally synchronized clocks. Signed bounds are retained. Missing calibration is never replaced with zero.

Completion means the last actual text change that yields the verified complete response. Message-finalization time is also retained. The requested last-token metric can be negative because all assistant text may arrive before the provider finishes its later tool arguments; last-assistant-text-token comparisons are retained separately.

Tables report medians [Q1–Q3] across the three Object medians, using linear-interpolated quartiles. Within an Object, warm values use the median of its three repeats; cold has one sample. Visibility and completion aggregate the nine per-call lags within each turn. Per-call results and same-Object repeat ranges are in [summary.json](summary.json); the settling turn is excluded from headline aggregates.

## Reproduce

Use the repository's pinned dependencies and `vp run -F @yielded/agent-example-durable-bench vendor`. Credentials come from `direnv exec .` as `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`. Run the controller with `direnv exec . vp exec node examples/durable-bench/results/first-text/run.mjs <action> <phase>`:

1. `init`, then build with `vp exec node examples/durable-bench/results/first-text/build.mjs`.
2. `deploy-seed baseline`, `seed baseline`, `deploy-measure baseline`, `measure baseline`.
3. Recompute tables with `vp exec node examples/durable-bench/results/first-text/analyze.mjs`.
4. `cleanup` destroys the Alchemy stacks and verifies that both Worker and Durable Object namespace listings contain no `first-text` prefix.

A fresh run needs a fresh artifact directory or archived prior phase plans. The controller refuses to replay a recorded measured input after an uncertain outcome. Alchemy state and large raw receipts live in a private mode-700 temporary directory outside the repository and are removed after cleanup. Committed evidence retains only compact receipts, source fingerprints, tables, and outcomes.

## Outcomes and cleanup

Pilot 1 established the observation paths but normalized away native framing differences. Pilot 2's exact-input check rejected one Yielded comparison because pi tool results were JSON-quoted; the native turns themselves settled. That cohort was retired without replay. Pilot 3 corrected the fixture and passed exact request parity and complete-text checks. Pilot results are excluded from the full baseline table.

The authoritative cleanup status is [cleanup.json](cleanup.json). Prototype results, design limitations, and final deployed outcomes will be recorded here after verification.
