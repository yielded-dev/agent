# Effect Stream live-text adapter: deployed comparison

The proposed Effect Stream adapter showed **no material latency penalty in this sample**. Warm first text was 679 / 805 ms at 50 / 250 seeded turns, versus 751 / 832 ms with the existing native byte stream. Warm settlement was 1.1% / 1.2% lower. These are different Object cohorts with overlapping variation, not proof that the refactor is faster or a statistical non-regression guarantee.

The native control is `a59d51e6`. The tested Effect Stream replacement is now applied to [draft #829](https://github.com/yielded-dev/agent/pull/829) in `5e4a1d8c`; its pre-commit linter added one blank line without changing the executable implementation. [The application receipt](validation-applied.json) records the fresh-checkout gate. This branch keeps the experiment outside the product PR, and the original evidence commit `3cb24168` remains reachable.

## Client latency

Milliseconds, median [Q1–Q3] across three Objects. Each warm Object value is the median of three same-incarnation repeats.

| Seeded turns / state | Native first text | Effect first text | Native settlement | Effect settlement |
| --- | ---: | ---: | ---: | ---: |
| 50 / warm | 751 [703–767] | 679 [642–696] | 16507 [16351–16588] | 16323 [16280–16393] |
| 250 / warm | 832 [788–934] | 805 [752–809] | 16588 [16419–16588] | 16383 [16346–16451] |
| 50 / cold | 1149 [1075–1231] | 1205 [1075–1216] | 16967 [16805–17174] | 17093 [16867–17113] |
| 250 / cold | 1615 [1368–1621] | 1535 [1298–1557] | 17549 [17148–17579] | 17390 [17085–17396] |

Cold 50-turn first text increased by 56 ms; the Object distributions overlap almost completely. Cold 250-turn first text decreased by 80 ms. Same-Object warm first-text ranges were 9–153 ms for native and 23–131 ms for Effect; settlement ranges were 22–179 and 33–145 ms respectively. These mixed differences do not establish an adapter speedup.

Warm provider-first-token → client-first-text lag was 25 [19–35] → 20 [18–21] ms at 50 turns and 20 [3–29] → 30 [25–30] ms at 250 turns. Those differences are below the typical cross-clock calibration width. See [the complete latency table](tables.md), [per-call summaries and repeat ranges](summary.json), and [signed clock bounds](visibility-bounds.json).

The last-token → complete-*text* metric can be negative: eight of nine responses emit tool-call argument tokens after assistant text has ended. Per-call results separately retain text completion and canonical finalization; no signed value was clamped to zero.

## Scope and method

- 60 successful measured turns, 540 model calls, 12 Objects: six per variant, three at each seed size. Every model-visible request fingerprint and complete visible text matched the deterministic reference. [Receipts](turns.jsonl)
- Each Object ran five turns: verified cold reconstruction (`m0`), one predefined settling turn (`m1`), then three verified warm repeats (`m2`–`m4`). Settling turns remain in the receipts but are excluded from the cold/warm table. No failed or slow measured sample was removed.
- Both variants ran concurrently in two lanes each, through the same deployed provider: 400 ms to first token, 48 text fragments at 25 ms intervals, nine model calls and eight tool steps per turn. Production alarms, durable receipts, canonical reconciliation and public `CloudflareThreadClient.watchText` observation were unchanged.
- Cloudflare Workers and Durable Objects were deployed through Alchemy, with compatibility date `2026-08-18`, driver placement `aws:us-west-1`, and Object location hint `wnam`. Timing evidence is deployed only. [Pre-measurement method](method.json)
- First text and settlement use one driver I/O clock, starting before observation setup in the submit workflow. Provider/driver lag uses same-colo echo receipts and the full observed offset envelope plus 1 ms quantization. The resulting interval widths were 23–215 ms, median 45.5 ms, with no missing calibration. Midpoints are conditional estimates, not synchronized-clock measurements.

Cloudflare alarm CPU telemetry was incomplete: only 54 of 60 turns had a unique matching receipt in the collection used for the diagnostic. Alarm CPU also excludes admission RPC, observer and driver work. [CPU receipts](alarm-cpu.json) preserve missing values; **no CPU improvement is established**. A strict completeness check initially failed on a missing receipt, which was retained as a measurement limitation rather than filled with zero.

## What was tested

The [candidate patch](effect-stream-plain.patch) replaces manual native `start`/`pull`/`cancel` handling with `Stream.fromEffectRepeat(Queue.take(queue))`, a prepended Reset frame, `Stream.mapEffect`, `Stream.ensuring(close)`, and `Stream.toReadableStreamEffect`. It retains the same observer Scope, sliding queue, frame bounds and non-awaiting publisher. It does not implement the larger suggested observer-acquisition redesign.

The deployed direct adapter successfully streamed over DO RPC without an additional byte bridge. The installed Effect adapter produces a regular `ReadableStream<Uint8Array>`; [Cloudflare's RPC documentation](https://developers.cloudflare.com/workers/runtime-apis/rpc/#readablestream-writablestream-request-and-response) specifies an underlying byte source. This experiment establishes behavior on the tested runtime, not BYOB support or portability. A byte-bridge variant compiled but was not deployed or timed after the direct probe succeeded. [Compatibility receipts and qualification](compatibility-note.json)

The existing deployed probes all passed: matching observation authorization, abort with matching discard/Attempt end, watcher cancellation plus ten successful reopen/close cycles, a large streamed response, and a stalled consumer whose turn settled before reading resumed and whose dropped frames were detectable. These probes ran after the timing comparison. [Proof receipts](proofs.jsonl)

All 188 native package inputs match `a59d51e6`; the only package or harness-source difference between measured bundles is `packages/platform-cloudflare/src/internal/live-text.ts`. [Input comparison](comparison-inputs.json)

Public receipts hash provider-request, Run, Turn and Attempt identifiers. The publication audit found and replaced ephemeral UUIDs before publishing, verifying that every non-identity receipt field remained unchanged. [Publication scan](publication-scan.json)

| Bundle | SHA-256 |
| --- | --- |
| Native control | `874f8d75e590373c1702de302d31fa43a06bb355cc93cbb1226dd1776764aaf5` |
| Direct Effect Stream | `111a29fbb8cb0d35a4947a278aba62f004fdb27ae35694f4bbf20c4f495702df` |
| Shared provider | `017d55796b2aa5bb4538ef516d5a9363ab5f748cd51514da9fd7f26ccfce94ab` |

The candidate package check passed during the experiment. [The original validation receipt](validation-experiment.json) records that run and the native control's clean-checkout gate. [The application receipt](validation-applied.json) records the full `vp run ready` gate for the replacement now in the PR, in another fresh CI-style checkout without vendored `third-party/node_modules`.

## Non-ok outcomes and cleanup

No measured turn or behavior proof failed. The first direct-adapter probe failed locally with `ENOTFOUND` before reaching a Worker; its original false compatibility row remains, qualified as a controller transport failure. The authorized read-only probe then passed. Native measurement readiness also exhausted 20 local DNS failures before any deployment change; an initial telemetry request had the same local restriction. No uncertain turn was replayed. [Controller failures](controller-non-ok.jsonl)

There were also five `workers.dev` readiness 404s. Platform telemetry captured 239 canceled invocations and 11 aborted fetch invocations across seeding, timing, forced reconstruction and the lifetime probes; these are not failed measured turns. Every captured failed request and non-ok Cloudflare invocation is retained in [platform-non-ok.json](platform-non-ok.json). Cancellation and forced-cold-reset outcomes are reported separately from successful turn receipts.

Cleanup is verified: all three Workers were absent (API 404), no `first-text` Workers or Durable Object namespaces remained, telemetry tail collection succeeded, and private Alchemy state was removed. [cleanup.json](cleanup.json) records the checks.

## Repeat the experiment

Use a disposable checkout of `a59d51e6`, this evidence directory, and the benchmark `package.json`/`bun.lock` from `8c8b27e4` (needed by the existing network harness). Run `vp install --frozen-lockfile --ignore-scripts` and `vp run -F @yielded/agent-example-durable-bench vendor`. Use the original report's prerequisites and keep all credentials in `direnv exec .`.

With an empty prior-results directory, run the following controller stages through `direnv exec . vp exec node`; use `build.mjs` for builds and `run.mjs` for controller actions. Preserve only harness source and the deterministic `baseline-request-parity.json` when starting a fresh run.

1. `run.mjs init`; `build.mjs provider network`; `run.mjs deploy-seed native-prototype`; `run.mjs compatibility native-prototype`; `run.mjs seed native-prototype`; `run.mjs deploy-measure native-prototype`.
2. Apply `effect-stream-plain.patch`, run the Cloudflare package check, then `build.mjs network`; `run.mjs deploy-seed plain-prototype`; `run.mjs compatibility plain-prototype`; `run.mjs seed plain-prototype`; `run.mjs deploy-measure plain-prototype`.
3. Run `run.mjs measure native-prototype` and `run.mjs measure plain-prototype` concurrently. Then run `analyze.mjs` and `run.mjs prove plain-prototype-lifecycle`.
4. Run `run.mjs cleanup` even after failure. It destroys only recorded resources, checks all prefixed Workers and namespaces through the API, records every captured non-ok outcome, scans for exact private values, and removes private state. Restore temporary product/dependency edits afterward.

The controller freezes each bundle by its digest, so compiling the second adapter cannot replace the native deployment during the seed-to-measure transition. Do not replay recorded turn submissions; use a new disposable experiment if an accepted turn is uncertain.
