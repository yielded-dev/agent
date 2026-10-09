# Memory-only draft snapshots: deployed comparison

The snapshot implementation delivered a complete prefix to a client connecting mid-response in **30 ms**, followed by live deltas from the same generation. A deliberately stalled consumer received a fresh Reset in **172 ms** after resuming and its active generation's prefix in **244 ms**, without reconnecting. Both ultimately matched all nine committed responses. Drafts remain in memory only and disappear with the Object incarnation.

The product source is `15b3b93bb895037febf3bacafd371db712b99b29` on `dan/first-text-latency`. This evidence stays on `dan/first-text-evidence`, outside the product PR. Original evidence commit `3cb24168` remains reachable. The [original main/pi comparison](../first-text/README.md) and [native-to-Effect adapter comparison](../first-text-stream/README.md) are historical experiments; this report measures the subsequent snapshot change.

## Client latency

Milliseconds, median [Q1–Q3] across three Objects. Warm values first take each Object's median of three same-incarnation repeats. The control is the already implemented Effect Stream preview at `5e4a1d8ce2d65ec8fb9cb7f3b1f1f892352faa55`; the candidate adds draft snapshots and overflow catch-up.

| Seed / state | Existing preview first text | Snapshot first text | Existing preview settlement | Snapshot settlement |
| --- | ---: | ---: | ---: | ---: |
| 50 / warm | 695 [686–707] | 665 [653–692] | 16426 [16351–16460] | 16238 [16222–16257] |
| 250 / warm | 704 [696–710] | 831 [796–875] | 16239 [16232–16269] | 16566 [16484–16596] |
| 50 / cold | 1143 [1076–1179] | 1035 [1035–1194] | 17029 [16864–17097] | 16764 [16745–16973] |
| 250 / cold | 1255 [1153–1258] | 1375 [1308–1446] | 16809 [16708–16890] | 17246 [17123–17269] |

Warm settlement is 1.1% lower at 50 turns and 2.0% higher at 250; cold differences are −1.6% and +2.6%. These are separate Object cohorts with mixed differences, not a zero-overhead guarantee. Cold first text still trails pi's historical ~810 / 800 ms at 50 / 250 turns; cold admission is owned by a sibling task.

| Seed / state | Existing preview visibility lag | Snapshot visibility lag | Existing preview last token → complete text | Snapshot last token → complete text |
| --- | ---: | ---: | ---: | ---: |
| 50 / warm | 34 [32–38] | 11 [11–12] | −45 [−46–−41] | −67 [−68–−66] |
| 250 / warm | 30 [29–30] | 34 [31–34] | −52 [−53–−51] | −52 [−53–−50] |
| 50 / cold | 33 [30–36] | 29 [21–54] | −46 [−48–−42] | −53 [−58–−27] |
| 250 / cold | 32 [27–34] | 28 [27–29] | −50 [−54–−48] | −55 [−56–−55] |

Cross-clock lag differences this small are below or comparable to calibration uncertainty. Last-token → complete-text can be negative because tool-argument tokens follow assistant text in eight of nine responses. [Per-call results](summary.json) retain all nine model calls for every cell, with text completion and canonical finalization separately; [signed clock bounds](visibility-bounds.json) are not clamped to zero.

Same-Object warm first-text ranges were 9–43 ms in the control and 12–112 ms with snapshots. Settlement ranges were 45–126 ms and 47–2538 ms respectively. The latter includes a **19091 ms** candidate turn, retained without adjustment: call 2 spent **2581 ms** between fetch start and receipt of provider headers, while the provider reported 400 ms to its first token. That observed await interval does not identify a network or CPU cause. [Diagnostics and all repeat ranges](diagnostics.json)

## Same-Object subscription check

The mixed 250-turn comparison motivated one predefined follow-up using the existing observer-noise path. The same three candidate Objects each ran a warmup, then four turns alternating `watchText` on/off; order was reversed on one Object. Retention remains enabled in both modes, so this isolates subscription/frame delivery rather than retention versus the previous implementation.

| Object | Watch on settlement | Watch off settlement | Paired difference | On repeat range | Off repeat range |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 16533.5 | 16647.5 | −114 | 125 | 9 |
| 1 | 16576.5 | 16525.5 | +51 | 77 | 41 |
| 2 | 16375 | 16309 | +66 | 24 | 22 |

The paired median is **+51 [−31.5–58.5] ms**, or **+0.31 [−0.19–0.36]%**, including subscription setup. Subtracting the measured setup interval yields +27 [−63–35.25] ms, a diagnostic only; the headline preserves end-to-end latency. The small sample does not prove zero cost or explain the entire unpaired cohort difference. All 15 turns, including warmups, passed exact request and canonical-text parity. [Receipts](observer-noise.jsonl), [summary](observer-noise-summary.json), [request fingerprints](observer-noise-parity.json)

## Snapshot and lifetime proofs

All six deployed proofs passed on fresh proof Objects, separately from the performance workload. These Objects were unseeded; the proof route's `h50` label does not mean they contained 50 historical turns. Timings below are individual observations, not population medians. [Proof receipts](proofs.jsonl)

| Proof | Observed result |
| --- | --- |
| Mid-response connection | No prior subscriber. Connected while call 0 was in flight; 58-character complete prefix arrived in 30 ms, with same-generation continuation 17 ms later. Submit → first visible was 1742 ms including the deliberate attach delay; settlement 17224 ms. All nine reconstructed texts matched canonical records. |
| Slow consumer | Paused after initial Reset, resumed after the independent observer saw delta 350 while generation 8 was active. Fresh Reset in 172 ms; 57344-character active prefix in 244 ms; later continuation reached 118784 characters. Two Reset epochs on the paused subscription, one on the reference. All nine texts matched; turn settled in 17379 ms. |
| Wide response | 432 deltas, 1769472 characters, nine matching canonical responses, settlement 16667 ms. |
| Authorization | `watchText`, `readPage`, and `awaitProgress` all returned `OperationDenied` before a preview Reset. |
| Abort | A matching Discard and AttemptEnded followed first visible text; canonical outcome was aborted, with no committed ModelResponse. |
| Cancellation | Canceling the watcher did not cancel its accepted turn; ten observer reopen/cancel cycles succeeded and the turn completed. |

## Scope, clocks, and source identity

- 60 successful comparison turns and 540 model calls across 12 Objects: three per seed size and variant. Each Object ran verified cold reconstruction (`m0`), a predefined settling turn (`m1`, retained but excluded), and three warm repeats (`m2`–`m4`). Cold is Object reconstruction, not a fresh Worker isolate. Turns append history, so this does not isolate warming from history growth.
- Both variants ran concurrently, two lanes each, against one provider Worker: 400 ms to first text fragment, 48 deterministic fragments at 25 ms intervals, nine model calls and eight tool steps per turn. These are simulated tokens, not tokenizer counts. All exact model-visible request hashes, visible text and canonical reconciliation matched. [Turn receipts](turns.jsonl)
- Cloudflare deployments used Alchemy, compatibility date `2026-08-18`, driver placement `aws:us-west-1`, and Object hint `wnam`. The production admission, alarm, receipt and settlement paths were used. No laptop timing is performance evidence. [Method](method.json)
- First-visible and settlement use the driver I/O clock, starting before observation setup in the submit workflow. `Date.now()` advances at I/O and is quantized to 1 ms; it is not a synchronous CPU timer. Cross-Worker lags use before/after same-colo echo receipts and the full provider/driver offset envelope plus 1 ms. The assumption that the offset during a turn lies inside that envelope is explicit. Widths were **20–165 ms, median 38 ms**, across all 540 calls, with no missing calibration. No CPU improvement is claimed.
- The candidate changes three bundled product files; all other bundled product inputs and the normal benchmark driver/provider/fixtures match the control. The only harness-source difference is the separately invoked proof endpoint, extended for snapshots. [Input comparison](comparison-inputs.json)

The implementation keeps upstream Effect AI text parts and a no-op core publisher port. Object-scoped memory retains at most 32 parts and 2 Mi UTF-16 code units. Each of eight observers has a 32-event queue. A fresh stream epoch (Reset followed by bounded native parts) represents the snapshot; slow-consumer overflow requests another snapshot without adding producer I/O, encoding, or awaiting the consumer. Effect Stream owns delivery cleanup and converts to ReadableStream only at the RPC boundary. Invalid/overbound drafts still fail closed without becoming canonical facts. Attempts, commits, recovery, durability and model-visible transcripts are unchanged.

| Bundle | SHA-256 |
| --- | --- |
| Existing Effect Stream preview | `9e21998e7f3ad250a5bb8ca7f522476936bd574458625c3e60e682dca34daed5` |
| Snapshot candidate | `19884ccdef18694fc3a6d88ac6902840b20dca897864325b5abf6bc93f4ec567` |
| Shared provider | `c74336c8f356dc0229193d950a4a86d4ef65b4ad468f39dec27195498910b988` |

Both bundles were built from the `5e4a1d8ce` checkout with temporary benchmark dependencies; the candidate additionally applied [snapshot.patch](snapshot.patch). Its input hashes match committed product `15b3b93b`. The bundle manifest preserves that pre-commit dirty state instead of falsely relabeling the build. Fixture SHA-256 is `4d781648cf5870b9c9f5fbbbf72cd4103cf680e9b7a4b3776834c8675eca7461`; Effect 4.0.0 and esbuild 0.28.1 were used.

`vp run ready` passed, 71 tasks with 43 cache hits, in an independent checkout containing the exact committed candidate inputs, frozen dependencies, Node 24.20.0, Bun 1.4.2 and PostgreSQL 18.6. The checkout contained neither evidence nor vendored `third-party/node_modules`. The guide's SSE example passed the documentation build. [Validation receipt](validation.json)

## Non-ok outcomes and cleanup

No measured turn, follow-up turn, or behavior proof failed. Readiness polling recorded **seven HTTP 404s and two HTTP 500s** before benchmark submission; the latter have no classified cause. Captured Cloudflare telemetry recorded **295 canceled invocations and 12 aborted fetches**, across seeding, forced reconstruction, observations and deliberate cancellation/abort proofs. These are not failed accepted performance turns; telemetry can be sampled. Every captured outcome and failed request is retained in [platform-non-ok.json](platform-non-ok.json).

Local development had a sandbox tempdir EPERM during temporary dependency installation and blank-line lint diagnostics before the final successful gate. A later sandbox process inspection was unavailable, and a report-edit command had a missing path separator; neither replayed any submission or affected measured inputs. [Validation receipt](validation.json) retains these local failures.

All three Workers were destroyed through Alchemy and individually returned API 404. A separate prefix inventory found **zero `first-text` Workers and zero Durable Object namespaces**. Telemetry tail collection succeeded, exact private-value scanning passed, and private mode-700 Alchemy state was removed. [cleanup.json](cleanup.json)

Public receipts retain hashes instead of provider-request, Run, Turn, Attempt, stream, and Object identities. No raw telemetry archive or deployed bundle is published. [Publication scan](publication-scan.json)

## Repeat the experiment

Use a disposable checkout of `5e4a1d8ce2d65ec8fb9cb7f3b1f1f892352faa55`, this harness directory, and the benchmark `package.json`/`bun.lock` from evidence commit `01821cd0`. Install with `vp install --frozen-lockfile --ignore-scripts`, then `vp run -F @yielded/agent-example-durable-bench vendor`. Keep credentials in `direnv exec .`; use fresh output files and private external Alchemy state. Preserve only harness source, `snapshot.patch`, and `baseline-request-parity.json` when starting another experiment.

Run `build.mjs` and `run.mjs` below through `direnv exec . vp exec node examples/durable-bench/results/first-text-snapshot/…`:

1. For the exact control bundle, initially use `network/proof.ts` from `01821cd0:examples/durable-bench/results/first-text-stream/network/proof.ts`. Run `run.mjs init`, `build.mjs provider network`, `run.mjs deploy-seed plain-prototype`, `run.mjs compatibility plain-prototype`, `run.mjs seed plain-prototype`, then `run.mjs deploy-measure plain-prototype`.
2. Apply `snapshot.patch` and restore this directory's snapshot-aware `network/proof.ts`. Run `build.mjs network`, `run.mjs deploy-seed snapshot-prototype`, `run.mjs compatibility snapshot-prototype`, `run.mjs seed snapshot-prototype`, then `run.mjs deploy-measure snapshot-prototype`.
3. Run `run.mjs measure plain-prototype` and `run.mjs measure snapshot-prototype` concurrently. After both complete, run `run.mjs prove snapshot-prototype-lifecycle`, then `run.mjs observer-noise snapshot-prototype-observer-noise` and `analyze.mjs`.
4. Run `run.mjs cleanup` even after a failure. It destroys recorded stacks, checks every prefixed Worker/namespace through the API, retains non-ok outcomes, scans private values and deletes private state. Audit the public artifacts before publishing them separately. Restore temporary manifest/lockfile edits and remove vendored dependencies from the product checkout.

Builds freeze bundles by digest, so later compilation cannot silently change a deployment's seed-to-measure transition. Never replay an uncertain recorded submission; start a new disposable experiment instead.
