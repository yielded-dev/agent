# Inline Cloudflare wake deferral

Inline Thread processing now defers immediate local alarm hints through the existing wake scheduler and requests a prompt wake when processing exits. The change covers `processThread`, `processThreadResolved`, and `processThreadHead` for a Thread owned by the current Object. Durable pre-armed alarms remain active throughout processing.

The deployed experiment used **Danieljmerwe@gmail.com's Account**, Alchemy stacks prefixed `wake-defer`, one network-provider Worker, one Thread-Object Worker, and one nearby driver Worker. Product baseline: `8c05714de84d68961b14e5ab7a3b7d809599563f`; candidate: `9e0c2a15c2fd8320cdc10c676934f2687dc17b64`.

All **224 measured turns on 28 Objects** returned successfully. The paired analysis admits **200 turns on 25 Objects**: four cold observations on three 250-turn/zero-delay Objects failed the predeclared constructor check, so those entire Objects are excluded from both warm and cold summaries. That cell has four independent Objects; every other cell has seven. [Exclusion details](measurement-exclusions.json) and all rejected observations remain in the evidence. Collection is complete; the reducer deliberately retains its “partial; no complete-matrix claim” status because the full planned matrix was not admitted.

The deterministic original/candidate proof reduces immediate arm attempts from **14 to 2 per turn**. Deployed per-cell medians reduce nondeferred hints from **10.3–14 to 2**, while pre-armed alarms continue to run. The strongest driver result is **810.5 [411.5, 1,199] ms saved per cold turn at 50 seed turns and 400 ms TTFT**, with seven Object pairs and a largest within-arm repeat difference of 543 ms. This is the only cell whose positive median effect exceeds the largest observed repeat difference in either arm. The other cells' observed shifts remain within that conservative noise bound; this run does **not** establish a general warm-turn speedup. These are descriptive ranges, not confidence intervals.

Captured CPU attributed to entered alarm handlers does **not** consistently decrease, and total native alarm CPU savings are not established. At 400 ms TTFT the entered-handler count remains about nine per turn because pre-armed deadlines still fire. All 28 seeds retain the required fingerprints—50: `b017b487524e44a4`, 250: `dcea9f30b0917245`—and all **2,016 measured model-request fingerprints** match the independent growing transcript, including the excluded observations.

## Mechanism established before the product change

The local probe uses the real SQLite-backed Thread Object under workerd, two seed turns, and three measured turns containing nine model calls and eight lookup tools. Scripted model waits only permit alarm interleaving. **No local elapsed time or CPU value is used as performance evidence.** [The provenance record](local-counts/provenance.json) identifies the exact original and candidate compiled bytes. Both raw files retain the original commit field; the candidate's dirty source snapshot and bundle digest distinguish it from the original.

| Count during each inline turn | Original, three turns | Candidate, three turns |
| --- | --- | --- |
| All `scheduleNow` calls | 23 / 23 / 23 | 23 / 25 / 25 |
| Calls reaching `armNow` | 14 / 14 / 14 | 2 / 2 / 2 |
| Overlapping entered alarm handlers | 9 / 9 / 9 | 4 / 5 / 5 |
| Recovery observations | 18 / 18 / 18 | 8 / 10 / 10 |
| Maintenance scans | 27 / 27 / 27 | 14 / 15 / 15 |
| Model calls | 9 / 9 / 9 | 9 / 9 / 9 |
| Successful execution claims | 1 / 1 / 1 | 1 / 1 / 1 |

The original 14 immediate hints comprise admission, eleven canonical append batches, canonical settlement progress, and finalized settlement. The append batches are UserInput, RunStarted/RunContext, eight model/tool batches, and final response/RunCompleted. Nine maintenance recovery hints are already deferred inside their passes, giving 23 total calls. The candidate adds an exit hint; nested public-head exit hints remain deferred, so total calls can increase while effective immediate hints decrease.

All 27 original and 14 candidate overlapping handlers start while the inline turn awaits a model response. Each original pass observes an actionable generation and the nonterminal Thread, performs two recovery observations that defer to the live Attempt, scans the ledger, fails to claim the owned head, settles nothing, and retains/re-arms maintenance. One cleanup alarm follows the response in each recorded turn. The inline Attempt remains the only successful claim; there is no ownership theft or extra model execution.

These passes add serialized Object work, storage reads, and recovery/generation checks during model I/O. Deferring hints also avoids repeated wake-maintenance transactions and native alarm writes, as the deployed counts show. The A/B isolates wake deferral but does not partition its latency effect between Object contention, storage I/O and native scheduler overhead. In particular, the observed latency saving must not be described as an equivalent CPU saving; the captured handler CPU does not support that conclusion.

## Deployed comparison

The harness is adapted from `origin/dan/cf-latency-breakdown:examples/durable-bench/results/cf-latency/`, including its OpenAI-compatible SSE mock, native Effect provider adapter, instrumentation and Alchemy boundary. [Build identities](build-identities/all.json) and compressed sources/bundles identify the exact uploaded bytes. The controller compares the retrieved module byte-for-byte and verifies its active version.

Both variants execute in the same Object, namespace and Worker bundle. Task-only instrumentation disables the new processing hook for the baseline arm and enables the product hook for the candidate. No experimental switch enters product code. Both arms share the candidate's interruption-safe maintenance-counter bracket and observer code. This isolates wake deferral; it is not an exact deployed binary comparison of the two whole revisions. The separately archived original product bundle supplies the pre-change mechanism proof. Observer overhead and receipt transfer are included.

The driver is a deployed Worker targeted to `aws:us-west-1`; every compared Object uses `locationHint: "wnam"`. **Driver fetch-to-complete-response latency is the primary client metric.** Laptop latency and the CF-Ray colo of every response are retained separately. Public Worker-to-Worker HTTP calls use Cloudflare's documented [`global_fetch_strictly_public` flag](https://developers.cloudflare.com/workers/runtime-apis/fetch/).

The planned primary matrix uses seven fresh Objects per history/delay cell, with two baseline and two candidate repeats per temperature on each Object. Temperature/repeat blocks and variant order within each block are randomized and interleaved. The Object is the comparison unit. An Object enters paired summaries only when every scheduled turn passes the predeclared admission checks; rejected observations remain in the raw evidence. Both arms use the same provider profile: immediate SSE at zero delay, or 400 ms to first byte plus 10 ms chunk spacing, as in the reused harness.

The 50- and 250-turn seeds must match `b017b487524e44a4` and `dcea9f30b0917245`. Every measured native provider request is checked against an independently reconstructed transcript. Eight prelude turns at history 50 and four at history 250 place the fixture's large lookup outputs outside the comparison; the eight compared turns begin with 58–65 or 254–261 historical turns. Table labels refer to the verified initial seed size.

“Cold” means a verified new incarnation of the same persisted Object after `ctx.abort`, with no prior alarm or harness request in that incarnation. It does not guarantee a fresh Worker isolate or JIT state. Warm turns must retain the preceding completed turn's incarnation.

First-model dispatch is measured on the Object's I/O clock, from immediately before the turn operation to immediately before its first provider fetch. The Object-local model gap runs from the preceding response stream wrapper's EOF/disposal finalization to the next fetch dispatch; it is not the exact `[DONE]` parse interval. The reused provider-gap metric runs from the previous provider invocation's final receipt timestamp to the next provider invocation's handler entry and can include cross-clock skew. Driver and laptop durations each use their own elapsed clock. The dispatch interval excludes Object initialization; driver-to-first-provider arrival is retained separately with cross-clock skew as a limitation. Workers timers advance at I/O, so these intervals and provider-subtracted residuals are not CPU measurements. See [Cloudflare timer behavior](https://developers.cloudflare.com/workers/runtime-apis/performance/).

partial; no complete-matrix claim. All times below are milliseconds.

Two repeats per arm reduce to one median per Object and temperature. Each reported metric uses only Objects with both arm medians known. Positive delta is baseline minus candidate. Repeat columns are absolute differences between the two repeats; the conservative positive-effect flag additionally requires every paired effect positive and the median larger than the largest observed within-arm repeat difference. This is descriptive, not a confidence interval.

### Driver latency and repeats

| Seed | TTFT | State | Objects | Driver baseline → candidate | Paired Δ median [min, max] | Baseline repeat spread median [min, max] | Candidate repeat spread median [min, max] |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 7 | 2308.0 → 1664.0 | 910.5 [62.0, 2595.0] | 637.0 [30.0, 986.0] | 205.0 [7.0, 492.0] |
| 50 | 0 | cold | 7 | 3170.5 → 1898.0 | 939.0 [47.5, 2177.0] | 118.0 [9.0, 992.0] | 250.0 [4.0, 856.0] |
| 50 | 400 | warm | 7 | 6289.5 → 5617.5 | 605.0 [-111.0, 1164.0] | 61.0 [31.0, 430.0] | 88.0 [33.0, 1741.0] |
| 50 | 400 | cold | 7 | 6802.0 → 5887.5 | 810.5 [411.5, 1199.0] | 124.0 [8.0, 543.0] | 117.0 [24.0, 326.0] |
| 250 | 0 | warm | 4 | 1826.0 → 1478.5 | 40.3 [-75.5, 679.5] | 467.5 [233.0, 557.0] | 71.0 [6.0, 106.0] |
| 250 | 0 | cold | 4 | 2664.3 → 2076.0 | 629.0 [82.5, 987.0] | 378.0 [217.0, 803.0] | 201.0 [7.0, 558.0] |
| 250 | 400 | warm | 7 | 6431.5 → 5768.5 | 593.5 [338.0, 985.0] | 143.0 [13.0, 537.0] | 42.0 [1.0, 680.0] |
| 250 | 400 | cold | 7 | 6750.0 → 6079.5 | 498.5 [350.0, 975.0] | 90.0 [19.0, 265.0] | 70.0 [12.0, 640.0] |

### Model timing and alarm CPU

| Seed | TTFT | State | First dispatch from turn start | Driver → provider arrival* | Provider gap median* | Object I/O-clock gap median | Alarm CPU (paired Objects) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 0.0 → 0.0 | 311.0 → 305.0 | 188.5 → 94.5 | 0.0 → 0.0 | 476.0 → 442.0 (n=5) |
| 50 | 0 | cold | 0.0 → 0.0 | 636.5 → 628.0 | 278.3 → 144.0 | 0.0 → 0.0 | 417.0 → 424.5 (n=5) |
| 50 | 400 | warm | 0.0 → 0.0 | 350.5 → 359.5 | 192.3 → 104.5 | 0.0 → 0.0 | 901.8 → 1113.3 (n=4) |
| 50 | 400 | cold | 0.0 → 0.0 | 759.5 → 667.0 | 197.5 → 107.3 | 0.0 → 0.0 | 532.8 → 835.0 (n=6) |
| 250 | 0 | warm | 0.0 → 0.0 | 361.0 → 402.0 | 130.5 → 120.6 | 0.0 → 0.0 | 623.5 → 585.5 (n=3) |
| 250 | 0 | cold | 0.0 → 0.0 | 714.3 → 761.0 | 190.5 → 151.1 | 0.0 → 0.0 | 555.0 → 688.5 (n=3) |
| 250 | 400 | warm | 0.0 → 0.0 | 469.0 → 434.5 | 195.5 → 106.3 | 0.0 → 0.0 | 937.5 → 1098.8 (n=6) |
| 250 | 400 | cold | 0.0 → 0.0 | 801.0 → 820.5 | 196.5 → 111.8 | 0.0 → 0.0 | 949.0 → 1115.0 (n=6) |

*Provider-arrival comparisons can include cross-clock skew. Object timers advance at I/O; zero does not exclude intervening CPU work. Alarm CPU is whole invocation cost for uniquely joined entered handlers, including labeled boundary work; it is not all native scheduling CPU.

### Counts per turn

| Seed | TTFT | State | All scheduleNow | Nondeferred scheduleNow | Overlapping entered handlers | Native setAlarm calls |
| --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | warm | 20.5 → 25.0 | 12.5 → 2.0 | 6.5 → 5.0 | 32.0 → 21.5 |
| 50 | 0 | cold | 21.0 → 25.0 | 12.5 → 2.0 | 7.0 → 5.0 | 33.0 → 24.5 |
| 50 | 400 | warm | 23.0 → 33.0 | 14.0 → 2.0 | 9.0 → 9.0 | 38.0 → 30.0 |
| 50 | 400 | cold | 23.0 → 33.0 | 14.0 → 2.0 | 9.0 → 9.0 | 38.0 → 30.0 |
| 250 | 0 | warm | 17.0 → 19.3 | 10.3 → 2.0 | 3.0 → 2.3 | 19.5 → 16.3 |
| 250 | 0 | cold | 19.0 → 22.8 | 11.0 → 2.0 | 5.0 → 4.0 | 28.0 → 21.5 |
| 250 | 400 | warm | 23.0 → 33.0 | 14.0 → 2.0 | 9.0 → 9.0 | 38.0 → 29.5 |
| 250 | 400 | cold | 22.5 → 32.0 | 13.5 → 2.0 | 8.5 → 8.5 | 37.0 → 29.5 |

### Secondary laptop latency

| Seed | TTFT | State | Laptop baseline → candidate | Response CF-Ray colos (turns) |
| --- | --- | --- | --- | --- |
| 50 | 0 | warm | 2330.0 → 1685.7 | SJC: 28 |
| 50 | 0 | cold | 3204.4 → 1921.7 | SJC: 28 |
| 50 | 400 | warm | 6315.7 → 5645.0 | SJC: 28 |
| 50 | 400 | cold | 6828.6 → 5913.6 | SJC: 28 |
| 250 | 0 | warm | 1852.5 → 1500.3 | SJC: 16 |
| 250 | 0 | cold | 2683.9 → 2102.2 | SJC: 16 |
| 250 | 400 | warm | 6457.3 → 5794.4 | SJC: 28 |
| 250 | 400 | cold | 6777.8 → 6102.1 | SJC: 28 |

Every response's complete CF-Ray and placement receipt is retained in requests.jsonl.gz. The driver is the primary client clock; laptop elapsed time is secondary.

An effect smaller than the repeat spread is not treated as a demonstrated improvement. The offline reduction retains per-Object paired effects, baseline and candidate repeat differences, individual model gaps, count receipts, and CPU-join coverage. Whole CPU of a handler crossing a turn boundary is labeled separately and never prorated; an invocation shared by turns cannot be counted twice. Missing or ambiguous evidence is unknown, not zero.

Complete entered-handler CPU joins exist for **203/224 returned measured turns** (186/200 admitted turns): 1,462 of 1,502 observed overlapping handlers join uniquely to native invocation records. Forty overlaps remain unknown; none is assigned zero CPU. Paired CPU columns use only Objects with complete values for both repeats of both variants (n=3–6, shown in the table). Main-fetch CPU exists for 221/224 returned turns. Provider stream receipts verify all 2,016 fingerprints; 1,986 also have matching provider logs.

Across every captured phase, including the preliminary run, setup, seeding, warmup, resets, measurement and cleanup, the native alarm records total **8,355 invocations and 514,303 ms of known CPU**. The joined subset is not an additional cost to add. Canceled alarm records alone carry **55,568 ms**, including 1,890 records with nonzero CPU; missing handler logs do not establish that no handler work happened or explain cancellation. These global totals cannot be divided into per-variant turn costs.

| Outcome | Turn join | Handler log | Records | Nonzero CPU records | Known CPU ms |
| --- | --- | --- | --- | --- | --- |
| aborted | unjoined | not seen | 7 | 6 | 521 |
| canceled | unjoined | not seen | 2988 | 1890 | 55568 |
| ok | unjoined | not seen | 861 | 853 | 11146 |
| ok | unjoined | seen | 1941 | 1941 | 173676 |
| ok | joined | seen | 2558 | 2558 | 273392 |

Every captured invocation outcome is reported below. There are **0 `exceededCpu` and 0 `exceededMemory`** records, including the separately inspected non-invocation outcome records.

| Worker role | Invocation | Outcome | Captured records |
| --- | --- | --- | --- |
| provider | fetch | ok | 5131 |
| network | fetch | ok | 4435 |
| network | alarm | canceled | 2988 |
| network | alarm | ok | 5360 |
| network | fetch | aborted | 256 |
| network | fetch | canceled | 2 |
| network | alarm | aborted | 7 |
| driver | fetch | ok | 2221 |

The seven failed HTTP requests are readiness checks returning 401 while the rotated benchmark secret propagated. All 224 measured requests returned 200; there are no unanswered attempts, controller errors, provider stream failures, or recorded invocation exceptions in the fresh run. The two canceled fetch records lack an Object identity: one is a seed request and one is a warmup request; both have zero reported CPU and are retained without attributing a cause. The 256 aborted fetch records use the explicit cold-reset endpoint. All 513 captured error logs say `wake-defer explicit cold incarnation`. Alarm cancellations and seven alarm aborts remain in the native outcome accounting rather than being relabeled as successful turns.

These are unweighted counts of **captured** records. Telemetry `sampleInterval` exceeds one (up to 2.8 for the network Worker) despite query `abr_level=1`; capture cannot certify that every platform delivery was retained. See the [Cloudflare telemetry API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/). Unknown CPU and missing logs stay explicit in [the full reduction](analysis.json.gz). All 224 driver responses report `local-SJC`, and every measured laptop response's CF-Ray ends in `SJC`; the raw per-response values are retained.

## Supported paths and progress

Inline processing is a supported public runtime path. Existing Cloudflare consumers include durable-bench, the context-continuity replay worker, the Cloudflare memory worker, and the reconstructed runtime in the shared-Object input-drain check. The optional platform-neutral `WakeScheduler.withProcessing` hook wraps the complete FIFO drain shared by `processThread` and `processThreadResolved`, plus the separate public single-head operation. Schedulers without the hook execute the body unchanged.

Cloudflare defers hints only for a Thread owned by the current Object. Local progress and settlement registrations still fire immediately; remote Thread wakes still use the owning stub. Standard `ThreadObject` submit admits and returns, then the alarm handler processes the Thread inside the existing pass-wide deferral. That path already avoids this cost; its new nested exit hint is a no-op inside the pass.

Pending approvals and built-in attached-child joins suspend the Attempt and return without settlement, unless a racing result permits immediate continuation. Programmatic `Subagent.await` and worker waits can keep inline processing pending while another Thread in the same Object needs alarm-driven execution. Approval decisions and port mutations pre-arm and return after recording. Message delivery and publication retries can require later maintenance. `publishCommitted` awaits one drain; remaining required-publication debt defers native Attempts. Blocking native alarm delivery until inline processing returns could therefore deadlock a wait.

The existing deferral counter suppresses only `scheduleNow`. Mutation pre-arming, native alarm delivery, durable generations, recovery, claims, leases and fencing are unchanged. Scope acquisition and release are balanced on interruption; after release, the best-effort exit hint promptly schedules remaining due work. The after traces still show pre-armed native alarms during inline processing, as required for these waits to progress.

## Alternatives and verification

- A bench-only wrapper would miss supported runtime consumers and reconstructed runtimes.
- Removing pre-arming or blocking native alarm handlers could break recovery and strand maintenance-dependent work.
- Removing all local notifications would break progress/settlement subscribers. Removing their alarm hints globally would also change promptness outside owned inline processing. Dropping only progress hints leaves settlement/recovery hints and provides no guaranteed exit wake after suspension, failure or interruption.
- A separate runtime mode or Cloudflare dependency is unnecessary: the existing wake scheduler already owns platform scheduling hints.

No committed unit tests or new test infrastructure were added. Existing contract/recovery checks, real-workerd count traces, and the deployed same-Object experiment provide the evidence.

`vp run ready` passed locally on the unchanged product commit, including static checks, test suites and builds. The existing [CI ready gate](https://github.com/yielded-dev/agent/actions/runs/37844252228) also passed on `9e0c2a1`. [Validation receipts and the redacted gate log](validation/summary.json) are retained. [Offline replay](validation/reduction-replay.json) reproduced the identical reduction from compressed inputs, excluding only the generation timestamp; all six final raw archives' byte hashes were verified. The temporary local Postgres server was stopped and removed afterward. A conflict check against the subsequently advanced `main` was clean; no branch was merged.

## Interrupted preliminary run and cleanup

The first run lost access to the checkout during an unmatched warmup. Its affected Object was retired without replaying that input. A later session interruption removed the private temporary directory and active processes. Only the earlier checkout snapshot survived: 32 measured receipts on four complete Objects. Later temporary receipts and telemetry are unavailable. [The interruption record](interrupted-run/session-loss.json) preserves that limitation; none of those observations is pooled into the primary comparison.

The task-owned Workers remained deployed. The controller restored management through Alchemy, rotated the lost disposable benchmark secret, verified the same archived bundle bytes, and used fresh `r2` Objects for the new matrix. Alchemy state stayed in a private mode-700 temporary directory outside the repository. Native Cloudflare identifiers in evidence are represented by hashes; credentials and raw identifiers are excluded.

**Cleanup verified at 2026-10-08T22:22:43.386Z.** Alchemy destroyed all three task Workers. The Cloudflare API returned 404 for each Worker and complete account-wide listings contained **no `wake-defer` Workers or Durable Object namespaces**. Verification used **Danieljmerwe@gmail.com's Account**, the same named account and account digest as deployment. The [cleanup receipt](cleanup.json) records no errors, successful final telemetry collection and credential scanning, and removal of the private Alchemy directory. A direct filesystem check also confirmed that directory no longer exists. The scan covered raw and decompressed artifacts; no credential or known opaque identifier matched.
