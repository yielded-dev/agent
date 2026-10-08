## Recommendation

Prototype a **synchronous fact-building and reduction core**, while retaining Effect for Crypto, storage ports, ownership, gates, failpoints, and scoped execution. Do not flatten the entire commit into one uninterrupted operation.

The read-only review is complete. I edited no files and ran no builds, tests, or timing workloads. No performance improvement is established.

### Important baseline findings

- [`canonical-json.ts:10–62`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/canonical-json.ts#L10-L62) is **already plain synchronous code**, including traversal and text-budget charging.
- [`record-encoding.ts:132–193`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/record-encoding.ts#L132-L193) already captures owned, frozen records and shares their canonical encoding with accounting and storage. Preserve this mechanism; do not add another cache.
- [`Usage.ts:260–570`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/core/Usage.ts#L260-L570) still yields an Effect for each checked addition. A fully processed model call has **18–20 checked-add call sites**, depending on optional web-search accounting. These are static source observations—not measured evaluations.
- Switching to `Schema.decodeResult` does **not** eliminate Schema’s internal Effect work: [`SchemaParser.ts:483–487,1011–1043`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/SchemaParser.ts#L483-L487) routes it through the Effect parser and `runSyncExit`. Instrumentation must include nested runtime evaluations.

## Eight-stage map

| Stage | Concrete boundary | Nesting and suspension considerations |
|---|---|---|
| **Admission** | [`DurableAgentRuntime.submit:11023–11215`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L11023-L11215): input encoding → digest → admission → materialization/Thread creation → readiness → Receipt. | Includes administrative appends. Application input codecs can require services; their `EncodingServices` must remain in `R`. Ledger, policy, wake, and failpoint calls remain Effects. |
| **Ownership acquisition** | [`processThreadHead:8815–8840`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L8815-L8840) calls `RunStorage.claim`. The native SQLite implementation is [`SqlRunStorage.claimImpl:335–445`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-sql/src/SqlRunStorage.ts#L335-L445); the generic implementation is [`RunStorage.claim:277–346`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunStorage.ts#L277-L346). | Measure claim acquisition, not the whole `processThreadHead`, which encloses execution and settlement. Preserve atomic finalizer registration and token/epoch handling. |
| **Context assembly** | [`runAttempt:8538–8602`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L8538-L8602), [`initialContext`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/initial-context.ts), and [`projectRunJournalStream:621–1621`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunJournal.ts#L621-L1621). Recovery uses [`readRunContext`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/run-context.ts#L109-L160). | This is several regions, not one contiguous existing function. Original-context publication in `durability.initialize:5744–5822` nests continuation preparation and append. Storage stream pulls remain effectful; resident projection folds can be plain code. |
| **Model-response commit** | [`durability.commitTurn:5884–6186`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L5884-L6186), selecting [`turnResponseBatch` / `turnCanonicalBatch`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunJournal.ts#L1894-L1939). | Readonly responses can be **deferred**: preparation occurs without an append, followed by combined response/results publication or promotion. Count deferred, response-only, and combined commits separately. |
| **Tool-settlement commit** | The same `commitTurn`, selecting [`turnResultsBatch:1942–1978`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunJournal.ts#L1942-L1978). Partial sibling results append individually at runtime lines 6043–6059. | Combined readonly commits belong to both logical categories but are one physical commit. Never sum those overlapping parent intervals. Actual tool execution is outside this stage. |
| **Continuation preparation** | [`makeProgressWriter.captureFacts/prepare/advanceFacts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts#L1279-L1532), with `advanceFacts` at 576–824. | Nested under response, results, context, input, and terminal commits. `loadState`, initial selected evidence, and evidence digests are effectful. Adapter-side `validateProgressAppend` is a separate validation region **inside append**, not another invocation of preparation. |
| **Durable Object append** | [`DoThreadStore.append:379–424`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/DoThreadStore.ts#L379-L424) → [`prepareCanonicalAppend`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/internal/canonical-append.ts) → [`DoJournal.appendPrepared/append:690–1012`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/internal/do-journal.ts#L690-L1012). | Settlement bypasses `DoThreadStore.append` and calls `journal.appendPrepared` inside [`DoSubmissionLedger.publishSettlement`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/DoSubmissionLedger.ts#L2011-L2167). Instrumenting only the facade misses those writes. **This DO stage is not exercised by the Node SQLite workload.** |
| **Settlement** | [`currentUsageSummary:5173 onward`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L5173) and [`terminalize:3513–3585`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L3513-L3585). | Includes continuation preparation, authority-checked canonical publication, notifications, ledger finalization, and joined settlements. The benchmark’s subsequent canonical completion read is additional work, not terminalization itself. |

For attribution, retain inclusive parent stages, exclusive synchronous substages, and an explicit remainder for provider/tool execution, wake/polling, background ownership maintenance, finalizers, fixture checks, and unassigned allocations. Process CPU sampled across an asynchronous interval is not automatically CPU belonging only to that stage.

## Concrete implementation recipe

All production edits should remain with the **single integrator**.

1. **Flatten usage arithmetic first.**  
   In [`core/Usage.ts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/core/Usage.ts), replace per-field Effect additions with plain checked arithmetic returning tagged failure data. Preserve:
   - safe-integer checks and field-specific errors;
   - unknown versus known-zero coverage;
   - optional web-search accounting;
   - grouping identity and ordering;
   - seed validation and nonmutation.

   Keep `sumRunTotals` and `summarizeModelUsage` returning their existing typed Effects. If journal/continuation code needs the same plain implementation, place it in a private `core/internal/usage.ts`; use an explicit public selector rather than accidentally exporting synchronous internals from the public Usage module.

2. **Split journal construction at actual dependency boundaries.**  
   In [`durable/RunJournal.ts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunJournal.ts):
   - make staged-usage validation, prompt preparation, tool-result record construction, and final batch assembly plain functions;
   - preserve Schema validation and `copyJson`;
   - retain Crypto Effects for message and completion digests;
   - preserve deterministic identities, record order, and existing batch shapes.

   Use one lazy Effect boundary per synchronous stage, translating tagged expected failures to `RunJournalError` once. Avoid a new generic pipeline executor.

3. **Make continuation reduction plain, not continuation orchestration.**  
   In [`durable/RunContinuation.ts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts), separate:
   - state/evidence acquisition and digest calls;
   - the plain semantic reducer currently in `advanceFacts`;
   - continuation construction, byte fixed-point calculation, capacity checks, and future-envelope checks.

   Pass resolved immutable evidence into the reducer; do not add retained lookup caches. Reuse the **same reducer** from `verifyRunContinuations`, retaining its independent canonical reconstruction and comparison.

   Preserve acceptance only after successful append, including replay invalidation:

```1553:1567:/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts
      const result = yield* writer.append(prepared.batch).pipe(
        Effect.catchTag("AppendConflict", (conflict) => {
          if (conflict.reason !== "tail" || retries >= 8) return Effect.fail(conflict);
          cached.clear();

          return Effect.succeed(undefined);
        }),
      );

      if (result === undefined) continue;

      if (result.replayed) cached.clear();
      else prepared.accept();

      return result;
```

4. **Keep capture ownership intact; normalize expected bounds failures.**  
   The exact ownership surface is:
   - [`internal/canonical-json.ts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/canonical-json.ts);
   - [`internal/record-encoding.ts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/record-encoding.ts);
   - [`ThreadStore.PreparedAppend.capture:360–459`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/ThreadStore.ts#L360-L459).

   Bounds refusals can become private tagged values instead of `RangeError` control flow, mapped to the existing typed error at capture’s Effect boundary. Preserve Schema ownership, sparse-slot validation, UTF-16 key ordering, exact UTF-8 width, frozen payload copies, and original exported wire. This is primarily error-boundary consolidation: serialization is already synchronous.

5. **Flatten resident projection work without flattening history I/O.**  
   In `RunJournal.ts`, make `accountResponse`, `flushTools`, usage accumulation, and record visiting plain reducer operations. Share the reducer between array and streaming entrypoints; do not duplicate projection semantics.

   [`Stream.runForEachArray`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/Stream.ts#L19908) offers an existing chunk boundary. Explicitly cap work: a caller-created stream chunk is not necessarily bounded. Keep interruption opportunities between bounded portions rather than folding an arbitrary uncompacted Thread in one synchronous call.

6. **Limit surrounding changes.**  
   In [`DurableAgentRuntime.ts`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts), flatten the local usage-summary arithmetic and synchronous commit-input assembly only. In [`storage-cloudflare/internal/do-journal.ts:438–453,646–688`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/internal/do-journal.ts#L646-L688), `checkValueBound` and `prepareAppend` are suitable plain validation functions.

   Leave SQL statements, transaction ownership, publication authority, lease maintenance, and append retry orchestration unchanged. Shared capture already reaches SQLite through [`prepareSqlAppend`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-sql/src/SqlThreadStore.ts#L68-L111); a separate SQL rewrite is unnecessary for this prototype.

### Real suspension versus Effect evaluation

Node SHA-256 uses synchronous `createHash` inside `Effect.try` in [`NodeCrypto.ts:30–41`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/.bun/node_modules/@effect/platform-node-shared/src/NodeCrypto.ts#L30-L41). Node SQLite statements likewise use `DatabaseSync` inside Effects in [`SqliteClient.ts:136–183`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/@effect/sql-sqlite-node/src/SqliteClient.ts#L136-L183).

Those yields are not necessarily asynchronous waits. Nevertheless, retain their ports: browser Crypto uses `crypto.subtle.digest` through `Effect.tryPromise` in [`BrowserCrypto.ts:69–92`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/@effect/platform-browser/src/BrowserCrypto.ts#L69-L92). Gates can wait when contended; storage transactions, callbacks, custom codecs, and failpoints can suspend or fail. No Node-specific synchronous hashing should enter core.

## Bounds and regression risks

Preserve the existing limits, not merely successful fixture behavior:

- JSON: depth **64**, collection length **4,096**, **65,536** serialized occurrences, **1 MiB** persisted values.
- Records **4 MiB**; batches **256 records / 16 MiB**; continuation envelope **8,192 bytes**.
- Run evidence **16,384 records / 32 MiB**; recovery suffix **64 records / 2 MiB**.
- Terminal reserves, future-result reservations, and the bounded continuation-byte convergence loop.

Sources: [`Records.ts:109–114,1137–1149`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/Records.ts#L1137-L1149), [`ThreadArchiveRange.ts:8–10`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/ThreadArchiveRange.ts#L8-L10).

The highest-risk regressions are:

- Accepting speculative progress before commit, or retrying stale continuation bytes after a tail conflict.
- Moving fencing or settlement authority checks outside the transaction.
- Treating unknown historical results as canonical settlements or replay permission.
- Changing declaration-order tool projection or compaction coverage.
- Reconstructing context from the latest Thread instead of the original saved range and `historyDigest`.
- Losing typed Schema failures, or converting genuine defects into expected refusals.
- Removing enough Effect checkpoints to starve input or lease-maintenance work.

## Existing proof to reuse

I inspected assertions—not just test names—in:

- [`thread.test.ts:41–110`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/test/durable/thread.test.ts#L41-L110): exact digest vectors, Unicode ordering, shared-DAG accounting, cycle and bound rejection.
- [`run-journal.test.ts:384–546,956–1061`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/test/durable/run-journal.test.ts#L384-L546): Unknown history, late result placement, and saved-range projection equality.
- [`provider-usage.test.ts:44–181`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-memory/test/provider-usage.test.ts#L44-L181): failed-call accounting, missing usage, and replay without recounting.
- [`sqlite-ledger.test.ts:142–280`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-sqlite/test/sqlite-ledger.test.ts#L142-L280): same-epoch administrative append forces settlement progress re-preparation.
- [`sqlite-storage.test.ts:209–315`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-sqlite/test/sqlite-storage.test.ts#L209-L315) and [`do-storage.test.ts:260–373`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/test/do-storage.test.ts#L260-L373): rollback versus lost acknowledgement, atomic continuation publication, and replay.
- [`crash/run-continuation.test.ts:34–194`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/platform-node/test/crash/run-continuation.test.ts#L34-L194): process loss, original context, preserved history, usage totals, and no repeated completed tools.
- [`runtime-history-cost.test.ts:333–359`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-memory/test/runtime-history-cost.test.ts#L333-L359): interrupted reads release resources/ownership and racing later appends do not replace original context.

No new unit tests or infrastructure are justified by this review.

**Rollout recommendation:** keep the candidate local until the requested fresh counts, seven alternating CPU cohorts with control, transcript fingerprints, long-thread diagnostics, and `vp run ready` establish the result. Existing [`stageCheckout`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/runtime-benchmark.ts#L173-L250) and the [`resident checked loop`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/tooling/runtime-benchmark/src/steady-state.ts#L175-L297) are appropriate reuse points; the stock comparison alone does not supply the requested seven-cohort, three-way CPU evidence.

```json
{
  "changed_files": [],
  "complete": true,
  "evidence": [
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/core/Usage.ts:260-570 contains per-addition Effects suitable for a private synchronous arithmetic core.",
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunJournal.ts:329-1621 and 1648-1978 establish projection, record-building, split-commit, and combined-commit boundaries.",
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts:576-1008 and 1279-1606 establish recomputable reduction, capacity accounting, retry, and post-commit acceptance requirements.",
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/record-encoding.ts:132-193 and durable/ThreadStore.ts:340-459 show existing synchronous owned capture and shared canonical wire.",
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/internal/do-journal.ts:646-1012 and DoSubmissionLedger.ts:2011-2167 show distinct ordinary-append and settlement-publication paths.",
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-sql/src/SqlRunStorage.ts:335-810 and SqlJournal.ts:357-618 show native SQLite ownership, append, fencing, and transaction boundaries.",
    "/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/SchemaParser.ts:483-487 and 1011-1043 show that Result decoding still invokes the Effect parser/runtime.",
    "Reviewed existing canonical JSON, journal projection, usage accounting, SQLite/DO append recovery, same-epoch settlement retry, original-context, and process-crash assertions without executing them."
  ],
  "gaps": [
    "Completion applies only to the assigned read-only architecture role. No prototype or production patch was created, and Git branch, HEAD, and worktree cleanliness were not independently rechecked.",
    "No before/after deterministic stage evaluations, primitive allocations, call measurements, or Node CPU attribution were collected. Neither performance target is verified.",
    "No seven alternating baseline/prototype/identical-code control cohorts were run; runtime environment, CPU spread, paired ratios, and host contention remain unmeasured.",
    "No informational pi-durable/tardie cold/warm comparisons or exact pi transcript fingerprints at 50, 250, 1000, and 3500 turns were verified.",
    "Neither long-thread-aging-256-131328 nor long-thread-store-size nor vp run ready was run, as prohibited by this role.",
    "Historical instrumentation discovery was not exhaustive across sibling worktrees. The inspected KOM-432 results directory listed no artifacts; current stageCheckout and resident-loop sources were located. No instrumentation or retained evidence files were written."
  ],
  "summary": "Completed the read-only commit-path architecture review. Recommend one bounded synchronous fact-building, usage, projection, and continuation-reduction prototype, with tagged expected failures translated at Effect boundaries and all Crypto, storage, ownership, resource, interruption, and failpoint contracts preserved. Canonical JSON and owned wire capture are already synchronous. No rollout or performance claim is supported until the integrator completes the required measurements and correctness proof."
}
```