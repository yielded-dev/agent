## Review result

The synchronous-core approach is compatible with the durability model **if it preserves lazy execution, snapshot ownership, typed failure boundaries, and transactional authority checks**. Continuation preparation is not entirely synchronous: it performs real Crypto and selected-storage operations.

I changed no files and ran no builds, tests, or benchmarks. The commands below are source-verified recommendations, not passing results.

## Contract and failure inventory

| Boundary | Contract the prototype must preserve |
|---|---|
| **Capture and encoding** | Capture must occur when the Effect executes, before Crypto or writer acquisition can suspend. Schema validation does not transfer ownership: retain private, frozen payloads and use the same captured JSON for hashing, accounting, and storage. Repeated execution must not share speculative mutable state. See [PreparedAppend.capture](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/ThreadStore.ts#L340) and [captureRecord](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/record-encoding.ts#L130). |
| **Canonical JSON and bounds** | Preserve UTF-16 key ordering, JSON escaping, exact UTF-8 charging, sparse-array rejection, ancestor-cycle rejection, and charging every occurrence of shared acyclic values. Limits include 64 levels, 4,096 collection entries, 65,536 nodes, 1 MiB persisted values, 4 MiB records, 256 records per batch, and 16 MiB batches. Do not replace these checks with ordinary `JSON.stringify`. See [persistence validation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/Records.ts#L109), [canonical serialization](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/canonical-json.ts#L10), and [batch limit](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/ThreadArchiveRange.ts#L10). |
| **Usage accounting** | Preserve nonnegative safe-integer checks, aggregate/component equality, pricing-group identities, uncertainty markers, unobserved-call counts, and seed immutability. Missing usage is not free execution. Keep failures as `UsageAggregationError`, or their existing `RunJournalError` mapping—not constructor defects. See [checked arithmetic and aggregation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/core/Usage.ts#L251) and [commit-time usage validation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunJournal.ts#L1642). |
| **Continuation preparation** | Facts precede their continuation atomically. Preserve original-input/context references, monotonic revisions/accounting, declaration-ordered failure streaks, pending-result/terminal reserves, and self-inclusive byte-accounting convergence. Accept prepared state only after successful append; on tail conflict, clear retained state and recompute against the winner. See [append validation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts#L383), [fact advancement](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts#L575), and [preparation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts#L1279). |
| **Append, fencing, settlement** | Preserve epoch validation, original batch-replay identity, expected-tail CAS, atomic fact/index/tail publication, rollback, and acknowledgement-loss behavior. Settlement publication must validate live authority atomically, including owned replays. Rotating ownership tokens must be read under their existing gate—not captured early. See [RunStorage](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunStorage.ts#L277), [DO append](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/src/internal/do-journal.ts#L690), and [SettlementPublisher](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/SettlementPublisher.ts#L58). |
| **Recovery and projection** | Checkpoints are disposable; continuations and referenced facts own recovery truth. Missing/corrupt evidence must fail closed without a full-Thread fallback. Cold reconstruction must preserve the original admission boundary, instructions/input, projected Prompt, and `historyDigest`. Unresolved ordinary calls remain Unknown without automatic replay. See [recovery view](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L1618), [original-context verification](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/run-context.ts#L49), and [recovery classification](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/Recovery.ts#L824). |

This ordering must remain after flattening:

```1562:1567:/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/RunContinuation.ts
      if (result === undefined) continue;

      if (result.replayed) cached.clear();
      else prepared.accept();

      return result;
```

### Non-obvious synchronous-conversion hazards

- **Evaluation timing:** `Effect.succeed(compute())` executes `compute` eagerly; it is not equivalent to the current suspended capture. Keep synchronous stages inside the execution thunk, with fresh local accumulators.
- **Errors versus defects:** `Effect.sync` turns thrown exceptions into defects. Replacing `makeEffect` with unchecked `.make` can therefore change `E`. Preserve existing error tags and causes, including capacity errors nested in `ThreadStoreError`. Do not blanket-catch an entire stage and reclassify unrelated defects or interruptions. See [Effect.sync semantics](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/Effect.ts#L1706).
- **Hidden Effect work:** `Schema.decodeResult` still goes through an Effect-backed synchronous runner. It returns schema failures as values but throws for defects, interruption, or asynchronous work. Merely changing codec adapters does not establish fewer evaluations. See [SchemaParser result semantics](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/SchemaParser.ts#L458) and [runner implementation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/SchemaParser.ts#L1011).
- **Interruptibility:** removing primitives removes scheduler/interruption opportunities. Keep bounded stages and page-level waits; do not turn an unbounded history stream into one synchronous traversal. Effect checks yielding during its [run loop](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/effect/src/internal/effect.ts#L642).
- **Dependency and observation timing:** retain Crypto, storage, Clock, current writer/token reads, Scope finalizers, semaphore gates, and meaningful failpoint/tracing boundaries. In particular, preserve [response/results commit and dispatch-fence ordering](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/DurableAgentRuntime.ts#L6143). `reference` itself performs [effectful hashing](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/internal/evidence.ts#L18).

## Existing proof commands

Run from the task workspace root, sequentially and outside CPU measurement. These are focused alternatives to waiting for the final full gate; do not rerun successful evidence unnecessarily.

| Proof | Existing command |
|---|---|
| Canonical ordering, fixed digests, cycles/shared occurrences, projection and compaction behavior | `vp -C packages/effect-agent test test/durable/thread.test.ts test/durable/run-journal.test.ts` |
| Original-context recovery, missing/corrupt evidence, interrupted reads/ownership release, retained usage without double counting | `vp -C packages/storage-memory test test/recovery-checkpoint.test.ts test/runtime-history-cost.test.ts test/provider-usage.test.ts` |
| SQLite append/checkpoint conformance, rollback/acknowledgement loss, fencing, settlement-progress reprepare, ledger interruption | `vp -C packages/storage-sqlite test test/sqlite-storage.test.ts test/sqlite-ledger.test.ts` |
| DO transactional append, retirement/reopen, typed value-bound rejection before writes, ledger authority/failpoints | `vp run -F @yielded/agent-storage-cloudflare test -- test/do-storage.test.ts test/do-ledger.test.ts test/do-ledger-failpoints.test.ts` |
| Malformed selected continuation causes retained, content-free recovery faults | `vp run -F @yielded/agent-platform-cloudflare test -- test/recovery-faults.test.ts` |
| Actual process loss around compaction; current-format export/import preserving original context and history | `vp -C packages/platform-node test test/crash/run-continuation.test.ts test/crash/thread-transfer.test.ts` |
| Focused stale-owner, terminal-publication, Unknown, and post-handler crash windows | `vp -C packages/platform-node test test/crash/crash.test.ts -t 'stale Attempt|before settlement publication|terminalize:after-canonical-append|turn:after-response-append|handler returns'` |
| Required final gate | `vp run ready` |

Notable assertions actually inspected:

- [Cycles, shared-node charging, Unicode/fixed digests](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/test/durable/thread.test.ts#L41).
- [Blocked corrupt/missing recovery evidence without execution or history mutation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-memory/test/recovery-checkpoint.test.ts#L697).
- [Same-epoch settlement race must reprepare progress](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-sqlite/test/sqlite-ledger.test.ts#L140).
- [DO bound refusal leaves both record and batch tables empty](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/storage-cloudflare/test/do-storage.test.ts#L755).
- [Unknown blocks even a handler that had not started before ownership loss](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/platform-node/test/crash/crash.test.ts#L856).

**Coverage gaps:** I found no existing explicit safe-integer aggregation-overflow test, canonical-append caller-mutation/laziness regression, or schema-valid continuation-counter corruption negative. Existing malformed-continuation coverage removes required fields; it does not independently prove semantic recomputation rejects otherwise well-formed false accounting.

For a retained **Node SQLite** copy, the existing read-only verifier is:

`vp run admin:durable -- --database "$DB" verify --thread "$THREAD"`

It invokes [continuation recomputation](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/effect-agent/src/durable/ThreadInvariants.ts#L52) and [fails the command on failed checks](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/durable-admin.ts#L182). Healthy verification does not replace the missing negative controls.

`vp run ready` also needs local Postgres for existing certification; [the default is localhost:55432](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/packages/testing/test/certification-postgres.test.ts#L21). Its availability is unverified.

## Benchmark and fixture reuse

### Long-thread proof

After production builds, one matched selection avoids separate staging:

`vp run perf:diagnose --base-dir "$BASE" --case long-thread-aging-256-131328 --case long-thread-store-size --out-dir "$EVIDENCE/long-thread"`

Here `$BASE` must be the prepared post-#816 baseline, and `$EVIDENCE` is `/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/results/kom433/spike-01a119d5`. Use a new output directory.

The fixture checks original Receipt/input/context identity, one original mutation, zero changed-handler calls, unchanged archived facts, bounded selected reads, and identical request/record-count shapes across ages/store sizes—not merely successful completion. See [diagnostic assertions](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/tooling/runtime-benchmark/src/diagnostic-aging.ts#L555).

### Resident CPU limitation

The existing command is:

`vp run perf:compare --base-dir "$BASE" --steady-state-profile --case sqlite-tool-rounds-4 --out-dir "$EVIDENCE/resident-profile"`

It uses [production `stageCheckout`](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/runtime-benchmark.ts#L172), 500 warmups, and 1,000 checked operations with 5,000 model calls and 4,000 tool calls/finalizers. However:

- It records Inspector samples, **not Node user+system CPU**.
- Profiling runs one base/head pair; unprofiled resident timing runs three pairs. Neither provides seven baseline/prototype/identical-control cohorts.
- It provides no eight-stage evaluation/allocation inventory.
- The resident Node workload does not execute `DoJournal.append`; DO evidence needs a separate local workerd lane.
- Commit/settlement stages enclose continuation and append work. Inclusive intervals must not be summed.

The old named KOM-432 helpers were not found in the searched repository/sibling artifact paths; the preserved `kom432` directory listed empty.

### Transcript fingerprints and informational comparisons

Existing commands producing fresh fingerprint receipts are:

- `vp run -F @yielded/agent-example-durable-bench seed -- pi 50 250 1000 3500`
- `vp run -F @yielded/agent-example-durable-bench seed -- yielded 50 250 1000 3500`

Compare all four resulting sidecars explicitly. **Neither `bench` nor `report` asserts cross-target fingerprint equality.**

Preserved fixture directories exist in [dan-bench-810](/Users/dan/dev/effect-agent/.worktrees/dan-bench-810/examples/durable-bench/fixtures) for all three targets and all four sizes, and in [dan-spike-saved-context-range](/Users/dan/dev/effect-agent/.worktrees/dan-spike-saved-context-range/examples/durable-bench/fixtures) for pi/yielded. Their inspected metadata agrees:

| Turns | Historical fingerprint |
|---:|---|
| 50 | `b017b487524e44a4` |
| 250 | `dcea9f30b0917245` |
| 1,000 | `ac520308146f2a8f` |
| 3,500 | `0a8c8e4b0d9a0794` |

The inspected workload plans match current source, and competitor metadata names pi 1.0.4/tardie 0.44.0. These are reusable input candidates, **not current-code proof**: sidecars lack source/build identities, and current-format reopen/closure remains unverified. Listings include WAL/SHM files; preserve the complete persistence directory when taking a verified closed copy.

The fingerprint is the first eight SHA-256 bytes of normalized `[role,text,calls]` messages. It fingerprints the last provider request, excluding system messages and the final answer subsequently emitted—not the complete canonical archive. See [fingerprint definition](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/src/plan.ts#L59) and [provider capture](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/src/yielded.ts#L68).

For informational timing, use `bench -- TARGET 50 250 --samples 3 --cpu-max 0.08` through the same workspace task for each target. Preserve raw JSONL separately: [report grouping uses only target/version/turns](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/bench/results.ts#L31), so same-version baseline/prototype samples would otherwise mix.

**Safety:** [seed unconditionally deletes its work/output fixture directories](/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/examples/durable-bench/bench/seed.ts#L10). Run only in verified task-owned destinations, never directly in preserved sibling worktrees.

```json
{
  "complete": true,
  "summary": "Completed the read-only durability and proof review. Identified required synchronous-conversion invariants, existing focused proof commands, reusable historical fixture candidates, and concrete coverage gaps. This does not approve the prototype or establish a performance improvement.",
  "evidence": [
    "Inspected append capture, canonical encoding, usage arithmetic, continuation preparation and recomputation, ownership/settlement transactions, original-context recovery, and neighboring boundary tests.",
    "Inspected production staging and the resident 500-warmup/1000-operation workload, plus long-thread aging and store-size assertions.",
    "Inspected preserved pi/yielded/tardie fixture metadata at all four requested sizes and the current fingerprint identity rules.",
    "Read the required repository, Effect, testing, simplification, performance, and CLI guidance.",
    "No files were modified and no builds, tests, benchmarks, hosted services, or paid models were run."
  ],
  "gaps": [
    "Current branch cleanliness and the supplied baseline SHA were not independently verified by this reviewer; no prototype diff was validated.",
    "Explicit existing negative proof was not found for safe-integer usage aggregation overflow, canonical-append laziness/caller mutation, or schema-valid continuation-accounting corruption.",
    "Before/after eight-stage evaluations, primitive allocations, call counts, Node CPU attribution, seven alternating baseline/prototype/identical-control cohorts, spread, and host contention remain unmeasured.",
    "The stock resident harness does not supply Node user+system CPU or a seven-cohort three-arm comparison; it does not exercise Durable Object append.",
    "Current-code transcript fingerprints at 50, 250, 1000, and 3500 turns, both long-thread diagnostic passes, and informational competitor comparisons remain unverified.",
    "Historical fixture closure and current-format reopen compatibility remain unverified; historical matching fingerprints are not prototype proof.",
    "The old named KOM-432 instrumentation was not found in the searched artifact locations.",
    "Final vp run ready and its local Postgres prerequisite remain unverified."
  ],
  "changed_files": []
}
```