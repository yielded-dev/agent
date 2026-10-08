# Deterministic append counts

Run from the spike checkout, with dependencies installed in both source trees:

```sh
vp run -F @yielded/agent-example-durable-bench spike:sync-do-append:counts <source-root> <new-output-directory> counted 50
vp run -F @yielded/agent-example-durable-bench spike:sync-do-append:counts <source-root> <new-output-directory> counted 250
```

This copies and instruments the durable-bench bundle. It never edits the installed Effect package. Miniflare is used only for deterministic counts and transcript verification; the harness collects no timings.

Each run seeds the reference transcript, asserts its fingerprint, restarts the Object, opens it, and performs ten turns with eight scripted readonly tools and nine model calls each. `m0` has exactly the requested historical turn count; `m1` through `m9` have progressively longer histories. The report preserves all ten rows. Count medians are not timing measurements.

`evaluations` counts Effect interpreter evaluations; `inlineSuccesses` counts the interpreter's inline success path separately. `allocations` counts Effect primitive constructors, including primitives allocated by synchronous Schema decoding. It is not a count of all JavaScript objects or allocated bytes. `sqlStatements` counts calls to `SqlStorage.exec`; implicit transaction/savepoint operations are not counted.

The original append-stage boundary is retained for comparison with KOM-433: `DoThreadStore.append` plus the settlement's `journal.appendPrepared`. The outer settlement authority checks and its final due-queue flush fall outside that boundary. `stageSites` partitions evaluations and allocations within each stage by the innermost attributed call site; `sites` includes nested work and must not be added together. Statement evaluation and `useSpan` hooks cover both yielded statements and methods such as `withoutTransform` without adding Effects to the workload.

`sites.json` records hashes and source locations for the instrumented functions. The raw reports also record the instrumented bundle hash, SQL templates, per-append counts, and input/output fingerprints. One suspended ownership check may remain open when the runtime returns from a turn; the harness records it and closes its attribution at that boundary. It does not extend the append stage.

The deployed comparison uses the existing `perf:cloudflare:cpu:build` and `perf:cloudflare:cpu` commands. Its fixture exercises the public Cloudflare ThreadObject append path with two model calls and two tools per phase; it is a different workload from these counts. See the task report for exact builds and receipts.
