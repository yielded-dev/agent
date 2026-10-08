# Deterministic append counts

Run from the spike checkout, with dependencies installed in both source trees:

```sh
vp run -F @yielded/agent-example-durable-bench spike:sync-do-append:counts <source-root> <new-output-directory> counted 50
vp run -F @yielded/agent-example-durable-bench spike:sync-do-append:counts <source-root> <new-output-directory> counted 250
```

This copies and instruments the durable-bench bundle. It never edits the installed Effect package. Miniflare is used only for deterministic counts and transcript verification; the harness collects no timings.

Each run seeds the reference transcript, asserts its fingerprint, restarts the Object, opens it, and performs ten turns with eight scripted readonly tools and nine model calls each. `m0` has exactly the requested historical turn count; `m1` through `m9` have progressively longer histories. The report preserves all ten rows. Count medians are not timing measurements.

`evaluations` counts Effect interpreter evaluations; `inlineSuccesses` counts the interpreter's inline success path separately. `allocations` counts Effect primitive constructors, including primitives allocated by synchronous Schema decoding. It is not a count of all JavaScript objects or allocated bytes. `sqlStatements` counts calls to `SqlStorage.exec`; `syncTransactions` separately counts `transactionSync` calls. Workerd's implicit transaction/savepoint statements are not exposed as `exec` calls and cannot be included in the SQL counter.

The original append-stage boundary is retained for comparison with KOM-433: `DoThreadStore.append` plus the settlement's `journal.appendPrepared`. The outer settlement authority checks and its final due-queue flush fall outside that boundary. `stageSites` partitions evaluations and allocations within each stage by the innermost attributed call site; `sites` includes nested work and must not be added together. Statement evaluation and `useSpan` hooks cover both yielded statements and methods such as `withoutTransform` without adding Effects to the workload.

`sites.json` records hashes and source locations for the instrumented functions. The raw reports also record the instrumented bundle hash, SQL templates, per-append counts, and input/output fingerprints. One suspended ownership check may remain open when the runtime returns from a turn; the harness records it and closes its attribution at that boundary. It does not extend the append stage.

The deployed comparison uses the existing `perf:cloudflare:cpu:build` and `perf:cloudflare:cpu` commands. Its fixture exercises the public Cloudflare ThreadObject append path with two model calls and two tools per phase; it is a different workload from these counts. See the task report for exact builds and receipts.

From the repository root, with clean baseline and candidate source trees:

```sh
vp run perf:cloudflare:cpu:build --source-root <baseline-tree> --output-dir <new-baseline-build>
vp run perf:cloudflare:cpu:build --source-root <candidate-tree> --output-dir <new-candidate-build>
vp run perf:cloudflare:cpu --baseline-dir <new-baseline-build> --candidate-dir <new-candidate-build> --output-dir <new-run-directory>
```

The controller on this branch creates its own identical-code control from the baseline bundle. It uses task-prefixed Alchemy resources and private temporary state, verifies cleanup, and retains failed requests and missing telemetry. A nonzero exit for an incomplete sample set does not mean cleanup failed; inspect both `completeness.json` and `cleanup.json`.

`analyze.py` reads retained evidence without running a workload. It uses only complete baseline/candidate/control triples for paired CPU tables, preserves unmatched sample counts, and includes background alarm outcomes separately. `export-telemetry.py` performs a read-only Cloudflare API export for a closed run; credentials stay in the environment. `verify-cleanup.py` independently checks every created Worker and scans for task-prefixed Workers and Durable Object namespaces. Large final proof JSON files and build source maps are stored as lossless gzip, with original and compressed hashes in `../compression.json`.
