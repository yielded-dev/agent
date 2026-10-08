# Deterministic count attribution

Read [summary.md](summary.md) for results and [summary.json.gz](summary.json.gz) for complete first-turn tables. This directory owns only counting harnesses and evidence. It does not measure local CPU, elapsed latency, heap size, or Cloudflare billing.

## Replay

Prerequisites are the installed dependency tree at `8c05714de84d68961b14e5ab7a3b7d809599563f`, including the repository's Effect 4.0.0 patch. The parent installed dependencies and built the Cloudflare platform dependency graph. This preserved harness bundles **workspace TS source**, not those dist outputs; that choice was explicitly authorized to retain the original counting path. No repository manifests are rewritten.

Run from the repository root. Choose new output directories; the script refuses an existing directory. Miniflare/workerd is used only to execute the deterministic fixture. A restricted sandbox may require loopback-listener approval.

```sh
task_root="$PWD"
task_attribution="$task_root/examples/durable-bench/results/effect-eval-cost/attribution"
vp node "$task_attribution/counts.mjs" "$task_root" "$task_attribution/replay-capture"
vp node "$task_attribution/counts.mjs" "$task_root" "$task_attribution/replay-repeat"
vp node "$task_attribution/counts.mjs" "$task_root" "$task_attribution/replay-plain" plain
vp node "$task_attribution/summarize.mjs" "$task_attribution/replay-capture" "$task_attribution/replay-repeat" "$task_attribution/replay-plain" "$task_attribution/replay-summary"
```

No `bench/targets.ts`, `quiet`, `busy`, CPU profiler, Inspector, or timing controller is imported. `counts.mjs` has its own untimed HTTP dispatcher. Normal application clocks, deadlines, tracing behavior and timer fibers remain intact; suppressing them would change the workload. The fixture creates new local storage only, seeds 50 turns, closes/reopens the worker, runs recovery outside counting, and counts m0–m9. Calls to `/stats` occur after the windows. Failure reports are persisted. Generated bundles, transformed files and persistence directories are reproducible scratch outputs; final retained evidence is compressed raw JSON plus input/selector inventories.

To inspect compressed JSON without unpacking into another directory:

```sh
vp node --input-type=module -e 'import {readFileSync} from "node:fs"; import {gunzipSync} from "node:zlib"; console.log(gunzipSync(readFileSync(process.argv[1])).toString())' "$task_attribution/capture/report.json.gz"
```

## Counter semantics

`evaluations` counts each entry immediately before `FiberImpl.runLoop` invokes a primitive evaluator (including the tracer-context branch). It counts nested synchronous runtimes: constructors of nested fibers inherit the current caller's attribution stack. It is not a count of every `yield*`, function call, or asynchronous suspension.

`inlineSuccesses` counts entry to `FiberImpl.succeedWith`, including the ContImpl fast-continuation path. Its periodic inline-depth fallback returns an Exit to the loop, so not every entry is a zero-dispatch success. `iteratorSuccesses` counts successful Exits consumed directly by `fromIteratorUnsafe` and `fromIteratorEagerUnsafe`. Both are recorded separately from evaluations. They do not claim to exhaust every synchronous decoder/eager-combinator path. `Schema.decodeEffect` may return a newly allocated Success without dispatch; `Effect.void` may reuse a singleton without either a fresh allocation or a dispatch. Zero dispatch is not zero work.

`allocations` counts construction through `PrimitiveImpl`, `ExitPrimitive`, `AsyncImpl`, `IteratorImpl`, `ContImpl`, `OnFailureImpl`, `OnSuccessAndFailureImpl`, `MatchImpl`, and `OnExitImpl`. Constructor subtype counts are in `allocationKinds`. Service tags, SQL Statements, Toolkit values and yieldable errors have their own Effect protocol implementations and can dispatch without using these nine constructors. Their origin gaps are explicit `unassigned:<identifier>` rows. Ordinary JS objects, closures, spans, arrays, strings, SQL allocation, bytes, GC and the harness's own bookkeeping are outside this metric.

`calls` means entry to a selected JS function/generator body. Generator construction alone is not entry. It does not mean provider requests, tool calls, or all JS calls. More selectors increase this counter; do not compare it to the old narrow-selector harness. Stage `calls` count every selected boundary entry. Inclusive stage `calls` count outermost entries for that stage name, so nested append boundaries collapse. Site inclusive/exclusive `calls` both count each entry, including recursion. Module call fields are unused zeros.

### Dynamic execution attribution

Every selected non-async function body gets `enter`/`leave` in `try/finally`. Expression arrows use a lexical arrow wrapper, preserving `this` and arguments; no Effect combinator is added. Generator bodies retain their token across yielded work. Async JS functions are intentionally not wrapped: an ambient JS stack would be wrong across `await`.

Stacks are fiber-local. A new fiber snapshots its parent's active token references. Inherited tokens count only while the owning token is still open: detached work after a parent's scope closes is not charged to that scope. Plain nested synchronous callbacks and `runSync` fibers inherit the active caller scopes. A JavaScript callback that resumes another fiber uses that fiber's stack. This is logical execution ancestry, not an async wall-time interval.

The generator iterator is tracked during each interpreter `next`. Effect can abandon a generator on failure/interruption without invoking JS `return`, so `finally` alone is insufficient. Runtime hooks close tokens when an Iterator continuation is discarded, when a directly yielded Failure bypasses it, or when its owning fiber exits. `abandonedSites` records these closures. All final rows have zero `unclosedSites`; expected materialization-conflict, abort-watcher, ownership-maintenance and release paths appear in abandonment records. The harness does not resume or close product iterators and does not add finalizers or scheduler operations.

- **Exclusive site/module:** one innermost open selected function receives each event. If none exists, `unattributed` receives it. Module counts sum to the same totals as site counts.
- **Inclusive site/module:** each distinct open site or module receives the event once. Recursive occurrences of the same site/module are deduplicated per event. Inclusive rows overlap and cannot be summed.
- **Exclusive stage:** the deepest open token with a non-null stage wins, including repeated stages nested under another stage. Site-only tokens do not replace stages.
- **Inclusive stage:** each distinct open non-null stage receives an event once. Reentrant instances of the same stage are deduplicated per event.
- **Outside-eight-stages:** a coarse lifecycle remainder, completely partitioned in `outsideStageSites`. It is not the unassigned-site remainder.

### Construction-origin attribution

A WeakMap records the innermost selected function when each primitive is constructed, including during uncounted seed/module initialization. Each counted allocation is charged to its construction origin; subsequent evaluations and iterator-success consumptions of that object retain that origin. Reused primitives retain their earlier origin even after the original function returns. A primitive may be allocated but never dispatched, dispatched repeatedly, or dispatched during a later window.

`runtime-constructor:<family>` means the selected constructor is known but no selected caller scope existed at construction (often a reusable module-level primitive). These rows explicitly lack a business caller. `unassigned:<identifier>` means the evaluated object was not created through an instrumented constructor. The largest such row at 50 history turns is `Service`, 665 evaluations / 1.78%. `inlineSuccesses` has no primitive object argument and is therefore omitted from construction-origin tables.

Origin and dynamic views are **alternative complete partitions**, not costs to add together. For example, schema primitives can be built in SchemaAST and later dispatched while the prompt-read generator is the nearest surviving dynamic scope. This explains the large dynamic `readPrompt` bucket and reveals its schema origins without pretending to have CPU samples.

## Exact original eight-stage map

Source lines below refer to the pinned revision. The complete selectors, source-mapped locations and dynamic expressions are retained in `capture/sites.json.gz` and `summary.json`.

| Stage | Exact selected body | Boundaries / exclusions |
|---|---|---|
| Admission | `DurableAgentRuntime.ts:11023 submit` first generator body | Encoding, admission, materialization and readiness inside submit; surrounding registered-submission orchestration is separately attributed. |
| Ownership acquisition | `RunStorage.ts:277 claim` and `DoSubmissionLedger.ts:1623 claim`, first generator bodies | Acquisition only. The claimed run, heartbeat and release are separate named sites. Nested occurrences use outermost versus all-entry counters. |
| Context assembly | `internal/initial-context.ts:15 initialContext`; `RunJournal.ts:621 projectRunJournalStream` | Includes selected prompt read/reduction beneath those bodies. Original-context publication in `durability.initialize` is separately attributed, not silently included. |
| Model-response commit | `DurableAgentRuntime.ts:5884 commitTurn` generator, when `commit._tag === "Response"` OR `commit._tag === "Settled" && commit.results.length === 0 && commit.response !== undefined` | Deferred readonly response preparation and final response-only settlement are included; actual model requests/stream decoding are outside this stage. |
| Tool-settlement commit | Same commitTurn generator for every other commit | Combined readonly response+results commits are charged here once, not duplicated under model-response commit. Actual tool execution is outside. |
| Continuation preparation | `RunContinuation.ts:1279 prepare` generator | Can nest under commit/settlement. `advanceFacts`, capture and publication are additional named sites; adapter validation inside append is not preparation. |
| Durable Object append | `DoThreadStore.ts:379 append` and `internal/do-journal.ts:691 appendPrepared` generators | Includes journal append from settlement, which bypasses the facade. Duplicate nested stage names do not double-count inclusive events. |
| Settlement | `DurableAgentRuntime.ts:3513 terminalize` generator | Canonical publication and ledger terminalization inside that body. Surrounding completion reads, processThread orchestration and release are separate sites. |

The old eight-stage definitions are deliberately preserved; they are not a claim to cover all logical context, all ownership work, or the complete engine. Broader selectors cover all reachable package functions and the schema parser/AST/getter/transformation, response/LanguageModel, semaphore, tracing, timeout/race helpers. Selectors are source-mapped, so no product source is edited.

## Proof and remaining limits

The reducer checks exact repeated raw rows, both fingerprints, uninstrumented canonical table counts, constructor/opcode closure, all exclusive partitions, outside-stage partition closure, origin partitions, zero open scopes, and the 5% individual unassigned-bucket bound at all ten positions. These are task evidence checks, not a new product test suite.

Counts match across the final independent captures, but workload timers and deadlines remain real product behavior. Slow or overloaded hosts could change scheduler/maintenance activity; reruns must compare the raw counters rather than assume determinism. Coverage does not establish runtime latency or billing. Instrumented source counts should not silently be treated as optimization-identical to a separately built uninstrumented hosted bundle. The combined `vp run ready` handoff gate and final report are retained in the parent task directory.
