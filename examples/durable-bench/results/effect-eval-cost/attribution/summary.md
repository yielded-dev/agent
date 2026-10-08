# Deterministic attribution: effect-eval-cost

At exactly 50 historical turns, the first reopened turn performs **37431 run-loop evaluations**, **44024 selected primitive allocations**, **2303 inline-success continuations**, and **1167 successful Exits consumed directly by generators**. The dynamic unassigned remainder is **30 evaluations (0.08%) / 111 allocations (0.25%)**. The largest unknown protocol origin is Service dispatch: **665 evaluations (1.78%)**. The largest known-constructor bucket without a selected caller is Success: **1,259 evaluations (3.36%)**. Every individual unassigned bucket is below 5% for all ten turns.

Two independent final captures have identical complete counter rows, including all site/module tables and scope-abandonment records. Both retain the historical fingerprint **b017b487524e44a4** and measured fingerprint **b73859cee894aca6**. The uninstrumented control has both fingerprints and identical canonical table row counts. This is deterministic work attribution, with no CPU, latency, Inspector, profiling, or clock sampling.

## Identity and scope

- Source: `8c05714de84d68961b14e5ab7a3b7d809599563f`; tree `2281b0377b65cbc5355223a518472e5ce228dfae`.
- Counted bundle SHA-256: `51a339ea864c60f1660dfea371f290c1f56f768ee4ca04ffd0724fc3a090ade3`.
- Build-input inventory digest: `8b2c2334d8c4da9c881b64620b1488a590e12865b707ba311c41eb547ca12e19`.
- Harness SHA-256: `99f65a1557707d51fe50b09ab58e148aa181c6db0b9d62a23c7a5af5d6b71daa`; probe: `d1b06fdc49e8b6d413823ee179bbcd916369e9273918ab60da721354a2ecf427`.
- Effect 4.0.0 with the repository patch; esbuild 0.28.1; Node v24.21.0; darwin/arm64.
- The preserved harness path is retained: workspace TypeScript source plus installed Effect JavaScript, bundled for workerd. These are source attribution counts, not a claim that a separately built uninstrumented hosted bundle has identical optimization behavior. Every bundle input is hashed in `capture/build-inputs.json`.
- Seed h0–h49 uses the repeating tool pattern 1,1,0. The worker closes/reopens, runs recovery outside the window, then m0–m9 each issue eight sequential tool calls and nine model requests. Only m0 has exactly 50 preceding turns; later rows include prior measured turns. No provider network calls occur.
- Wake/recovery warms storage, but m0 still constructs some model/decoder caches after reopening. The original fixture is preserved; it is not a separate steady-cache run at a fixed 50-turn history.
- The fingerprint hashes normalized last-provider-request messages; it excludes system messages and the final answer emitted afterward. It is not a canonical archive hash.

## Growing-history rows

| Turn | History | Evaluations | Allocations | Inline success | Generator success |
|---|---:|---:|---:|---:|---:|
| m0 | 50 | 37431 | 44024 | 2303 | 1167 |
| m1 | 51 | 38338 | 45194 | 2450 | 1167 |
| m2 | 52 | 39335 | 46622 | 2597 | 1167 |
| m3 | 53 | 40336 | 48052 | 2744 | 1167 |
| m4 | 54 | 41333 | 49480 | 2891 | 1167 |
| m5 | 55 | 42335 | 50911 | 3038 | 1167 |
| m6 | 56 | 43332 | 52339 | 3185 | 1167 |
| m7 | 57 | 44331 | 53767 | 3332 | 1167 |
| m8 | 58 | 45329 | 55194 | 3479 | 1167 |
| m9 | 59 | 46329 | 56623 | 3626 | 1167 |

## Eight-stage counters at 50 history turns

| Stage | Exclusive evals | Inclusive evals | Exclusive allocations | Inclusive allocations | Entries | Outermost entries |
|---|---:|---:|---:|---:|---:|---:|
| admission | 693 | 693 | 717 | 717 | 1 | 1 |
| ownership-acquisition | 537 | 537 | 501 | 501 | 4 | 2 |
| context-assembly | 9976 | 9976 | 12412 | 12412 | 2 | 2 |
| model-response-commit | 926 | 2283 | 1950 | 4010 | 9 | 9 |
| tool-settlement-commit | 1040 | 6189 | 1116 | 6997 | 8 | 8 |
| continuation-preparation | 2062 | 2062 | 3424 | 3424 | 20 | 20 |
| durable-object-append | 6179 | 6179 | 6348 | 6348 | 23 | 12 |
| settlement | 869 | 1297 | 937 | 1410 | 1 | 1 |
| Outside eight named stages | 15149 | — | 16619 | — | — | — |
| Whole turn | 37431 | — | 44024 | — | — | — |

The 15,149-evaluation stage remainder is classified by the site tables below; it is not a single unknown bucket. Stage entries count all selected boundaries, while outermost entries deduplicate nested identical stage names (e.g. append facade plus journal: 23 entries, 12 outermost entries). Inclusive stages overlap and must not be summed. Exact boundaries and scope semantics are in [README.md](README.md).

## Construction-origin modules, ranked by evaluations

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| `effect/dist/SchemaAST.js` | 6826 | 18.24% | 15054 | 34.19% |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts` | 3906 | 10.44% | 3898 | 8.85% |
| `effect/dist/internal/effect.js` | 3196 | 8.54% | 2966 | 6.74% |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts` | 2414 | 6.45% | 2016 | 4.58% |
| `packages/effect-agent/src/durable/RunJournal.ts` | 2346 | 6.27% | 2368 | 5.38% |
| `packages/effect-agent/src/durable/RunContinuation.ts` | 1920 | 5.13% | 1776 | 4.03% |
| `packages/storage-cloudflare/src/internal/do-journal.ts` | 1883 | 5.03% | 1765 | 4.01% |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts` | 1734 | 4.63% | 1673 | 3.80% |
| `packages/storage-sql/src/SqlThreadNativeReads.ts` | 1661 | 4.44% | 1626 | 3.69% |
| `effect/dist/internal/core.js` | 1492 | 3.99% | 74 | 0.17% |
| `effect/dist/Semaphore.js` | 1254 | 3.35% | 1239 | 2.81% |
| `packages/storage-sql/src/SqlThreadWork.ts` | 1243 | 3.32% | 1186 | 2.69% |
| `effect/dist/SchemaParser.js` | 1030 | 2.75% | 1915 | 4.35% |
| `packages/storage-cloudflare/src/internal/owned-state.ts` | 712 | 1.90% | 688 | 1.56% |
| `unassigned:Service` | 665 | 1.78% | 0 | 0.00% |
| `effect/dist/SchemaGetter.js` | 625 | 1.67% | 966 | 2.19% |
| `packages/storage-cloudflare/src/DoThreadStore.ts` | 529 | 1.41% | 529 | 1.20% |
| `packages/platform-cloudflare/src/internal/due-queue.ts` | 464 | 1.24% | 464 | 1.05% |
| `packages/platform-cloudflare/src/Alarm.ts` | 453 | 1.21% | 381 | 0.87% |
| `packages/platform-cloudflare/src/ThreadObject.ts` | 430 | 1.15% | 0 | 0.00% |

## Construction-origin sites, ranked by evaluations

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| [SchemaAST.js:1581 · Objects.getParser](../../../../../node_modules/effect/dist/SchemaAST.js) | 1962 | 5.24% | 7223 | 16.41% |
| [SchemaAST.js:1569 · Objects.getParser.resume](../../../../../node_modules/effect/dist/SchemaAST.js) | 1546 | 4.13% | 1546 | 3.51% |
| [effect.js:raceAllFirst · raceAllFirst](../../../../../node_modules/effect/dist/internal/effect.js) | 1531 | 4.09% | 1431 | 3.25% |
| `runtime-constructor:ExitPrimitive:Success` | 1259 | 3.36% | 65 | 0.15% |
| [SqlThreadNativeReads.ts:1075 · makeSelectedReads.readPrompt](../../../../../packages/storage-sql/src/SqlThreadNativeReads.ts) | 1169 | 3.12% | 1165 | 2.65% |
| [RunJournal.ts:1257 · projectRunJournalStream](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 992 | 2.65% | 992 | 2.25% |
| [do-journal.ts:691 · makeJournal.appendPrepared](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 984 | 2.63% | 924 | 2.10% |
| [SchemaParser.js:829 · runWithCompiler](../../../../../node_modules/effect/dist/SchemaParser.js) | 801 | 2.14% | 1085 | 2.46% |
| [SchemaAST.js:1574 · Objects.getParser.resume.done](../../../../../node_modules/effect/dist/SchemaAST.js) | 773 | 2.07% | 773 | 1.76% |
| `unassigned:Service` | 665 | 1.78% | 0 | 0.00% |
| [DurableAgentRuntime.ts:5550 · make.runModel.checkpoint](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 606 | 1.62% | 505 | 1.15% |
| [RunJournal.ts:621 · projectRunJournalStream](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 563 | 1.50% | 559 | 1.27% |
| [Semaphore.js:111 · withPermits.acquire](../../../../../node_modules/effect/dist/Semaphore.js) | 558 | 1.49% | 558 | 1.27% |
| [SchemaAST.js:1180 · Arrays.getParser.parse](../../../../../node_modules/effect/dist/SchemaAST.js) | 513 | 1.37% | 513 | 1.17% |
| [RunContinuation.ts:1686 · makeProgressWriter.check](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 505 | 1.35% | 505 | 1.15% |
| [SchemaAST.js:2080 · catchSingleUnionCandidate](../../../../../node_modules/effect/dist/SchemaAST.js) | 471 | 1.26% | 471 | 1.07% |
| [ThreadObject.ts:1180 · make.runtime](../../../../../packages/platform-cloudflare/src/ThreadObject.ts) | 430 | 1.15% | 0 | 0.00% |
| [RunContinuation.ts:1279 · makeProgressWriter.prepare](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 401 | 1.07% | 402 | 0.91% |
| [do-journal.ts:971 · makeJournal.append](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 385 | 1.03% | 352 | 0.80% |
| [SqlThreadWork.ts:351 · makeSqlThreadWork.fold](../../../../../packages/storage-sql/src/SqlThreadWork.ts) | 370 | 0.99% | 354 | 0.80% |
| [agent-runtime.ts:2462 · executeToolBatch](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 361 | 0.96% | 353 | 0.80% |
| [SchemaAST.js:1158 · Arrays.getParser.finish](../../../../../node_modules/effect/dist/SchemaAST.js) | 348 | 0.93% | 1156 | 2.63% |
| [agent-runtime.ts:8884 · executeWithCompletion.interpreted.execution.turns](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 345 | 0.92% | 334 | 0.76% |
| [DoSubmissionLedger.ts:2535 · makeServices.claimJoining](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) | 330 | 0.88% | 300 | 0.68% |
| `runtime-constructor:OnFailureImpl` | 317 | 0.85% | 0 | 0.00% |

## Construction-origin modules, ranked by allocations

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| `effect/dist/SchemaAST.js` | 6826 | 18.24% | 15054 | 34.19% |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts` | 3906 | 10.44% | 3898 | 8.85% |
| `effect/dist/internal/effect.js` | 3196 | 8.54% | 2966 | 6.74% |
| `packages/effect-agent/src/durable/RunJournal.ts` | 2346 | 6.27% | 2368 | 5.38% |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts` | 2414 | 6.45% | 2016 | 4.58% |
| `effect/dist/SchemaParser.js` | 1030 | 2.75% | 1915 | 4.35% |
| `packages/effect-agent/src/durable/RunContinuation.ts` | 1920 | 5.13% | 1776 | 4.03% |
| `packages/storage-cloudflare/src/internal/do-journal.ts` | 1883 | 5.03% | 1765 | 4.01% |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts` | 1734 | 4.63% | 1673 | 3.80% |
| `packages/storage-sql/src/SqlThreadNativeReads.ts` | 1661 | 4.44% | 1626 | 3.69% |
| `effect/dist/Semaphore.js` | 1254 | 3.35% | 1239 | 2.81% |
| `packages/storage-sql/src/SqlThreadWork.ts` | 1243 | 3.32% | 1186 | 2.69% |
| `effect/dist/SchemaGetter.js` | 625 | 1.67% | 966 | 2.19% |
| `packages/effect-agent/src/core/Usage.ts` | 0 | 0.00% | 700 | 1.59% |
| `packages/storage-cloudflare/src/internal/owned-state.ts` | 712 | 1.90% | 688 | 1.56% |
| `effect/dist/internal/schema/parser.js` | 315 | 0.84% | 656 | 1.49% |
| `packages/storage-cloudflare/src/DoThreadStore.ts` | 529 | 1.41% | 529 | 1.20% |
| `packages/platform-cloudflare/src/internal/due-queue.ts` | 464 | 1.24% | 464 | 1.05% |
| `packages/platform-cloudflare/src/Alarm.ts` | 453 | 1.21% | 381 | 0.87% |
| `packages/effect-agent/src/durable/Digest.ts` | 247 | 0.66% | 247 | 0.56% |

## Construction-origin sites, ranked by allocations

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| [SchemaAST.js:1581 · Objects.getParser](../../../../../node_modules/effect/dist/SchemaAST.js) | 1962 | 5.24% | 7223 | 16.41% |
| [SchemaAST.js:1569 · Objects.getParser.resume](../../../../../node_modules/effect/dist/SchemaAST.js) | 1546 | 4.13% | 1546 | 3.51% |
| [effect.js:raceAllFirst · raceAllFirst](../../../../../node_modules/effect/dist/internal/effect.js) | 1531 | 4.09% | 1431 | 3.25% |
| [SchemaAST.js:2009 · Union.getParser](../../../../../node_modules/effect/dist/SchemaAST.js) | 0 | 0.00% | 1236 | 2.81% |
| [SqlThreadNativeReads.ts:1075 · makeSelectedReads.readPrompt](../../../../../packages/storage-sql/src/SqlThreadNativeReads.ts) | 1169 | 3.12% | 1165 | 2.65% |
| [SchemaAST.js:1158 · Arrays.getParser.finish](../../../../../node_modules/effect/dist/SchemaAST.js) | 348 | 0.93% | 1156 | 2.63% |
| [SchemaParser.js:829 · runWithCompiler](../../../../../node_modules/effect/dist/SchemaParser.js) | 801 | 2.14% | 1085 | 2.46% |
| [RunJournal.ts:1257 · projectRunJournalStream](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 992 | 2.65% | 992 | 2.25% |
| [do-journal.ts:691 · makeJournal.appendPrepared](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 984 | 2.63% | 924 | 2.10% |
| [SchemaAST.js:392 · Declaration.getParser](../../../../../node_modules/effect/dist/SchemaAST.js) | 225 | 0.60% | 786 | 1.79% |
| [SchemaAST.js:1574 · Objects.getParser.resume.done](../../../../../node_modules/effect/dist/SchemaAST.js) | 773 | 2.07% | 773 | 1.76% |
| [Usage.ts:260 · checkedAdd](../../../../../packages/effect-agent/src/core/Usage.ts) | 0 | 0.00% | 700 | 1.59% |
| [parser.js:19 · fromOptionExit](../../../../../node_modules/effect/dist/internal/schema/parser.js) | 315 | 0.84% | 656 | 1.49% |
| [SchemaGetter.js:596 · withDefault](../../../../../node_modules/effect/dist/SchemaGetter.js) | 315 | 0.84% | 656 | 1.49% |
| [RunJournal.ts:621 · projectRunJournalStream](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 563 | 1.50% | 559 | 1.27% |
| [Semaphore.js:111 · withPermits.acquire](../../../../../node_modules/effect/dist/Semaphore.js) | 558 | 1.49% | 558 | 1.27% |
| [SchemaAST.js:1180 · Arrays.getParser.parse](../../../../../node_modules/effect/dist/SchemaAST.js) | 513 | 1.37% | 513 | 1.17% |
| [DurableAgentRuntime.ts:5550 · make.runModel.checkpoint](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 606 | 1.62% | 505 | 1.15% |
| [RunContinuation.ts:1686 · makeProgressWriter.check](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 505 | 1.35% | 505 | 1.15% |
| [SchemaAST.js:2080 · catchSingleUnionCandidate](../../../../../node_modules/effect/dist/SchemaAST.js) | 471 | 1.26% | 471 | 1.07% |

## Dynamic exclusive modules, ranked by evaluations

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| `packages/storage-sql/src/SqlThreadNativeReads.ts` | 8499 | 22.71% | 1626 | 3.69% |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts` | 5960 | 15.92% | 3898 | 8.85% |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts` | 3887 | 10.38% | 2016 | 4.58% |
| `packages/effect-agent/src/durable/RunContinuation.ts` | 3109 | 8.31% | 1776 | 4.03% |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts` | 2991 | 7.99% | 1673 | 3.80% |
| `packages/storage-cloudflare/src/internal/do-journal.ts` | 2852 | 7.62% | 1765 | 4.01% |
| `packages/effect-agent/src/durable/RunJournal.ts` | 2652 | 7.09% | 2368 | 5.38% |
| `effect/dist/internal/effect.js` | 2325 | 6.21% | 2929 | 6.65% |
| `packages/storage-sql/src/SqlThreadWork.ts` | 1601 | 4.28% | 1186 | 2.69% |
| `packages/storage-cloudflare/src/DoThreadStore.ts` | 756 | 2.02% | 529 | 1.20% |
| `packages/storage-cloudflare/src/internal/owned-state.ts` | 423 | 1.13% | 688 | 1.56% |
| `packages/platform-cloudflare/src/Alarm.ts` | 391 | 1.04% | 381 | 0.87% |
| `packages/effect-agent/src/durable/RunStorage.ts` | 366 | 0.98% | 237 | 0.54% |
| `packages/effect-agent/src/durable/Digest.ts` | 329 | 0.88% | 247 | 0.56% |
| `packages/storage-sql/src/SqlThreadArchiveRange.ts` | 264 | 0.71% | 168 | 0.38% |
| `packages/effect-agent/src/durable/internal/evidence.ts` | 214 | 0.57% | 191 | 0.43% |
| `effect/dist/ai/LanguageModel.js` | 167 | 0.45% | 151 | 0.34% |
| `packages/storage-sql/src/internal/settlement-intervals.ts` | 123 | 0.33% | 79 | 0.18% |
| `packages/effect-agent/src/durable/internal/initial-context.ts` | 117 | 0.31% | 86 | 0.20% |
| `packages/effect-agent/src/durable/WakeScheduler.ts` | 112 | 0.30% | 96 | 0.22% |

## Dynamic exclusive sites, ranked by evaluations

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| [SqlThreadNativeReads.ts:1075 · makeSelectedReads.readPrompt](../../../../../packages/storage-sql/src/SqlThreadNativeReads.ts) | 7615 | 20.34% | 1165 | 2.65% |
| [effect.js:raceAllFirst · raceAllFirst](../../../../../node_modules/effect/dist/internal/effect.js) | 2325 | 6.21% | 1431 | 3.25% |
| [do-journal.ts:691 · makeJournal.appendPrepared](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 1368 | 3.65% | 924 | 2.10% |
| [RunJournal.ts:1257 · projectRunJournalStream](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 1196 | 3.20% | 992 | 2.25% |
| [DurableAgentRuntime.ts:5550 · make.runModel.checkpoint](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 1111 | 2.97% | 505 | 1.15% |
| [RunContinuation.ts:576 · advanceFacts](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 1054 | 2.82% | 196 | 0.45% |
| [agent-runtime.ts:8210 · beforeExecutionDeadline](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 888 | 2.37% | 288 | 0.65% |
| [agent-runtime.ts:8884 · executeWithCompletion.interpreted.execution.turns](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 883 | 2.36% | 334 | 0.76% |
| [do-journal.ts:971 · makeJournal.append](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 869 | 2.32% | 352 | 0.80% |
| [RunJournal.ts:621 · projectRunJournalStream](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 838 | 2.24% | 559 | 1.27% |
| [DoSubmissionLedger.ts:2535 · makeServices.claimJoining](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) | 810 | 2.16% | 300 | 0.68% |
| [agent-runtime.ts:2462 · executeToolBatch](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 609 | 1.63% | 353 | 0.80% |
| [RunContinuation.ts:1686 · makeProgressWriter.check](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 606 | 1.62% | 505 | 1.15% |
| [DurableAgentRuntime.ts:5884 · make.runModel.durability.commitTurn](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 596 | 1.59% | 217 | 0.49% |
| [DurableAgentRuntime.ts:5745 · make.runModel.durability.initialize](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 594 | 1.59% | 191 | 0.43% |
| [do-journal.ts:622 · makeJournal.getThread](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 573 | 1.53% | 259 | 0.59% |
| [RunContinuation.ts:1279 · makeProgressWriter.prepare](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 541 | 1.45% | 402 | 0.91% |
| [SqlThreadWork.ts:351 · makeSqlThreadWork.fold](../../../../../packages/storage-sql/src/SqlThreadWork.ts) | 506 | 1.35% | 354 | 0.80% |
| [DoThreadStore.ts:380 · makeServices.append](../../../../../packages/storage-cloudflare/src/DoThreadStore.ts) | 418 | 1.12% | 220 | 0.50% |
| [agent-runtime.ts:1210 · ownModelResponsePart](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 403 | 1.08% | 83 | 0.19% |
| [agent-runtime.ts:7460 · toolBatchContinuation](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 392 | 1.05% | 128 | 0.29% |
| [RunJournal.ts:1707 · modelResponseRecord](../../../../../packages/effect-agent/src/durable/RunJournal.ts) | 385 | 1.03% | 139 | 0.32% |
| [Alarm.ts:965 · layer.recordProgress](../../../../../packages/platform-cloudflare/src/Alarm.ts) | 361 | 0.96% | 136 | 0.31% |
| [RunContinuation.ts:1534 · makeProgressWriter.tail](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 360 | 0.96% | 120 | 0.27% |
| [owned-state.ts:188 · ownedRows.apply](../../../../../packages/storage-cloudflare/src/internal/owned-state.ts) | 350 | 0.94% | 200 | 0.45% |

## Outside the eight stages, by dynamic exclusive site

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| [effect.js:raceAllFirst · raceAllFirst](../../../../../node_modules/effect/dist/internal/effect.js) | 2325 | 6.21% | 1431 | 3.25% |
| [DurableAgentRuntime.ts:5550 · make.runModel.checkpoint](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 1111 | 2.97% | 505 | 1.15% |
| [agent-runtime.ts:8210 · beforeExecutionDeadline](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 888 | 2.37% | 288 | 0.65% |
| [agent-runtime.ts:8884 · executeWithCompletion.interpreted.execution.turns](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 883 | 2.36% | 334 | 0.76% |
| [DoSubmissionLedger.ts:2535 · makeServices.claimJoining](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) | 810 | 2.16% | 300 | 0.68% |
| [agent-runtime.ts:2462 · executeToolBatch](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 609 | 1.63% | 353 | 0.80% |
| [RunContinuation.ts:1686 · makeProgressWriter.check](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 606 | 1.62% | 505 | 1.15% |
| [DurableAgentRuntime.ts:5745 · make.runModel.durability.initialize](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 594 | 1.59% | 191 | 0.43% |
| [agent-runtime.ts:1210 · ownModelResponsePart](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 403 | 1.08% | 83 | 0.19% |
| [agent-runtime.ts:7460 · toolBatchContinuation](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 392 | 1.05% | 128 | 0.29% |
| [agent-runtime.ts:2150 · executePreparedToolCall](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 328 | 0.88% | 224 | 0.51% |
| [RunContinuation.ts:1534 · makeProgressWriter.tail](../../../../../packages/effect-agent/src/durable/RunContinuation.ts) | 309 | 0.83% | 103 | 0.23% |
| [DoSubmissionLedger.ts:2548 · makeServices.claimJoining.claims](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) | 260 | 0.69% | 170 | 0.39% |
| [agent-runtime.ts:7220 · makeTurn.continuation](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 248 | 0.66% | 104 | 0.24% |
| [agent-runtime.ts:5431 · makeTurn](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 243 | 0.65% | 261 | 0.59% |
| [agent-runtime.ts:2788 · executeToolBatch.handlers.callBody](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 240 | 0.64% | 184 | 0.42% |
| [agent-runtime.ts:3297 · consumeUsage](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 225 | 0.60% | 27 | 0.06% |
| [do-journal.ts:622 · makeJournal.getThread](../../../../../packages/storage-cloudflare/src/internal/do-journal.ts) | 209 | 0.56% | 99 | 0.22% |
| [agent-runtime.ts:2779 · executeToolBatch.handlers](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 168 | 0.45% | 88 | 0.20% |
| [agent-runtime.ts:4724 · processModelPart](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 160 | 0.43% | 77 | 0.17% |
| [DurableAgentRuntime.ts:8139 · make.runAttempt](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 150 | 0.40% | 98 | 0.22% |
| [DurableAgentRuntime.ts:6815 · make.runModel.claimInputs](../../../../../packages/effect-agent/src/durable/DurableAgentRuntime.ts) | 140 | 0.37% | 20 | 0.05% |
| [DoSubmissionLedger.ts:3874 · makeServices.loadRecoverySnapshot](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) | 140 | 0.37% | 97 | 0.22% |
| [agent-runtime.ts:2298 · executePreparedToolCall.commitTerminalResult](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 128 | 0.34% | 8 | 0.02% |
| [agent-runtime.ts:6454 · makeTurn.attempt](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 120 | 0.32% | 120 | 0.27% |

## Engine tool/model loops and response handling, inclusive dynamic sites

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| [agent-runtime.ts:2462 · executeToolBatch](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 4329 | 11.57% | 3857 | 8.76% |
| [agent-runtime.ts:2779 · executeToolBatch.handlers](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 2536 | 6.78% | 2288 | 5.20% |
| [agent-runtime.ts:6851 · makeTurn.continuation.afterValidatedResponse](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 2498 | 6.67% | 4043 | 9.18% |
| [agent-runtime.ts:2786 · executeToolBatch.handlers](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 2208 | 5.90% | 2000 | 4.54% |
| [agent-runtime.ts:2788 · executeToolBatch.handlers.callBody](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 2112 | 5.64% | 1904 | 4.32% |
| [agent-runtime.ts:7220 · makeTurn.continuation](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 1988 | 5.31% | 3493 | 7.93% |
| [agent-runtime.ts:2150 · executePreparedToolCall](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 1680 | 4.49% | 1528 | 3.47% |
| [agent-runtime.ts:6454 · makeTurn.attempt](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 818 | 2.19% | 994 | 2.26% |
| [agent-runtime.ts:5431 · makeTurn](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 549 | 1.47% | 675 | 1.53% |
| [agent-runtime.ts:1210 · ownModelResponsePart](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 505 | 1.35% | 682 | 1.55% |
| [agent-runtime.ts:6340 · makeTurn.attempt](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 261 | 0.70% | 297 | 0.67% |
| [agent-runtime.ts:7014 · makeTurn.continuation.settleOrFollowUp](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 213 | 0.57% | 205 | 0.47% |
| [agent-runtime.ts:2298 · executePreparedToolCall.commitTerminalResult](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 192 | 0.51% | 168 | 0.38% |
| [LanguageModel.js:355 · make.streamText2](../../../../../node_modules/effect/dist/ai/LanguageModel.js) | 162 | 0.43% | 146 | 0.33% |
| [agent-runtime.ts:1128 · captureModelResponsePartGeneral](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 102 | 0.27% | 196 | 0.45% |
| [agent-runtime.ts:2209 · executePreparedToolCall.started](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 72 | 0.19% | 64 | 0.15% |
| [LanguageModel.js:561 · make.streamContent](../../../../../node_modules/effect/dist/ai/LanguageModel.js) | 54 | 0.14% | 65 | 0.15% |
| [agent-runtime.ts:5685 · makeTurn.started](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 54 | 0.14% | 45 | 0.10% |
| [agent-runtime.ts:2265 · executePreparedToolCall.results](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 24 | 0.06% | 24 | 0.05% |

These rows overlap: for example executeToolBatch includes executePreparedToolCall. At m0 there are eight executeToolBatch entries, eight executePreparedToolCall entries, nine makeTurn / LanguageModel.streamText entries, and twenty ownModelResponsePart entries (sixteen tool-call/finish parts, then four final-text parts). General response capture runs seventeen times; the three primitive text parts use the direct path. Raw call counters and selectors preserve the exact boundaries. Decoder factories can return effects that run after the factory scope closes, so construction-origin schema tables supply the complementary view.

## Semaphore, tracing and failpoint construction origins

| Site / module | Evaluations | % evals | Allocations | % allocations |
|---|---:|---:|---:|---:|
| [Semaphore.js:111 · withPermits.acquire](../../../../../node_modules/effect/dist/Semaphore.js) | 558 | 1.49% | 558 | 1.27% |
| [Semaphore.js:110 · withPermits](../../../../../node_modules/effect/dist/Semaphore.js) | 312 | 0.83% | 297 | 0.67% |
| [Semaphore.js:110 · withPermits](../../../../../node_modules/effect/dist/Semaphore.js) | 312 | 0.83% | 312 | 0.71% |
| [effect.js:endSpan · endSpan](../../../../../node_modules/effect/dist/internal/effect.js) | 280 | 0.75% | 280 | 0.64% |
| [effect.js:useSpan · useSpan](../../../../../node_modules/effect/dist/internal/effect.js) | 183 | 0.49% | 183 | 0.42% |
| [effect.js:makeSpanUnsafe · makeSpanUnsafe](../../../../../node_modules/effect/dist/internal/effect.js) | 152 | 0.41% | 0 | 0.00% |
| [effect.js:withParentSpan · withParentSpan](../../../../../node_modules/effect/dist/internal/effect.js) | 150 | 0.40% | 150 | 0.34% |
| [effect.js:useSpan · useSpan](../../../../../node_modules/effect/dist/internal/effect.js) | 102 | 0.27% | 285 | 0.65% |
| [DoSubmissionLedger.ts:606 · makeServices.hitFailpoint](../../../../../packages/storage-cloudflare/src/DoSubmissionLedger.ts) | 35 | 0.09% | 35 | 0.08% |
| [effect.js:makeSpanScoped · makeSpanScoped](../../../../../node_modules/effect/dist/internal/effect.js) | 27 | 0.07% | 27 | 0.06% |
| [agent-runtime.ts:2033 · modelTelemetryTracer](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 27 | 0.07% | 27 | 0.06% |
| [DoThreadStore.ts:344 · makeServices.hitFailpoint](../../../../../packages/storage-cloudflare/src/DoThreadStore.ts) | 25 | 0.07% | 25 | 0.06% |
| [agent-runtime.ts:2104 · terminalToolTelemetry](../../../../../packages/effect-agent/src/engine/internal/agent-runtime.ts) | 24 | 0.06% | 24 | 0.05% |
| [effect.js:annotateCurrentSpan · annotateCurrentSpan](../../../../../node_modules/effect/dist/internal/effect.js) | 20 | 0.05% | 20 | 0.05% |
| [effect.js:makeSpanScoped · makeSpanScoped](../../../../../node_modules/effect/dist/internal/effect.js) | 18 | 0.05% | 18 | 0.04% |
| [effect.js:withParentSpan · withParentSpan](../../../../../node_modules/effect/dist/internal/effect.js) | 16 | 0.04% | 16 | 0.04% |
| [Semaphore.js:171 · make](../../../../../node_modules/effect/dist/Semaphore.js) | 16 | 0.04% | 16 | 0.04% |
| [tool-telemetry.ts:428 · layer.isolateEffectSpanLifecycle](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 16 | 0.04% | 16 | 0.04% |
| [tool-telemetry.ts:438 · layer.isolateToolkitHandle](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 16 | 0.04% | 16 | 0.04% |
| [Semaphore.js:103 · release](../../../../../node_modules/effect/dist/Semaphore.js) | 14 | 0.04% | 14 | 0.03% |
| [Semaphore.js:104 · release](../../../../../node_modules/effect/dist/Semaphore.js) | 14 | 0.04% | 14 | 0.03% |
| [Semaphore.js:66 · take](../../../../../node_modules/effect/dist/Semaphore.js) | 14 | 0.04% | 14 | 0.03% |
| [Semaphore.js:67 · take.take2](../../../../../node_modules/effect/dist/Semaphore.js) | 14 | 0.04% | 14 | 0.03% |
| [effect.js:makeFn · makeFn](../../../../../node_modules/effect/dist/internal/effect.js) | 12 | 0.03% | 12 | 0.03% |
| [tool-telemetry.ts:273 · makeIsolatedToolTracer](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 8 | 0.02% | 8 | 0.02% |
| [tool-telemetry.ts:386 · makeIsolatedToolTracer.reportLifecycleDefects](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 8 | 0.02% | 8 | 0.02% |
| [tool-telemetry.ts:427 · layer.isolateEffectSpanLifecycle](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 8 | 0.02% | 8 | 0.02% |
| [tool-telemetry.ts:441 · layer.isolateToolkitHandle](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 8 | 0.02% | 8 | 0.02% |
| [tool-telemetry.ts:461 · layer.isolateToolkitHandle](../../../../../packages/effect-agent/src/engine/internal/tool-telemetry.ts) | 8 | 0.02% | 8 | 0.02% |
| [effect.js:makeFn · makeFn](../../../../../node_modules/effect/dist/internal/effect.js) | 6 | 0.02% | 6 | 0.01% |

These origin counts describe primitives constructed by each helper and subsequent dispatches of those objects. A helper returning a shared singleton can have calls but no allocations. A tracing helper's primitive count excludes ordinary JavaScript span-object allocation.

## Zero-dispatch successes and interpretation

The patched Effect interpreter can bypass runLoop for successful Exits, and ContImpl continuations can call succeedWith directly. Schema.decodeEffect may parse synchronously, allocate success values, and never dispatch those values. Effect.void is a reusable Success singleton; yielding it need not allocate or dispatch. Therefore evaluation count is neither allocation count nor the count of semantic Effect operations.

The 2303 succeedWith entries and 1167 direct successful-Exit consumptions are separate observations, not synthetic run-loop evaluations. succeedWith periodically returns an Exit to the loop, so not every entry bypasses dispatch. The iterator counter covers the ordinary/eager generator interpreter paths, not every eager combinator or every decoder fast path. Allocations are nine selected constructor families (primitive/Exit plus Async, Iterator, Cont, OnFailure, OnSuccessAndFailure, Match, OnExit), not bytes or all JavaScript heap allocation. Don't estimate total CPU from evaluations alone, or apply one unit cost to both dispatching and zero-dispatch primitives.

The dynamic readPrompt body owns 7,615 evaluations, while its construction-origin count is much smaller: schema parser continuations execute beneath the prompt-read scope after their factory functions returned. Expanded SchemaAST selectors expose the construction source rather than leaving that work lumped into the parent. Dynamic and origin views are alternative partitions, never additive.

## Evidence

- [summary.json.gz](summary.json.gz): all first-turn ranked tables, exact stage selector map, per-turn closure/coverage checks and identities.
- `capture/report.json.gz`: complete final raw rows; `repeat/report.json.gz`: independent identical raw rows; `plain-control/report.json.gz`: uninstrumented fixture control.
- `capture/sites.json.gz`: every selector, original module hash, source-mapped location and transformed-function range; `capture/build-inputs.json`: complete source hashes; `capture/inputs.json`: instrumented module hashes.
- [attempts.json](attempts.json): concise intermediate/failed capture metadata. Earlier stage tables are superseded.

No product file or toolchain manifest changed. The parent owns the combined repository gate and final report; no suite was added or run by this attribution harness.
