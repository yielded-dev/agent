# Arithmetic projections

These separate count-only models fail held-out and whole-turn validation. Estimates are illustrative composition scenarios, not measured stage CPU, promises, upper bounds, or additive savings. Never add the evaluation and allocation predictions or different attribution views.

| Exclusive stage | Evaluations | Allocations | E-only model ms | A-only model ms |
|---|---:|---:|---:|---:|
| outside-eight-stages | 15,149 | 16,619 | 2.36–4.03 | 3.21–5.62 |
| context-assembly | 9,976 | 12,412 | 1.55–2.65 | 2.40–4.20 |
| durable-object-append | 6,179 | 6,348 | 0.96–1.64 | 1.23–2.15 |
| continuation-preparation | 2,062 | 3,424 | 0.32–0.55 | 0.66–1.16 |
| tool-settlement-commit | 1,040 | 1,116 | 0.16–0.28 | 0.22–0.38 |
| model-response-commit | 926 | 1,950 | 0.14–0.25 | 0.38–0.66 |
| settlement | 869 | 937 | 0.14–0.23 | 0.18–0.32 |
| admission | 693 | 717 | 0.11–0.18 | 0.14–0.24 |
| ownership-acquisition | 537 | 501 | 0.08–0.14 | 0.10–0.17 |
| **Whole turn** | **37,431** | **44,024** | **5.82–9.95** | **8.50–14.88** |

## Dynamic execution sites

| Site / module | E | A | E-only model ms | A-only model ms |
|---|---:|---:|---:|---:|
| `packages/storage-sql/src/SqlThreadNativeReads.ts:1075:19:makeSelectedReads.readPrompt` | 7,615 | 1,165 | 1.18–2.02 | 0.22–0.39 |
| `effect/dist/internal/effect.js:raceAllFirst:3` | 2,325 | 1,431 | 0.36–0.62 | 0.28–0.48 |
| `do-journal.appendPrepared` | 1,368 | 924 | 0.21–0.36 | 0.18–0.31 |
| `packages/effect-agent/src/durable/RunJournal.ts:1257:15:projectRunJournalStream` | 1,196 | 992 | 0.19–0.32 | 0.19–0.34 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts:5550:19:make.runModel.checkpoint` | 1,111 | 505 | 0.17–0.30 | 0.10–0.17 |
| `RunContinuation.advanceFacts` | 1,054 | 196 | 0.16–0.28 | 0.04–0.07 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:8210:13:beforeExecutionDeadline` | 888 | 288 | 0.14–0.24 | 0.06–0.10 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:8884:39:executeWithCompletion.interpreted.execution.turns` | 883 | 334 | 0.14–0.23 | 0.06–0.11 |
| `packages/storage-cloudflare/src/internal/do-journal.ts:971:6:makeJournal.append` | 869 | 352 | 0.14–0.23 | 0.07–0.12 |
| `RunJournal.projectRunJournalStream` | 838 | 559 | 0.13–0.22 | 0.11–0.19 |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts:2535:86:makeServices.claimJoining` | 810 | 300 | 0.13–0.22 | 0.06–0.10 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:2462:43:executeToolBatch` | 609 | 353 | 0.09–0.16 | 0.07–0.12 |
| `packages/effect-agent/src/durable/RunContinuation.ts:1686:17:makeProgressWriter.check` | 606 | 505 | 0.09–0.16 | 0.10–0.17 |
| `DurableAgentRuntime.commitTurn.Settled` | 596 | 217 | 0.09–0.16 | 0.04–0.07 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts:5745:23:make.runModel.durability.initialize` | 594 | 191 | 0.09–0.16 | 0.04–0.06 |
| `packages/storage-cloudflare/src/internal/do-journal.ts:622:40:makeJournal.getThread` | 573 | 259 | 0.09–0.15 | 0.05–0.09 |
| `RunContinuation.prepare` | 541 | 402 | 0.08–0.14 | 0.08–0.14 |
| `packages/storage-sql/src/SqlThreadWork.ts:351:33:makeSqlThreadWork.fold` | 506 | 354 | 0.08–0.13 | 0.07–0.12 |
| `DoThreadStore.append` | 418 | 220 | 0.07–0.11 | 0.04–0.07 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:1210:47:ownModelResponsePart` | 403 | 83 | 0.06–0.11 | 0.02–0.03 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:7460:15:toolBatchContinuation` | 392 | 128 | 0.06–0.10 | 0.02–0.04 |
| `RunJournal.modelResponseRecord` | 385 | 139 | 0.06–0.10 | 0.03–0.05 |
| `packages/platform-cloudflare/src/Alarm.ts:965:47:layer.recordProgress` | 361 | 136 | 0.06–0.10 | 0.03–0.05 |
| `packages/effect-agent/src/durable/RunContinuation.ts:1534:26:makeProgressWriter.tail` | 360 | 120 | 0.06–0.10 | 0.02–0.04 |
| `packages/storage-cloudflare/src/internal/owned-state.ts:188:19:ownedRows.apply` | 350 | 200 | 0.05–0.09 | 0.04–0.07 |

## Construction-origin modules

| Site / module | E | A | E-only model ms | A-only model ms |
|---|---:|---:|---:|---:|
| `effect/dist/SchemaAST.js` | 6,826 | 15,054 | 1.06–1.82 | 2.91–5.09 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts` | 3,906 | 3,898 | 0.61–1.04 | 0.75–1.32 |
| `effect/dist/internal/effect.js` | 3,196 | 2,966 | 0.50–0.85 | 0.57–1.00 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts` | 2,414 | 2,016 | 0.38–0.64 | 0.39–0.68 |
| `packages/effect-agent/src/durable/RunJournal.ts` | 2,346 | 2,368 | 0.36–0.62 | 0.46–0.80 |
| `packages/effect-agent/src/durable/RunContinuation.ts` | 1,920 | 1,776 | 0.30–0.51 | 0.34–0.60 |
| `packages/storage-cloudflare/src/internal/do-journal.ts` | 1,883 | 1,765 | 0.29–0.50 | 0.34–0.60 |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts` | 1,734 | 1,673 | 0.27–0.46 | 0.32–0.57 |
| `packages/storage-sql/src/SqlThreadNativeReads.ts` | 1,661 | 1,626 | 0.26–0.44 | 0.31–0.55 |
| `effect/dist/internal/core.js` | 1,492 | 74 | 0.23–0.40 | 0.01–0.03 |
| `effect/dist/Semaphore.js` | 1,254 | 1,239 | 0.20–0.33 | 0.24–0.42 |
| `packages/storage-sql/src/SqlThreadWork.ts` | 1,243 | 1,186 | 0.19–0.33 | 0.23–0.40 |
| `effect/dist/SchemaParser.js` | 1,030 | 1,915 | 0.16–0.27 | 0.37–0.65 |
| `packages/storage-cloudflare/src/internal/owned-state.ts` | 712 | 688 | 0.11–0.19 | 0.13–0.23 |
| `unassigned:Service` | 665 | 0 | 0.10–0.18 | 0.00–0.00 |
| `effect/dist/SchemaGetter.js` | 625 | 966 | 0.10–0.17 | 0.19–0.33 |
| `packages/storage-cloudflare/src/DoThreadStore.ts` | 529 | 529 | 0.08–0.14 | 0.10–0.18 |
| `packages/platform-cloudflare/src/internal/due-queue.ts` | 464 | 464 | 0.07–0.12 | 0.09–0.16 |
| `packages/platform-cloudflare/src/Alarm.ts` | 453 | 381 | 0.07–0.12 | 0.07–0.13 |
| `packages/platform-cloudflare/src/ThreadObject.ts` | 430 | 0 | 0.07–0.11 | 0.00–0.00 |
| `packages/effect-agent/src/durable/RunStorage.ts` | 374 | 237 | 0.06–0.10 | 0.05–0.08 |
| `effect/dist/internal/schema/parser.js` | 315 | 656 | 0.05–0.08 | 0.13–0.22 |
| `packages/effect-agent/src/durable/Digest.ts` | 247 | 247 | 0.04–0.07 | 0.05–0.08 |
| `packages/storage-cloudflare/src/internal/storage-span.ts` | 194 | 194 | 0.03–0.05 | 0.04–0.07 |
| `packages/effect-agent/src/durable/internal/evidence.ts` | 191 | 191 | 0.03–0.05 | 0.04–0.06 |

## Sites outside the eight stage boundaries

| Site / module | E | A | E-only model ms | A-only model ms |
|---|---:|---:|---:|---:|
| `effect/dist/internal/effect.js:raceAllFirst:3` | 2,325 | 1,431 | 0.36–0.62 | 0.28–0.48 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts:5550:19:make.runModel.checkpoint` | 1,111 | 505 | 0.17–0.30 | 0.10–0.17 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:8210:13:beforeExecutionDeadline` | 888 | 288 | 0.14–0.24 | 0.06–0.10 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:8884:39:executeWithCompletion.interpreted.execution.turns` | 883 | 334 | 0.14–0.23 | 0.06–0.11 |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts:2535:86:makeServices.claimJoining` | 810 | 300 | 0.13–0.22 | 0.06–0.10 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:2462:43:executeToolBatch` | 609 | 353 | 0.09–0.16 | 0.07–0.12 |
| `packages/effect-agent/src/durable/RunContinuation.ts:1686:17:makeProgressWriter.check` | 606 | 505 | 0.09–0.16 | 0.10–0.17 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts:5745:23:make.runModel.durability.initialize` | 594 | 191 | 0.09–0.16 | 0.04–0.06 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:1210:47:ownModelResponsePart` | 403 | 83 | 0.06–0.11 | 0.02–0.03 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:7460:15:toolBatchContinuation` | 392 | 128 | 0.06–0.10 | 0.02–0.04 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:2150:50:executePreparedToolCall` | 328 | 224 | 0.05–0.09 | 0.04–0.08 |
| `packages/effect-agent/src/durable/RunContinuation.ts:1534:26:makeProgressWriter.tail` | 309 | 103 | 0.05–0.08 | 0.02–0.03 |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts:2548:17:makeServices.claimJoining.claims` | 260 | 170 | 0.04–0.07 | 0.03–0.06 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:7220:25:makeTurn.continuation` | 248 | 104 | 0.04–0.07 | 0.02–0.04 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:5431:15:makeTurn` | 243 | 261 | 0.04–0.06 | 0.05–0.09 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:2788:29:executeToolBatch.handlers.callBody` | 240 | 184 | 0.04–0.06 | 0.04–0.06 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:3297:13:consumeUsage` | 225 | 27 | 0.04–0.06 | 0.01–0.01 |
| `packages/storage-cloudflare/src/internal/do-journal.ts:622:40:makeJournal.getThread` | 209 | 99 | 0.03–0.06 | 0.02–0.03 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:2779:30:executeToolBatch.handlers` | 168 | 88 | 0.03–0.04 | 0.02–0.03 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:4724:43:processModelPart` | 160 | 77 | 0.02–0.04 | 0.01–0.03 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts:8139:15:make.runAttempt` | 150 | 98 | 0.02–0.04 | 0.02–0.03 |
| `packages/effect-agent/src/durable/DurableAgentRuntime.ts:6815:44:make.runModel.claimInputs` | 140 | 20 | 0.02–0.04 | 0.00–0.01 |
| `packages/storage-cloudflare/src/DoSubmissionLedger.ts:3874:19:makeServices.loadRecoverySnapshot` | 140 | 97 | 0.02–0.04 | 0.02–0.03 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:2298:42:executePreparedToolCall.commitTerminalResult` | 128 | 8 | 0.02–0.03 | 0.00–0.00 |
| `packages/effect-agent/src/engine/internal/agent-runtime.ts:6454:35:makeTurn.attempt` | 120 | 120 | 0.02–0.03 | 0.02–0.04 |

## Comparison with deployed turns

| Position | History | E | A | E-only ms | A-only ms | Observed pin CPU ms [Q1–Q3] | Observed control CPU ms |
|---|---:|---:|---:|---:|---:|---|---|
| m0 | 50 | 37,431 | 44,024 | 5.82–9.95 | 8.50–14.88 | 510.0 [422.0–672.2] (n=6) | 650.0 [348.0–681.5] (n=7) |
| m1 | 51 | 38,338 | 45,194 | 5.96–10.19 | 8.73–15.27 | 278.0 [223.8–330.0] (n=6) | 328.0 [248.5–336.0] (n=7) |
| m2 | 52 | 39,335 | 46,622 | 6.12–10.46 | 9.00–15.76 | 240.5 [165.0–345.2] (n=6) | 343.0 [151.5–369.0] (n=7) |
| m3 | 53 | 40,336 | 48,052 | 6.28–10.73 | 9.28–16.24 | 224.5 [195.5–264.8] (n=6) | 319.0 [154.0–367.5] (n=7) |
| m4 | 54 | 41,333 | 49,480 | 6.43–10.99 | 9.55–16.72 | 227.5 [173.8–294.8] (n=6) | 268.0 [162.5–317.5] (n=7) |
| m5 | 55 | 42,335 | 50,911 | 6.59–11.26 | 9.83–17.21 | 252.5 [206.5–321.8] (n=6) | 258.5 [213.2–340.5] (n=6) |
| m6 | 56 | 43,332 | 52,339 | 6.74–11.52 | 10.11–17.69 | 306.5 [214.8–377.2] (n=6) | 286.0 [232.0–360.5] (n=7) |
| m7 | 57 | 44,331 | 53,767 | 6.90–11.79 | 10.38–18.17 | 201.0 [197.8–212.0] (n=4) | 321.5 [285.5–335.8] (n=4) |
| m8 | 58 | 45,329 | 55,194 | 7.05–12.05 | 10.66–18.65 | 261.5 [214.0–297.8] (n=6) | 293.0 [196.5–377.5] (n=7) |
| m9 | 59 | 46,329 | 56,623 | 7.21–12.32 | 10.93–19.14 | 167.0 [145.0–245.0] (n=5) | 283.0 [175.5–317.5] (n=7) |

## Conditional removal scenarios

| Scenario | Modeled ms | Limitation |
|---|---:|---|
| All success-path OnFailure frames | 1.763–3.433 | 4030 frames divided by the microcase's two guard frames. Many actual handlers protect asynchronous failures, interruption or schema errors and cannot be removed; no proven removable fraction. |
| All span entries represented by 280 endSpan calls | 1.676–2.908 | Measures the no-exporter withSpan microshape. End counts include named fn spans; actual costs and removable leaf spans vary. Removing them loses tracing detail. |
| KOM-433's actually demonstrated synchronous conversions | 0.419–1.025 | A conditional extrapolation of the old first-position count delta, not a deployed prototype measurement. |
| Uncontended permit wrappers | 0.171–0.333 | 312 acquire callback entries. Contended permits and resource lifetime remain necessary; genuine Scopes have no supported removal estimate. |
| The 56 race orchestration entries | 0.042–0.161 | The simple synchronous races omit timer registration, asynchronous winners/finalizers and engine payload work. Preserve deadlines, interruption and joined cleanup. |
| Disabled failpoint calls and wrappers | 0.026–0.032 | 135 no-op calls have zero E/A; 60 storage wrappers originate one E/A each. Removing fault-injection boundaries would sacrifice recovery evidence for tens of microseconds. |
