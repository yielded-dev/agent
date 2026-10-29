import {
  DurableObjectContext,
  ThreadObjectIdentity,
} from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import * as ThreadObject from "@yielded/agent-platform-cloudflare/thread-object";
import * as Agent from "@yielded/agent/agent";
import { ContextCompactor } from "@yielded/agent/context-compactor";
import { digestDefinitions, digestJson } from "@yielded/agent/digest";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { RunId, SubmissionId, TurnId } from "@yielded/agent/identifiers";
import {
  BatchId,
  CanonicalBatch,
  DeploymentId,
  DefinitionDigestInput,
  ModelResponseRecorded,
  PersistedJson,
  RecordEnvelope,
  RecordId,
  RunCompleted,
  RunContinuation,
  ThreadCreated,
  UserInputRecorded,
} from "@yielded/agent/records";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import { RunContextPreparation } from "@yielded/agent/run-options";
import { IdempotencyKey, Principal } from "@yielded/agent/submission-ledger";
import {
  AppendRequest,
  ThreadExportRequest,
  ThreadExportSource,
  streamExport,
  ThreadMaterialization,
  ThreadStore,
  ThreadTailRequest,
  type ExecutionCheckpoint,
} from "@yielded/agent/thread-store";
import { Cause, Context, DateTime, Effect, Layer, Option, Schema, Stream } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import {
  AiError,
  LanguageModel,
  Model,
  Prompt,
  Tool,
  Toolkit,
  type Response as AiResponse,
} from "effect/ai";
import * as CryptoDigest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";

import {
  currentLargePhase,
  isCompactionPhase,
  REPLAY_CPU_PHASES,
  REPLAY_CPU_PROTOCOL,
  ReplayCpuIncarnation,
  ReplayCpuObjectName,
  ReplayCpuOperation,
} from "./replay-cpu-contracts.ts";

declare const BENCH_REVISION: string;
declare const BENCH_FIXTURE_SHA256: string;
declare const BENCH_PACKAGE_VERSIONS: string;

declare global {
  namespace Cloudflare {
    interface Env {
      REPLAY_CPU_THREADS: DurableObjectNamespace<ReplayCpuThread>;
      REPLAY_CPU_TOKEN: string;
      REPLAY_CPU_RUN: string;
      REPLAY_CPU_VERSION: { id: string };
    }
  }
}

const State = Schema.Struct({
  seedRecords: Schema.Natural,
  phase: Schema.Natural,
  seedDigest: Schema.NullOr(Schema.String),
  incarnation: Schema.NullOr(ReplayCpuIncarnation),
  operations: Schema.Array(ReplayCpuOperation).check(Schema.isMaxLength(10)),
});

const SeedRequest = Schema.Struct({ records: Schema.Literals([10, 1000]) });
// Mint lazily inside a request: Workers do not allow random values at module evaluation.
let moduleIncarnation: string | undefined;
const beforeSettlementByContext = new WeakMap<object, Effect.Effect<void>>();

class BenchError extends Schema.TaggedError<BenchError>()("BenchError", {
  message: Schema.String,
}) {}
const JsonObject = Schema.Record(Schema.String, Schema.Json);

class Bench extends Context.Service<
  Bench,
  {
    seed(records: 10 | 1000): Effect.Effect<typeof JsonObject.Type, BenchError>;
    operate(phase: number): Effect.Effect<typeof JsonObject.Type, BenchError>;
    readonly evidence: Effect.Effect<typeof JsonObject.Type, BenchError>;
  }
>()("issue692/Bench") {}

const check = (valid: boolean, message: string) =>
  valid ? Effect.void : Effect.fail(BenchError.make({ message }));

const bytes = (text: string) => new TextEncoder().encode(text).length;

const largeInput = (phase: number) => {
  const row =
    "Synthetic retained project context: preserve the measured conversation and verify the small tool results. ";

  return `LARGE_CONTEXT_BEGIN_${phase}\n${row.repeat(Math.ceil(400_000 / row.length)).slice(0, 400_000)}\nLARGE_CONTEXT_END_${phase}`;
};

const work = Tool.make("work", {
  parameters: Schema.Struct({ index: Schema.Literals([0, 1]) }),
  success: Schema.Struct({ index: Schema.Natural, value: Schema.String }),
});

const toolkit = Toolkit.make(work);
const outputSchema = Schema.Struct({ answer: Schema.String });

const definition = Agent.make("issue692-hosted", {
  input: Schema.String,
  output: outputSchema,
  instructions:
    "Use the retained project context. Execute the two small work calls, then return the requested phase answer.",
  toolkit,
  policy: {
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "60 seconds",
    toolConcurrency: 2,
    contextTokenLimit: 250_000,
  },
});

const declarations = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "scripted", name: "issue692-hosted" },
  tools: [{ name: "work", revision: 1 }],
});

const usage = { inputTokens: {}, outputTokens: {} };

const parts = (phase: number, call: number): ReadonlyArray<AiResponse.StreamPartEncoded> =>
  call === 1
    ? [0, 1].map((index) => ({
        type: "tool-call" as const,
        id: `phase-${phase}-work-${index}`,
        name: "work",
        params: { index },
        providerExecuted: false,
      }))
    : [
        { type: "text-start", id: "answer" },
        {
          type: "text-delta",
          id: "answer",
          delta: JSON.stringify({ answer: `phase-${phase}-ok` }),
        },
        { type: "text-end", id: "answer" },
        { type: "finish", reason: "stop", usage },
      ];

const application = Layer.unwrap(
  Effect.gen(function* () {
    const { ctx } = yield* DurableObjectContext;
    const { threadId, producerId } = yield* ThreadObjectIdentity;
    const env = yield* WorkerEnvironment;
    const crypto = yield* CryptoDigest.Digest;
    const random = yield* SecureRandom.SecureRandom;

    moduleIncarnation ??= yield* random.uuidV4();
    const incarnation = { module: moduleIncarnation, runtime: yield* random.uuidV4() };
    const native = yield* ContextCompactor.pipe(Effect.provide(ContextCompactor.layerRollover));

    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS issue692_evidence (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );

    const prior = ctx.storage.sql
      .exec<{ value: string }>("SELECT value FROM issue692_evidence WHERE key='state'")
      .toArray()[0]?.value;

    let state: typeof State.Type =
      prior === undefined
        ? {
            seedRecords: 0,
            phase: 0,
            seedDigest: null,
            incarnation: null,
            operations: [],
          }
        : yield* Schema.decodeEffect(Schema.fromJsonString(State))(prior);

    const persist = () =>
      ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO issue692_evidence(key,value) VALUES ('state',?)",
        Schema.encodeSync(Schema.fromJsonString(State))(state),
      );

    let calls = 0;
    let finalizers = 0;
    let toolCalls = 0;
    const activeExecutions: Array<{ phase: number; execution: ExecutionCheckpoint }> = [];
    let retiredExecutions = 0;

    const captures: Array<{
      phase: number;
      call: number;
      prompt: Prompt.Prompt;
    }> = [];

    const model = Model.make(
      "scripted",
      "issue692-hosted",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: ({ prompt }) =>
            Stream.unwrap(
              Effect.gen(function* () {
                calls++;
                if (calls > 2 || captures.length >= 20)
                  return yield* AiError.AiError.make({
                    module: "issue692",
                    method: "script",
                    reason: AiError.UnknownError.make({
                      description: "Finite provider script exceeded two calls",
                    }),
                  });
                // Retain the actual immutable provider prompt. Encoding, estimates and hashes happen in evidence().
                captures.push({
                  phase: state.phase,
                  call: calls,
                  prompt,
                });

                const response =
                  calls === 1
                    ? [
                        ...parts(state.phase, calls),
                        { type: "finish" as const, reason: "tool-calls" as const, usage },
                      ]
                    : parts(state.phase, calls);

                return Stream.fromIterable(response).pipe(
                  Stream.ensuring(
                    Effect.sync(() => {
                      finalizers++;
                    }),
                  ),
                );
              }),
            ),
        }),
      ),
    );

    const tools = toolkit.toLayer({
      work: ({ index }) =>
        Effect.sync(() => {
          toolCalls++;

          return { index, value: `small-result-${index}` };
        }),
    });

    const context = Layer.succeed(RunContextPreparation, {
      hook: {
        prepare: (request) =>
          Effect.succeed({
            prompt: request.source,
            ...(isCompactionPhase(state.phase) && request.turn === 1
              ? { rollover: { handoff: `Native issue692 handoff for phase ${state.phase}.` } }
              : {}),
          }),
      },
    });

    const host = ThreadObject.layer([{ agent: definition, model, definitions: declarations }]).pipe(
      Layer.provide(Layer.mergeAll(tools, context, Layer.succeed(ContextCompactor, native))),
    );

    return Layer.effectContext(
      Effect.gen(function* () {
        const services = yield* Effect.context<ThreadObject.Services>();
        const store = yield* ThreadStore;
        const runtime = yield* DurableAgentRuntime;
        const definitions = yield* digestDefinitions(declarations);
        const deploymentId = DeploymentId.make("issue692-hosted-v1");
        const json = (value: unknown) => Schema.decodeUnknownEffect(JsonObject)(value);
        const current = typeof store.readExecution === "function";
        let activeRunId: RunId | undefined;

        // Observe authoritative state before normal terminal publication retires it.
        // This read belongs to the operation; encoding and audit stay in evidence().
        beforeSettlementByContext.set(
          ctx,
          Effect.gen(function* () {
            if (!current) return;
            if (activeRunId === undefined)
              return yield* BenchError.make({ message: "Missing active operation identity" });
            const execution = yield* store.readExecution({ threadId, runId: activeRunId });

            if (
              Option.isNone(execution) ||
              execution.value.progress.runId !== activeRunId ||
              activeExecutions.length !== state.phase - 1
            )
              return yield* BenchError.make({
                message: "Missing or repeated active checkpoint capture",
              });
            activeExecutions.push({ phase: state.phase, execution: execution.value });
          }).pipe(Effect.orDie),
        );

        const boundary = <A, E>(effect: Effect.Effect<A, E, CryptoDigest.Digest>) =>
          effect.pipe(
            Effect.provideService(CryptoDigest.Digest, crypto),
            Effect.catchCause((cause) =>
              Effect.fail(BenchError.make({ message: Cause.pretty(cause).slice(0, 2_000) })),
            ),
          );

        const readLog = Effect.gen(function* () {
          const pages = yield* Stream.runCollect(
            streamExport(ThreadExportRequest.make({ threadId })).pipe(
              Stream.provideService(ThreadExportSource, store),
            ),
          );

          const last = pages.at(-1);

          if (last === undefined)
            return yield* BenchError.make({ message: "Missing canonical export snapshot" });

          return {
            records: pages.flatMap((page) => page.records),
            batches: pages.flatMap((page) => page.batches),
            tailSequence: last.tailSequence,
          };
        });

        const seed = Effect.fn("issue692.seed")(function* (count: 10 | 1000) {
          yield* check(state.seedRecords === 0 && state.phase === 0, "Seed already initialized");
          yield* store.materialize(ThreadMaterialization.make({ threadId }));
          for (let start = 0; start < count; start += 128) {
            const records: Array<RecordEnvelope> = [];

            for (let position = start; position < Math.min(start + 128, count); position++) {
              const index = Math.floor((position - 1) / 3);
              const runId = RunId.make(`seed-run-${index}`);
              const input = `retired-seed-input-${index}`;
              const output = { answer: `retired-seed-output-${index}` };
              let payload: RecordEnvelope["payload"];

              if (position === 0)
                payload = ThreadCreated.make({ agentId: definition.id, definitions });
              else if ((position - 1) % 3 === 0)
                payload = UserInputRecorded.make({ kind: "user", runId, input });
              else if ((position - 1) % 3 === 1) {
                const messages = yield* Schema.decodeUnknownEffect(PersistedJson)(
                  yield* Schema.encodeEffect(Prompt.Prompt)(
                    Prompt.make([
                      { role: "user", content: JSON.stringify(input) },
                      { role: "assistant", content: JSON.stringify(output) },
                    ]),
                  ),
                );

                payload = ModelResponseRecorded.make({
                  toolResultMaxBytes: 1024 * 1024,
                  toolSelectionMaxBytes: 0,
                  toolOperations: [],
                  runId,
                  turnId: TurnId.make(`seed-turn-${index}`),
                  turn: 1,
                  messages: yield* Schema.decodeEffect(ModelResponseRecorded.fields.messages)(
                    messages,
                  ),
                  runScopedPrefixLength: 1,
                });
              } else payload = RunCompleted.make({ runId, output });
              records.push(
                RecordEnvelope.make({
                  recordId: RecordId.make(`seed-${position}`),
                  family: "thread",
                  schemaVersion: 1,
                  createdAt: DateTime.makeUnsafe("2026-09-30T00:00:00.000Z"),
                  deploymentId,
                  payload,
                }),
              );
            }
            const [first, ...rest] = records;

            if (first === undefined) return yield* BenchError.make({ message: "Empty seed batch" });
            const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

            yield* store.append(
              AppendRequest.make({
                threadId,
                batch: CanonicalBatch.make({
                  batchId: BatchId.make(`seed-${start}`),
                  producerId,
                  records: [first, ...rest],
                }),
                expectedTailSequence: tail.tailSequence,
              }),
            );
          }
          const log = yield* readLog;

          yield* check(log.records.length === count, "Seed record count differs");
          state = {
            ...state,
            seedRecords: count,
            incarnation,
            seedDigest: yield* digestJson(
              yield* Schema.encodeEffect(Schema.Array(RecordEnvelope))(
                log.records.map((entry) => entry.record),
              ),
            ),
          };
          persist();

          return yield* json({
            seeded: count,
            seedDigest: state.seedDigest,
            incarnation,
            objectId: ctx.id.toString(),
            threadId,
          });
        });

        const operate = Effect.fn("issue692.operate")(
          function* (phase: number) {
            yield* check(
              state.seedRecords > 0 && phase === state.phase + 1 && phase <= 10,
              "Out-of-order or repeated phase",
            );
            yield* check(
              captures.length === (phase - 1) * 2 &&
                state.incarnation?.module === incarnation.module &&
                state.incarnation.runtime === incarnation.runtime,
              "Runtime restarted or earlier provider captures are incomplete",
            );
            const method = REPLAY_CPU_PHASES[phase - 1];

            if (method === undefined) return yield* BenchError.make({ message: "Unknown phase" });

            calls = 0;
            finalizers = 0;
            toolCalls = 0;
            state = { ...state, phase };
            persist();

            const input = isCompactionPhase(phase)
              ? largeInput(phase)
              : `Fresh sequential reply phase ${phase}; use the retained large context.`;

            const receipt = yield* runtime.submit({ definition }, input, {
              threadId,
              principal: Principal.make("issue692-benchmark"),
              idempotencyKey: IdempotencyKey.make(`phase-${phase}`),
              definitions,
            });

            activeRunId = runIdForSubmission(receipt.submissionId);

            yield* runtime.processThreadResolved(threadId);
            const settlement = yield* runtime.awaitSettlement(receipt);

            if (current && settlement.outcome === "completed") {
              yield* check(
                Option.isNone(yield* store.readExecution({ threadId, runId: activeRunId })),
                "Completed replay-CPU operation retained execution obligations",
              );
              retiredExecutions++;
            }

            const result = {
              phase,
              method,
              incarnation,
              submissionId: receipt.submissionId,
              outcome: settlement.outcome,
              modelCalls: calls,
              modelFinalizers: finalizers,
              toolCalls,
            };

            state = {
              ...state,
              operations: [...state.operations, result],
            };
            persist();
            yield* check(
              settlement.outcome === "completed",
              `Submission failed: ${JSON.stringify(settlement)}`,
            );
            yield* check(
              calls === 2 && finalizers === 2 && toolCalls === 2,
              "Finite script/tool/finalizer counts differ",
            );

            return yield* json(result);
          },
          Effect.timeout("90 seconds"),
          boundary,
        );

        const evidence = Effect.gen(function* () {
          const log = yield* readLog;

          const encodedRecords = yield* Schema.encodeEffect(Schema.Array(RecordEnvelope))(
            log.records.map((entry) => entry.record),
          );

          const prefixDigest = yield* digestJson(encodedRecords.slice(0, state.seedRecords));
          const prefixUnchanged = prefixDigest === state.seedDigest;

          const compactions = log.records.filter(
            ({ record }) => record.payload._tag === "CompactionCreated",
          );

          const expectedCompactions = [1, 4, 6, 9].filter((phase) => phase <= state.phase).length;
          let nextSequence = state.seedRecords + 1;
          const producers = new Map(log.batches.map((batch) => [batch.batchId, batch.producerId]));
          const seen = new Set<string>();

          let batchesUnchanged =
            log.records.length === log.tailSequence && producers.size === log.batches.length;

          let start = state.seedRecords;

          while (start < log.records.length) {
            const entry = log.records[start];

            if (entry === undefined) break;
            let end = start + 1;

            while (end < log.records.length && log.records[end]?.batchId === entry.batchId) end++;
            const records = log.records.slice(start, end);
            const producerId = producers.get(entry.batchId);

            if (
              producerId === undefined ||
              seen.has(entry.batchId) ||
              records.some(
                (record, index) =>
                  record.threadId !== threadId || record.sequence !== nextSequence + index,
              )
            ) {
              batchesUnchanged = false;
              break;
            }
            seen.add(entry.batchId);
            nextSequence += records.length;
            start = end;
          }
          batchesUnchanged &&= nextSequence === log.tailSequence + 1;

          const audits = yield* Effect.forEach(captures, ({ phase, call, prompt }) =>
            Effect.gen(function* () {
              const encoded = yield* Schema.decodeUnknownEffect(PersistedJson)(
                yield* Schema.encodeEffect(Prompt.Prompt)(prompt),
              );

              const text = JSON.stringify(encoded);
              const largePhase = currentLargePhase(phase);

              const results = prompt.content
                .filter((message) => message.role === "tool")
                .flatMap((message) => message.content)
                .filter(
                  (part): part is Prompt.ToolResultPart =>
                    part.type === "tool-result" && part.id.startsWith(`phase-${phase}-`),
                );

              return {
                phase,
                call,
                encodedBytes: bytes(text),
                nativeEstimatedTokens: native.estimate(prompt.content),
                promptDigest: yield* digestJson(encoded),
                messageCount: prompt.content.length,
                currentLargeInputPresent:
                  text.includes(`LARGE_CONTEXT_BEGIN_${largePhase}`) &&
                  text.includes(`LARGE_CONTEXT_END_${largePhase}`),
                retiredSeedAbsent: !text.includes("retired-seed-input-"),
                retiredLargeInputAbsent: [1, 4, 6, 9]
                  .filter((priorPhase) => priorPhase < largePhase)
                  .every((priorPhase) => !text.includes(`LARGE_CONTEXT_BEGIN_${priorPhase}`)),
                currentToolResults: results.length,
                validToolResults: results.every(
                  (part) =>
                    !part.isFailure && JSON.stringify(part.result).includes("small-result-"),
                ),
                // A real encoded provider prompt is retained once per phase, outside the measured RPC.
                ...(call === 2 ? { prompt: encoded } : {}),
              };
            }),
          );

          // Only historical comparison builds have a canonical progress row to inspect.
          // Current builds report the real live observation, separately from retirement.
          const latest = current
            ? undefined
            : log.records.findLast(({ record }) => record.payload._tag === "RunContinuation");

          const captured = activeExecutions.at(-1);

          const progress = current ? captured?.execution.progress : latest?.record.payload;

          const encodedProgress =
            progress?._tag === "RunContinuation"
              ? yield* Schema.encodeEffect(RunContinuation)(progress)
              : undefined;

          const continuation =
            encodedProgress !== undefined
              ? {
                  present: true,
                  observation: current ? "active-before-settlement" : "historical-canonical",
                  ...(captured === undefined
                    ? { sequence: latest?.sequence }
                    : {
                        phase: captured.phase,
                        throughSequence: captured.execution.throughSequence,
                        operational: yield* Schema.encodeEffect(Schema.Array(RecordEnvelope))(
                          captured.execution.operational,
                        ),
                        retired: retiredExecutions === state.phase,
                      }),
                  encodedBytes: bytes(JSON.stringify(encodedProgress)),
                  progress: encodedProgress,
                }
              : { present: false };

          const successful =
            state.operations.length === state.phase &&
            state.operations.every((operation) => operation.outcome === "completed");

          const checks = {
            complete: state.phase === 10,
            sameIncarnation:
              state.incarnation?.module === incarnation.module &&
              state.incarnation.runtime === incarnation.runtime,
            batchesUnchanged,
            prefixUnchanged,
            successful,
            activeExecutionsMatch:
              !current ||
              (activeExecutions.length === state.phase &&
                activeExecutions.every(({ phase, execution }, index) => {
                  const operation = state.operations[index];

                  const runId =
                    operation === undefined
                      ? undefined
                      : runIdForSubmission(SubmissionId.make(operation.submissionId));

                  const starts = execution.operational.filter(
                    ({ payload }) => payload._tag === "RunStarted",
                  );

                  const contexts = execution.operational.filter(
                    ({ payload }) => payload._tag === "RunContextRecorded",
                  );

                  const completed = execution.operational.filter(
                    ({ payload }) => payload._tag === "RunCompleted",
                  );

                  const result = completed[0]?.payload;

                  const output =
                    result?._tag === "RunCompleted"
                      ? Schema.decodeUnknownOption(outputSchema)(result.output)
                      : Option.none();

                  return (
                    phase === index + 1 &&
                    execution.progress.runId === runId &&
                    execution.progress.submissionId === operation?.submissionId &&
                    execution.progress.accounting.modelCalls === 2 &&
                    starts.length === 1 &&
                    starts[0]?.payload._tag === "RunStarted" &&
                    starts[0].payload.runId === runId &&
                    contexts.length === 1 &&
                    contexts[0]?.recordId === execution.progress.savedContext?.recordId &&
                    completed.length === 1 &&
                    result?._tag === "RunCompleted" &&
                    result.runId === runId &&
                    result.finishReason === undefined &&
                    Option.isSome(output) &&
                    output.value.answer === `phase-${phase}-ok`
                  );
                })),
            executionsRetired: !current || retiredExecutions === state.phase,
            compactionCountMatches: compactions.length === expectedCompactions,
            capturesComplete:
              audits.length === state.phase * 2 &&
              audits.every(
                (audit, index) =>
                  audit.phase === Math.floor(index / 2) + 1 && audit.call === (index % 2) + 1,
              ),
            retainedContextMatches: audits.every(
              (audit) =>
                audit.currentLargeInputPresent &&
                audit.retiredSeedAbsent &&
                audit.retiredLargeInputAbsent &&
                audit.nativeEstimatedTokens >= 99_000 &&
                audit.nativeEstimatedTokens <= 110_000,
            ),
            toolResultsMatch: audits.every(
              (audit) =>
                audit.validToolResults && audit.currentToolResults === (audit.call === 2 ? 2 : 0),
            ),
            canonicalToolsMatch:
              log.records.filter(({ record }) => record.payload._tag === "ToolCallSettled")
                .length ===
              state.phase * 2,
            canonicalSettlementsMatch:
              log.records.filter(({ record }) => record.payload._tag === "SubmissionSettled")
                .length === state.phase &&
              state.operations.every((operation) => {
                const settlements = log.records.filter(
                  ({ record }) =>
                    record.payload._tag === "SubmissionSettled" &&
                    record.payload.submissionId === operation.submissionId,
                );

                const settled = settlements[0]?.record.payload;

                const output =
                  settled?._tag === "SubmissionSettled"
                    ? Schema.decodeUnknownOption(outputSchema)(settled.result)
                    : Option.none();

                return (
                  settlements.length === 1 &&
                  settled?._tag === "SubmissionSettled" &&
                  settled.runId === runIdForSubmission(SubmissionId.make(operation.submissionId)) &&
                  settled.outcome === "completed" &&
                  settled.finishReason === undefined &&
                  (!current || settled.usageSummary?.modelCalls === 2) &&
                  Option.isSome(output) &&
                  output.value.answer === `phase-${operation.phase}-ok`
                );
              }),
          };

          const result = yield* json({
            protocol: REPLAY_CPU_PROTOCOL,
            incarnation,
            revision: BENCH_REVISION,
            fixtureSha256: BENCH_FIXTURE_SHA256,
            deploymentVersion: env.REPLAY_CPU_VERSION.id,
            objectId: ctx.id.toString(),
            threadId,
            phase: state.phase,
            seedRecords: state.seedRecords,
            canonicalRecords: log.records.length,
            compactions: yield* Schema.encodeEffect(Schema.Array(RecordEnvelope))(
              compactions.map((entry) => entry.record),
            ),
            operations: state.operations,
            audits,
            continuation,
            databaseBytes: ctx.storage.sql.databaseSize,
            checks,
            valid: Object.values(checks).every(Boolean),
            records: encodedRecords,
          });

          captures.length = 0;

          return result;
        });

        return Context.add(services, Bench, {
          seed: (count) => boundary(seed(count)),
          operate,
          evidence: boundary(evidence),
        });
      }),
    ).pipe(Layer.provide(host));
  }),
);

export class ReplayCpuThread extends ThreadObject.make(application, {
  namespaceBinding: "REPLAY_CPU_THREADS",
  deploymentId: "issue692-hosted-v1",
  producerPrefix: "issue692-hosted",
  maxDatabaseBytes: 32 * 1024 * 1024,
  runtimeFailpoint: (ctx) => (location) =>
    location === "terminalize:before-publication"
      ? Effect.suspend(
          () => beforeSettlementByContext.get(ctx) ?? Effect.die("Replay observer not initialized"),
        )
      : Effect.void,
}) {
  seed(records: 10 | 1000) {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.seed(records)));
  }
  compactFirst() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(1)));
  }
  freshFirst() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(2)));
  }
  freshSecond() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(3)));
  }
  compactAgain() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(4)));
  }
  freshThird() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(5)));
  }
  warmCompactFirst() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(6)));
  }
  warmFreshFirst() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(7)));
  }
  warmFreshSecond() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(8)));
  }
  warmCompactAgain() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(9)));
  }
  warmFreshThird() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.operate(10)));
  }
  evidence() {
    return this[DurableObject.RunSymbol](Effect.flatMap(Bench, (bench) => bench.evidence));
  }
}

export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (
          !env.REPLAY_CPU_TOKEN ||
          request.headers.get("authorization") !== `Bearer ${env.REPLAY_CPU_TOKEN}`
        )
          return new Response("Unauthorized", { status: 401 });
        const url = new URL(request.url);

        if (url.pathname === "/identity")
          return Response.json({
            protocol: REPLAY_CPU_PROTOCOL,
            revision: BENCH_REVISION,
            fixtureSha256: BENCH_FIXTURE_SHA256,
            packageVersions: JSON.parse(BENCH_PACKAGE_VERSIONS),
            deploymentVersion: env.REPLAY_CPU_VERSION.id,
            maxObjects: 8,
            phasesPerObject: 10,
            modelCallsPerPhase: 2,
            toolCallsPerPhase: 2,
            modelKind: "scripted-no-network",
          });

        const name = yield* Schema.decodeUnknownEffect(ReplayCpuObjectName)(
          url.searchParams.get("object"),
        );

        const object = env.REPLAY_CPU_THREADS.getByName(`issue692-${env.REPLAY_CPU_RUN}-${name}`);

        const rpc = <A>(run: () => Promise<A>) =>
          Effect.tryPromise({
            try: run,
            catch: (cause) => BenchError.make({ message: String(cause).slice(0, 2_000) }),
          });

        if (url.pathname === "/evidence" && request.method === "GET")
          return Response.json(yield* rpc(() => object.evidence()));
        if (request.method !== "POST") return new Response("Not found", { status: 404 });
        switch (url.pathname) {
          case "/seed": {
            const input = yield* Schema.decodeUnknownEffect(SeedRequest)(
              yield* rpc(() => request.json()),
            );

            yield* check(
              input.records === (name.startsWith("small-") ? 10 : 1000),
              "Object and seed size disagree",
            );

            return Response.json(yield* rpc(() => object.seed(input.records)));
          }
          case "/compactFirst":
            return Response.json(yield* rpc(() => object.compactFirst()));
          case "/freshFirst":
            return Response.json(yield* rpc(() => object.freshFirst()));
          case "/freshSecond":
            return Response.json(yield* rpc(() => object.freshSecond()));
          case "/compactAgain":
            return Response.json(yield* rpc(() => object.compactAgain()));
          case "/freshThird":
            return Response.json(yield* rpc(() => object.freshThird()));
          case "/warmCompactFirst":
            return Response.json(yield* rpc(() => object.warmCompactFirst()));
          case "/warmFreshFirst":
            return Response.json(yield* rpc(() => object.warmFreshFirst()));
          case "/warmFreshSecond":
            return Response.json(yield* rpc(() => object.warmFreshSecond()));
          case "/warmCompactAgain":
            return Response.json(yield* rpc(() => object.warmCompactAgain()));
          case "/warmFreshThird":
            return Response.json(yield* rpc(() => object.warmFreshThird()));
          default:
            return new Response("Not found", { status: 404 });
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.succeed(
            Response.json({ error: Cause.pretty(cause).slice(0, 2_000) }, { status: 500 }),
          ),
        ),
      ),
    );
  },
};
