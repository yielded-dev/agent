import * as NodeHost from "@effect-agent/platform-node/node-durable-host";
import { Clock, Effect, FileSystem, Layer, Option, References, Schema, Stream } from "effect";
import { Agent } from "effect-agent";
import { digestDefinitions } from "effect-agent/digest";
import { ThreadId } from "effect-agent/identifiers";
import { DefinitionDigestInput } from "effect-agent/records";
import { runCompletedRecordId, runIdForSubmission } from "effect-agent/run-journal";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { getRecord } from "effect-agent/thread-store";
import { AiError, LanguageModel, Model, Tool, Toolkit, type Response } from "effect/ai";
import type { PlatformError } from "effect/PlatformError";

import { BenchmarkError, check, STEADY_STATE, type SteadyStateResult } from "./contracts.js";
import { makeCpuProfiler } from "./cpu-profile.js";

/** A native provider with bounded counters, without retaining every normalized prompt. */
export const runSteadyStateProfile = Effect.fn("benchmark.steadyStateProfile")(function* (
  filename: string,
  onWarmed: (operations: number, elapsedMs: number) => Effect.Effect<void, PlatformError>,
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-steady-state-" });
  const profiler = yield* makeCpuProfiler;
  const outputSchema = Schema.Struct({ text: Schema.String });
  const output = { text: "x".repeat(21) };
  const encodedOutput = JSON.stringify(output);
  const toolOutput = { value: "x".repeat(20) };
  const encodedToolOutput = JSON.stringify(toolOutput);
  const input = "i".repeat(32);
  const principal = Principal.make("steady-state-benchmark");
  let serial = 0;
  let round = 0;
  let modelCalls = 0;
  let modelFinalizers = 0;
  let toolCalls = 0;
  let toolFinalizers = 0;
  let canonicalCompletions = 0;
  let orderedTools = true;

  const providerError = (description: string) =>
    AiError.AiError.make({
      module: "runtime-steady-state",
      method: "streamText",
      reason: AiError.UnknownError.make({ description }),
    });

  const model = Model.make(
    "benchmark",
    "steady-state",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.fail(providerError("Expected native streaming request")),
        streamText: (request) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const results = request.prompt.content
                .filter((message) => message.role === "tool")
                .flatMap((message) => message.content)
                .filter((part) => part.type === "tool-result");

              const users = request.prompt.content.filter((message) => message.role === "user");

              if (
                round > 4 ||
                users.length !== 1 ||
                !users[0]?.content.some((part) => part.type === "text" && part.text === input) ||
                results.length !== round ||
                results.some(
                  (part, index) =>
                    part.id !== `call-${serial}-${index}` ||
                    part.isFailure ||
                    JSON.stringify(part.result) !== encodedToolOutput,
                )
              )
                return yield* providerError("Provider lost input or prior successful tool results");

              const current = round++;

              modelCalls++;

              const parts: Stream.Stream<Response.StreamPartEncoded> =
                current < 4
                  ? Stream.make(
                      {
                        type: "tool-call" as const,
                        id: `call-${serial}-${current}`,
                        name: "bench",
                        params: { index: current },
                      },
                      {
                        type: "finish" as const,
                        reason: "tool-calls" as const,
                        usage: { inputTokens: {}, outputTokens: {} },
                      },
                    )
                  : Stream.make(
                      { type: "text-start" as const, id: "answer" },
                      { type: "text-delta" as const, id: "answer", delta: encodedOutput },
                      { type: "text-end" as const, id: "answer" },
                      {
                        type: "finish" as const,
                        reason: "stop" as const,
                        usage: { inputTokens: {}, outputTokens: {} },
                      },
                    );

              return parts.pipe(
                Stream.ensuring(
                  Effect.sync(() => {
                    modelFinalizers++;
                  }),
                ),
              );
            }),
          ),
      }),
    ),
  );

  const tool = Tool.make("bench", {
    description: "Return the fixed benchmark result.",
    parameters: Schema.Struct({ index: Schema.Number }),
    success: Schema.Struct({ value: Schema.String }),
  });

  const toolkit = Toolkit.make(tool);

  const handlers = toolkit.toLayer({
    bench: ({ index }) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          toolCalls++;
          orderedTools &&= index === round - 1;
        }),
        () => Effect.succeed(toolOutput),
        () =>
          Effect.sync(() => {
            toolFinalizers++;
          }),
      ),
  });

  const agent = Agent.withModel(
    Agent.make("steady-state-benchmark", {
      input: Schema.String,
      inputPrompt: (value) => value,
      output: outputSchema,
      instructions: "Return the supplied JSON result. Use the bench tool only when requested.",
      toolkit,
      policy: {
        maxTurns: 8,
        maxToolCalls: 8,
        maxDuration: "120 seconds",
        toolConcurrency: 4,
        runStatus: "off",
      },
    }),
    model,
  );

  const definitions = DefinitionDigestInput.make({
    agent: "steady-state-benchmark-v1",
    model: "native-scripted-v1",
    tools: ["bench-v1"],
  });

  const definitionsDigest = yield* digestDefinitions(definitions);

  return yield* Effect.gen(function* () {
    const host = yield* NodeHost.NodeDurableHost;

    const execute = Effect.gen(function* () {
      serial++;
      round = 0;

      const receipt = yield* host.submit(agent, input, {
        threadId: ThreadId.make(`steady-state-${serial}`),
        idempotencyKey: IdempotencyKey.make(`steady-state-${serial}`),
        principal,
        definitions: definitionsDigest,
      });

      const settlement = yield* host.awaitSettlement(receipt);

      yield* check(settlement.outcome === "completed", "Steady-state Submission did not complete");
      const runId = runIdForSubmission(receipt.submissionId);

      const record = yield* getRecord({
        threadId: receipt.threadId,
        recordId: runCompletedRecordId(runId),
      });

      if (
        Option.isNone(record) ||
        record.value.record.payload._tag !== "RunCompleted" ||
        record.value.record.payload.runId !== runId
      )
        return yield* BenchmarkError.make({ message: "Missing canonical steady-state completion" });

      const result = yield* Schema.decodeUnknownEffect(outputSchema)(
        record.value.record.payload.output,
      );

      yield* check(
        result.text === output.text && round === 5 && orderedTools,
        "Steady-state output, tool order or provider count changed",
      );
      canonicalCompletions++;
    }).pipe(Effect.timeout("120 seconds"));

    const warmupStarted = yield* Clock.monotonicTimeNanos;
    const warmupOperations = STEADY_STATE.warmupOperations;

    for (let index = 0; index < warmupOperations; index++) {
      yield* execute;
    }
    const warmupMs = Number((yield* Clock.monotonicTimeNanos) - warmupStarted) / 1e6;

    yield* check(
      modelCalls === warmupOperations * 5 &&
        modelFinalizers === modelCalls &&
        toolCalls === warmupOperations * 4 &&
        toolFinalizers === toolCalls &&
        canonicalCompletions === warmupOperations,
      "Steady-state warmup did not finish all work",
    );
    modelCalls = modelFinalizers = toolCalls = toolFinalizers = canonicalCompletions = 0;
    yield* onWarmed(warmupOperations, warmupMs);

    const measured = yield* profiler.capture(
      Effect.gen(function* () {
        const started = yield* Clock.monotonicTimeNanos;

        for (let index = 0; index < STEADY_STATE.operations; index++) yield* execute;

        return Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;
      }),
      filename,
    );

    yield* check(
      modelCalls === STEADY_STATE.operations * 5 &&
        modelFinalizers === modelCalls &&
        toolCalls === STEADY_STATE.operations * 4 &&
        toolFinalizers === toolCalls &&
        canonicalCompletions === STEADY_STATE.operations,
      "Steady-state measurement did not finish all work",
    );

    return {
      case: "sqlite-tool-rounds-4",
      workload: "resident file-backed SQLite host; fresh Thread and Submission per operation",
      capture:
        "one continuous operation loop; imports, host acquisition, warmup, reporting and host disposal excluded",
      provider: "native Effect LanguageModel with Stream.make; no inference or network",
      warmupOperations,
      warmupMs,
      operations: STEADY_STATE.operations,
      operationMs: measured.value,
      profileDurationMs: measured.profileDurationMs,
      samplingIntervalMicros: STEADY_STATE.samplingIntervalMicros,
      modelCalls,
      modelFinalizers,
      toolCalls,
      toolFinalizers,
      canonicalCompletions,
      inputBytes: 32,
      outputBytes: 32,
      toolResultBytes: 32,
      syntheticDelayMs: 0,
    } satisfies SteadyStateResult;
  }).pipe(
    Effect.provide(
      NodeHost.layer([{ agent, definitions }], {
        filename: `${directory}/thread.sqlite`,
        deploymentId: "steady-state-benchmark",
        producerId: "steady-state-benchmark",
      }).pipe(Layer.provide(handlers)),
    ),
    Effect.provideService(References.MinimumLogLevel, "None"),
    Effect.scoped,
  );
});
