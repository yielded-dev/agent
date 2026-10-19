import { expect, layer } from "@effect/vitest";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy, CompactionPolicy } from "@yielded/agent/agent-policy";
import * as AgentRuntime from "@yielded/agent/agent-runtime";
import { ContextCompactor } from "@yielded/agent/context-compactor";
import { IdGenerator } from "@yielded/agent/id-generator";
import { RunId, ThreadId, TurnId } from "@yielded/agent/identifiers";
import { RunContextPreparation } from "@yielded/agent/run-options";
import * as AiError from "effect/ai/AiError";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() =>
    Schema.decodeSync(ThreadId)(`transient-thread-${++threadSequence}`),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("transient-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("transient-turn")),
});

const testLayer = Layer.mergeAll(identifiers, ThreadHistory.layer, ContextCompactor.layer);

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const makeModel = (requests: Array<Prompt.Prompt>) =>
  Model.make(
    "scripted",
    "transient-context",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          requests.push(request.prompt);

          return Stream.fromIterable(finalParts);
        },
      }),
    ),
  );

const makeAgent = (requests: Array<Prompt.Prompt>, contextTokenLimit?: number) =>
  Agent.withModel(
    Agent.make("transient-context", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Answer the question.",
      toolkit: Toolkit.empty,
      policy: AgentPolicy.make({
        maxTurns: 1,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        ...(contextTokenLimit === undefined ? {} : { contextTokenLimit }),
      }),
    }),
    makeModel(requests),
  );

class TransientContextFailure extends Schema.TaggedError<TransientContextFailure>()(
  "TransientContextFailure",
  { message: Schema.String },
) {}

class TransientContextDependency extends Context.Service<
  TransientContextDependency,
  { readonly value: string }
>()("@effect-agent/engine/test/TransientContextDependency") {}

layer(testLayer)("transient model context", (it) => {
  // Regression: fcf63d5d8be7a0b363f13e99d09e1647a88bad27 added transient PNG screenshots.
  it.effect("uses image reservations while retaining native text admission", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const image = Prompt.filePart({
        mediaType: "image/png",
        data: new Uint8Array(256 * 1024),
      });

      const compactor = yield* ContextCompactor;

      const imageCompactor = {
        ...compactor,
        estimateMessageTokens: (message: Prompt.Message) =>
          message.role === "user" && message.content.some((part) => part.type === "file")
            ? 1_000
            : undefined,
      };

      const result = yield* AgentRuntime.run(makeAgent(requests, 2_000), "Describe the image.", {
        transientContext: {
          load: () => Effect.succeed(Prompt.make([Prompt.userMessage({ content: [image] })])),
        },
      }).pipe(Effect.provideService(ContextCompactor, imageCompactor));

      expect(result.output).toBe("done");
      expect(requests).toHaveLength(1);
      expect(requests[0]?.content).toContainEqual(Prompt.userMessage({ content: [image] }));

      const sourceAgent = Agent.withModel(
        Agent.make("source-image", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Describe the image.",
          inputPrompt: () => Prompt.make([Prompt.userMessage({ content: [image] })]),
          toolkit: Toolkit.empty,
          policy: { maxTurns: 1, maxToolCalls: 1, contextTokenLimit: 2_000 },
        }),
        makeModel(requests),
      );

      const sourceResult = yield* AgentRuntime.run(sourceAgent, "image").pipe(
        Effect.provideService(ContextCompactor, imageCompactor),
      );

      expect(sourceResult.output).toBe("done");
      expect(requests).toHaveLength(2);
      expect(requests[1]?.content).toContainEqual(Prompt.userMessage({ content: [image] }));

      const oversizedText = yield* AgentRuntime.run(makeAgent(requests, 2_000), "Read the text.", {
        transientContext: { load: () => Effect.succeed(Prompt.make("text ".repeat(2_000))) },
      }).pipe(Effect.provideService(ContextCompactor, imageCompactor), Effect.result);

      expect(oversizedText._tag).toBe("Failure");
      if (oversizedText._tag === "Failure") {
        expect(oversizedText.failure._tag).toBe("ContextBudgetError");
      }
      expect(requests).toHaveLength(2);
    }),
  );

  it.effect("reuses one transient snapshot for a same-Turn overflow retry", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const model = Model.make(
        "scripted",
        "transient-overflow-retry",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);

              return requests.length === 1
                ? Stream.fail(
                    AiError.AiError.make({
                      module: "test",
                      method: "streamText",
                      reason: AiError.UnknownError.make({
                        description: "context_length_exceeded",
                      }),
                    }),
                  )
                : Stream.fromIterable(finalParts);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-overflow-retry", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer the question.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 100_000,
            compaction: CompactionPolicy.make({ keepRecentTokens: 300 }),
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);

      const result = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () =>
              Ref.getAndUpdate(loads, (count) => count + 1).pipe(
                Effect.map((count) => Prompt.make(`snapshot ${count + 1}`)),
              ),
          },
        }),
      );

      expect(result.output).toBe("done");
      expect(yield* Ref.get(loads)).toBe(1);
      expect(requests).toHaveLength(2);
      for (const request of requests) {
        expect(JSON.stringify(request.content)).toContain("snapshot 1");
        expect(JSON.stringify(request.content)).not.toContain("snapshot 2");
      }
    }),
  );
});

export const verifyPreservesAPerRunOverridesTypedErrorAndServiceRequirement = () => {
  const requests: Array<Prompt.Prompt> = [];

  const program = AgentRuntime.run(makeAgent(requests), "question", {
    transientContext: {
      load: Effect.fnUntraced(function* () {
        const dependency = yield* TransientContextDependency;

        return yield* TransientContextFailure.make({ message: dependency.value });
      }),
    },
  });

  const dependencyRequired: TransientContextDependency extends Effect.Services<typeof program>
    ? true
    : false = true;

  const failurePreserved: TransientContextFailure extends Effect.Error<typeof program>
    ? true
    : false = true;

  void (dependencyRequired && failurePreserved);
};
