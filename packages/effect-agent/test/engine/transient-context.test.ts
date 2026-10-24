import { expect, layer } from "@effect/vitest";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy, CompactionPolicy } from "@yielded/agent/agent-policy";
import * as AgentRuntime from "@yielded/agent/agent-runtime";
import { estimateMessageTokens } from "@yielded/agent/compaction";
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
  nextThreadId: Effect.suspend(() =>
    Schema.decodeEffect(ThreadId)(`transient-thread-${++threadSequence}`).pipe(Effect.orDie),
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
  // Regression: fcf63d5d8be7a0b363f13e99d09e1647a88bad27 added native image inputs.
  it.effect("counts image content independently of its transport encoding", () =>
    Effect.gen(function* () {
      const base64 =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jP1sAAAAASUVORK5CYII=";

      const bytes = yield* Schema.decodeEffect(Schema.Uint8ArrayFromBase64)(base64);
      const dataUrl = `data:image/png;base64,${base64}`;
      const wrapped = new Uint8Array(bytes.length + 10);

      wrapped.set(bytes, 5);

      const estimates = [bytes, wrapped.subarray(5, -5), base64, dataUrl, new URL(dataUrl)].map(
        (data) =>
          estimateMessageTokens(
            Prompt.userMessage({ content: [Prompt.filePart({ mediaType: "image/png", data })] }),
          ),
      );

      expect(new Set(estimates).size).toBe(1);
      expect(estimates[0]).toBeGreaterThan(0);
      expect(estimates[0]).toBeLessThan(20_000);

      const remote = estimateMessageTokens(
        Prompt.userMessage({
          content: [
            Prompt.filePart({
              mediaType: "image/png",
              data: new URL("https://example.com/image.png"),
            }),
          ],
        }),
      );

      expect(remote).toBeGreaterThan(1_000);
      expect(remote).toBeLessThan(20_000);
      expect(
        estimateMessageTokens(
          Prompt.assistantMessage({
            content: [
              Prompt.filePart({ mediaType: "image/png", data: new Uint8Array(256 * 1024) }),
            ],
          }),
        ),
      ).toBeLessThan(20_000);
    }),
  );

  // Regression: fcf63d5d8be7a0b363f13e99d09e1647a88bad27 added transient PNG screenshots.
  it.effect("admits native images by default while retaining text limits", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const image = Prompt.filePart({
        mediaType: "image/png",
        data: new Uint8Array(256 * 1024),
      });

      const result = yield* AgentRuntime.run(makeAgent(requests, 20_000), "Describe the image.", {
        transientContext: {
          load: () => Effect.succeed(Prompt.make([Prompt.userMessage({ content: [image] })])),
        },
      });

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
          policy: { maxTurns: 1, maxToolCalls: 1, contextTokenLimit: 20_000 },
        }),
        makeModel(requests),
      );

      const sourceResult = yield* AgentRuntime.run(sourceAgent, "image");

      expect(sourceResult.output).toBe("done");
      expect(requests).toHaveLength(2);
      expect(requests[1]?.content).toContainEqual(Prompt.userMessage({ content: [image] }));

      const oversizedText = yield* AgentRuntime.run(makeAgent(requests, 2_000), "Read the text.", {
        transientContext: { load: () => Effect.succeed(Prompt.make("text ".repeat(2_000))) },
      }).pipe(Effect.result);

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
