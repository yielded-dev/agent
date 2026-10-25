import { expect, layer } from "@effect/vitest";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import * as AgentRuntime from "@yielded/agent/agent-runtime";
import { IdGenerator } from "@yielded/agent/id-generator";
import { RunId, ThreadId, TurnId } from "@yielded/agent/identifiers";
import { ThreadHistory } from "@yielded/agent/thread-history";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.suspend(() =>
    Schema.decodeEffect(ThreadId)(`turn-lifetime-thread-${++threadSequence}`).pipe(Effect.orDie),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("turn-lifetime-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-lifetime-turn")),
});

layer(Layer.mergeAll(identifiers, ThreadHistory.layer))("Turn lifetime", (it) => {
  {
    const ending = "interrupt" as const;

    it.effect(`releases completed Turn resources before the next Turn and handles ${ending}`, () =>
      Effect.gen(function* () {
        const thirdTurnEntered = yield* Deferred.make<void>();
        const active = new Set<number>();
        const activeBeforePreparation: Array<number> = [];
        const finalized: Array<number> = [];
        let modelCalls = 0;
        let modelFinalizers = 0;

        const tools = Toolkit.make(
          Tool.make("next", { parameters: Schema.Struct({}), success: Schema.String }),
        );

        const model = Model.make(
          "scripted",
          "turn-lifetime",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  modelFinalizers++;
                }),
              );

              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () => {
                  modelCalls++;
                  expect(modelFinalizers).toBe(0);

                  const parts: ReadonlyArray<Response.StreamPartEncoded> =
                    modelCalls < 3
                      ? [
                          {
                            type: "tool-call",
                            id: `call-${modelCalls}`,
                            name: "next",
                            params: {},
                          },
                          {
                            type: "finish",
                            reason: "tool-calls",
                            usage: { inputTokens: {}, outputTokens: {} },
                          },
                        ]
                      : [
                          { type: "text-start", id: "answer" },
                          { type: "text-delta", id: "answer", delta: '"done"' },
                          { type: "text-end", id: "answer" },
                          {
                            type: "finish",
                            reason: "stop",
                            usage: { inputTokens: {}, outputTokens: {} },
                          },
                        ];

                  return Stream.fromIterable(parts);
                },
              });
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("turn-lifetime", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Use next twice, then answer.",
            toolkit: tools,
            policy: AgentPolicy.make({
              maxTurns: 3,
              maxToolCalls: 2,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
            }),
          }),
          model,
        );

        const run = AgentRuntime.stream(agent, "begin", {
          context: {
            prepare: Effect.fnUntraced(function* ({ source, turn }) {
              activeBeforePreparation.push(active.size);
              yield* Effect.acquireRelease(
                Effect.sync(() => active.add(turn)),
                () =>
                  Effect.sync(() => {
                    active.delete(turn);
                    finalized.push(turn);
                  }),
              );
              if (turn === 3) {
                yield* Deferred.succeed(thirdTurnEntered, undefined);

                return yield* Effect.never;
              }

              return { prompt: source };
            }),
          },
        }).pipe(
          Stream.runDrain,
          Effect.provide(
            tools.toLayer({
              next: () => Effect.succeed("continue"),
            }),
          ),
        );

        const exit = yield* Effect.scoped(
          Effect.gen(function* () {
            const fiber = yield* Effect.forkChild(run);

            yield* Deferred.await(thirdTurnEntered);
            yield* Fiber.interrupt(fiber);

            return yield* Fiber.await(fiber);
          }),
        );

        expect(activeBeforePreparation).toEqual([0, 0, 0]);
        expect(finalized).toEqual([1, 2, 3]);
        expect(active.size).toBe(0);
        expect(modelFinalizers).toBe(1);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) throw new Error("Expected the selected failure");

        expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      }),
    );
  }
});
