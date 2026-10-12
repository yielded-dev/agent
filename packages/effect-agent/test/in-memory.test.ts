import { describe, expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, type Response, Toolkit } from "effect/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

import * as Agent from "../src/core/Agent.ts";
import { RunId, ThreadId } from "../src/core/Identifiers.ts";
import * as Thread from "../src/core/Thread.ts";
import * as AgentRuntime from "../src/engine/AgentRuntime.ts";
import { ThreadHistory } from "../src/engine/ThreadHistory.ts";
import * as InMemory from "../src/InMemory.ts";

const answer: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const scriptedModel = (
  name: string,
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  observe: (options: LanguageModel.ProviderOptions) => void = () => {},
  beforeTurn: Effect.Effect<void> = Effect.void,
) =>
  Model.make(
    "test",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* beforeTurn;
                observe(options);
                const index = yield* Ref.getAndUpdate(turn, (value) => value + 1);

                return Stream.fromIterable(turns[index] ?? answer);
              }),
            ),
        });
      }),
    ),
  );

const child = Agent.make("child", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
});

describe("in-memory assembly", () => {
  // #785: settled conversation history must not consume a lifetime allowance.
  it.effect("retains an unbounded conversation while bounding individual messages", () =>
    Effect.gen(function* () {
      const store = yield* Thread.Store;
      const threadId = ThreadId.make("continuing");
      const messages: Array<Prompt.Message> = [];

      yield* store.create(threadId);
      for (let slice = 0; slice < 9; slice++) {
        for (let index = 0; index < 128; index++)
          messages.push(
            Prompt.userMessage({
              content: [Prompt.textPart({ text: `${slice}:${index}:${"x".repeat(4_200)}` })],
            }),
          );
        yield* store.recordHistory(
          threadId,
          RunId.make(`run-${slice}`),
          Prompt.fromMessages(messages),
        );
      }
      const saved = yield* store.snapshot(threadId);

      expect(Thread.toPrompt(saved).content).toEqual(messages);
      expect(saved.contentBytes).toBeGreaterThan(4 * 1024 * 1024);
      expect((yield* store.export(threadId)).snapshot).toEqual(saved);
      expect(
        yield* store
          .append(
            threadId,
            Thread.ThreadAppend.make({
              message: Prompt.userMessage({
                content: [Prompt.textPart({ text: "x".repeat(4 * 1024 * 1024) })],
              }),
            }),
          )
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ThreadLimitExceeded", limit: "content-bytes" });
      expect(yield* store.snapshot(threadId)).toEqual(saved);
    }).pipe(Effect.provide(InMemory.layer)),
  );

  it.effect(
    "rejects stale concurrent history without erasing the winning conversation or retrying",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const threadId = ThreadId.make("concurrent");
        let modelCalls = 0;

        const slow = scriptedModel(
          "slow",
          [answer],
          () => {
            modelCalls++;
          },
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(resume))),
        );

        const fast = scriptedModel("fast", [answer], () => {
          modelCalls++;
        });

        yield* Effect.gen(function* () {
          const first = yield* AgentRuntime.run(Agent.withModel(child, slow), "First request", {
            threadId,
          }).pipe(Effect.forkChild);

          yield* Deferred.await(entered);
          yield* AgentRuntime.run(Agent.withModel(child, fast), "Second request", { threadId });
          yield* Deferred.succeed(resume, undefined);
          const failed = yield* Fiber.join(first).pipe(Effect.flip);

          expect(failed).toMatchObject({ _tag: "ThreadHistoryError", reason: "conflict" });
          expect(modelCalls).toBe(2);
          const history = yield* ThreadHistory;
          const stored = JSON.stringify(yield* history.load(threadId));

          expect(stored).toContain("First request");
          expect(stored).toContain("Second request");
        }).pipe(Effect.provide(InMemory.layer));
      }),
  );
});

class ProviderClient extends Context.Service<ProviderClient, string>()(
  "in-memory/ProviderClient",
) {}

export const verifyProviderRequirementsAndTypedFailures = () => {
  const model = Model.make(
    "test",
    "required-client",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* ProviderClient;

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.fromIterable(answer),
        });
      }),
    ),
  );

  const program = AgentRuntime.run(Agent.withModel(child, model), "question").pipe(
    Effect.provide(InMemory.layer),
  );

  expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<ProviderClient>();
  expectTypeOf<Effect.Error<typeof program>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof child>
  >();
};

declare const expectTypeOf: typeof ExpectTypeOf;
