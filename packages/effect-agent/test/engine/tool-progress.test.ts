import { expect, layer } from "@effect/vitest";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import * as AgentRuntime from "@yielded/agent/agent-runtime";
import { IdGenerator } from "@yielded/agent/id-generator";
import { ThreadId, RunId, TurnId } from "@yielded/agent/identifiers";
import { RunContextPreparationPassthrough } from "@yielded/agent/run-options";
import { ThreadHistory } from "@yielded/agent/thread-history";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

let threadSequence = 0;

const Progress = Tool.make("report_progress", {
  parameters: Schema.Struct({}),
  success: Schema.Any,
});

const Hosted = Tool.providerDefined({
  id: "test.hosted_progress",
  customName: "Hosted",
  providerName: "hosted",
  parameters: Schema.Struct({}),
  success: Schema.Struct({ status: Schema.String }),
})(undefined);

const toolkit = Toolkit.make(Progress, Hosted);
const usage = { inputTokens: {}, outputTokens: {} };

const applicationCall: Response.StreamPartEncoded = {
  type: "tool-call",
  id: "application",
  name: "report_progress",
  params: {},
  providerExecuted: false,
};

const makeAgent = (firstTurn: ReadonlyArray<Response.StreamPartEncoded> = [applicationCall]) =>
  Agent.withModel(
    Agent.make("progress-bounds", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Report progress, then answer.",
      toolkit,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 2,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    }),
    Model.make(
      "scripted",
      "progress-bounds",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const turn = yield* Ref.make(0);

          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                  Effect.map((value) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      value === 0
                        ? [...firstTurn, { type: "finish", reason: "tool-calls", usage }]
                        : [
                            { type: "text-start", id: "answer" },
                            { type: "text-delta", id: "answer", delta: '"done"' },
                            { type: "text-end", id: "answer" },
                            { type: "finish", reason: "stop", usage },
                          ],
                    ),
                  ),
                ),
              ),
          });
        }),
      ),
    ),
  );

const testLayer = Layer.mergeAll(
  Layer.succeed(IdGenerator, {
    nextThreadId: Effect.suspend(() =>
      Schema.decodeEffect(ThreadId)(`progress-thread-${++threadSequence}`).pipe(Effect.orDie),
    ),
    nextRunId: Effect.succeed(Schema.decodeSync(RunId)("progress-run")),
    nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("progress-turn")),
  }),
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("Tool progress ownership and byte limits", (it) => {
  it.effect("retains owned progress in detached replay after the handler mutates its result", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const retained = { nested: { value: "original" } };

      const detached = yield* AgentRuntime.start(makeAgent(), "go").pipe(
        Effect.provide(
          toolkit.toLayer({
            report_progress: Effect.fnUntraced(function* (_, context) {
              yield* context.preliminary(retained);
              yield* Deferred.await(release);

              return "terminal result";
            }),
          }),
        ),
      );

      yield* detached.observe.pipe(
        Stream.filter((event) => event._tag === "ToolProgress"),
        Stream.take(1),
        Stream.runDrain,
      );
      retained.nested.value = "changed";
      yield* Deferred.succeed(release, undefined);
      expect((yield* detached.await).output).toBe("done");
      const progress = (yield* detached.events).filter((event) => event._tag === "ToolProgress");

      expect(progress).toHaveLength(1);
      expect(progress[0]?.result).toEqual({ nested: { value: "original" } });
      expect(progress[0]?.result).not.toBe(retained);
    }),
  );

  it.effect(
    "shares the cumulative progress allowance between provider and application results",
    () =>
      Effect.gen(function* () {
        const detached = yield* AgentRuntime.start(
          makeAgent([
            { type: "tool-call", id: "hosted", name: "Hosted", params: {}, providerExecuted: true },
            {
              type: "tool-result",
              id: "hosted",
              name: "Hosted",
              result: { status: "x" },
              isFailure: false,
              providerExecuted: true,
              preliminary: true,
            },
            {
              type: "tool-result",
              id: "hosted",
              name: "Hosted",
              result: { status: "done" },
              isFailure: false,
              providerExecuted: true,
            },
            applicationCall,
          ]),
          "go",
          { bufferLimits: { maxToolProgressBytes: 14 } },
        ).pipe(
          Effect.provide(
            toolkit.toLayer({
              report_progress: (_, context) => context.preliminary("x").pipe(Effect.as("terminal")),
            }),
          ),
        );

        const exit = yield* Effect.exit(detached.await);
        const events = yield* detached.events;

        expect(Exit.isFailure(exit)).toBe(true);
        expect(
          events.filter((event) => event._tag === "ToolProgress").map((event) => event.result),
        ).toEqual([{ status: "x" }]);
        expect(events.at(-1)).toMatchObject({ _tag: "RunFailed", errorTag: "ModelProtocolError" });
      }),
  );
});
