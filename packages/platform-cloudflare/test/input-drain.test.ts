import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { layer as runStorageLayer } from "@yielded/agent/run-storage";
import { AbortCommand, LedgerError, SubmissionLedger } from "@yielded/agent/submission-ledger";
import { readTestThread } from "@yielded/agent/testing/thread-store-conformance";
import { ThreadStore, ThreadExportRequest } from "@yielded/agent/thread-store";
import { runInDurableObject } from "cloudflare:test";
import { DurableObject } from "effect-cf";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { expect, it } from "vite-plus/test";

import { submitOptions } from "./fixtures.ts";
import { stubFor } from "./harness.ts";

// Regression: https://github.com/yielded-dev/agent/commit/efa46d1378493e60102080798064fa8e73f477b9
// A returned Tool failure was followed by a failed input-drain authority read, leaving
// the completed call unknown and blocking its FIFO successor indefinitely.
it("preserves a returned Tool result across an input-drain failure without replaying its handler", () => {
  const thread = "cf-returned-result-before-drain";

  return runInDurableObject(stubFor(thread), (instance) => {
    let toolCalls = 0;
    let drainFailures = 0;

    const Failed = Tool.make("failed", {
      parameters: Schema.Struct({}),
      success: Schema.String,
      failure: Schema.String,
      failureMode: "return",
    });

    const tools = Toolkit.make(Failed);
    const usage = { inputTokens: {}, outputTokens: {} };

    const model = Model.make(
      "scripted",
      "input-drain",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.fromIterable<Response.StreamPartEncoded>(
              JSON.stringify(options.prompt).includes('"tool-result"')
                ? [
                    { type: "text-start", id: "answer" },
                    { type: "text-delta", id: "answer", delta: '"fallback"' },
                    { type: "text-end", id: "answer" },
                    { type: "finish", reason: "stop", usage },
                  ]
                : [
                    {
                      type: "tool-call",
                      id: "returned-failure",
                      name: "failed",
                      params: {},
                      providerExecuted: false,
                    },
                    { type: "finish", reason: "tool-calls", usage },
                  ],
            ),
        }),
      ),
    );

    const agent = Agent.withModel(
      Agent.make("input-drain", {
        input: Schema.Struct({ question: Schema.String, ref: Schema.String }),
        output: Schema.String,
        instructions: "Try the Tool, then answer.",
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

    const failingDrain = Layer.effect(
      SubmissionLedger,
      Effect.map(SubmissionLedger, (ledger) =>
        SubmissionLedger.of({
          ...ledger,
          claimJoining: (request) =>
            Effect.suspend(() => {
              if (toolCalls > 0 && drainFailures === 0) {
                drainFailures++;

                return Effect.fail(
                  LedgerError.make({
                    operation: "claimJoining",
                    message: "Input admission authority is unavailable",
                  }),
                );
              }

              return ledger.claimJoining(request);
            }),
        }),
      ),
    );

    const runtimeLayer = DurableAgentRuntime.layer.pipe(
      Layer.provide(runStorageLayer),
      Layer.provide(failingDrain),
    );

    return instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const store = yield* ThreadStore;

        const receipt = yield* runtime.submit(
          agent,
          { question: "try", ref: thread },
          submitOptions(thread, "first"),
        );

        const process = runtime.processThread(agent, receipt.threadId).pipe(
          Effect.provide(
            tools.toLayer({
              failed: () =>
                Effect.suspend(() => {
                  toolCalls++;

                  return Effect.fail("unavailable");
                }),
            }),
          ),
        );

        const failure = yield* process.pipe(Effect.flip);

        expect(failure).toMatchObject({ _tag: "LedgerError", operation: "claimJoining" });
        expect(drainFailures).toBe(1);

        const before = yield* readTestThread(
          store,
          ThreadExportRequest.make({ threadId: receipt.threadId }),
        );

        expect(
          before.records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
          "a returned Tool outcome must be durable before the next input-drain can fail",
        ).toHaveLength(1);

        const follower = yield* runtime.submit(
          agent,
          { question: "follow up", ref: thread },
          submitOptions(thread, "second"),
        );

        yield* process;
        expect(yield* runtime.submissionStatus(receipt)).toMatchObject({ _tag: "settled" });
        expect(yield* runtime.submissionStatus(follower)).toMatchObject({ _tag: "settled" });
        expect(toolCalls).toBe(1);

        const after = yield* readTestThread(
          store,
          ThreadExportRequest.make({ threadId: receipt.threadId }),
        );

        expect(
          after.records.filter(({ record }) => record.payload._tag === "ToolCallUnknown"),
        ).toHaveLength(0);
      }).pipe(Effect.provide(runtimeLayer)),
    );
  });
});

// Regression: b6e82a802 reused the live tail as an earlier verified-empty startup boundary.
it("settles an abort during fresh instructions without a recovery attempt", () => {
  const thread = `cf-abort-fresh-instructions-${crypto.randomUUID()}`;

  return runInDurableObject(stubFor(thread), (instance) => {
    const runtimeLayer = DurableAgentRuntime.layer.pipe(Layer.provide(runStorageLayer));

    return instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const store = yield* ThreadStore;
        const entered = yield* Deferred.make<void>();
        let modelCalls = 0;

        const model = Model.make(
          "scripted",
          "startup-abort",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.suspend(() => {
                  modelCalls++;

                  return Stream.empty;
                }),
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("startup-abort", {
            input: Schema.Struct({ question: Schema.String, ref: Schema.String }),
            output: Schema.String,
            instructions: () =>
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
            toolkit: Toolkit.empty,
          }),
          model,
        );

        const receipt = yield* runtime.submit(
          agent,
          { question: "wait", ref: thread },
          submitOptions(thread, "first"),
        );

        const attempt = yield* Effect.forkChild(runtime.processThread(agent, receipt.threadId));

        yield* Deferred.await(entered);
        yield* runtime.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "operator",
            reason: "cancel instructions",
          }),
        );
        yield* Fiber.join(attempt);
        expect(yield* runtime.submissionStatus(receipt)).toMatchObject({
          _tag: "settled",
          settlement: { outcome: "aborted" },
        });
        expect(modelCalls).toBe(0);

        const history = yield* readTestThread(
          store,
          ThreadExportRequest.make({ threadId: receipt.threadId }),
        );

        expect(
          history.records.filter(({ record }) => record.payload._tag === "AbortRequested"),
        ).toHaveLength(1);
      }).pipe(Effect.scoped, Effect.provide(runtimeLayer)),
    );
  });
});
