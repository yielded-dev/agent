import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, layer } from "@effect/vitest";
import { MemorySubmissionLedgerLive } from "@yielded/agent-storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@yielded/agent-storage-memory/memory-thread-store";
import { inProcessCodeExecutorLayer } from "@yielded/agent-testing/code-executor-substitute";
import { ScriptedModel } from "@yielded/agent-testing/scripted-model";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import * as AgentRuntime from "@yielded/agent/agent-runtime";
import * as BrowserUse from "@yielded/agent/browser-use";
import * as CodeMode from "@yielded/agent/code-mode";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "@yielded/agent/durable-failpoint";
import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { IdGenerator } from "@yielded/agent/id-generator";
import { ThreadId, RunId, TurnId } from "@yielded/agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@yielded/agent/records";
import {
  RunContextPreparationPassthrough,
  toolFailureObserverLayer,
  type ToolFailureObservation,
} from "@yielded/agent/run-options";
import { layer as runStorageLayer } from "@yielded/agent/run-storage";
import { IdempotencyKey, Principal } from "@yielded/agent/submission-ledger";
import { ThreadHistory } from "@yielded/agent/thread-history";
import { ThreadRead, ThreadStore } from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

let threadSequence = 0;

const usage = { inputTokens: {}, outputTokens: {} };

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.suspend(() =>
    Schema.decodeEffect(ThreadId)(`thread-cm-e2e-${++threadSequence}`).pipe(Effect.orDie),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-cm-e2e")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-cm-e2e")),
});

// The suite opts out of the injected test services because the in-process
// executor's wall-clock deadline runs on the live Clock (see the substitute
// suite for the rationale).
const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer, { excludeTestServices: true })("Code Mode failure reporting", (it) => {
  // User-requested red/green at the model boundary: a CDP page exception must not
  // disappear when generated code reads only result.value. A preceding write
  // makes the no-replay requirement observable without mutating a live store.
  // Regression in 2cc267f9: successful Runtime.getExceptionDetails must remain
  // available to a corrective program despite returning exception metadata.
  // User-requested regression: a host policy refusal must survive the real
  // BrowserUse return-mode/broker path without exposing payloads to observers.
  // A live replay cannot safely reproduce the original pre-dispatch refusal.
  it.effect(
    "surfaces policy refusals and page exceptions to the model without replaying a browser mutation",
    () =>
      Effect.gen(function* () {
        const refusal = BrowserUse.BrowserUsePolicyError.make({
          reason: "observation-required",
          dispatch: "not-dispatched",
          nextStep: "fresh-observation",
          message: "Inspect the current page before attempting this browser action.",
        });

        const observations: Array<ToolFailureObservation> = [];
        const browser = BrowserUse.make();
        const sends: Array<typeof BrowserUse.SendRequest.Type> = [];
        const diagnostic = "TypeError: Cannot read properties of null (reading 'innerText')";
        const lookup = { exceptionDetails: { text: "Error: diagnostic" } };

        const code = [
          `async () => cdp.send({method: "Runtime.evaluate", params: {expression: "guardedMutation()"}})`,
          `async () => {
          await cdp.send({method: "Runtime.evaluate", params: {expression: "addToCart()"}});
          const r = await cdp.send({method: "Runtime.evaluate", params: {expression: "document.querySelector('#missing').innerText"}});
          return {value: r.result.value ?? null};
        }`,
          `async () => {
          const error = await cdp.send({method: "Runtime.evaluate", params: {expression: "new Error('diagnostic')"}});
          const details = await cdp.send({method: "Runtime.getExceptionDetails", params: {errorObjectId: error.result.objectId}});
          const r = await cdp.send({method: "Runtime.evaluate", params: {expression: "readCart()"}});
          return {cart: r.result.value, details};
        }`,
        ];

        const model = Model.make(
          "scripted",
          "browser-repair",
          ScriptedModel.layer([
            ...code.map((source, index) => ({
              _tag: "Stream" as const,
              parts: [
                {
                  type: "tool-call" as const,
                  id: `inspect-${index}`,
                  name: "browser_execute",
                  params: { code: source },
                  providerExecuted: false,
                },
                { type: "finish" as const, reason: "tool-calls" as const, usage },
              ],
              termination: { _tag: "Complete" as const },
            })),
            {
              _tag: "Stream",
              parts: [
                { type: "text-start", id: "answer" },
                { type: "text-delta", id: "answer", delta: '"Cart contains one bag"' },
                { type: "text-end", id: "answer" },
                { type: "finish", reason: "stop", usage },
              ],
              termination: { _tag: "Complete" },
            },
          ]),
        );

        const definition = Agent.make("browser-repair", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Inspect the cart.",
          toolkit: Toolkit.make(browser.tool),
          policy: { maxTurns: 4, maxToolCalls: 12, maxDuration: "30 seconds" },
        });

        const handlers = browser.handlers.pipe(
          Layer.provide([
            inProcessCodeExecutorLayer,
            BrowserUse.layer.pipe(
              Layer.provide(
                Layer.succeed(BrowserUse.BrowserCdp, {
                  send: Effect.fnUntraced(function* (
                    request: typeof BrowserUse.SendRequest.Type,
                  ): Effect.fn.Return<Schema.Json, BrowserUse.BrowserUsePolicyError> {
                    sends.push(request);

                    if (request.params?.expression === "guardedMutation()") return yield* refusal;

                    if (request.method === "Runtime.getExceptionDetails") return lookup;

                    if (request.params?.expression === "new Error('diagnostic')")
                      return {
                        result: { type: "object", subtype: "error", objectId: "error-1" },
                      };

                    return request.params?.expression ===
                      "document.querySelector('#missing').innerText"
                      ? {
                          result: { type: "object", subtype: "error" },
                          exceptionDetails: {
                            text: "Uncaught",
                            exception: { description: diagnostic },
                          },
                        }
                      : { result: { value: { quantity: 1 } } };
                  }),
                  tabs: Effect.succeed([]),
                  frames: () => Effect.succeed([]),
                  newTab: Effect.die("must not open a tab"),
                  closeTab: () => Effect.die("must not close a tab"),
                }),
              ),
            ),
          ]),
        );

        yield* Effect.gen(function* () {
          const result = yield* AgentRuntime.run(definition, "inspect").pipe(
            Effect.provide([
              handlers,
              toolFailureObserverLayer({
                observe: (observation) =>
                  Effect.sync(() => {
                    observations.push(observation);
                  }),
              }),
            ]),
          );

          const script = yield* ScriptedModel;
          const requests = yield* script.requests;

          const policyResult = requests[1]?.options.prompt.content.flatMap((message) =>
            message.role === "tool" ? message.content : [],
          )[0];

          expect(policyResult).toMatchObject({
            isFailure: true,
            result: {
              _tag: "CodeModeFailure",
              thrown: {
                _tag: "BrowserUsePolicyError",
                reason: "observation-required",
                dispatch: "not-dispatched",
                nextStep: "fresh-observation",
              },
              calls: [{ status: "failed" }],
            },
          });
          expect(observations[0]).toMatchObject({
            _tag: "ProgrammaticToolFailure",
            kind: "declared-failure",
            tag: "BrowserUsePolicyError",
            toolName: "browser_cdp_send",
          });
          expect(Cause.isCause(observations[0]?.cause)).toBe(true);
          for (const field of ["message", "result", "encodedResult"])
            expect(observations[0]).not.toHaveProperty(field);

          const failure = requests[2]?.options.prompt.content
            .flatMap((message) => (message.role === "tool" ? message.content : []))
            .at(-1);

          expect(failure).toMatchObject({
            isFailure: true,
            result: {
              _tag: "CodeModeFailure",
              thrown: {
                _tag: "BrowserUseError",
                dispatch: "possibly-dispatched",
                message: expect.stringContaining(diagnostic),
              },
              calls: [{ status: "succeeded" }, { status: "failed" }],
            },
          });
          expect(result.output).toBe("Cart contains one bag");
          expect(
            requests[3]?.options.prompt.content
              .flatMap((message) => (message.role === "tool" ? message.content : []))
              .at(-1),
          ).toMatchObject({
            isFailure: false,
            result: { result: { cart: { quantity: 1 }, details: lookup } },
          });
          expect(sends).toEqual([
            { method: "Runtime.evaluate", params: { expression: "guardedMutation()" } },
            { method: "Runtime.evaluate", params: { expression: "addToCart()" } },
            {
              method: "Runtime.evaluate",
              params: { expression: "document.querySelector('#missing').innerText" },
            },
            { method: "Runtime.evaluate", params: { expression: "new Error('diagnostic')" } },
            { method: "Runtime.getExceptionDetails", params: { errorObjectId: "error-1" } },
            { method: "Runtime.evaluate", params: { expression: "readCart()" } },
          ]);
        }).pipe(Effect.provide(model));
      }),
  );

  it.effect(
    "RUN-036 CAP-016 observes the inner Cause while CodeModeFailure recovers to a completed durable Run",
    () => {
      class LookupFailure extends Schema.TaggedError<LookupFailure>()("LookupFailure", {
        message: Schema.String,
        privateDetail: Schema.String,
      }) {}

      const original = LookupFailure.make({
        message: "Lookup unavailable",
        privateDetail: "CODE_MODE_CAUSE_SECRET",
      });

      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: LookupFailure,
      }).annotate(ToolExecutionClass, "readonly");

      const Declared = Tool.make("declared", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: LookupFailure,
        failureMode: "return",
      }).annotate(ToolExecutionClass, "readonly");

      const codeMode = CodeMode.make("search_workspace", {
        description: "Search the workspace",
        tools: { workspace: { declared: Declared, lookup: Lookup } },
      });

      const definition = Agent.make("code-mode-observer-regression", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Search, then answer.",
        toolkit: Toolkit.make(codeMode.tool),
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 4,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });

      const observations: Array<ToolFailureObservation> = [];
      const logs: Array<unknown> = [];

      const logger = Logger.make<unknown, void>(({ message, cause, fiber }) => {
        logs.push({
          message,
          cause: Cause.pretty(cause),
          annotations: fiber.getRef(References.CurrentLogAnnotations),
        });
      });

      const runtimeLayer = DurableAgentRuntime.layer.pipe(
        Layer.provide(runStorageLayer),
        Layer.provideMerge(
          Layer.mergeAll(
            MemorySubmissionLedgerLive.pipe(Layer.provideMerge(MemoryThreadStoreLive)),
            WakeScheduler.layerNoop,
            DurableRuntimeFailpoint.layer,
            ToolReconciler.uncertain,
            DurableRuntimeConfig.layer({
              deploymentId: DeploymentId.make("observer-regression"),
              producerId: ProducerId.make("observer-regression"),
            }),
            toolFailureObserverLayer({
              observe: (observation) =>
                Effect.sync(() => {
                  observations.push(observation);
                }),
            }),
          ),
        ),
        Layer.provide(NodeCrypto.layer),
      );

      const handlers = codeMode.handlers.pipe(
        Layer.provide([
          Toolkit.make(Declared, Lookup).toLayer({
            declared: () =>
              Effect.fail(
                LookupFailure.make({
                  message: "Expected lookup failure",
                  privateDetail: "CODE_MODE_DECLARED_SECRET",
                }),
              ),
            lookup: () => Effect.fail(original),
          }),
          inProcessCodeExecutorLayer,
        ]),
      );

      return Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        const model = Model.make(
          "scripted",
          "observer-regression",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(turn, (n) => n + 1).pipe(
                    Effect.map((n) =>
                      Stream.fromIterable<Response.StreamPartEncoded>(
                        n === 0
                          ? [
                              {
                                type: "tool-call",
                                id: "search-1",
                                name: "search_workspace",
                                params: {
                                  code: `async () => {
                                    try { await workspace.declared({ value: 1 }) } catch {}
                                    return await workspace.lookup({ value: 1 })
                                  }`,
                                },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : [
                              { type: "text-start", id: "answer" },
                              { type: "text-delta", id: "answer", delta: '"fallback answer"' },
                              { type: "text-end", id: "answer" },
                              { type: "finish", reason: "stop", usage },
                            ],
                      ),
                    ),
                  ),
                ),
            }),
          ),
        );

        const agent = Agent.withModel(definition, model);
        const runtime = yield* DurableAgentRuntime;
        const threadId = ThreadId.make("code-mode-observer");
        const digest = Digest.make("a".repeat(64));

        const receipt = yield* runtime.submit(agent, "search", {
          threadId,
          principal: Principal.make("test"),
          idempotencyKey: IdempotencyKey.make("search"),
          definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
        });

        yield* runtime.processThread(agent, threadId).pipe(Effect.provide(handlers));
        const settlement = yield* runtime.awaitSettlement(receipt);

        expect(settlement.outcome).toBe("completed");
        expect(observations).toHaveLength(3);
        expect(observations[0]).toMatchObject({
          _tag: "ProgrammaticToolFailure",
          kind: "declared-failure",
          tag: "LookupFailure",
          parentToolCallId: "search-1",
          toolCallId: "search-1#0",
        });
        expect(Cause.isCause(observations[0]?.cause)).toBe(true);
        for (const field of ["message", "result", "encodedResult"])
          expect(observations[0]).not.toHaveProperty(field);
        expect(observations[1]).toMatchObject({
          _tag: "ProgrammaticToolFailure",
          kind: "handler-error",
          tag: "LookupFailure",
          parentToolCallId: "search-1",
          toolCallId: "search-1#1",
        });
        expect(observations[1]?.cause?.reasons.filter(Cause.isFailReason)[0]?.error).toBe(original);
        expect(observations[1]).not.toHaveProperty("message");
        expect(observations[2]).toMatchObject({
          _tag: "ModelToolFailure",
          kind: "declared-failure",
          tag: "CodeModeFailure",
          toolCallId: "search-1",
        });
        expect(Cause.isCause(observations[2]?.cause)).toBe(true);
        expect(observations[2]).not.toHaveProperty("message");
        const store = yield* ThreadStore;

        const records = yield* store
          .read(ThreadRead.make({ threadId, limit: 1_024 }))
          .pipe(Stream.runCollect);

        const tools = records.filter(
          (envelope) => envelope.record.payload._tag === "ToolCallSettled",
        );

        expect(tools).toHaveLength(1);
        expect(tools[0]?.record.payload).toMatchObject({
          toolCallId: "search-1",
          isFailure: true,
          result: { _tag: "CodeModeFailure" },
        });
        expect(JSON.stringify({ records, logs })).not.toContain("CODE_MODE_CAUSE_SECRET");
        expect(JSON.stringify({ records, logs })).not.toContain("CODE_MODE_DECLARED_SECRET");
        expect(JSON.stringify(records)).not.toContain("search-1#0");
        expect(JSON.stringify(records)).not.toContain("search-1#1");
      }).pipe(Effect.provide([runtimeLayer, Logger.layer([logger])]));
    },
  );
});
