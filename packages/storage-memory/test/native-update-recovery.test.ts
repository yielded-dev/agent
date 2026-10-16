import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import * as Agent from "@yielded/agent/agent";
import { DurableWorkerBinding } from "@yielded/agent/agent-registration";
import { ContextCompactor } from "@yielded/agent/context-compactor";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "@yielded/agent/durable-failpoint";
import { DurableStep, DurableStepError, ToolExecutionClass } from "@yielded/agent/durable-step";
import { ThreadId, ToolCallId } from "@yielded/agent/identifiers";
import {
  CanonicalBatch,
  BatchId,
  RecordEnvelope,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  ToolCallUnknown,
} from "@yielded/agent/records";
import { CurrentRunWriter, makeProgressWriter } from "@yielded/agent/run-continuation";
import { runIdForSubmission, toolCallUnknownRecordId } from "@yielded/agent/run-journal";
import { RunToolAuthorization } from "@yielded/agent/run-options";
import { RunStorage, layer as runStorageLayer } from "@yielded/agent/run-storage";
import {
  ClaimRequest,
  MarkUnknownRequest,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  ResolutionSafeToRetry,
  UnknownResolutionCommand,
  SubmissionLedger,
} from "@yielded/agent/submission-ledger";
import { ThreadRead, ThreadReader, ThreadStore } from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { ToolResultBounds } from "@yielded/agent/tool-result";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { MemorySubmissionLedgerLive } from "../src/MemorySubmissionLedger.ts";
import { MemoryThreadStoreLive } from "../src/MemoryThreadStore.ts";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const base = Layer.mergeAll(
  MemorySubmissionLedgerLive.pipe(Layer.provideMerge(MemoryThreadStoreLive)),
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layer,
  ToolReconciler.uncertain,
  DurableRuntimeConfig.layer({
    deploymentId: DeploymentId.make("repro"),
    producerId: ProducerId.make("repro"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const finish = {
  type: "finish",
  reason: "tool-calls",
  usage: { inputTokens: { total: 100 }, outputTokens: { total: 10 } },
} satisfies Response.StreamPartEncoded;

for (const scenario of [
  {
    name: "three reads plus update, concurrent",
    concurrency: 4,
    update: true,
    defect: false,
    parked: false,
  },
  {
    name: "three reads alone, concurrent",
    concurrency: 4,
    update: false,
    defect: false,
    parked: false,
  },
  {
    name: "three reads plus update, serial",
    concurrency: 1,
    update: true,
    defect: false,
    parked: false,
  },
  {
    name: "three reads plus update, defect",
    concurrency: 4,
    update: true,
    defect: true,
    parked: false,
  },
  {
    name: "recovers a previously unknown accepted native update",
    concurrency: 4,
    update: true,
    defect: true,
    parked: "unknown",
  },
  {
    name: "recovers an accepted legacy native update with retained retry permission",
    concurrency: 4,
    update: true,
    defect: true,
    parked: "retry",
  },
]) {
  it.effect(scenario.name, () =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const ledger = yield* SubmissionLedger;
      const bodies: number[] = [];
      let requests = 0;
      let defect = scenario.defect;

      const tools = Toolkit.make(
        Tool.make("read_page", {
          parameters: Schema.Struct({ page: Schema.Number }),
          success: Schema.String,
          failure: DurableStepError,
          dependencies: [DurableStep],
        }).annotate(ToolExecutionClass, "readonly"),
      );

      const handlers = tools.toLayer({
        read_page: Effect.fnUntraced(function* ({ page }) {
          const step = yield* DurableStep;

          return yield* step
            .do(
              "execute",
              Schema.String,
              Effect.gen(function* () {
                bodies.push(page);
                yield* Effect.sleep("1 second");

                return "ok";
              }),
            )
            .pipe(
              Effect.catchTag("DurableStepError", (error) =>
                (() => {
                  if (!defect) return Effect.fail(error);
                  defect = false;

                  return Effect.die(error);
                })(),
              ),
            );
        }),
      });

      const model = Model.make(
        "scripted",
        "repro",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              requests++;

              return Stream.fromIterable<Response.StreamPartEncoded>(
                requests === 1
                  ? [
                      ...(scenario.update
                        ? [
                            {
                              type: "tool-call" as const,
                              id: "update",
                              name: "emit_update",
                              params: { value: "Reading pages" },
                            },
                          ]
                        : []),
                      ...[1, 2, 3].map((page) => ({
                        type: "tool-call" as const,
                        id: `read-${page}`,
                        name: "read_page",
                        params: { page },
                      })),
                      finish,
                    ]
                  : [
                      { type: "text-start", id: "answer" },
                      { type: "text-delta", id: "answer", delta: '"done"' },
                      { type: "text-end", id: "answer" },
                      { ...finish, reason: "stop" },
                    ],
              );
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("repro", {
          input: Schema.String,
          output: Schema.String,
          updates: Schema.String,
          instructions: "Read pages, emit progress, then finish.",
          toolkit: tools,
          policy: {
            maxTurns: 3,
            maxToolCalls: 10,
            maxDuration: "30 seconds",
            toolConcurrency: scenario.concurrency,
            toolResultBounds: ToolResultBounds.make({ maxBytes: 262144 }),
          },
        }),
        model,
      );

      const makeRuntime = Effect.fnUntraced(function* (selected: typeof agent) {
        const binding = yield* DurableWorkerBinding.make(selected, definitions).pipe(
          Effect.provide(handlers),
        );

        return yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings([binding]).pipe(
              Layer.provide(runStorageLayer()),
              Layer.provide(
                Layer.mergeAll(RunToolAuthorization.allowAll, ContextCompactor.layerRollover),
              ),
            ),
          ),
        );
      });

      // The retained declaration predates the native idempotent annotation.
      const originalAgent =
        scenario.parked === "retry"
          ? Agent.withModel(
              {
                ...agent.definition,
                toolkit: Toolkit.make(
                  tools.tools.read_page,
                  agent.definition.toolkit.tools.emit_update.annotate(
                    ToolExecutionClass,
                    "uncertain",
                  ),
                ),
              },
              model,
            )
          : agent;

      let runtime = yield* makeRuntime(originalAgent);

      const receipt = yield* runtime.submit(originalAgent, "read", {
        threadId: ThreadId.make("repro"),
        principal: Principal.make("test"),
        idempotencyKey: IdempotencyKey.make("first"),
        definitions,
      });

      const running = yield* runtime
        .processThreadHead(receipt.threadId)
        .pipe(Effect.provide(handlers), Effect.exit, Effect.forkChild);

      yield* TestClock.adjust("10 seconds");
      const result = yield* Fiber.join(running);

      if (scenario.parked) {
        // Reproduce the original persisted Unknown protocol, independently of the new reconciler.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* RunStorage;

            const session = Option.getOrThrow(
              yield* storage.claim(ClaimRequest.make({ threadId: receipt.threadId })),
            );

            const runId = runIdForSubmission(receipt.submissionId);
            const toolCallId = ToolCallId.make("update");
            const recordId = toolCallUnknownRecordId(runId, 1, toolCallId);

            const progress = yield* makeProgressWriter(
              receipt.threadId,
              DeploymentId.make("repro"),
            );

            yield* progress
              .commit(
                CanonicalBatch.make({
                  batchId: BatchId.make(recordId),
                  producerId: ProducerId.make("repro"),
                  records: [
                    RecordEnvelope.make({
                      recordId,
                      family: "thread",
                      schemaVersion: 1,
                      createdAt: yield* DateTime.now,
                      deploymentId: DeploymentId.make("repro"),
                      payload: ToolCallUnknown.make({
                        runId,
                        turn: 1,
                        toolCallId,
                        toolName: "emit_update",
                        reason: "Synthetic prior unknown marking",
                      }),
                    }),
                  ],
                }),
              )
              .pipe(Effect.provideService(CurrentRunWriter, session));
            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: receipt.submissionId,
                toolCallIds: [toolCallId],
                reason: "Synthetic prior unknown marking",
              }),
            );
          }).pipe(
            Effect.provide(runStorageLayer()),
            Effect.provideService(ThreadReader, ThreadReader.fromStore(store)),
          ),
        );
      }
      if (scenario.parked === "retry") {
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: ToolCallId.make("update"),
            author: "operator",
            reason: "Synthetic retained retry permission",
            resolution: ResolutionSafeToRetry.make({}),
          }),
        );
        runtime = yield* makeRuntime(agent);
      }
      if (scenario.parked) yield* runtime.runRecovery({ threadId: receipt.threadId });

      const retrying = yield* runtime
        .processThreadHead(receipt.threadId)
        .pipe(Effect.provide(handlers), Effect.exit, Effect.forkChild);

      yield* TestClock.adjust("10 seconds");
      const retry = yield* Fiber.join(retrying);

      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
      );

      const records = yield* Stream.runCollect(
        store.read(ThreadRead.make({ threadId: receipt.threadId, limit: 100 })),
      );

      const payloads = records.map(({ record: { payload } }) => payload);

      // Human-requested regression: accepted native updates must survive a sibling defect.
      // Capacity rejection remains intentional; a later typed rejection settles failed.
      expect(snapshot.submission.settledOutcome).toBe(
        scenario.concurrency === 1 ? "completed" : "failed",
      );
      expect(snapshot.hasOwner).toBe(false);
      if (scenario.parked === "retry") {
        expect(snapshot.unknownResolutions).toMatchObject([
          {
            toolCallId: "update",
            resolution: { _tag: "SafeToRetry" },
          },
        ]);
      }
      expect(payloads.filter((record) => record._tag === "ToolCallUnknown")).toHaveLength(
        scenario.parked ? 1 : 0,
      );
      expect(Exit.isSuccess(retry)).toBe(true);
      if (scenario.defect) {
        expect(Exit.isFailure(result)).toBe(true);
        expect(
          payloads.filter(
            (record) => record._tag === "ToolCallSettled" && record.toolName === "emit_update",
          ),
        ).toMatchObject([{ toolCallId: "update", result: { emitted: true }, isFailure: false }]);
      } else {
        expect(Exit.isSuccess(result)).toBe(true);
      }
      if (scenario.update) {
        expect(payloads.filter((record) => record._tag === "AgentUpdateEmitted")).toMatchObject([
          {
            update: {
              value: "Reading pages",
              sequence: 1,
              updateId: `tool:run:${receipt.submissionId}:update`,
            },
          },
        ]);
      }
      if (scenario.concurrency === 1) expect(bodies).toEqual([1, 2, 3]);
      else
        expect(payloads.find((record) => record._tag === "SubmissionSettled")).toMatchObject({
          outcome: "failed",
        });
    }).pipe(Effect.provide(base)),
  );
}
