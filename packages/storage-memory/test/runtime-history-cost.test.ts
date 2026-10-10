import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { MemorySubmissionLedgerLive } from "@yielded/agent-storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@yielded/agent-storage-memory/memory-thread-store";
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import { digestJson, EMPTY_TAIL_DIGEST } from "@yielded/agent/digest";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "@yielded/agent/durable-failpoint";
import { RunId, ThreadId } from "@yielded/agent/identifiers";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ModelCompleted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  ThreadCreated,
} from "@yielded/agent/records";
import { promptFromCanonicalRecords } from "@yielded/agent/run-journal";
import { RunToolAuthorization } from "@yielded/agent/run-options";
import { layer as runStorageLayer, RunStorage } from "@yielded/agent/run-storage";
import {
  ClaimRequest,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "@yielded/agent/submission-ledger";
import {
  FencedAppendRequest,
  ThreadMaterialization,
  ThreadStore,
  ThreadTailRequest,
  ThreadRead,
} from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import { Array, Context, DateTime, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, Toolkit, type Response } from "effect/ai";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const definition = Agent.make("history-cost", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const response: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const base = Layer.mergeAll(
  MemorySubmissionLedgerLive.pipe(Layer.provideMerge(MemoryThreadStoreLive)),
  WakeScheduler.layerNoop,
  ToolReconciler.uncertain,
  DurableRuntimeFailpoint.layer,
  RunToolAuthorization.allowAll,
  DurableRuntimeConfig.layer({
    deploymentId: Schema.decodeSync(DeploymentId)("history-cost"),
    producerId: Schema.decodeSync(ProducerId)("history-cost"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

type ReadFault = "interruption";
type ReadPhase = "selected-run";

const measure = Effect.fn("RuntimeHistoryCost.measure")(function* (
  historySize: number,
  fault?: { readonly kind: ReadFault; readonly phase: ReadPhase },
  raceAppend = false,
) {
  const store = yield* ThreadStore;
  const threadId = Schema.decodeSync(ThreadId)(`history-cost-${historySize}`);
  const producerEpoch = Schema.decodeSync(ProducerEpoch)(0);
  const createdAt = yield* DateTime.now;
  let tailSequence = Schema.decodeSync(CanonicalSequence)(0);
  let tailDigest = EMPTY_TAIL_DIGEST;
  const retainedInputs: Array<string> = [];

  yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch }));
  for (let start = 0; start < historySize; start += 256) {
    const records = Array.makeBy(Math.min(256, historySize - start), (offset) => {
      const position = start + offset;
      const input = `retained input ${position}`;
      const retained = position > 0 && position % 4 === 0;

      if (retained) retainedInputs.push(input);

      return RecordEnvelope.make({
        recordId: Schema.decodeSync(RecordId)(`history-seed:${start + offset}`),
        family: "thread",
        schemaVersion: 1,
        deploymentId: Schema.decodeSync(DeploymentId)("history-cost"),
        createdAt,
        payload:
          start + offset === 0
            ? ThreadCreated.make({ agentId: definition.id, definitions })
            : retained
              ? ModelCompleted.make({
                  runId: RunId.make(`retained:${position}`),
                  output: "retained answer",
                  messages: Schema.decodeUnknownSync(PersistedJson)(
                    Schema.encodeSync(Prompt.Prompt)(
                      Prompt.make([
                        { role: "user", content: input },
                        { role: "assistant", content: "retained answer" },
                      ]),
                    ),
                  ),
                  history: Schema.decodeUnknownSync(PersistedJson)(
                    Schema.encodeSync(Prompt.Prompt)(
                      Prompt.make(
                        retainedInputs.flatMap((text) => [
                          { role: "user" as const, content: text },
                          { role: "assistant" as const, content: "retained answer" },
                        ]),
                      ),
                    ),
                  ),
                })
              : RepairAnnotated.make({
                  reason: "history-cost",
                  details: { text: "x".repeat(128) },
                }),
      });
    });

    const appended = yield* store.append(
      FencedAppendRequest.make({
        threadId,
        producerEpoch,
        expectedTailSequence: tailSequence,
        expectedTailDigest: tailDigest,
        batch: CanonicalBatch.make({
          batchId: Schema.decodeSync(BatchId)(`history-seed:${start}`),
          producerId: Schema.decodeSync(ProducerId)("history-cost"),
          records,
        }),
      }),
    );

    tailSequence = appended.lastSequence;
    tailDigest = appended.tailDigest;
  }
  let measured = false;

  let openedPages = 0;
  let closedPages = 0;
  let injected = false;
  let raced = false;
  const requests: Array<ThreadRead> = [];

  const counted = ThreadStore.of({
    ...store,
    read: (request) =>
      Stream.suspend(() => {
        openedPages++;
        if (!("selection" in request)) requests.push(request);

        const inject =
          !injected &&
          fault !== undefined &&
          "selection" in request &&
          request.selection._tag === "RunEvidence";

        if (inject) {
          injected = true;

          return Stream.fromEffect(Effect.interrupt);
        }

        return store.read(request);
      }).pipe(
        Stream.ensuring(
          Effect.sync(() => {
            closedPages++;
          }),
        ),
      ),
    readPrompt: (request) =>
      Stream.suspend(() => {
        openedPages++;
        requests.push(request);
        if (raceAppend && !raced) {
          raced = true;

          return Stream.unwrap(
            Effect.gen(function* () {
              const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
              const input = "retained input racing append";

              yield* store
                .append(
                  FencedAppendRequest.make({
                    threadId,
                    producerEpoch: tail.producerEpoch,
                    expectedTailSequence: tail.tailSequence,
                    expectedTailDigest: tail.tailDigest,
                    batch: CanonicalBatch.make({
                      batchId: BatchId.make("racing-history"),
                      producerId: ProducerId.make("history-cost"),
                      records: [
                        RecordEnvelope.make({
                          recordId: RecordId.make("racing-history"),
                          family: "thread",
                          schemaVersion: 1,
                          createdAt,
                          deploymentId: DeploymentId.make("history-cost"),
                          payload: ModelCompleted.make({
                            runId: RunId.make("racing-history"),
                            output: "racing answer",
                            messages: Schema.decodeUnknownSync(PersistedJson)(
                              Schema.encodeSync(Prompt.Prompt)(
                                Prompt.make([
                                  { role: "user", content: input },
                                  { role: "assistant", content: "racing answer" },
                                ]),
                              ),
                            ),
                            history: Schema.decodeUnknownSync(PersistedJson)(
                              Schema.encodeSync(Prompt.Prompt)(
                                Prompt.make([
                                  ...retainedInputs.flatMap((text) => [
                                    { role: "user" as const, content: text },
                                    { role: "assistant" as const, content: "retained answer" },
                                  ]),
                                  { role: "user", content: input },
                                  { role: "assistant", content: "racing answer" },
                                ]),
                              ),
                            ),
                          }),
                        }),
                      ],
                    }),
                  }),
                )
                .pipe(Effect.orDie);

              return store.readPrompt(request);
            }),
          );
        }

        return store.readPrompt(request);
      }).pipe(
        Stream.ensuring(
          Effect.sync(() => {
            closedPages++;
          }),
        ),
      ),
  });

  const model = Model.make(
    "scripted",
    "history-cost",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) =>
          Stream.fromEffect(
            Effect.sync(() => {
              measured = true;

              const userTexts = request.prompt.content.flatMap((message) =>
                message.role === "user"
                  ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                  : [],
              );

              expect(userTexts.filter((text) => text.startsWith("retained input "))).toEqual(
                retainedInputs,
              );
            }),
          ).pipe(Stream.flatMap(() => Stream.fromIterable(response))),
      }),
    ),
  );

  const agent = Agent.withModel(definition, model);

  const runtime = yield* DurableAgentRuntime.pipe(
    Effect.provide(DurableAgentRuntime.layer.pipe(Layer.provide(runStorageLayer()))),
    Effect.provideService(ThreadStore, counted),
  );

  const receipt = yield* runtime.submit(agent, "measure", {
    threadId,
    principal: Schema.decodeSync(Principal)("history-cost"),
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("history-cost"),
    definitions,
  });

  const exit = yield* runtime.processThread(agent, threadId).pipe(Effect.exit);
  const ledger = yield* SubmissionLedger;

  const snapshot = yield* ledger.loadRecoverySnapshot(
    RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
  );

  return {
    measured,
    exit,
    openedPages,
    closedPages,
    requests,
    injected,
    raced,
    snapshot,
  };
});

it.live.each([{ kind: "interruption", phase: "selected-run" }] satisfies ReadonlyArray<{
  readonly kind: ReadFault;
  readonly phase: ReadPhase;
}>)("releases startup reads and ownership after $phase $kind", (fault) =>
  Effect.gen(function* () {
    const result = yield* measure(1_025, fault).pipe(Effect.provide(base));

    expect(result.injected).toBe(true);
    expect(result.measured).toBe(false);
    expect(Exit.isFailure(result.exit)).toBe(true);
    expect(result.openedPages).toBe(result.closedPages);
    expect(result.snapshot.ownership).toBeUndefined();
    expect(result.requests.every((request) => request.limit <= 1_024)).toBe(true);
  }),
);

it.live("captures original context before racing later appends", () =>
  Effect.gen(function* () {
    const result = yield* measure(1_025, undefined, true).pipe(Effect.provide(base));

    expect(result.raced).toBe(true);
    expect(result.measured).toBe(true);
    expect(Exit.isSuccess(result.exit)).toBe(true);
    expect(result.openedPages).toBe(result.closedPages);
    expect(result.snapshot.ownership).toBeUndefined();
  }),
);

// Counting the complete Run prevents moving the repeated history read past model dispatch.
it.live(
  "reads only new history across verified local claims without changing the Prompt or digest",
  () =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const threadId = ThreadId.make("warm-history-cost");
      let promptRecords = 0;
      let observed = Prompt.empty;
      const readSequences = new Set<number>();

      const counted = ThreadStore.of({
        ...store,
        read: (request) =>
          store
            .read(request)
            .pipe(Stream.tap((entry) => Effect.sync(() => readSequences.add(entry.sequence)))),
        readPrompt: (request) =>
          store.readPrompt(request).pipe(
            Stream.tap((entry) =>
              Effect.sync(() => {
                promptRecords++;
                readSequences.add(entry.sequence);
              }),
            ),
          ),
      });

      const services = yield* Layer.build(
        DurableAgentRuntime.layer.pipe(
          Layer.provideMerge(runStorageLayer()),
          Layer.provide(Layer.succeed(ThreadStore, counted)),
        ),
      );

      const runtime = Context.get(services, DurableAgentRuntime);
      const storage = Context.get(services, RunStorage);

      const model = Model.make(
        "scripted",
        "warm-history-cost",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              observed = request.prompt;

              return Stream.fromIterable(response);
            },
          }),
        ),
      );

      const agent = Agent.withModel(definition, model);
      const encode = Schema.encodeSync(Schema.toCodecJson(Prompt.Prompt));
      const counts: Array<number> = [];

      for (let turn = 0; turn < 28; turn++) {
        if (turn === 26) {
          const before = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

          yield* store.append(
            FencedAppendRequest.make({
              threadId,
              producerEpoch: before.producerEpoch,
              expectedTailSequence: before.tailSequence,
              expectedTailDigest: before.tailDigest,
              batch: CanonicalBatch.make({
                batchId: BatchId.make("external-history-append"),
                producerId: ProducerId.make("independent-writer"),
                records: [
                  RecordEnvelope.make({
                    recordId: RecordId.make("external-history-append"),
                    family: "thread",
                    schemaVersion: 1,
                    deploymentId: DeploymentId.make("history-cost"),
                    createdAt: yield* DateTime.now,
                    payload: RepairAnnotated.make({ reason: "independent append", details: {} }),
                  }),
                ],
              }),
            }),
          );
        }

        const prior =
          turn === 0
            ? []
            : yield* store
                .read(ThreadRead.make({ threadId, limit: 1_024 }))
                .pipe(Stream.runCollect);

        const expected = yield* promptFromCanonicalRecords(prior);
        const before = promptRecords;
        const input = `input ${turn}: café 😀`;

        readSequences.clear();

        yield* runtime.submit(agent, input, {
          threadId,
          principal: Principal.make("history-cost"),
          idempotencyKey: IdempotencyKey.make(`warm-${turn}`),
          definitions,
        });
        if (turn === 24) {
          const beforeClaim = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

          const foreign = yield* storage
            .claim(ClaimRequest.make({ threadId, producerId: ProducerId.make("foreign-owner") }))
            .pipe(Effect.scoped);

          if (Option.isNone(foreign)) return yield* Effect.die("Foreign claim was not granted");
          const afterClaim = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

          expect(afterClaim.tailSequence).toBe(beforeClaim.tailSequence);
          expect(afterClaim.tailDigest).toBe(beforeClaim.tailDigest);
          expect(afterClaim.producerEpoch).toBe(beforeClaim.producerEpoch + 1);
        }
        const settled = yield* runtime.processThread(agent, threadId);

        counts.push(promptRecords - before);
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");
        expect(
          encode(
            Prompt.fromMessages(observed.content.filter((message) => message.role !== "system")),
          ),
        ).toEqual(
          encode(
            Prompt.concat(
              expected,
              Prompt.make([{ role: "user", content: JSON.stringify(input) }]),
            ),
          ),
        );
        if (turn === 24 || turn === 26) {
          const prefix = prior.find(
            ({ record }) => record.payload._tag === "ModelResponseRecorded",
          );

          expect(prefix).toBeDefined();
          if (prefix === undefined) return yield* Effect.die("Missing cached prefix record");
          expect(readSequences.has(prefix.sequence)).toBe(true);
        }

        const records = yield* store
          .read(ThreadRead.make({ threadId, limit: 1_024 }))
          .pipe(Stream.runCollect);

        const context = records.findLast(
          ({ record }) => record.payload._tag === "RunContextRecorded",
        )?.record.payload;

        expect(context?._tag).toBe("RunContextRecorded");
        if (context?._tag !== "RunContextRecorded")
          return yield* Effect.die("Missing saved context");
        expect(context.historyDigest).toBe(yield* digestJson(encode(expected)));
      }
      const warm = [...counts.slice(1, 24), counts[25], counts[27]];

      expect(warm).toEqual(Array.makeBy(25, () => counts[1]));
      expect(Math.max(...warm.map((count) => count ?? Infinity))).toBeLessThanOrEqual(8);
    }).pipe(Effect.provide(base), Effect.scoped),
);
