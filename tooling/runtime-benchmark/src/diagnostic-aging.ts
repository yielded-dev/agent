import { Agent } from "@yielded/agent";
import { NodeDurableAgentRuntime } from "@yielded/agent-platform-node/node-durable-agent-runtime";
import { DurableWorkerBinding } from "@yielded/agent/agent-registration";
import { ContextCompactor } from "@yielded/agent/context-compactor";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "@yielded/agent/durable-failpoint";
import { AgentId, ThreadId, ToolCallId } from "@yielded/agent/identifiers";
import { ReadEnvelope } from "@yielded/agent/record-format";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecord,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerEpoch,
  ProducerId,
  RecordId,
  RepairAnnotated,
  ThreadCreated,
} from "@yielded/agent/records";
import { canonicalRecordBytes } from "@yielded/agent/run-continuation";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import { RunContextPreparation, RunToolAuthorization } from "@yielded/agent/run-options";
import { layer as runStorageLayer } from "@yielded/agent/run-storage";
import {
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  submissionInputRecordId,
  UnknownResolutionCommand,
} from "@yielded/agent/submission-ledger";
import {
  FencedAppendRequest,
  LoadCheckpointRequest,
  ThreadReader,
  ThreadMaterialization,
  ThreadStore,
  ThreadTailRequest,
  type ThreadSelection,
} from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import {
  Cause,
  Clock,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/ai";
import { SqlClient } from "effect/sql/SqlClient";

import { BenchmarkError, check } from "./contracts.js";
import { AgingSeeds, DiagnosticProgress, type DiagnosticResult } from "./diagnostic-contracts.js";

const threadId = ThreadId.make("long-thread-aging");
const deploymentId = DeploymentId.make("long-thread-aging");
const producerId = ProducerId.make("long-thread-aging");
const tailRequest = ThreadTailRequest.make({ threadId });
const ages = [256, 131_328] as const;
const backgroundSizes = [100, 10_000, 1_000_000] as const;
const backgroundThreadId = ThreadId.make("long-thread-aging-background");
const selectedThreadRecords = 100_000;

const definitions = DefinitionDigests.make({
  agent: Digest.make("a".repeat(64)),
  model: Digest.make("a".repeat(64)),
  tools: Digest.make("a".repeat(64)),
});

const changedDefinitions = DefinitionDigests.make({
  ...definitions,
  agent: Digest.make("b".repeat(64)),
  model: Digest.make("b".repeat(64)),
});

const host = (filename: string) =>
  NodeDurableAgentRuntime.layer({ filename, deploymentId, producerId }).pipe(
    Layer.provide(ContextCompactor.layerRollover),
  );

const select = Effect.fnUntraced(function* (selection: ThreadSelection, limit = 1) {
  const store = yield* ThreadStore;

  return yield* store.read({ threadId, selection, page: { limit } }).pipe(Stream.runCollect);
});

const appendPadding = Effect.fn("diagnostic.aging.appendPadding")(function* (
  from: number,
  through: number,
  paddingThreadId = threadId,
) {
  const store = yield* ThreadStore;
  const at = DateTime.makeUnsafe("2026-10-06T00:00:00Z");
  const request = ThreadTailRequest.make({ threadId: paddingThreadId });
  const initial = yield* store.inspectTail(request);

  for (let offset = from; offset < through; offset += 256) {
    const tail = yield* store.inspectTail(request);

    const records = Array.from({ length: Math.min(256, through - offset) }, (_, index) =>
      CanonicalRecord.make({
        recordId: RecordId.make(`aging-padding-${paddingThreadId}-${offset + index}`),
        family: "thread",
        schemaVersion: 1,
        createdAt: at,
        deploymentId,
        payload:
          paddingThreadId === backgroundThreadId && offset + index === 0
            ? ThreadCreated.make({ agentId: AgentId.make("aging-background"), definitions })
            : RepairAnnotated.make({
                reason: "Unrelated aging benchmark fact",
                details: { ordinal: offset + index },
              }),
      }),
    );

    const [first, ...rest] = records;

    if (first === undefined)
      return yield* BenchmarkError.make({ message: "Empty aging padding batch" });

    yield* store.append(
      FencedAppendRequest.make({
        threadId: paddingThreadId,
        producerEpoch: tail.producerEpoch,
        expectedTailSequence: tail.tailSequence,
        expectedTailDigest: tail.tailDigest,
        batch: CanonicalBatch.make({
          batchId: BatchId.make(`aging-padding-${paddingThreadId}-${offset}`),
          producerId,
          records: [first, ...rest],
        }),
      }),
    );
  }
  const tail = yield* store.inspectTail(request);

  yield* check(
    tail.tailSequence - initial.tailSequence === through - from,
    "Aging seed did not append the exact unrelated fact count",
  );

  return tail;
});

/** Retain one uncertain Run and its original context in closed recovery snapshots. */
export const prepareSeeds = Effect.fn("diagnostic.aging.prepareSeeds")(function* (
  directory: string,
  storeSize: boolean,
) {
  const fs = yield* FileSystem.FileSystem;
  const filename = `${directory}/source.sqlite`;
  const mutationFile = `${directory}/mutation.txt`;
  let phase: "prefix" | "unknown" | "later" | "resume" = "prefix";
  let originalMutations = 0;
  let laterMutations = 0;
  let changedHandlerCalls = 0;
  let laterTurns = 0;
  let resumedPrompt = "";
  let resumedModelCalls = 0;
  const mutated = yield* Deferred.make<void>();

  const toolkit = Toolkit.make(
    Tool.make("write", { parameters: Tool.EmptyParams, success: Schema.String }),
  );

  const handlers = toolkit.toLayer({
    write: () =>
      Effect.gen(function* () {
        if (phase === "unknown") {
          originalMutations++;
          yield* fs
            .writeFileString(mutationFile, `call-a:${originalMutations}\n`)
            .pipe(Effect.orDie);
          yield* Deferred.succeed(mutated, undefined);

          return yield* Effect.never;
        }
        laterMutations++;

        return "recorded";
      }),
  });

  const changedHandlers = toolkit.toLayer({
    write: () =>
      Effect.suspend(() => {
        changedHandlerCalls++;

        return Effect.die("Uncertain mutation replayed under the changed binding");
      }),
  });

  const finish = {
    type: "finish",
    reason: "tool-calls",
    usage: { inputTokens: { total: 100 }, outputTokens: { total: 10 } },
  } satisfies Response.StreamPartEncoded;

  const final: ReadonlyArray<Response.StreamPartEncoded> = [
    { type: "text-start", id: "answer" },
    { type: "text-delta", id: "answer", delta: '"done"' },
    { type: "text-end", id: "answer" },
    { ...finish, reason: "stop" },
  ];

  const model = Model.make(
    "scripted",
    "long-thread-aging",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.die("Expected the streaming model boundary"),
        streamText: (request) => {
          if (phase === "later") laterTurns++;
          if (phase === "resume") {
            resumedPrompt = JSON.stringify(request.prompt);
            resumedModelCalls++;
          }

          const call =
            phase === "unknown"
              ? "call-a"
              : phase === "later" && laterTurns <= 2
                ? `call-b-${laterTurns}`
                : undefined;

          return Stream.fromIterable<Response.StreamPartEncoded>(
            call === undefined
              ? final
              : [{ type: "tool-call", id: call, name: "write", params: {} }, finish],
          );
        },
      }),
    ),
  );

  const makeAgent = (instructions: string) =>
    Agent.withModel(
      Agent.make("long-thread-aging", {
        input: Schema.String,
        output: Schema.String,
        instructions,
        toolkit,
        policy: {
          maxTurns: 10,
          maxToolCalls: 10,
          maxDuration: "30 seconds",
          contextTokenLimit: 20_000,
        },
      }),
      model,
    );

  const originalAgent = makeAgent("Keep original instructions.");
  const changedAgent = makeAgent("CHANGED BINDING must not replace retained instructions.");

  const makeRuntime = Effect.fnUntraced(function* (changed: boolean) {
    const binding = yield* DurableWorkerBinding.make(
      changed ? changedAgent : originalAgent,
      changed ? changedDefinitions : definitions,
    ).pipe(Effect.provide(changed ? changedHandlers : handlers));

    return yield* DurableAgentRuntime.pipe(
      Effect.provide(
        DurableAgentRuntime.layerWithBindings([binding]).pipe(
          Layer.provide(runStorageLayer()),
          Layer.provide(
            Layer.mergeAll(
              RunToolAuthorization.allowAll,
              ContextCompactor.layerRollover,
              DurableRuntimeFailpoint.layer,
              WakeScheduler.layerNoop,
              ToolReconciler.uncertain,
              DurableRuntimeConfig.layer({ deploymentId, producerId }),
            ),
          ),
        ),
      ),
      Effect.provideService(RunContextPreparation, {
        hook: {
          prepare: (request) =>
            Effect.succeed({
              prompt: request.source,
              ...(phase === "later" && request.turn === 3
                ? { rollover: { handoff: "The initial request completed.", through: 2 } }
                : {}),
            }),
        },
      }),
    );
  });

  const retained = yield* Effect.gen(function* () {
    const raw = yield* ThreadStore;
    const ledger = yield* SubmissionLedger;
    const sql = yield* SqlClient;
    const runtime = yield* makeRuntime(false);

    const submit = (input: string) =>
      runtime.submit(originalAgent, input, {
        threadId,
        principal: Principal.make("aging"),
        idempotencyKey: IdempotencyKey.make(input),
        definitions,
      });

    yield* submit("settled prefix");
    const prefix = yield* runtime.processThreadHead(threadId);

    yield* check(Option.isSome(prefix) && prefix.value.outcome === "completed", "Prefix failed");
    phase = "unknown";
    const older = yield* submit("older unresolved input");
    const worker = yield* runtime.processThreadHead(threadId).pipe(Effect.forkChild);

    yield* Deferred.await(mutated).pipe(Effect.timeout("10 seconds"));
    yield* Fiber.interrupt(worker);
    const interrupted = yield* Fiber.await(worker);

    yield* check(
      Exit.isFailure(interrupted) && Cause.hasInterrupts(interrupted.cause),
      "Original mutation did not lose its execution owner",
    );
    const recovery = yield* runtime.runRecovery();

    const pending = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: older.submissionId }),
    );

    yield* check(
      recovery.blocked.length === 0 &&
        pending.submission.state === "unknown" &&
        pending.ownership === undefined &&
        originalMutations === 1,
      "Original mutation did not remain uncertain without ownership",
    );
    const tail = yield* raw.inspectTail(tailRequest);

    const cursor = (yield* select({
      _tag: "RunContinuation",
      runId: runIdForSubmission(older.submissionId),
      throughSequence: tail.tailSequence,
    }))[0]?.record.payload;

    if (cursor?._tag !== "RunContinuation" || cursor.savedContext === undefined)
      return yield* BenchmarkError.make({ message: "Missing original Run context references" });
    const input = (yield* select({ _tag: "RecordId", recordId: cursor.originalInput.recordId }))[0];

    const context = (yield* select({
      _tag: "RecordId",
      recordId: cursor.savedContext.recordId,
    }))[0];

    if (input === undefined || context === undefined || raw.archives === undefined)
      return yield* BenchmarkError.make({
        message: "Missing original input, context or archive port",
      });
    yield* check(
      input.record.recordId === submissionInputRecordId(older.submissionId) &&
        context.record.payload._tag === "RunContextRecorded",
      "Original retained references selected the wrong records",
    );
    const sealed = yield* raw.archives.seal({ threadId, producerEpoch: tail.producerEpoch });

    if (Option.isNone(sealed))
      return yield* BenchmarkError.make({ message: "Original Run range did not seal" });

    const archived = yield* raw.archives.archive({
      threadId,
      firstSequence: sealed.value.firstSequence,
      producerEpoch: tail.producerEpoch,
    });

    yield* raw.archives.verify({ threadId, firstSequence: archived.firstSequence });
    yield* check(
      archived.state === "archived" &&
        archived.firstSequence <= input.sequence &&
        archived.lastSequence >= context.sequence,
      "Original input/context did not enter the archived range",
    );
    phase = "later";
    const later = yield* submit("later input");
    const laterOutcome = yield* runtime.processThreadHead(threadId);

    yield* check(
      Option.isSome(laterOutcome) &&
        laterOutcome.value.submissionId === later.submissionId &&
        laterOutcome.value.outcome === "completed" &&
        laterMutations === 2,
      "Later Run did not finish independently",
    );
    // Disposable checkpoints are deliberately absent; no canonical or archive data is removed.
    yield* sql`DELETE FROM effect_agent_checkpoints WHERE thread_id = ${threadId}`;
    const beforePadding = yield* raw.inspectTail(tailRequest);
    const padding = storeSize ? selectedThreadRecords - beforePadding.tailSequence : ages[0];
    const lowTail = yield* appendPadding(0, padding);

    return { older, cursor, input, context, lowTail, registryKey: runtime.bindingRegistryKey };
  }).pipe(Effect.provide(host(filename)), Effect.scoped);

  const copyClosed = Effect.fnUntraced(function* (source: string, destination: string) {
    yield* check(
      !(yield* fs.exists(`${source}-wal`)) && !(yield* fs.exists(`${source}-shm`)),
      "Aging seed still owns SQLite sidecars",
    );
    yield* fs.copyFile(source, destination);
  });

  const snapshots: Array<{ name: string; path: string; size: number }> = [];

  if (storeSize) {
    yield* check(
      retained.lowTail.tailSequence === selectedThreadRecords,
      "Selected Thread seed is not exactly 100k records",
    );
    let previousSize = 0;

    for (const size of backgroundSizes) {
      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;

        if (previousSize === 0)
          yield* store.materialize(
            ThreadMaterialization.make({
              threadId: backgroundThreadId,
              producerEpoch: ProducerEpoch.make(1),
            }),
          );
        const tail = yield* appendPadding(previousSize, size, backgroundThreadId);

        yield* check(
          tail.tailSequence === size,
          "Background seed does not have the exact record count",
        );
        const selected = yield* store.inspectTail(tailRequest);

        yield* check(
          selected.tailSequence === selectedThreadRecords &&
            selected.tailDigest === retained.lowTail.tailDigest,
          "Background seeding changed the selected Thread",
        );
      }).pipe(Effect.provide(host(filename)), Effect.scoped);
      const path = `${directory}/background-${size}.sqlite`;

      yield* copyClosed(filename, path);
      snapshots.push({ name: `store${size}`, path, size });
      previousSize = size;
    }
  } else {
    const low = `${directory}/${ages[0]}.sqlite`;
    const high = `${directory}/${ages[1]}.sqlite`;

    yield* copyClosed(filename, low);

    const highTail = yield* appendPadding(ages[0], ages[1]).pipe(
      Effect.provide(host(filename)),
      Effect.scoped,
    );

    yield* check(
      highTail.tailSequence - retained.lowTail.tailSequence === ages[1] - ages[0],
      "Aging changed facts outside the unrelated padding",
    );
    yield* copyClosed(filename, high);
    snapshots.push(
      { name: `age${ages[0]}`, path: low, size: ages[0] },
      { name: `age${ages[1]}`, path: high, size: ages[1] },
    );
  }
  phase = "resume";
  let ordinal = 0;

  return {
    ...retained,
    snapshots: () => (ordinal++ % 2 === 0 ? snapshots : [...snapshots].reverse()),
    makeRuntime,
    resetModel: () => {
      resumedPrompt = "";
      resumedModelCalls = 0;
    },
    observation: () => ({
      originalMutations,
      laterMutations,
      changedHandlerCalls,
      resumedPrompt,
      resumedModelCalls,
    }),
    verifyMutation: fs
      .readFileString(mutationFile)
      .pipe(
        Effect.flatMap((contents) =>
          check(contents === "call-a:1\n", "Original mutation evidence changed"),
        ),
      ),
  };
});

export const runAgingCase = Effect.fn("diagnostic.aging.run")(function* (storeSize: boolean) {
  const progress = yield* DiagnosticProgress;
  const fs = yield* FileSystem.FileSystem;
  const seeds = yield* AgingSeeds;
  const setupStarted = yield* Clock.monotonicTimeNanos;

  yield* progress.phase("setup");
  const seed = yield* seeds.get(storeSize);
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "diagnostic-aging-sample-" });
  const counters: Array<{ name: string; value: number }> = [];
  const metrics: Array<{ name: string; value: number }> = [];
  const shapes: Array<string> = [];
  let totalMs = 0;

  for (const snapshot of seed.snapshots()) {
    const { name, size } = snapshot;
    const filename = `${directory}/${name}.sqlite`;

    yield* fs.copyFile(snapshot.path, filename);
    yield* Effect.gen(function* () {
      const raw = yield* ThreadStore;
      const ledger = yield* SubmissionLedger;
      const before = yield* raw.inspectTail(tailRequest);

      if (storeSize) {
        const background = yield* raw.inspectTail(
          ThreadTailRequest.make({ threadId: backgroundThreadId }),
        );

        yield* check(
          before.tailSequence === selectedThreadRecords && background.tailSequence === size,
          "Store-size sample does not match its seed counts",
        );
      }

      const pending = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: seed.older.submissionId }),
      );

      yield* check(
        pending.submission.state === "unknown",
        "Snapshot already resolved the original Run",
      );
      if (raw.checkpoints === undefined)
        return yield* BenchmarkError.make({ message: "Missing SQLite checkpoint port" });
      yield* check(
        Option.isNone(yield* raw.checkpoints.load(LoadCheckpointRequest.make({ threadId }))),
        "Aging recovery retained a disposable checkpoint",
      );
      const reads: Array<{ tag: string; records: number; bytes: number }> = [];
      let maxPageRecords = 0;
      let maxPageBytes = 0;

      const guarded = ThreadStore.of({
        ...raw,
        read: (request) =>
          Stream.unwrap(
            Effect.sync(() => {
              if (!("selection" in request))
                return Stream.die("Aging recovery attempted whole-Thread read");
              if (request.page.limit > 256)
                return Stream.die("Aging selected page limit exceeded 256");
              const observed = { tag: request.selection._tag, records: 0, bytes: 0 };

              reads.push(observed);

              return raw.read(request).pipe(
                Stream.mapArrayEffect((chunk) =>
                  Effect.gen(function* () {
                    const bytes = chunk.reduce(
                      (sum, entry) => sum + canonicalRecordBytes(entry.record),
                      0,
                    );

                    observed.records += chunk.length;
                    observed.bytes += bytes;
                    maxPageRecords = Math.max(maxPageRecords, chunk.length);
                    maxPageBytes = Math.max(maxPageBytes, bytes);
                    yield* check(
                      observed.records <= request.page.limit && observed.bytes <= 32 * 1024 * 1024,
                      "Aging selected read exceeded its bounded payload budget",
                    );
                    yield* check(
                      !chunk.some(({ record }) => record.recordId.startsWith("aging-padding-")),
                      "Aging recovery hydrated unrelated lifetime facts",
                    );

                    return chunk;
                  }).pipe(Effect.orDie),
                ),
              );
            }),
          ),
        export: () => Effect.die("Aging recovery attempted whole-Thread export"),
        observe: () => Stream.die("Aging recovery attempted whole-Thread observation"),
      });

      const runtime = yield* seed
        .makeRuntime(true)
        .pipe(
          Effect.provideService(ThreadStore, guarded),
          Effect.provideService(ThreadReader, ThreadReader.fromStore(guarded)),
        );

      yield* check(runtime.bindingRegistryKey !== seed.registryKey, "Binding did not change");
      seed.resetModel();
      yield* progress.phase("operation");
      const started = yield* Clock.monotonicTimeNanos;

      yield* ledger.recordUnknownResolution(
        UnknownResolutionCommand.make({
          submissionId: seed.older.submissionId,
          toolCallId: ToolCallId.make("call-a"),
          author: "aging-benchmark",
          reason: "The external mutation receipt confirms completion",
          resolution: ResolutionCompletedWithResult.make({
            result: "confirmed older result",
            isFailure: false,
          }),
        }),
      );
      const resumed = yield* runtime.processThreadHead(threadId);
      const elapsedMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

      totalMs += elapsedMs;
      yield* progress.phase("verification");
      yield* check(
        Option.isSome(resumed) &&
          resumed.value.submissionId === seed.older.submissionId &&
          resumed.value.receiptId === seed.older.receiptId &&
          resumed.value.outcome === "completed" &&
          resumed.value.usageSummary?.modelCalls === 2,
        "Aged Run lost its original identity, result or cumulative model accounting",
      );
      const observed = seed.observation();

      yield* seed.verifyMutation;
      yield* check(
        observed.originalMutations === 1 &&
          observed.changedHandlerCalls === 0 &&
          observed.laterMutations === 2 &&
          observed.resumedModelCalls === 1 &&
          observed.resumedPrompt.includes("confirmed older result") &&
          observed.resumedPrompt.includes("older unresolved input") &&
          observed.resumedPrompt.includes("Keep original instructions.") &&
          !observed.resumedPrompt.includes("later input") &&
          !observed.resumedPrompt.includes("CHANGED BINDING"),
        "Aged recovery replayed a handler or lost the original prompt",
      );
      const after = yield* raw.inspectTail(tailRequest);

      const cursor = (yield* select({
        _tag: "RunContinuation",
        runId: runIdForSubmission(seed.older.submissionId),
        throughSequence: after.tailSequence,
      }))[0]?.record.payload;

      yield* check(
        cursor?._tag === "RunContinuation" &&
          JSON.stringify(cursor.originalInput) === JSON.stringify(seed.cursor.originalInput) &&
          JSON.stringify(cursor.savedContext) === JSON.stringify(seed.cursor.savedContext),
        "Aged recovery changed the original input/context references",
      );
      for (const expected of [seed.input, seed.context]) {
        const actual = (yield* select({ _tag: "RecordId", recordId: expected.record.recordId }))[0];

        yield* check(
          actual !== undefined &&
            JSON.stringify(Schema.encodeSync(ReadEnvelope)(actual)) ===
              JSON.stringify(Schema.encodeSync(ReadEnvelope)(expected)),
          "Aged recovery changed archived canonical data",
        );
      }

      const own = yield* select(
        {
          _tag: "RunEvidence",
          runId: runIdForSubmission(seed.older.submissionId),
          submissionId: seed.older.submissionId,
          throughSequence: after.tailSequence,
        },
        64,
      );

      const settled = own.filter(({ record }) => record.payload._tag === "ToolCallSettled");

      yield* check(
        settled.length === 1 &&
          settled[0]?.record.payload._tag === "ToolCallSettled" &&
          settled[0].record.payload.result === "confirmed older result" &&
          settled[0].sequence > before.tailSequence &&
          own.filter(({ record }) => record.payload._tag === "RunStarted").length === 1 &&
          own.some(
            ({ record }) =>
              record.payload._tag === "RunCompleted" && record.payload.output === "done",
          ) &&
          own.some(
            ({ record, sequence }) =>
              record.payload._tag === "SubmissionSettled" && sequence > before.tailSequence,
          ),
        "Missing original Run completion or late factual settlement",
      );
      yield* check(reads.length > 0, "Recovery instrumentation observed no selected requests");
      shapes.push(JSON.stringify(reads.map(({ tag, records }) => ({ tag, records }))));
      for (const [ordinal, read] of reads.entries()) {
        yield* progress.mark({
          name: `${name}.read${ordinal}.${read.tag}.records${read.records}.bytes${read.bytes}`,
          elapsedMs,
        });
      }
      counters.push(
        { name: `${name}.${storeSize ? "backgroundRecords" : "unrelatedFacts"}`, value: size },
        { name: `${name}.tailBefore`, value: before.tailSequence },
        { name: `${name}.selectedRequests`, value: reads.length },
        {
          name: `${name}.returnedRecords`,
          value: reads.reduce((sum, read) => sum + read.records, 0),
        },
        { name: `${name}.returnedBytes`, value: reads.reduce((sum, read) => sum + read.bytes, 0) },
        { name: `${name}.maxPageRecords`, value: maxPageRecords },
        { name: `${name}.maxPageBytes`, value: maxPageBytes },
      );
      metrics.push({ name: `${name}.resume`, value: elapsedMs });
    }).pipe(Effect.provide(host(filename)), Effect.scoped);
  }
  yield* check(
    shapes.every((shape) => shape === shapes[0]),
    "Selected request/returned-record counts grew with unrelated facts",
  );
  const observed = seed.observation();

  counters.push(
    { name: "originalMutations", value: observed.originalMutations },
    { name: "changedHandlerCalls", value: observed.changedHandlerCalls },
    { name: "equalReadShape", value: 1 },
    { name: "wholeThreadReads", value: 0 },
    { name: "wholeThreadExports", value: 0 },
    { name: "wholeThreadObservations", value: 0 },
  );
  yield* progress.mark({ name: `receipt.${seed.older.receiptId}`, elapsedMs: totalMs });
  metrics.push({
    name: "setupAndVerification",
    value: Number((yield* Clock.monotonicTimeNanos) - setupStarted) / 1e6 - totalMs,
  });

  return { totalMs, metrics, counters } satisfies DiagnosticResult;
}, Effect.scoped);
