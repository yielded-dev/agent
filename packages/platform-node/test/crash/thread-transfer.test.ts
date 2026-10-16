import { DatabaseSync } from "node:sqlite";

import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { expect, layer } from "@effect/vitest";
import { NodeDurableHost } from "@yielded/agent-platform-node/node-durable-host";
import * as SqliteThreadStore from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import * as Agent from "@yielded/agent/agent";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { Receipt } from "@yielded/agent/receipt";
import { CanonicalRecordEnvelope, CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import { ThreadArchive, ThreadImport } from "@yielded/agent/thread-import";
import {
  ThreadExport,
  ThreadExportRequest,
  ThreadTailRequest,
  ThreadStore,
  streamExport,
} from "@yielded/agent/thread-store";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  CHECKPOINT_HANDOFF,
  CHECKPOINT_INSTRUCTIONS,
  CRASH_QUESTION,
  checkpointContextLayer,
  checkpointDefinition,
  checkpointParts,
  decodeThreadId,
  crashSubmitOptions,
  makeCheckpointToolLayer,
  supplierCounts,
} from "./fixtures.ts";
import {
  assertConvergence,
  expectKilled,
  lookupByKey,
  readLog,
  runWorkerToExit,
  withCrashSite,
  withHost,
} from "./harness.ts";

const openDatabase = (filename: string) =>
  Effect.acquireRelease(
    Effect.sync(() => new DatabaseSync(filename)),
    (db) => Effect.sync(() => db.close()),
  );

// This process-loss fixture is small; bound its comparison buffers independently of the
// transfer API, which consumes an arbitrary number of pages without collecting them.
const exportFixture = Effect.fnUntraced(function* (filename: string, request: ThreadExportRequest) {
  let bytes = 0;

  const pages = yield* streamExport(request).pipe(
    Stream.provide(SqliteThreadStore.exportSourceLayer({ filename })),
    Stream.take(65),
    Stream.mapEffect((page) =>
      Effect.gen(function* () {
        const json = yield* Schema.encodeEffect(Schema.fromJsonString(ThreadExport))(page);

        bytes += new TextEncoder().encode(json).byteLength;
        expect(bytes).toBeLessThanOrEqual(32 * 1024 * 1024);

        return {
          page,
          archive: yield* Schema.decodeEffect(Schema.fromJsonString(ThreadArchive))(json),
        };
      }),
    ),
    Stream.runCollect,
  );

  expect(pages.length).toBeLessThanOrEqual(64);

  return pages;
});

const comparablePage = Effect.fnUntraced(function* (page: ThreadExport) {
  const { cursor: _, ...facts } = yield* Schema.encodeEffect(ThreadExport)(page);

  return facts;
});

// Lose a real process after compaction. Active work stays on its original owner;
// only completed history and durable receipt identities move to a fresh store.
layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "Thread transfer after process loss",
  (it) => {
    it.effect(
      "refuses active transfer, recovers the source, and moves its idle history and receipts",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            const threadId = decodeThreadId("checkpoint-crash");
            const request = ThreadExportRequest.make({ threadId });
            const target = `${site.db}.imported`;

            const child = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-checkpoint",
              thread: "checkpoint-crash",
              key: "checkpoint-crash",
              supplierDir: site.supplier,
              killAt: "compaction:after-canonical-append",
            });

            expectKilled(child);
            yield* Effect.scoped(
              Effect.gen(function* () {
                const db = yield* openDatabase(site.db);

                expect(db.prepare("SELECT record_format FROM effect_agent_schema").get()).toEqual({
                  record_format: CURRENT_RECORD_FORMAT,
                });
                expect(db.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
              }),
            );
            const sourceBytes = yield* fs.readFile(site.db);
            const refused = yield* Effect.result(exportFixture(site.db, request));

            expect(yield* fs.readFile(site.db)).toEqual(sourceBytes);
            expect(yield* fs.exists(target)).toBe(false);
            expect(Result.isFailure(refused)).toBe(true);
            if (Result.isFailure(refused))
              expect(refused.failure).toMatchObject({
                _tag: "ThreadExportRejected",
                reason: "non-idle",
              });

            const resumed = yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const submission = yield* lookupByKey("checkpoint-crash", "checkpoint-crash");
                const store = yield* ThreadStore;

                const execution = Option.getOrThrow(
                  yield* store.readExecution({
                    threadId,
                    runId: runIdForSubmission(submission.submissionId),
                  }),
                );

                const sourceRecords = yield* readLog("checkpoint-crash");

                expect(
                  execution.operational.filter(({ payload }) => payload._tag === "RunStarted"),
                ).toHaveLength(1);
                expect(execution.progress.savedContext).toBeDefined();
                if (store.verification === undefined)
                  return yield* Effect.die("Native source verification is unavailable");
                expect((yield* store.verification.verify({ threadId })).ok).toBe(true);

                expect(submission.state).toBe("input-applied");
                expect(
                  host.startupRecovery.find(
                    (entry) => entry.submissionId === submission.submissionId,
                  )?.decision._tag,
                ).toBe("ResumeFromTurnBoundary");
                const requests: Array<Prompt.Prompt> = [];

                const model = Model.make(
                  "scripted",
                  "crash-harness",
                  Layer.effect(
                    LanguageModel.LanguageModel,
                    LanguageModel.make({
                      generateText: () => Effect.succeed([]),
                      streamText: (request) => {
                        requests.push(request.prompt);

                        return Stream.fromIterable(checkpointParts(requests.length + 2));
                      },
                    }),
                  ),
                );

                const settlements = yield* runtime
                  .processThread(Agent.withModel(checkpointDefinition, model), threadId)
                  .pipe(Effect.provide(makeCheckpointToolLayer(site.supplier)));

                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(settlements[0]?.usageSummary?.modelCalls).toBe(4);
                expect(requests).toHaveLength(2);
                for (const request of requests) {
                  const prompt = JSON.stringify(request);

                  expect(prompt).toContain(CRASH_QUESTION);
                  expect(prompt).toContain(CHECKPOINT_INSTRUCTIONS);
                  expect(prompt).toContain(CHECKPOINT_HANDOFF);
                  expect(prompt).not.toContain('"id":"checkpoint-call-1"');
                  expect(prompt).not.toContain('"id":"checkpoint-call-2"');
                }
                const records = yield* readLog("checkpoint-crash");
                const encode = Schema.encodeEffect(Schema.Array(CanonicalRecordEnvelope));

                expect(yield* encode(records.slice(0, sourceRecords.length))).toEqual(
                  yield* encode(sourceRecords),
                );
                expect(
                  records.filter(
                    ({ record }) =>
                      record.payload._tag === "UserInputRecorded" &&
                      record.payload.kind === "user" &&
                      record.payload.runId === execution.progress.runId,
                  ),
                ).toHaveLength(1);
                expect(
                  records.filter(({ record }) =>
                    [
                      "RunStarted",
                      "RunContextRecorded",
                      "RunCompleted",
                      "RunContinuation",
                    ].includes(record.payload._tag),
                  ),
                ).toEqual([]);
                expect(
                  Option.isNone(
                    yield* store.readExecution({ threadId, runId: execution.progress.runId }),
                  ),
                ).toBe(true);
                expect(supplierCounts(site.supplier)).toEqual({
                  "checkpoint-read:checkpoint-read-1": 1,
                  "checkpoint-read:checkpoint-read-2": 1,
                  "checkpoint-read:checkpoint-read-3": 1,
                });
                yield* assertConvergence("checkpoint-crash", [submission.submissionId]);

                const receipt = Receipt.make({
                  threadId,
                  submissionId: submission.submissionId,
                  receiptId: submission.receiptId,
                  queueSequence: submission.queueSequence,
                });

                return { receipt, status: yield* runtime.submissionStatus(receipt) };
              }),
              { runContext: checkpointContextLayer },
            );

            const settledSourceBytes = yield* fs.readFile(site.db);
            const pages = yield* exportFixture(site.db, request);
            const exported = pages[0]?.page;

            if (exported === undefined) return yield* Effect.die("Missing idle snapshot");
            expect(yield* fs.readFile(site.db)).toEqual(settledSourceBytes);
            expect(pages.length).toBeGreaterThan(1);

            // Truncation must leave no partially published Thread in the destination.
            yield* Effect.gen(function* () {
              const importer = yield* ThreadImport;
              const store = yield* ThreadStore;

              const rejected = yield* Effect.result(
                importer.import(
                  Stream.fromIterable(pages.slice(0, -1).map(({ archive }) => archive)),
                ),
              );

              expect(Result.isFailure(rejected)).toBe(true);
              if (Result.isFailure(rejected))
                expect(rejected.failure).toMatchObject({ _tag: "ThreadImportRejected" });

              const tail = yield* Effect.result(
                store.inspectTail(ThreadTailRequest.make({ threadId })),
              );

              expect(Result.isFailure(tail)).toBe(true);
              if (Result.isFailure(tail))
                expect(tail.failure).toMatchObject({ _tag: "ThreadNotMaterialized" });
            }).pipe(Effect.provide(SqliteThreadStore.layer({ filename: target })));

            const imported = yield* Effect.gen(function* () {
              const importer = yield* ThreadImport;

              return yield* importer.import(
                Stream.fromIterable(pages.map(({ archive }) => archive)),
              );
            }).pipe(Effect.provide(SqliteThreadStore.layer({ filename: target })));

            expect(imported.tailSequence).toBe(exported.tailSequence);
            const restored = yield* exportFixture(target, request);

            expect(yield* Effect.forEach(restored, ({ page }) => comparablePage(page))).toEqual(
              yield* Effect.forEach(pages, ({ page }) => comparablePage(page)),
            );

            yield* withHost(
              target,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;

                const model = Model.make(
                  "scripted",
                  "idle-transfer",
                  Layer.effect(
                    LanguageModel.LanguageModel,
                    LanguageModel.make({
                      generateText: () => Effect.die("Idle import dispatched a model"),
                      streamText: () => Stream.die("Idle import dispatched a model"),
                    }),
                  ),
                );

                const definition = Agent.withModel(checkpointDefinition, model);

                const replayed = yield* runtime.submit(
                  definition,
                  { question: CRASH_QUESTION },
                  crashSubmitOptions("checkpoint-crash", "checkpoint-crash"),
                );

                expect(replayed).toEqual(resumed.receipt);
                expect(yield* runtime.submissionStatus(resumed.receipt)).toEqual(resumed.status);
                expect(
                  yield* runtime
                    .processThread(definition, threadId)
                    .pipe(Effect.provide(makeCheckpointToolLayer(site.supplier))),
                ).toEqual([]);
                yield* assertConvergence("checkpoint-crash", [resumed.receipt.submissionId]);
              }),
              { runContext: checkpointContextLayer },
            );
            expect(supplierCounts(site.supplier)).toEqual({
              "checkpoint-read:checkpoint-read-1": 1,
              "checkpoint-read:checkpoint-read-2": 1,
              "checkpoint-read:checkpoint-read-3": 1,
            });
          }),
        ),
      30_000,
    );
  },
);
