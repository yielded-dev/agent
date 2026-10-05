import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { NodeDurableHost } from "@yielded/agent-platform-node/node-durable-host";
import * as SqliteThreadStore from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import * as Agent from "@yielded/agent/agent";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { CanonicalRecordEnvelope } from "@yielded/agent/records";
import { ThreadArchive, ThreadImport, ThreadImportRequest } from "@yielded/agent/thread-import";
import { ThreadExport, ThreadExportRequest } from "@yielded/agent/thread-store";
import { Effect, FileSystem, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, type Prompt } from "effect/ai";

import {
  CHECKPOINT_HANDOFF,
  CHECKPOINT_INSTRUCTIONS,
  CRASH_QUESTION,
  checkpointContextLayer,
  checkpointDefinition,
  checkpointParts,
  decodeThreadId,
  makeCheckpointToolLayer,
  supplierCounts,
} from "./fixtures.ts";
import { assertConvergence, lookupByKey, readLog, withCrashSite, withHost } from "./harness.ts";

const openDatabase = (filename: string) =>
  Effect.acquireRelease(
    Effect.sync(() => new DatabaseSync(filename)),
    (db) => Effect.sync(() => db.close()),
  );

const derivativeCounts = (db: DatabaseSync) =>
  Schema.decodeUnknownSync(
    Schema.Struct({ ownership: Schema.Int, attempts: Schema.Int, recovery: Schema.Int }),
  )(
    db
      .prepare(
        `SELECT
          (SELECT COUNT(*) FROM effect_agent_submission_ownership) AS ownership,
          (SELECT COUNT(*) FROM effect_agent_attempts) AS attempts,
          (SELECT COUNT(*) FROM effect_agent_recovery_checkpoints) AS recovery`,
      )
      .get(),
  );

// Requested transfer proof: the fixture is a real format-16 worker killed after persisting
// a recovery checkpoint. Same-file recovery tests cannot prove that import discards authority
// and caches while preserving the log, admission facts and the unfinished Run.
layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "Thread transfer after process loss",
  (it) => {
    it.effect(
      "exports format 16, imports a fresh store, and resumes the original Run",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            const threadId = decodeThreadId("checkpoint-crash");
            const request = ThreadExportRequest.make({ threadId });
            const target = `${site.db}.imported`;

            const fixture = yield* fs.readFileString(
              fileURLToPath(new URL("./fixtures/format16-checkpoint.sql", import.meta.url)),
            );

            yield* Effect.scoped(
              Effect.gen(function* () {
                const db = yield* openDatabase(site.db);

                db.exec(fixture);
                expect(db.prepare("PRAGMA user_version").get()).toEqual({ user_version: 16 });
                expect(db.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
                expect(derivativeCounts(db)).toEqual({ ownership: 1, attempts: 1, recovery: 1 });
              }),
            );
            const sourceBytes = yield* fs.readFile(site.db);
            const exported = yield* SqliteThreadStore.exportThread({ filename: site.db }, request);

            const archiveJson = yield* Schema.encodeEffect(Schema.fromJsonString(ThreadExport))(
              exported,
            );

            const archive = yield* Schema.decodeEffect(Schema.fromJsonString(ThreadArchive))(
              archiveJson,
            );

            expect(exported.tailSequence).toBe(8);

            const imported = yield* Effect.gen(function* () {
              const importer = yield* ThreadImport;

              return yield* importer.import(ThreadImportRequest.make({ archive }));
            }).pipe(Effect.provide(SqliteThreadStore.layer({ filename: target })));

            expect(imported.tailSequence).toBe(exported.tailSequence);
            expect(imported.tailDigest).toBe(exported.tailDigest);
            expect(yield* fs.readFile(site.db)).toEqual(sourceBytes);
            const restored = yield* SqliteThreadStore.exportThread({ filename: target }, request);

            expect(yield* Schema.encodeEffect(ThreadExport)(restored)).toEqual(
              yield* Schema.encodeEffect(ThreadExport)(exported),
            );
            yield* Effect.scoped(
              Effect.gen(function* () {
                const db = yield* openDatabase(target);

                expect(derivativeCounts(db)).toEqual({ ownership: 0, attempts: 0, recovery: 0 });
              }),
            );

            yield* withHost(
              target,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const submission = yield* lookupByKey("checkpoint-crash", "checkpoint-crash");

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

                expect(yield* encode(records.slice(0, exported.records.length))).toEqual(
                  yield* encode(exported.records),
                );
                expect(
                  records.filter(({ record }) => record.payload._tag === "RunStarted"),
                ).toHaveLength(1);
                expect(supplierCounts(site.supplier)).toEqual({
                  "checkpoint-read:checkpoint-read-3": 1,
                });
                yield* assertConvergence("checkpoint-crash", [submission.submissionId]);
              }),
              { runContext: checkpointContextLayer },
            );
          }),
        ),
      30_000,
    );
  },
);
