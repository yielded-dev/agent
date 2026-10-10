import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, describe, it } from "@effect/vitest";
import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
} from "@yielded/agent-storage-sqlite/sqlite-storage-config";
import {
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteWriteContention,
} from "@yielded/agent-storage-sqlite/sqlite-storage-error";
import { SqliteStorageFailpoint } from "@yielded/agent-storage-sqlite/sqlite-storage-failpoint";
import { submissionLedgerLayer } from "@yielded/agent-storage-sqlite/sqlite-submission-ledger";
import {
  exclusiveHostClientLayer,
  exclusiveRunStorageLayer,
  threadStoreLayer,
  layer,
  storageConfigLayer,
} from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import { digestJson, EMPTY_TAIL_DIGEST } from "@yielded/agent/digest";
import { lifecyclePublicationLayer } from "@yielded/agent/lifecycle-publication";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  DefinitionDigests,
  Digest,
  ProducerEpoch,
  RunCompleted,
  RunStartedRecord,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "@yielded/agent/records";
import {
  CurrentRunWriter,
  makeProgressWriter,
  readContinuation,
} from "@yielded/agent/run-continuation";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import { makeRunWriter, RunStorage } from "@yielded/agent/run-storage";
import {
  AdmissionRequest,
  ClaimRequest,
  MarkReadyRequest,
  SubmissionLedger,
} from "@yielded/agent/submission-ledger";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "@yielded/agent/testing/thread-store-conformance";
import {
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
  ThreadTailRequest,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  type AppendResult,
  type ThreadReader,
  streamExport,
  ThreadExportSource,
} from "@yielded/agent/thread-store";
import type { PlatformError, Crypto } from "effect";
import {
  DateTime,
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  FileSystem,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import * as SqlClientService from "effect/sql/SqlClient";
import { TestClock } from "effect/testing";

import { seedCheckpoint, assertCheckpoint } from "../../../test/fixtures/checkpoints.ts";
import { snapshotStore } from "../../../test/fixtures/storage-upgrade.ts";

const threadId = Schema.decodeSync(ThreadMaterialization.fields.threadId)("thread-sqlite-1");
const secondThreadId = Schema.decodeSync(ThreadMaterialization.fields.threadId)("thread-sqlite-2");
const runId = Schema.decodeSync(RunCompleted.fields.runId)("run-sqlite-1");
const submissionId = Schema.decodeSync(SubmissionId)("submission-sqlite-1");

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const epoch = (value: number) => Schema.decodeSync(ProducerEpoch)(value);

const isThreadStoreError = Schema.is(ThreadStoreError);

const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const canonicalRecord = (recordId: string, payload: CanonicalRecordPayload): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/RecordId")),
      recordId,
    ),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/DeploymentId")),
      "deployment-sqlite",
    ),
    payload,
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/BatchId")), batchId),
    producerId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/ProducerId")),
      "producer-sqlite",
    ),
    records,
  });

const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  canonicalRecord(
    recordId,
    UserInputRecorded.make({
      submissionId,
      kind: "user",
      runId,
      input,
    }),
  );

const append = (
  store: ThreadStore["Service"],
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: sequence(0),
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch: ProducerEpoch = epoch(1),
) =>
  store.append(
    FencedAppendRequest.make({
      threadId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );

const withStorage = <A, E>(filename: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(effect, layer({ filename, observationPollInterval: 1 }));

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

const explicitTestStorageLayer = (filename: string) =>
  threadStoreLayer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(SqliteStorageConfig)(
          SqliteStorageConfigValue.make({
            observationPollInterval: 1,
            busyTimeout: 5_000,
            synchronous: "FULL",
            ownershipLeaseDuration: 30_000,
            verifyOnOpen: false,
          }),
        ),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
        NodeCrypto.layer,
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-storage-sqlite-",
      });

      return yield* use(`${directory}/thread.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteThreadStore", () => {
  // #792 retired recovery-checkpoint writes. Layout 21 persists progress in the same
  // append as its facts; run-continuation.test.ts covers process loss at compaction,
  // while these restored rows cover rollback and acknowledgement loss at that append.
  for (const [location, mode] of [
    ["append:before", "failure"],
    ["append:after", "interrupt"],
  ] as const) {
    it.effect(`reopens safely after ${mode} at ${location}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const nativeRunId = runIdForSubmission(submissionId);

          const input = canonicalRecord(
            "crash-reopen-input",
            UserInputRecorded.make({
              submissionId,
              runId: nativeRunId,
              kind: "user",
              input: "Kyoto",
            }),
          );

          const start = canonicalRecord(
            "crash-reopen-start",
            RunStartedRecord.make({
              runId: nativeRunId,
              policyAccountingVersion: 1,
              maxDurationMillis: 30_000,
            }),
          );

          let prepared: CanonicalBatch | undefined;
          let reached = false;

          const failed = yield* Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );
            const writer = yield* makeRunWriter(threadId, epoch(1));
            const progress = yield* makeProgressWriter(threadId, start.deploymentId);

            yield* progress.commit(batch("crash-reopen", [input, start])).pipe(
              Effect.provideService(CurrentRunWriter, {
                ...writer,
                append: (compiled) => {
                  prepared = compiled;

                  return writer.append(compiled);
                },
              }),
            );
          }).pipe(
            Effect.provide(
              layer({
                filename,
                failpoint: (point) => {
                  if (point !== location) return Effect.void;
                  reached = true;

                  return mode === "failure"
                    ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                    : Effect.interrupt;
                },
              }).pipe(Layer.provideMerge(NodeCrypto.layer)),
            ),
            Effect.exit,
          );

          expect(reached).toBe(true);
          expect(Exit.isFailure(failed)).toBe(true);
          const compiled = prepared;

          if (compiled === undefined) return yield* Effect.die("Progress append was not prepared");
          yield* Effect.gen(function* () {
            const store = yield* ThreadStore;
            const before = yield* store.export(ThreadExportRequest.make({ threadId }));
            const committed = location === "append:after";

            expect(before.records).toHaveLength(committed ? 3 : 0);
            expect(
              Option.isSome(yield* readContinuation(threadId, nativeRunId, before.tailSequence)),
            ).toBe(committed);
            expect((yield* append(store, compiled)).replayed).toBe(committed);
            const after = yield* store.export(ThreadExportRequest.make({ threadId }));

            expect(after.records.map(({ record }) => record.payload._tag)).toEqual([
              "UserInputRecorded",
              "RunStarted",
              "RunContinuation",
            ]);
            expect(
              Option.getOrUndefined(
                yield* readContinuation(threadId, nativeRunId, after.tailSequence),
              )?.continuation,
            ).toMatchObject({
              revision: 1,
              recordCount: 2,
              lastFact: { recordId: start.recordId },
            });
            expect((yield* append(store, compiled)).replayed).toBe(true);
          }).pipe(Effect.provide(layer({ filename }).pipe(Layer.provideMerge(NodeCrypto.layer))));
        }),
      ),
    );
  }

  it.effect("rejects canonical records beyond the captured export tail", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }));
        yield* store.append(
          FencedAppendRequest.make({
            threadId,
            producerEpoch: epoch(1),
            expectedTailSequence: sequence(0),
            expectedTailDigest: EMPTY_TAIL_DIGEST,
            batch: batch("bad-export-tail", [inputRecord("bad-export-tail", "Kyoto")]),
          }),
        );
        yield* withSql(
          filename,
          Effect.flatMap(
            SqlClientService.SqlClient,
            (sql) =>
              sql`UPDATE effect_agent_threads SET tail_sequence=0 WHERE thread_id=${threadId}`,
          ),
        );
        expect(
          yield* store.export(ThreadExportRequest.make({ threadId })).pipe(Effect.flip),
        ).toMatchObject({
          _tag: "ThreadStoreError",
          message: expect.stringContaining("beyond the captured thread tail"),
        });
      }).pipe(Effect.provide(layer({ filename }))),
    ),
  );

  for (const historical of [false]) {
    it.effect(
      `reopens ${historical ? "historical" : "metadata-free"} checkpoints without rewriting storage`,
      () =>
        withTemporaryDatabase((filename) =>
          Effect.gen(function* () {
            yield* seedCheckpoint(historical).pipe(
              Effect.provide(
                Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
                  Layer.provideMerge(
                    Layer.mergeAll(
                      storageConfigLayer({ filename }),
                      SqliteStorageFailpoint.layer,
                      NodeCrypto.layer,
                    ),
                  ),
                ),
              ),
            );
            const before = yield* snapshotStore;
            const sql = yield* SqlClientService.SqlClient;
            const version = yield* sql`PRAGMA user_version`;

            for (const verifyOnOpen of [true]) {
              yield* Effect.gen(function* () {
                yield* ThreadStore;
                expect(yield* snapshotStore).toEqual(before);
                yield* assertCheckpoint(historical);
                expect(yield* snapshotStore).toEqual(before);
                expect(yield* sql`PRAGMA user_version`).toEqual(version);
              }).pipe(Effect.provide(layer({ filename, verifyOnOpen })));
            }
          }).pipe(Effect.provide(SqliteClient.layer({ filename }))),
        ),
    );
  }

  for (const corruption of ["thread"]) {
    it.effect(`rejects checkpoint ${corruption} metadata that disagrees with its row`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const sql = yield* SqlClientService.SqlClient;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          yield* append(
            store,
            batch("checkpoint-metadata", [inputRecord("checkpoint-metadata-input", "Kyoto")]),
          );

          const checkpoint = ThreadCheckpoint.make({
            schemaVersion: 1,
            threadId,
            throughSequence: sequence(0),
            tailDigest: EMPTY_TAIL_DIGEST,
            state: {},
            createdAt: at(2),
          });

          yield* store.checkpoints!.save(SaveCheckpointRequest.make({ checkpoint }));

          const corrupted = ThreadCheckpoint.make({
            ...checkpoint,
            ...(corruption === "thread" ? { threadId: secondThreadId } : {}),
          });

          const corruptedJson = JSON.stringify(
            yield* Schema.encodeEffect(ThreadCheckpoint)(corrupted),
          );

          const rowDigest = EMPTY_TAIL_DIGEST;

          yield* sql`
            UPDATE effect_agent_checkpoints
            SET checkpoint_json = ${corruptedJson}, tail_digest = ${rowDigest}
            WHERE thread_id = ${threadId} AND through_sequence = 0
          `;

          const loaded = yield* store
            .checkpoints!.load(
              LoadCheckpointRequest.make({ threadId, atOrBeforeSequence: sequence(0) }),
            )
            .pipe(Effect.exit);

          expect(Exit.isFailure(loaded)).toBe(true);
          if (Exit.isFailure(loaded)) {
            const error = Cause.squash(loaded.cause);

            expect(error).toBeInstanceOf(ThreadStoreError);
            if (isThreadStoreError(error)) expect(error.operation).toBe("load checkpoint");
          }
        }).pipe(Effect.provide(explicitTestStorageLayer(filename))),
      ),
    );
  }

  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of [
      ...threadStoreConformanceCases,
      ...threadCheckpointConformanceCases,
    ]) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((filename) => withStorage(filename, conformanceCase.run)),
      );
    }
  });

  it.effect("rejects an unsupported storage version before creating canonical tables", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`PRAGMA user_version = 17`;
          }),
        );

        const opened = yield* withStorage(filename, ThreadStore).pipe(Effect.exit);

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          expect(Cause.squash(opened.cause)).toBeInstanceOf(SqliteStorageCompatibilityError);
        }

        const tables = yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT name
              FROM sqlite_master
              WHERE type = 'table'
                AND name LIKE 'effect_agent_%'
            `;
          }),
        );

        expect(tables).toEqual([]);
      }),
    ),
  );

  it.effect("fails clearly on corrupt current-version rows without mutating the log", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );
            yield* append(store, batch("corrupt-1", [inputRecord("corrupt-record-1", "Osaka")]));
          }),
        );
        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`
              UPDATE effect_agent_canonical_records
              SET record_json = '{"schemaVersion":2}'
              WHERE thread_id = ${threadId}
                AND sequence = 1
            `;
          }),
        );

        // The opt-in integrity scan refuses to open the corrupt database.
        const verified = yield* ThreadStore.pipe(
          Effect.provide(layer({ filename, observationPollInterval: 1, verifyOnOpen: true })),
          Effect.exit,
        );

        expect(Exit.isFailure(verified)).toBe(true);
        if (Exit.isFailure(verified)) {
          expect(Cause.squash(verified.cause)).toBeInstanceOf(SqliteStorageCorruptionError);
        }

        // The default lazy open succeeds; the corrupt row fails clearly at first decode.
        const lazyRead = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* store
              .read(ThreadRead.make({ threadId, limit: 1_024 }))
              .pipe(Stream.runCollect);
          }),
        ).pipe(Effect.exit);

        expect(Exit.isFailure(lazyRead)).toBe(true);
        if (Exit.isFailure(lazyRead)) {
          const error = Cause.squash(lazyRead.cause);

          expect(error).toBeInstanceOf(ThreadStoreError);
          if (isThreadStoreError(error)) {
            expect(error.operation).toBe("decode canonical record");
          }
        }

        const rows = yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT sequence, record_json
              FROM effect_agent_canonical_records
              WHERE thread_id = ${threadId}
            `;
          }),
        );

        expect(rows).toEqual([{ sequence: 1, record_json: '{"schemaVersion":2}' }]);
      }),
    ),
  );

  it.effect("exports one transactionally consistent snapshot during a concurrent append", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );

            return yield* append(
              store,
              batch("snapshot-1", [inputRecord("snapshot-record-1", "before")]),
            );
          }),
        );

        const exportStarted = yield* Deferred.make<void>();
        const releaseExport = yield* Deferred.make<void>();

        const exportFiber = yield* Effect.provide(
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* store.export(ThreadExportRequest.make({ threadId }));
          }),
          layer({
            filename,
            observationPollInterval: 1,
            failpoint: (location) =>
              location === "export:after-thread-read"
                ? Deferred.succeed(exportStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseExport)),
                    Effect.asVoid,
                  )
                : Effect.void,
          }),
        ).pipe(Effect.forkChild);

        yield* Deferred.await(exportStarted);
        yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* append(
              store,
              batch("snapshot-2", [inputRecord("snapshot-record-2", "after")]),
              first,
            );
          }),
        );
        yield* Deferred.succeed(releaseExport, undefined);

        const snapshot = yield* Fiber.join(exportFiber);

        expect(snapshot.tailSequence).toBe(first.lastSequence);
        expect(snapshot.tailDigest).toBe(first.tailDigest);
        expect(snapshot.records).toHaveLength(1);

        const current = yield* withStorage(
          filename,
          Effect.gen(function* () {
            return yield* streamExport({ threadId }).pipe(
              Stream.provide(ThreadExportSource.layer()),
              Stream.flatMap((page) => Stream.fromIterable(page.records)),
              Stream.take(3),
              Stream.runCollect,
            );
          }),
        );

        expect(current).toHaveLength(2);
      }),
    ),
  );

  it.effect("wakes a live observer whose poll found nothing once a new batch commits", () =>
    withTemporaryDatabase((filename) =>
      withStorage(
        filename,
        Effect.gen(function* () {
          const store = yield* ThreadStore;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          const observerFiber = yield* store
            .observe(ThreadObservation.make({ threadId }))
            .pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);

          // Let the observer reach its first empty poll before anything is committed.
          yield* Effect.yieldNow;

          yield* append(store, batch("live-1", [inputRecord("live-record-1", "first")]));

          // Drive the TestClock until the sleeping poll wakes and sees the new batch. The
          // adjust loop yields each step, so the observer always progresses deterministically.
          const observed = yield* Fiber.join(observerFiber).pipe(
            Effect.raceFirst(
              TestClock.adjust(1).pipe(Effect.andThen(Effect.yieldNow), Effect.forever),
            ),
          );

          expect(observed.map((record) => record.record.recordId)).toEqual(["live-record-1"]);
        }),
      ),
    ),
  );

  // An independently constructed store sharing the exclusive SQL client bypasses the
  // host's administrative cache refresh. The database must still own tail and fence CAS.
  it.effect.each(["tail", "fence"] as const)(
    "honors an independent same-client writer (%s)",
    (change) =>
      withTemporaryDatabase((filename) => {
        const dependencies = Layer.mergeAll(
          SqliteClient.layer({ filename, disableWAL: true }),
          storageConfigLayer({ filename }),
          SqliteStorageFailpoint.layer,
          NodeCrypto.layer,
        );

        const services = exclusiveRunStorageLayer.pipe(
          Layer.provideMerge(exclusiveHostClientLayer.pipe(Layer.provideMerge(dependencies))),
        );

        return Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const storage = yield* RunStorage;
          const independent = Context.get(yield* Layer.build(threadStoreLayer), ThreadStore);
          const producerId = id(ClaimRequest.fields.producerId, "independent-writer-proof");
          const digest = id(Digest, "a".repeat(64));

          const admitted = yield* ledger.admit(
            AdmissionRequest.make({
              threadId,
              principal: id(AdmissionRequest.fields.principal, "proof-principal"),
              idempotencyKey: id(AdmissionRequest.fields.idempotencyKey, change),
              agentId: id(AdmissionRequest.fields.agentId, "proof-agent"),
              agentDigests: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
              deploymentId: id(AdmissionRequest.fields.deploymentId, "proof-deployment"),
              inputPayload: "proof",
              inputDigest: yield* digestJson("proof"),
            }),
          );

          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          const claimed = yield* storage.claim(ClaimRequest.make({ threadId, producerId }));

          if (Option.isNone(claimed)) return yield* Effect.die("Run not claimed");
          const session = claimed.value;

          const fact = (name: string) =>
            batch(name, [
              canonicalRecord(
                name,
                UserInputRecorded.make({
                  submissionId: admitted.submissionId,
                  runId: runIdForSubmission(admitted.submissionId),
                  kind: "user",
                  input: name,
                }),
              ),
            ]);

          const tailRequest = ThreadTailRequest.make({ threadId });
          const before = yield* independent.inspectTail(tailRequest);

          if (change === "tail") {
            const external = yield* independent.append(
              FencedAppendRequest.make({
                threadId,
                producerEpoch: session.producerEpoch,
                expectedTailSequence: before.tailSequence,
                expectedTailDigest: before.tailDigest,
                batch: fact("independent-fact"),
              }),
            );

            const stale = yield* session.append(fact("session-fact")).pipe(Effect.exit);

            expect(Exit.isFailure(stale)).toBe(true);
            if (Exit.isSuccess(stale)) return yield* Effect.die("Stale append committed");
            expect(Cause.squash(stale.cause)).toMatchObject({
              _tag: "AppendConflict",
              reason: "tail",
            });
            expect(yield* session.tail).toEqual({
              sequence: external.lastSequence,
              digest: external.tailDigest,
            });

            const appended = yield* session.append(fact("session-fact"));

            expect(appended.firstSequence).toBe(external.lastSequence + 1);
            const sql = yield* SqlClientService.SqlClient;

            expect(
              yield* sql`SELECT record_id FROM effect_agent_canonical_records
            WHERE thread_id=${threadId} ORDER BY sequence`,
            ).toEqual([{ record_id: "independent-fact" }, { record_id: "session-fact" }]);
          } else {
            yield* independent.materialize(
              ThreadMaterialization.make({
                threadId,
                producerEpoch: epoch(session.producerEpoch + 1),
              }),
            );
            const appended = yield* session.append(fact("obsolete-fact")).pipe(Effect.exit);
            const after = yield* independent.inspectTail(tailRequest);

            expect(Exit.isFailure(appended)).toBe(true);
            if (Exit.isSuccess(appended)) return;
            expect(Cause.squash(appended.cause)).toMatchObject({ _tag: "FenceRejected" });
            expect(after.producerEpoch).toBe(session.producerEpoch + 1);
            expect(after.tailSequence).toBe(before.tailSequence);
            expect(after.tailDigest).toBe(before.tailDigest);
          }
        }).pipe(Effect.scoped, Effect.provide(services));
      }),
  );

  it.effect("classifies cross-connection write contention as retryable typed contention", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );

            return yield* append(store, batch("busy-1", [inputRecord("busy-record-1", "before")]));
          }),
        );

        const contendedBatch = batch("busy-2", [inputRecord("busy-record-2", "after")]);

        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            // Hold the write lock on a separate connection, as a transiently coexisting
            // producer would.
            yield* sql`BEGIN IMMEDIATE`;

            const contended = yield* Effect.provide(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                return yield* append(store, contendedBatch, first);
              }),
              layer({ filename, observationPollInterval: 1, busyTimeout: 0 }),
            ).pipe(Effect.exit);

            yield* sql`ROLLBACK`;

            expect(Exit.isFailure(contended)).toBe(true);
            if (Exit.isFailure(contended)) {
              const error = Cause.squash(contended.cause);

              expect(error).toBeInstanceOf(ThreadStoreError);
              if (isThreadStoreError(error)) {
                expect(error.cause).toBeInstanceOf(SqliteWriteContention);
              }
            }
          }),
        );

        // Once the competing writer releases the lock, the identical append commits.
        const recovered = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* append(store, contendedBatch, first);
          }),
        );

        expect(recovered.replayed).toBe(false);
        expect(recovered.firstSequence).toBe(first.lastSequence + 1);
      }),
    ),
  );

  it.effect(
    "rolls back partial appends and replays a committed append after acknowledgement loss",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);

          // Requested replacement for retired checkpoint crash matrices: native facts,
          // progress and lifecycle intent must reopen at one atomic prefix.
          const nativeRunId = runIdForSubmission(submissionId);

          const acceptedInput = canonicalRecord(
            "reopen-accepted-input",
            UserInputRecorded.make({
              submissionId,
              runId: nativeRunId,
              kind: "user",
              input: "Sapporo",
            }),
          );

          const start = canonicalRecord(
            "reopen-run-start",
            RunStartedRecord.make({
              runId: nativeRunId,
              policyAccountingVersion: 1,
              maxDurationMillis: 30_000,
            }),
          );

          let firstBatch: CanonicalBatch | undefined;

          const withFailpoints = <A, E>(
            effect: Effect.Effect<A, E, ThreadStore | ThreadReader | Crypto.Crypto>,
          ) =>
            Effect.provide(
              effect,
              layer({
                filename,
                observationPollInterval: 1,
                failpoint: (location) =>
                  Ref.get(active).pipe(
                    Effect.flatMap((selected) =>
                      selected === location
                        ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                        : Effect.void,
                    ),
                  ),
              }).pipe(
                Layer.provideMerge(
                  lifecyclePublicationLayer.pipe(Layer.provideMerge(NodeCrypto.layer)),
                ),
              ),
            );

          const select = (location: SqliteStorageFailpointLocation | undefined) =>
            Ref.set(active, location);

          const seeded = yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              yield* store.materialize(
                ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
              );

              return yield* append(store, batch("reopen-accepted-input", [acceptedInput]));
            }),
          );

          yield* Effect.forEach(
            [
              "append:after-batch-insert",
              "append:after-record-insert",
              "append:after-tail-update",
            ] as const,
            (location) =>
              Effect.gen(function* () {
                yield* select(location);

                const exit = yield* withFailpoints(
                  Effect.gen(function* () {
                    const store = yield* ThreadStore;

                    if (firstBatch !== undefined) {
                      yield* append(store, firstBatch, seeded);

                      return;
                    }
                    const writer = yield* makeRunWriter(threadId, epoch(1));
                    const progress = yield* makeProgressWriter(threadId, start.deploymentId);

                    yield* progress.commit(batch("failpoint-append", [start])).pipe(
                      Effect.provideService(CurrentRunWriter, {
                        ...writer,
                        append: (prepared) =>
                          Effect.sync(() => {
                            firstBatch = prepared;
                          }).pipe(Effect.andThen(writer.append(prepared))),
                      }),
                    );
                  }),
                ).pipe(Effect.exit);

                expect(Exit.isFailure(exit)).toBe(true);
                if (Exit.isFailure(exit)) {
                  const error = Cause.squash(exit.cause);

                  expect(error).toBeInstanceOf(ThreadStoreError);
                  if (isThreadStoreError(error)) {
                    expect(error.operation).toBe("append canonical batch");
                    expect(error.message).toContain(location);
                  }
                }
                yield* select(undefined);

                const exported = yield* withFailpoints(
                  Effect.gen(function* () {
                    const store = yield* ThreadStore;

                    expect(
                      Option.isNone(
                        yield* readContinuation(threadId, nativeRunId, seeded.lastSequence),
                      ),
                    ).toBe(true);
                    const pending = yield* store.lifecyclePublications!.pending(1, 1);

                    expect(pending.flat().map(({ fact }) => fact._tag)).toEqual([
                      "UserInputRecorded",
                    ]);

                    return yield* store.export(ThreadExportRequest.make({ threadId }));
                  }),
                );

                expect(exported.records.map(({ record }) => record.recordId)).toEqual([
                  acceptedInput.recordId,
                ]);
                expect(exported.tailSequence).toBe(seeded.lastSequence);
                expect(exported.tailDigest).toBe(seeded.tailDigest);
              }),
          );

          yield* select("append:after");
          expect(
            Exit.isFailure(
              yield* withFailpoints(
                Effect.gen(function* () {
                  const store = yield* ThreadStore;

                  if (firstBatch === undefined)
                    return yield* Effect.die("Start progress was not prepared");
                  yield* append(store, firstBatch, seeded);
                }),
              ).pipe(Effect.exit),
            ),
          ).toBe(true);
          yield* select(undefined);

          const recoveredAppend = yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              if (firstBatch === undefined)
                return yield* Effect.die("Start progress was not prepared");
              const replayed = yield* append(store, firstBatch, seeded);

              const continuation = yield* readContinuation(
                threadId,
                nativeRunId,
                replayed.lastSequence,
              );

              expect(Option.getOrUndefined(continuation)?.continuation).toMatchObject({
                revision: 1,
                recordCount: 2,
                lastFact: { recordId: start.recordId },
              });
              expect(
                (yield* store.lifecyclePublications!.pending(1, 1))
                  .flat()
                  .map(({ fact }) => fact._tag),
              ).toEqual(["UserInputRecorded", "RunStarted"]);

              return replayed;
            }),
          );

          expect(recoveredAppend.replayed).toBe(true);
        }),
      ),
  );
});
import { SubmissionId } from "@yielded/agent/identifiers";
