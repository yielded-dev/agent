import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import {
  SqliteStorageCompatibilityError,
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
} from "@yielded/agent-storage-sqlite/sqlite-storage-error";
import { SqliteStorageFailpoint } from "@yielded/agent-storage-sqlite/sqlite-storage-failpoint";
import { CurrentSqliteStorageVersion } from "@yielded/agent-storage-sqlite/sqlite-storage-version";
import {
  ledgerLayer,
  submissionLedgerLayer,
} from "@yielded/agent-storage-sqlite/sqlite-submission-ledger";
import {
  exclusiveHostClientLayer,
  exclusiveRunStorageLayer,
  threadStoreLayer,
  storageConfigLayer,
  storageFailpointLayer,
  type SqliteStorageOptions,
} from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import { digestJson } from "@yielded/agent/digest";
import {
  CanonicalBatch,
  AbortRequested,
  CanonicalRecord,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  RunStartedRecord,
  SubmissionSettled,
  SubmissionSettledRecord,
  UserInputRecorded,
  type PersistedJson,
  type SettlementOutcome,
} from "@yielded/agent/records";
import {
  CurrentRunSettlement,
  CurrentRunWriter,
  makeProgressWriter,
} from "@yielded/agent/run-continuation";
import { runIdForSubmission } from "@yielded/agent/run-journal";
import type { StorageOwner } from "@yielded/agent/run-storage";
import { makeRunWriter, RunStorage, layer as runStorageLayer } from "@yielded/agent/run-storage";
import { SettlementPublication, SettlementPublisher } from "@yielded/agent/settlement-publisher";
import {
  AdmissionPolicyError,
  SubmissionAdmissionFence,
  AbortCommand,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  Principal,
  RecoverySnapshotRequest,
  ReleaseChildBudgetRequest,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  RevertJoiningRequest,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionInputBatchId,
  submissionAbortRecordId,
  submissionAbortBatchId,
  submissionSettlementId,
  submissionSettlementBatchId,
  submissionSettlementRecordId,
  type AdmissionResult,
  type ParentLinkage,
} from "@yielded/agent/submission-ledger";
import { submissionLedgerConformanceCases } from "@yielded/agent/testing/submission-ledger-conformance";
import { readTestThread } from "@yielded/agent/testing/thread-store-conformance";
import {
  ThreadMaterialization,
  ThreadExportRequest,
  ThreadStore,
  AppendRequest,
} from "@yielded/agent/thread-store";
import type { Crypto, PlatformError } from "effect";
import {
  Cause,
  DateTime,
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
import { CurrentTransformer } from "effect/sql/Statement";

import { withSubmissionOwner } from "../../../test/fixtures/ledger-read-contracts.ts";

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const thread = (value: string) => id(ThreadMaterialization.fields.threadId, value);
const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const toolCall = (value: string) => id(ApprovalDecisionCommand.fields.toolCallId, value);
const isLedgerError = Schema.is(LedgerError);
const isSqliteStorageCompatibilityError = Schema.is(SqliteStorageCompatibilityError);
const isSqliteStorageFailpointError = Schema.is(SqliteStorageFailpointError);

const nativeLedgerLayer = (options: SqliteStorageOptions) =>
  Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
    Layer.provide(
      Layer.mergeAll(
        storageConfigLayer(options),
        storageFailpointLayer(options),
        SqliteClient.layer({ filename: options.filename, disableWAL: true }),
        NodeCrypto.layer,
      ),
    ),
  );

// Requested native CAS proof: administrative writes refresh native authority while
// settlement progress was prepared against the earlier frontier. It must reprepare.
it.effect("reprepares native settlement progress after an administrative append", () =>
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
      const store = yield* ThreadStore;
      const storage = yield* RunStorage;
      const lane = "native-settlement-reprepare";
      const admitted = yield* ledger.admit(yield* admission(lane, "native-owner", "Kyoto"));

      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

      const claimed = yield* storage.claim(ClaimRequest.make({ threadId: thread(lane) }));

      if (Option.isNone(claimed)) return yield* Effect.die("Native Run was not claimed");
      const session = claimed.value;
      const runId = runIdForSubmission(admitted.submissionId);

      const record = (recordId: CanonicalRecord["recordId"], payload: CanonicalRecord["payload"]) =>
        CanonicalRecord.make({
          recordId,
          family: "thread",
          schemaVersion: 1,
          createdAt: at(1),
          deploymentId: TEST_DEPLOYMENT,
          payload,
        });

      const progress = yield* makeProgressWriter(thread(lane), TEST_DEPLOYMENT);

      const input = record(
        submissionInputRecordId(admitted.submissionId),
        UserInputRecorded.make({
          submissionId: admitted.submissionId,
          runId,
          kind: "user",
          input: "Kyoto",
        }),
      );

      yield* progress
        .commit(
          CanonicalBatch.make({
            batchId: submissionInputBatchId(admitted.submissionId),
            producerId: TEST_PRODUCER,
            records: [input],
          }),
        )
        .pipe(Effect.provideService(CurrentRunWriter, session));
      yield* progress
        .commit(
          batch("native-reprepare-start", [
            record(
              id(RecordEnvelope.fields.recordId, "native-reprepare-start"),
              RunStartedRecord.make({
                runId,
                policyAccountingVersion: 1,
                maxDurationMillis: 30_000,
              }),
            ),
          ]),
        )
        .pipe(Effect.provideService(CurrentRunWriter, session));

      const administrative = yield* makeRunWriter(thread(lane));
      const repair = yield* makeProgressWriter(thread(lane), TEST_DEPLOYMENT);
      const publication = yield* settlementPublication(admitted, lane, "aborted");
      let injected = false;
      const [terminal, ...rest] = publication.append.batch.records;

      if (terminal === undefined) return yield* Effect.die("Missing settlement fact");

      const published = yield* progress
        .publish(CanonicalBatch.make({ ...publication.append.batch, records: [terminal, ...rest] }))
        .pipe(
          Effect.provideService(CurrentRunSettlement, {
            threadId: session.threadId,
            tail: session.tail,
            withProgress: session.withProgress,
            publishSettlement: Effect.fnUntraced(function* (prepared, executions) {
              if (!injected) {
                injected = true;

                const abort = record(
                  submissionAbortRecordId(admitted.submissionId),
                  AbortRequested.make({
                    submissionId: admitted.submissionId,
                    author: "operator",
                    reason: "Abort raced settlement preparation",
                  }),
                );

                yield* repair
                  .commit(
                    CanonicalBatch.make({
                      batchId: submissionAbortBatchId(admitted.submissionId),
                      producerId: OTHER_PRODUCER,
                      records: [abort],
                    }),
                  )
                  .pipe(Effect.provideService(CurrentRunWriter, administrative));
              }

              return yield* session.publishSettlement(prepared, executions);
            }),
          }),
        );

      expect(published.replayed).toBe(false);

      const exported = yield* readTestThread(
        store,
        ThreadExportRequest.make({ threadId: thread(lane) }),
      );

      expect(injected).toBe(true);
      expect(exported.records.map(({ record }) => record.payload._tag)).toEqual([
        "UserInputRecorded",
        "AbortRequested",
        "SubmissionSettled",
      ]);
      expect(yield* store.readExecution({ threadId: thread(lane), runId })).toEqual(Option.none());
      expect(
        exported.records.filter(({ record }) => record.payload._tag === "SubmissionSettled"),
      ).toHaveLength(1);

      const recovered = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: admitted.submissionId }),
      );

      expect(recovered.submission.state).toBe("settled");
      expect(recovered.hasOwner).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(services));
  }),
);

const TEST_PRINCIPAL = id(Principal, "principal-sqlite-ledger");
const TEST_PRODUCER = id(ProducerId, "producer-sqlite-ledger");
const OTHER_PRODUCER = id(ProducerId, "producer-sqlite-ledger-other");
const TEST_AGENT = id(AdmissionRequest.fields.agentId, "agent-sqlite-ledger");
const TEST_DEPLOYMENT = id(DeploymentId, "deployment-sqlite-ledger");
const TEST_DEFINITION_DIGEST = Schema.decodeSync(Digest)("a".repeat(64));

const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);

const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

const admission = Effect.fn("SqliteLedgerTest.admission")(function* (
  threadId: string,
  idempotencyKey: string,
  input: PersistedJson,
  parentLinkage?: ParentLinkage,
) {
  const inputDigest = yield* digestJson(input);

  return AdmissionRequest.make({
    threadId: thread(threadId),
    principal: TEST_PRINCIPAL,
    idempotencyKey: id(IdempotencyKey, idempotencyKey),
    agentId: TEST_AGENT,
    agentDigests: TEST_DIGESTS,
    deploymentId: TEST_DEPLOYMENT,
    inputPayload: input,
    inputDigest,
    ...(parentLinkage === undefined ? {} : { parentLinkage }),
  });
});

const settlementPublication = Effect.fn("SqliteLedgerTest.settlementPublication")(function* (
  admitted: AdmissionResult,
  threadId: string,
  outcome: SettlementOutcome,
) {
  const settlementId = submissionSettlementId(admitted.submissionId);

  const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)(
    SubmissionSettled.make({
      submissionId: admitted.submissionId,
      settlementId,
      receiptId: admitted.receiptId,
      outcome,
      runId: runIdForSubmission(admitted.submissionId),
      ...(outcome === "failed"
        ? {
            result: {
              errorTag: "SqliteLedgerTestFailure",
              message: "The SQLite ledger test Submission failed",
            },
          }
        : {}),
    }),
  ).pipe(Effect.orDie);

  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload,
  });

  return SettlementPublication.make({
    submissionId: admitted.submissionId,
    authority: { _tag: "Owned" },
    append: AppendRequest.make({
      threadId: thread(threadId),
      expectedTailSequence: sequence(0),

      batch: CanonicalBatch.make({
        batchId: submissionSettlementBatchId(admitted.submissionId),
        producerId: TEST_PRODUCER,
        records: [record],
      }),
    }),
  });
});

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/BatchId")), batchId),
    producerId: TEST_PRODUCER,
    records,
  });

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-storage-sqlite-ledger-",
      });

      return yield* use(`${directory}/ledger.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const withLedger = <A, E>(
  filename: string,
  effect: Effect.Effect<
    A,
    E,
    SubmissionLedger | SettlementPublisher | StorageOwner | Crypto.Crypto
  >,
) => Effect.provide(effect, [ledgerLayer({ filename }), NodeCrypto.layer]);

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

/** ThreadStore and SubmissionLedger sharing one SqlClient over one database file. */
const combinedLayer = (filename: string) =>
  Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        storageConfigLayer({ filename, observationPollInterval: 1 }),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
        NodeCrypto.layer,
      ),
    ),
  );

describe("SqliteSubmissionLedger", () => {
  it.effect(
    "fences local policy in the admission transaction and replays before mutable checks",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;

          yield* sql`CREATE TABLE host_admission_policy (revision TEXT NOT NULL, observations INTEGER NOT NULL)`;
          yield* sql`INSERT INTO host_admission_policy VALUES ('1', 0)`;
          let unavailable = false;

          const fence = Layer.succeed(SubmissionAdmissionFence)({
            check: Effect.fnUntraced(function* (request) {
              if (unavailable)
                return yield* AdmissionPolicyError.make({
                  reason: "unavailable",
                  code: "host-policy",
                });
              yield* sql`UPDATE host_admission_policy SET observations=observations+1`.pipe(
                Effect.mapError(() =>
                  AdmissionPolicyError.make({ reason: "unavailable", code: "host-policy" }),
                ),
              );

              const rows = yield* sql<{
                revision: string;
              }>`SELECT revision FROM host_admission_policy`.pipe(
                Effect.mapError(() =>
                  AdmissionPolicyError.make({ reason: "unavailable", code: "host-policy" }),
                ),
              );

              if (request.admissionFence?.revision !== rows[0]?.revision)
                return yield* AdmissionPolicyError.make({
                  reason: "refused",
                  code: "stale-revision",
                });
            }),
          });

          yield* Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const original = AdmissionRequest.make({
              ...(yield* admission("policy-transaction", "first", {})),
              admissionGroup: "entity",
              admissionFence: { policyId: "host", key: "entity", revision: "1" },
            });

            const first = yield* ledger.admit(original);

            yield* sql`UPDATE host_admission_policy SET revision='2'`;
            expect((yield* ledger.admit(original)).submissionId).toBe(first.submissionId);

            const fresh = AdmissionRequest.make({
              ...original,
              idempotencyKey: id(IdempotencyKey, "fresh"),
            });

            expect(yield* ledger.admit(fresh).pipe(Effect.flip)).toMatchObject({
              reason: "refused",
              code: "stale-revision",
            });
            // The policy callback's SQL and the admission have one rollback boundary.
            expect(yield* sql`SELECT observations FROM host_admission_policy`).toEqual([
              { observations: 1 },
            ]);
            expect((yield* ledger.resolveAdmission(SubmissionLookupByKey.make(fresh)))._tag).toBe(
              "NotAdmitted",
            );
            unavailable = true;
            expect(yield* ledger.admit(fresh).pipe(Effect.flip)).toMatchObject({
              reason: "unavailable",
            });
            expect((yield* ledger.admit(original)).replayed).toBe(true);
          }).pipe(
            Effect.provide(
              submissionLedgerLayer.pipe(
                Layer.provide(
                  Layer.mergeAll(
                    fence,
                    Layer.succeed(SqlClientService.SqlClient)(sql),
                    storageConfigLayer({ filename }),
                    SqliteStorageFailpoint.layer,
                    NodeCrypto.layer,
                  ),
                ),
              ),
            ),
          );
        }).pipe(Effect.provide([SqliteClient.layer({ filename }), NodeCrypto.layer])),
      ),
  );

  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((filename) =>
          conformanceCase.run.pipe(Effect.provide(combinedLayer(filename))),
        ),
      );
    }
  });

  it.effect(
    "discovers control state independently of poisoned payloads and rejects invalid control identities",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const sql = yield* SqlClientService.SqlClient;
          const request = yield* admission("retained-worker", "original", { work: "retained" });
          const retained = yield* ledger.admit(request);

          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: retained.submissionId }));
          yield* sql`UPDATE effect_agent_submissions
          SET input_json = '{', worker_admission_json = '{'
          WHERE submission_id = ${retained.submissionId}`;
          const work = yield* Stream.runCollect(ledger.scanNonterminal);

          expect(work).toEqual([
            expect.objectContaining({
              submissionId: retained.submissionId,
              receiptId: retained.receiptId,
              threadId: request.threadId,
              principal: request.principal,
              idempotencyKey: request.idempotencyKey,
              deploymentId: request.deploymentId,
              queueSequence: retained.queueSequence,
              state: "ready",
            }),
          ]);
          expect(work[0]).not.toHaveProperty("inputPayload");
          expect(
            yield* ledger
              .lookup(
                SubmissionLookupById.make({
                  submissionId: retained.submissionId,
                }),
              )
              .pipe(Effect.flip),
          ).toMatchObject({ _tag: "LedgerError" });
          expect(
            yield* sql`SELECT input_json, worker_admission_json, receipt_id, state
          FROM effect_agent_submissions WHERE submission_id = ${retained.submissionId}`,
          ).toEqual([
            {
              input_json: "{",
              worker_admission_json: "{",
              receipt_id: retained.receiptId,
              state: "ready",
            },
          ]);

          yield* sql`UPDATE effect_agent_submissions SET receipt_id = ''
          WHERE submission_id = ${retained.submissionId}`;
          expect(yield* Stream.runCollect(ledger.scanNonterminal).pipe(Effect.flip)).toMatchObject({
            _tag: "LedgerError",
            operation: "ledger scan nonterminal",
          });
          expect(
            yield* sql`SELECT state FROM effect_agent_submissions
          WHERE submission_id = ${retained.submissionId}`,
          ).toEqual([{ state: "ready" }]);
        }).pipe(Effect.provide(combinedLayer(filename))),
      ),
  );

  it.effect("preserves a later writer when unknown work is resolved after reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const lane = "thread-unknown-writer-reopen";
        const request = ClaimRequest.make({ threadId: thread(lane) });

        const before = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const older = yield* ledger.admit(yield* admission(lane, "older", { work: "older" }));
            const later = yield* ledger.admit(yield* admission(lane, "later", { work: "later" }));

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: older.submissionId }));
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: later.submissionId }));
            yield* ledger.selectHead(request);
            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: older.submissionId,
                toolCallIds: [toolCall("unknown-reopen-call")],
                reason: "ordinary Tool outcome is uncertain",
              }),
            );

            return { older, later };
          }),
        );

        yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const storage = yield* RunStorage;
          const acquired = yield* storage.claim(request);

          if (Option.isNone(acquired)) return yield* Effect.die("Missing later owner");
          expect(acquired.value.claim.submissionId).toBe(before.later.submissionId);
          yield* ledger.recordUnknownResolution(
            UnknownResolutionCommand.make({
              submissionId: before.older.submissionId,
              toolCallId: toolCall("unknown-reopen-call"),
              author: "operator",
              reason: "external service confirmed no effect",
              resolution: ResolutionNeverHappened.make(),
            }),
          );

          const later = yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: before.later.submissionId }),
          );

          expect(later.hasOwner).toBe(true);
          expect(Option.isNone(yield* storage.claim(request))).toBe(true);
          yield* acquired.value.checkActive;
          yield* acquired.value.release;
          const resumed = yield* storage.claim(request);

          expect(Option.isSome(resumed)).toBe(true);
          if (Option.isSome(resumed))
            expect(resumed.value.claim.submissionId).toBe(before.older.submissionId);
        }).pipe(
          Effect.scoped,
          Effect.provide(runStorageLayer().pipe(Layer.provideMerge(combinedLayer(filename)))),
        );
      }),
    ),
  );

  it.effect("releases recovery read transactions after typed failure and interruption", () =>
    withTemporaryDatabase((filename) =>
      withLedger(
        filename,
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const admitted = yield* ledger.admit(yield* admission("recovery-cleanup", "input", {}));
          const request = RecoverySnapshotRequest.make({ submissionId: admitted.submissionId });

          const missing = yield* ledger
            .loadRecoverySnapshot(
              RecoverySnapshotRequest.make({
                submissionId: id(RecoverySnapshotRequest.fields.submissionId, "missing-submission"),
              }),
            )
            .pipe(Effect.exit);

          expect(Exit.isFailure(missing)).toBe(true);
          if (Exit.isFailure(missing))
            expect(Cause.squash(missing.cause)).toBeInstanceOf(LedgerError);
          expect((yield* ledger.loadRecoverySnapshot(request)).submission.state).toBe("admitted");

          const paused = yield* Deferred.make<void>();
          const queries = yield* Ref.make(0);

          const reader = yield* ledger.loadRecoverySnapshot(request).pipe(
            Effect.provideService(CurrentTransformer, (statement) =>
              Ref.updateAndGet(queries, (count) => count + 1).pipe(
                Effect.flatMap((count) =>
                  count === 2
                    ? Deferred.succeed(paused, undefined).pipe(Effect.andThen(Effect.never))
                    : Effect.succeed(statement),
                ),
              ),
            ),
            Effect.forkChild,
          );

          yield* Deferred.await(paused);
          yield* Fiber.interrupt(reader);
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          expect((yield* ledger.loadRecoverySnapshot(request)).submission.state).toBe("ready");
        }),
      ),
    ),
  );

  it.effect("refuses ledger acquisition while another physical connection owns the file", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.admit(yield* admission("thread-busy", "busy-key-1", { step: 1 }));
          }),
        );

        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            // Hold the write lock on a separate connection, as a transiently coexisting
            // producer would.
            yield* sql`BEGIN IMMEDIATE`;

            const contended = yield* Effect.provide(
              Effect.gen(function* () {
                const ledger = yield* SubmissionLedger;

                return yield* ledger.admit(
                  yield* admission("thread-busy", "busy-key-2", { step: 2 }),
                );
              }),
              [ledgerLayer({ filename, busyTimeout: 0 }), NodeCrypto.layer],
            ).pipe(Effect.exit);

            yield* sql`ROLLBACK`;

            expect(Exit.isFailure(contended)).toBe(true);
            if (Exit.isFailure(contended)) {
              const error = Cause.squash(contended.cause);

              expect(error).toMatchObject({
                _tag: "SqliteStorageError",
                operation: "acquire exclusive host",
              });
            }
          }),
        );

        // Once the competing writer releases the lock, the identical admission commits.
        const recovered = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission("thread-busy", "busy-key-2", { step: 2 }));
          }),
        );

        expect(recovered.replayed).toBe(false);
      }),
    ),
  );

  it.effect("rejects unsupported older and newer versions with preservation guidance", () =>
    Effect.forEach([1, CurrentSqliteStorageVersion + 1], (storedVersion) =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          yield* withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              yield* sql.unsafe(`PRAGMA user_version = ${storedVersion}`);
            }),
          );

          const opened = yield* withLedger(filename, SubmissionLedger).pipe(Effect.exit);

          expect(Exit.isFailure(opened)).toBe(true);
          if (Exit.isFailure(opened)) {
            const error = Cause.squash(opened.cause);

            expect(error).toBeInstanceOf(SqliteStorageCompatibilityError);
            if (isSqliteStorageCompatibilityError(error)) {
              expect(error.actualVersion).toBe(storedVersion);
              expect(error.supportedVersion).toBe(CurrentSqliteStorageVersion);
              expect(error.message).toContain("Keep the original file");
            }
          }

          // Failing closed must not mutate the incompatible file.
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
    ),
  );

  it.effect("recovers ledger identity and input progress after lost acknowledgements", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);

        const select = (location: SqliteStorageFailpointLocation | undefined) =>
          Ref.set(active, location);

        const failingLedger = <A, E>(
          effect: Effect.Effect<
            A,
            E,
            SubmissionLedger | SettlementPublisher | ThreadStore | StorageOwner | Crypto.Crypto
          >,
        ) =>
          Effect.provide(effect, [
            nativeLedgerLayer({
              filename,
              failpoint: (location) =>
                Ref.get(active).pipe(
                  Effect.flatMap((selected) =>
                    selected === location
                      ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                      : Effect.void,
                  ),
                ),
            }),
            NodeCrypto.layer,
          ]);

        const expectInjectedFailure = <A>(
          exit: Exit.Exit<A, unknown>,
          location: SqliteStorageFailpointLocation,
        ) => {
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause);

            expect(error).toBeInstanceOf(LedgerError);
            if (isLedgerError(error)) {
              expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
              if (isSqliteStorageFailpointError(error.cause)) {
                expect(error.cause.location).toBe(location);
              }
            }
          }
        };

        const submissionStates = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, state, receipt_id, input_applied_record_id, finalized_at
              FROM effect_agent_submissions
              ORDER BY thread_id, queue_sequence
            `;
          }),
        );

        const canonicalRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT record_id, payload_json
              FROM effect_agent_canonical_records
            `;
          }),
        );

        const abortRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, reason, requested_at
              FROM effect_agent_abort_intents
            `;
          }),
        );

        const lane = "thread-failpoints";

        const admitOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission(lane, "failpoint-key", { work: "fail" }));
          }),
        );

        yield* select("ledger:admit:after");
        expectInjectedFailure(yield* admitOnce.pipe(Effect.exit), "ledger:admit:after");
        const admittedRows = yield* submissionStates;

        expect(admittedRows).toHaveLength(1);
        expect(admittedRows[0]?.state).toBe("admitted");
        yield* select(undefined);
        const admitted = yield* admitOnce;

        expect(admitted.replayed).toBe(true);
        expect(admitted.submissionId).toBe(admittedRows[0]?.submission_id);
        expect(admitted.receiptId).toBe(admittedRows[0]?.receipt_id);

        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );

        yield* select(undefined);
        yield* markReadyOnce;

        const claimOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.selectHead(ClaimRequest.make({ threadId: thread(lane) }));
          }),
        );

        yield* select("ledger:claim:after");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:after");
        expect((yield* submissionStates)[0]?.state).toBe("ready");
        yield* select(undefined);
        // Reopening does not wait for a persisted ownership lease.
        const claim = yield* claimOnce;

        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;

        const markInputOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger
              .markInputApplied(
                MarkInputAppliedRequest.make({
                  submissionId: admitted.submissionId,
                  recordId: submissionInputRecordId(admitted.submissionId),
                  sequence: sequence(1),
                }),
              )
              .pipe(withSubmissionOwner(admitted.submissionId));
          }),
        );

        yield* select(undefined);
        yield* markInputOnce;

        const publication = yield* settlementPublication(admitted, lane, "completed");

        const publishOnce = failingLedger(
          Effect.gen(function* () {
            const publisher = yield* SettlementPublisher;

            return yield* publisher
              .publish(publication)
              .pipe(withSubmissionOwner(publication.submissionId));
          }),
        );

        yield* select("append:after");
        expectInjectedFailure(yield* publishOnce.pipe(Effect.exit), "append:after");
        const publishedRows = yield* canonicalRows;

        expect(publishedRows).toHaveLength(1);
        expect(publishedRows[0]?.record_id).toBe(
          JSON.stringify(submissionSettlementRecordId(admitted.submissionId)),
        );
        expect((yield* submissionStates)[0]?.finalized_at).not.toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("settled");
        yield* select(undefined);
        // Cofinalization closed the owner; recover the committed receipt through finalization.
        expect(yield* publishOnce.pipe(Effect.flip)).toMatchObject({ _tag: "OwnershipLost" });

        const finalizeOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: admitted.submissionId,
                settlementId: submissionSettlementId(admitted.submissionId),
              }),
            );
          }),
        );

        yield* select("ledger:finalize-settlement:after");
        expectInjectedFailure(
          yield* finalizeOnce.pipe(Effect.exit),
          "ledger:finalize-settlement:after",
        );
        expect((yield* submissionStates)[0]?.finalized_at).not.toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("settled");
        yield* select(undefined);
        const settlement = yield* finalizeOnce;

        expect(settlement.outcome).toBe("completed");

        const abortLane = "thread-failpoints-abort";

        yield* select(undefined);

        const abortAdmitted = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const result = yield* ledger.admit(
              yield* admission(abortLane, "failpoint-abort-key", { work: "abort" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: result.submissionId }));

            return result;
          }),
        );

        const abortOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.requestAbort(
              AbortCommand.make({
                submissionId: abortAdmitted.submissionId,
                author: "failpoint-operator",
                reason: "failpoint abort",
              }),
            );
          }),
        );

        yield* select("ledger:request-abort:after");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:after");
        const abortIntents = yield* abortRows;

        expect(abortIntents).toHaveLength(1);
        yield* select(undefined);
        const intent = yield* abortOnce;

        expect(intent.reason).toBe("failpoint abort");
      }),
    ),
  );

  it.effect(
    "preserves join, approval and unknown-operation evidence after lost acknowledgements",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);

          const select = (location: SqliteStorageFailpointLocation | undefined) =>
            Ref.set(active, location);

          const failingLedger = <A, E>(
            effect: Effect.Effect<
              A,
              E,
              SubmissionLedger | SettlementPublisher | ThreadStore | StorageOwner | Crypto.Crypto
            >,
          ) =>
            Effect.provide(effect, [
              nativeLedgerLayer({
                filename,
                failpoint: (location) =>
                  Ref.get(active).pipe(
                    Effect.flatMap((selected) =>
                      selected === location
                        ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                        : Effect.void,
                    ),
                  ),
              }),
              NodeCrypto.layer,
            ]);

          const expectInjectedFailure = <A>(
            exit: Exit.Exit<A, unknown>,
            location: SqliteStorageFailpointLocation,
          ) => {
            expect(Exit.isFailure(exit)).toBe(true);
            if (Exit.isFailure(exit)) {
              const error = Cause.squash(exit.cause);

              expect(error).toBeInstanceOf(LedgerError);
              if (isLedgerError(error)) {
                expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
                if (isSqliteStorageFailpointError(error.cause)) {
                  expect(error.cause.location).toBe(location);
                }
              }
            }
          };

          const submissionMarkers = withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
              SELECT
                submission_id,
                state,
                joined_host_submission_id,
                input_applied_record_id,
                suspended_reason_json,
                unknown_reason,
                unknown_tool_call_ids_json
              FROM effect_agent_submissions
              ORDER BY thread_id, queue_sequence
            `;
            }),
          );

          const approvalRows = withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, decision, decided_at
              FROM effect_agent_approval_decisions
              ORDER BY tool_call_id
            `;
            }),
          );

          const resolutionRows = withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, resolution_json
              FROM effect_agent_unknown_resolutions
              ORDER BY tool_call_id
            `;
            }),
          );

          const markerFor = (rows: ReadonlyArray<Record<string, unknown>>, submissionId: string) =>
            rows.find((row) => row.submission_id === submissionId);

          const lane = "thread-p5-failpoints";

          const { host, queued, queuedSecond } = yield* failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              const host = yield* ledger.admit(
                yield* admission(lane, "p5-host-key", { work: "host" }),
              );

              yield* ledger.markReady(MarkReadyRequest.make({ submissionId: host.submissionId }));

              const queued = yield* ledger.admit(
                yield* admission(lane, "p5-queued-key", { queued: 2 }),
              );

              yield* ledger.markReady(MarkReadyRequest.make({ submissionId: queued.submissionId }));

              const queuedSecond = yield* ledger.admit(
                yield* admission(lane, "p5-queued-second-key", { queued: 3 }),
              );

              yield* ledger.markReady(
                MarkReadyRequest.make({ submissionId: queuedSecond.submissionId }),
              );

              const claim = yield* ledger.selectHead(ClaimRequest.make({ threadId: thread(lane) }));

              if (Option.isNone(claim)) return yield* Effect.die("missing host claim");

              return { host, hostClaim: claim.value, queued, queuedSecond };
            }),
          );

          const claimJoiningOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              return yield* ledger
                .claimJoining(
                  ClaimJoiningRequest.make({
                    threadId: thread(lane),
                    hostSubmissionId: host.submissionId,
                    maxCount: 1,
                  }),
                )
                .pipe(withSubmissionOwner(host.submissionId));
            }),
          );

          yield* select("ledger:claim-joining:after");
          expectInjectedFailure(
            yield* claimJoiningOnce.pipe(Effect.exit),
            "ledger:claim-joining:after",
          );
          const joiningMarker = markerFor(yield* submissionMarkers, queued.submissionId);

          expect(joiningMarker?.state).toBe("joining");
          expect(joiningMarker?.joined_host_submission_id).toBe(host.submissionId);
          expect(joiningMarker?.input_applied_record_id).toBeNull();
          yield* select(undefined);
          const secondClaims = yield* claimJoiningOnce;

          expect(secondClaims.map((claim) => claim.submissionId)).toEqual([
            queuedSecond.submissionId,
          ]);

          const markJoinedOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              yield* ledger
                .markJoined(
                  MarkJoinedRequest.make({
                    submissionId: queued.submissionId,
                    recordId: submissionInputRecordId(queued.submissionId),
                    sequence: sequence(2),
                  }),
                )
                .pipe(withSubmissionOwner(host.submissionId));
            }),
          );

          yield* select(undefined);
          yield* markJoinedOnce;

          const revertOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              yield* ledger.revertJoining(
                RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
              );
            }),
          );

          yield* select(undefined);
          yield* revertOnce;

          const decideOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              return yield* ledger.recordApprovalDecision(
                ApprovalDecisionCommand.make({
                  submissionId: host.submissionId,
                  toolCallId: toolCall("call-fp-a"),
                  decision: "approved",
                  resolver: "failpoint-approver",
                  reason: "failpoint decision",
                }),
              );
            }),
          );

          yield* select("ledger:approval-decision:after");
          expectInjectedFailure(
            yield* decideOnce.pipe(Effect.exit),
            "ledger:approval-decision:after",
          );
          const decidedRows = yield* approvalRows;

          expect(decidedRows).toHaveLength(1);
          yield* select(undefined);
          const replayedIntent = yield* decideOnce;

          expect(replayedIntent.decision).toBe("approved");
          expect(yield* approvalRows).toHaveLength(1);

          const suspendOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              return yield* ledger
                .suspend(
                  SuspendRequest.make({
                    submissionId: host.submissionId,
                    reason: ApprovalPendingSuspension.make({
                      toolCallIds: [toolCall("call-fp-b")],
                    }),
                  }),
                )
                .pipe(withSubmissionOwner(host.submissionId));
            }),
          );

          yield* select("ledger:suspend:after");
          expectInjectedFailure(yield* suspendOnce.pipe(Effect.exit), "ledger:suspend:after");
          const suspendedMarker = markerFor(yield* submissionMarkers, host.submissionId);

          expect(suspendedMarker?.state).toBe("suspended");
          expect(suspendedMarker?.suspended_reason_json).not.toBeNull();
          yield* select(undefined);
          const retriedSuspend = yield* suspendOnce.pipe(Effect.exit);

          expect(Exit.isFailure(retriedSuspend)).toBe(true);
          if (Exit.isFailure(retriedSuspend)) {
            expect(Cause.squash(retriedSuspend.cause)).toBeInstanceOf(OwnershipLost);
          }

          // Wake the lane and reclaim it for the unknown-outcome failpoints.
          yield* failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              yield* ledger.recordApprovalDecision(
                ApprovalDecisionCommand.make({
                  submissionId: host.submissionId,
                  toolCallId: toolCall("call-fp-b"),
                  decision: "approved",
                  resolver: "failpoint-approver",
                  reason: "wake the suspended lane",
                }),
              );
            }),
          );
          expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe(
            "input-applied",
          );

          const markUnknownOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              yield* ledger.markUnknown(
                MarkUnknownRequest.make({
                  submissionId: host.submissionId,
                  toolCallIds: [toolCall("call-fp-c"), toolCall("call-fp-d")],
                  reason: "failpoint uncertainty",
                }),
              );
            }),
          );

          yield* select("ledger:mark-unknown:after");
          expectInjectedFailure(
            yield* markUnknownOnce.pipe(Effect.exit),
            "ledger:mark-unknown:after",
          );
          const unknownMarker = markerFor(yield* submissionMarkers, host.submissionId);

          expect(unknownMarker?.state).toBe("unknown");
          expect(unknownMarker?.unknown_reason).toBe("failpoint uncertainty");
          expect(unknownMarker?.unknown_tool_call_ids_json).not.toBeNull();
          yield* select(undefined);
          yield* markUnknownOnce;

          const resolveOnce = (call: string, resolution: "never" | "completed") =>
            failingLedger(
              Effect.gen(function* () {
                const ledger = yield* SubmissionLedger;

                return yield* ledger.recordUnknownResolution(
                  UnknownResolutionCommand.make({
                    submissionId: host.submissionId,
                    toolCallId: toolCall(call),
                    author: "failpoint-operator",
                    reason: "failpoint resolution",
                    resolution:
                      resolution === "never"
                        ? ResolutionNeverHappened.make()
                        : ResolutionCompletedWithResult.make({
                            result: { bookingRef: "booking-fp-1" },
                            isFailure: false,
                          }),
                  }),
                );
              }),
            );

          yield* select("ledger:unknown-resolution:after");
          expectInjectedFailure(
            yield* resolveOnce("call-fp-c", "never").pipe(Effect.exit),
            "ledger:unknown-resolution:after",
          );
          expect(yield* resolutionRows).toHaveLength(1);
          expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("unknown");
          yield* select(undefined);
          yield* resolveOnce("call-fp-c", "never");

          yield* select("ledger:unknown-resolution:after");
          expectInjectedFailure(
            yield* resolveOnce("call-fp-d", "completed").pipe(Effect.exit),
            "ledger:unknown-resolution:after",
          );
          // The covering resolution and its wake transition are one atomic durable step.
          expect(yield* resolutionRows).toHaveLength(2);
          const wokenMarker = markerFor(yield* submissionMarkers, host.submissionId);

          expect(wokenMarker?.state).toBe("input-applied");
          expect(wokenMarker?.unknown_reason).toBeNull();
          expect(wokenMarker?.unknown_tool_call_ids_json).toBeNull();
          yield* select(undefined);
          const replayedResolution = yield* resolveOnce("call-fp-d", "completed");

          expect(replayedResolution.resolution._tag).toBe("CompletedWithResult");
          expect(yield* resolutionRows).toHaveLength(2);
        }),
      ),
  );

  it.effect("preserves child accounting and wake identity after lost acknowledgements", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);

        const select = (location: SqliteStorageFailpointLocation | undefined) =>
          Ref.set(active, location);

        const failingLedger = <A, E>(
          effect: Effect.Effect<
            A,
            E,
            SubmissionLedger | SettlementPublisher | ThreadStore | StorageOwner | Crypto.Crypto
          >,
        ) =>
          Effect.provide(effect, [
            nativeLedgerLayer({
              filename,
              failpoint: (location) =>
                Ref.get(active).pipe(
                  Effect.flatMap((selected) =>
                    selected === location
                      ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                      : Effect.void,
                  ),
                ),
            }),
            NodeCrypto.layer,
          ]);

        const expectInjectedFailure = <A>(
          exit: Exit.Exit<A, unknown>,
          location: SqliteStorageFailpointLocation,
        ) => {
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause);

            expect(error).toBeInstanceOf(LedgerError);
            if (isLedgerError(error)) {
              expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
              if (isSqliteStorageFailpointError(error.cause)) {
                expect(error.cause.location).toBe(location);
              }
            }
          }
        };

        const reservationRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT
                reservation_id,
                status,
                child_submission_id,
                accounting_json,
                release_began_at,
                released_at
              FROM effect_agent_child_reservations
            `;
          }),
        );

        const parentMarkers = (submissionId: string) =>
          withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
                SELECT state, suspended_reason_json
                FROM effect_agent_submissions
                WHERE submission_id = ${submissionId}
              `;
            }),
          );

        const parentLane = "thread-s2-failpoints";
        const childLane = "thread-s2-failpoints-child";
        const reservationId = S2_FAILPOINT_RESERVATION;
        const delegationCall = toolCall("call-s2-fp");

        const { child, parent } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const parent = yield* ledger.admit(
              yield* admission(parentLane, "s2-fp-parent-key", { work: "parent" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));

            const child = yield* ledger.admit(
              yield* admission(childLane, "s2-fp-child-key", { task: "child" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));

            const claim = yield* ledger.selectHead(
              ClaimRequest.make({
                threadId: thread(parentLane),
              }),
            );

            if (Option.isNone(claim)) return yield* Effect.die("missing parent claim");

            return { parent, child, parentClaim: claim.value };
          }),
        );

        const allocation = { turns: 2 };

        const allocationDigest = yield* digestJson(allocation).pipe(
          Effect.provide(NodeCrypto.layer),
        );

        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger
              .reserveChildBudget(
                ChildBudgetReservationRequest.make({
                  reservationId,
                  parentSubmissionId: parent.submissionId,
                  parentToolCallId: delegationCall,
                  allocation,
                  allocationDigest,
                }),
              )
              .pipe(withSubmissionOwner(parent.submissionId));
          }),
        );

        yield* select("ledger:child-reservation:after");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:child-reservation:after",
        );
        const reservedRows = yield* reservationRows;

        expect(reservedRows).toHaveLength(1);
        expect(reservedRows[0]?.status).toBe("reserved");
        expect(reservedRows[0]?.child_submission_id).toBeNull();
        yield* select(undefined);
        const replayedReserve = yield* reserveOnce;

        expect(replayedReserve.replayed).toBe(true);

        const attachOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger
              .attachChildToReservation(
                AttachChildToReservationRequest.make({
                  reservationId,
                  childSubmissionId: child.submissionId,
                }),
              )
              .pipe(withSubmissionOwner(parent.submissionId));
          }),
        );

        yield* select(undefined);
        const replayedAttach = yield* attachOnce;

        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

        const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };

        const beginOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
          }),
        );

        yield* select("ledger:child-release-pending:after");
        expectInjectedFailure(
          yield* beginOnce.pipe(Effect.exit),
          "ledger:child-release-pending:after",
        );
        const frozenRows = yield* reservationRows;

        expect(frozenRows[0]?.status).toBe("releasePending");
        expect(frozenRows[0]?.accounting_json).not.toBeNull();
        yield* select(undefined);
        const replayedBegin = yield* beginOnce;

        expect(replayedBegin.status).toBe("releasePending");

        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
          }),
        );

        yield* select("ledger:child-release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:after");
        expect((yield* reservationRows)[0]?.status).toBe("released");
        expect((yield* reservationRows)[0]?.released_at).not.toBeNull();
        yield* select(undefined);
        const replayedRelease = yield* releaseOnce;

        expect(replayedRelease.status).toBe("released");

        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const suspended = yield* ledger
              .suspend(
                SuspendRequest.make({
                  submissionId: parent.submissionId,
                  reason: WaitingForChildSuspension.make({
                    children: [
                      WaitingChild.make({
                        toolCallId: delegationCall,
                        childSubmissionId: child.submissionId,
                      }),
                    ],
                  }),
                }),
              )
              .pipe(withSubmissionOwner(parent.submissionId));

            expect(suspended).toBe("suspended");

            const childClaim = yield* ledger.selectHead(
              ClaimRequest.make({
                threadId: thread(childLane),
              }),
            );

            if (Option.isNone(childClaim)) return yield* Effect.die("missing child claim");

            const publisher = yield* SettlementPublisher;

            yield* publisher
              .publish(yield* settlementPublication(child, childLane, "completed"))
              .pipe(withSubmissionOwner(child.submissionId));
            yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: child.submissionId,
                settlementId: submissionSettlementId(child.submissionId),
              }),
            );
          }),
        );

        const notifyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: parent.submissionId,
                childSubmissionId: child.submissionId,
              }),
            );
          }),
        );

        yield* select("ledger:child-settled:after");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:after");
        const wokenMarkers = yield* parentMarkers(parent.submissionId);

        expect(wokenMarkers[0]?.state).toBe("input-applied");
        expect(wokenMarkers[0]?.suspended_reason_json).toBeNull();
        yield* select(undefined);
        const replayedNotification = yield* notifyOnce;

        expect(replayedNotification).toBe("not-waiting");
      }),
    ),
  );
});
