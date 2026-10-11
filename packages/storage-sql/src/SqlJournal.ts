import { ThreadId } from "@yielded/agent/identifiers";
import { LifecyclePublicationFact } from "@yielded/agent/lifecycle-publication";
import { CanonicalSequence, Digest, ProducerEpoch } from "@yielded/agent/records";
import {
  AppendConflict,
  CheckpointRejected,
  FenceRejected,
  type PreparedAppend,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { Effect, Option, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type { Statement } from "effect/sql/Statement";

import {
  makeCanonicalStorage,
  recordBatchId,
  runKey,
  type CapturedRecordStorage,
} from "./SqlCanonicalStorage.ts";
import { makeSqlLifecyclePublication } from "./SqlLifecyclePublication.ts";
import {
  makeRowDecoder,
  makeSqlQuery,
  SqlInteger,
  type Diagnostic,
  type SqlStorageErrors,
  type SqlStorageFailpoint,
  type SqlTransactions,
} from "./SqlStorage.ts";
import {
  canonicalRecordJson,
  canonicalBatchJson,
  makeSqlThreadArchiveRange,
} from "./SqlThreadArchiveRange.ts";
import {
  type CanonicalRecordMetadata,
  makeProgressAppendValidation,
  makeExecutionRead,
} from "./SqlThreadNativeReads.ts";
import { makeSqlThreadWork } from "./SqlThreadWork.ts";

export interface SqlJournalOptions<
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
> {
  readonly namespace?: string;
  readonly errors: SqlStorageErrors<S, C>;
  readonly transactions: SqlTransactions<S, W>;
  readonly hitFailpoint: SqlStorageFailpoint<F>;
}

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const MAX_STORED_TEXT_BYTES = 16 * 1024 * 1024;
const MAX_IDENTIFIER_LENGTH = 1_024;

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

export class ThreadRow extends Schema.Class<ThreadRow>("ThreadRow")({
  thread_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: SqlInteger.pipe(Schema.decodeTo(ProducerEpoch)),
  tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

export class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_id: Schema.fromJsonString(BoundedIdentifier),
  batch_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

export class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: Schema.fromJsonString(BoundedIdentifier),
  thread_id: BoundedIdentifier,
  record_id: Schema.fromJsonString(BoundedIdentifier),
  record_json: BoundedStoredText,
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  through_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

/**
 * Trusted adapter SPI produced by prepareSqlAppend, never a wire or caller-data boundary.
 * Canonical values, encoded strings and UTF-8 sizes remain paired; ThreadStore owns capture
 * and validation before entering this journal, whose caller already holds SQL storage authority.
 */
export interface RawAppendRequest {
  readonly batchId: PreparedAppend["batch"]["batchId"];
  readonly batchJson: string;
  readonly batchHeaderJson: string;
  readonly producerIdJson: string;
  readonly batchBytes: number;
  readonly threadId: PreparedAppend["threadId"];
  readonly expectedTailSequence: CanonicalSequence;
  readonly producerEpoch: ProducerEpoch;
  readonly records: ReadonlyArray<
    PreparedAppend["records"][number] & {
      readonly readMetadata: CanonicalRecordMetadata;
      readonly storage: CapturedRecordStorage;
    }
  >;
  readonly progress: PreparedAppend["progress"];
  readonly executionWrites: ReadonlyArray<
    PreparedAppend["executionWrites"][number] & {
      readonly checkpointJson: string;
    }
  >;
  /** Fingerprint of canonical bytes and new operational facts for bounded active retries. */
  readonly commitDigest: Digest;
}

export class RawAppendResult extends Schema.Class<RawAppendResult>(
  "@effect-agent/storage-sql/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
}) {}

const MAX_EXECUTION_RECEIPTS = 16_384;

/** Shared native transaction body; callers own the producer fence and mutation transaction. */
export const makeSqlExecutions = Effect.fnUntraced(function* (namespace?: string) {
  const sql = yield* SqlClient.SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);
  const read = yield* makeExecutionRead(namespace);

  const failed = (message: string, cause?: unknown) =>
    ThreadStoreError.make({
      operation: "commit active execution",
      message,
      ...(cause === undefined ? {} : { cause }),
    });

  const query = <A extends object>(statement: Statement<A>) =>
    execute(statement).pipe(
      Effect.mapError((cause) => failed("Active execution storage failed", cause)),
    );

  const decodeReceipt = Schema.decodeUnknownEffect(
    Schema.Array(
      Schema.Struct({
        commit_digest: Digest,
        first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
        last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
      }),
    ).check(Schema.isMaxLength(1)),
  );

  const conflict = (request: RawAppendRequest, reason: "batch-content" | "tail") =>
    AppendConflict.make({
      threadId: request.threadId,
      batchId: request.batchId,
      reason,
      ...(reason === "tail"
        ? {
            actualTailSequence: request.expectedTailSequence,
          }
        : {}),
    });

  const replay = Effect.fnUntraced(function* (request: RawAppendRequest) {
    const [receipt] = yield* decodeReceipt(
      yield* query(sql`
      SELECT commit_digest, first_sequence, last_sequence
      FROM ${table("effect_agent_execution_receipts")}
      WHERE thread_id=${request.threadId} AND batch_id=${request.batchId} LIMIT 1
    `),
    ).pipe(Effect.mapError((cause) => failed("Invalid execution receipt", cause)));

    if (receipt === undefined) return undefined;
    if (receipt.commit_digest !== request.commitDigest)
      return yield* conflict(request, "batch-content");

    return RawAppendResult.make({
      firstSequence: receipt.first_sequence,
      lastSequence: receipt.last_sequence,
      replayed: true,
    });
  });

  /** Active intent conflicts fail; retired settlements still adopt their canonical terminal proof. */
  const checkCommitIntent = (request: RawAppendRequest) => replay(request).pipe(Effect.asVoid);

  const validate = Effect.fnUntraced(function* (request: RawAppendRequest) {
    const seen = new Set<string>();

    for (const { update } of request.executionWrites) {
      const checkpoint = update.checkpoint;
      const next = checkpoint.progress;

      if (seen.has(next.runId)) return yield* failed("Duplicate execution update");
      seen.add(next.runId);

      const previous = Option.getOrUndefined(
        yield* read({ threadId: request.threadId, runId: next.runId }),
      );

      if ((previous?.progress.revision ?? 0) !== update.previousRevision)
        return yield* conflict(request, "tail");
      if (
        next.revision !== update.previousRevision + 1 ||
        (previous !== undefined && checkpoint.originalSequence !== previous.originalSequence) ||
        checkpoint.batchId !== request.batchId ||
        checkpoint.throughSequence !== request.expectedTailSequence + request.records.length
      )
        return yield* failed("Execution revision or history frontier is invalid");
      if (update.retire && (next.position !== "settled" || next.terminal === undefined))
        return yield* failed(
          "Execution retirement requires settled progress and canonical closure",
        );
      if (update.retire) {
        const terminal = next.terminal;

        if (terminal === undefined)
          return yield* failed("Execution retirement has no terminal reference");

        const settlement = request.records.find(
          ({ canonical }) => canonical.recordId === terminal.recordId,
        )?.canonical;

        const stored =
          settlement === undefined
            ? yield* query(sql`SELECT record_id FROM ${table("effect_agent_canonical_records")}
              WHERE thread_id=${request.threadId} AND record_id=${JSON.stringify(terminal.recordId)}
              AND record_tag=21 AND submission_id=${JSON.stringify(next.submissionId)}
              AND run_id=${runKey(sql, sql`${request.threadId}`, sql`${JSON.stringify(next.runId)}`, namespace)}`)
            : [];

        if (
          !(
            settlement?.payload._tag === "SubmissionSettled" &&
            settlement.payload.submissionId === next.submissionId &&
            settlement.payload.runId === next.runId
          ) &&
          stored.length !== 1
        )
          return yield* failed("Execution retirement has no canonical settlement proof");
      }
      if (!update.retire) {
        const [receipts] = yield* Schema.decodeUnknownEffect(
          Schema.Tuple([
            Schema.Struct({ count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)) }),
          ]),
        )(
          yield* query(sql`SELECT COUNT(*) AS count FROM (SELECT 1 FROM ${table("effect_agent_execution_receipts")}
            WHERE thread_id=${request.threadId} AND run_id=${JSON.stringify(next.runId)} LIMIT ${MAX_EXECUTION_RECEIPTS}) AS bounded_receipts`),
        ).pipe(Effect.mapError((cause) => failed("Invalid execution receipt count", cause)));

        if (receipts.count >= MAX_EXECUTION_RECEIPTS)
          return yield* failed("Active execution receipt limit exceeded");
      }
    }
  });

  const commit = Effect.fnUntraced(function* (request: RawAppendRequest, result: RawAppendResult) {
    for (const { update, checkpointJson } of request.executionWrites) {
      const run = JSON.stringify(update.checkpoint.progress.runId);

      if (update.retire) {
        yield* query(
          sql`DELETE FROM ${table("effect_agent_execution_receipts")} WHERE thread_id=${request.threadId} AND run_id=${run}`,
        );

        const deleted = yield* query(sql`DELETE FROM ${table("effect_agent_executions")}
          WHERE thread_id=${request.threadId} AND run_id=${run} AND revision=${update.previousRevision} RETURNING run_id`);

        if (deleted.length !== (update.previousRevision === 0 ? 0 : 1))
          return yield* conflict(request, "tail");
        continue;
      }

      const changed = yield* query(sql`INSERT INTO ${table("effect_agent_executions")}
        (thread_id, run_id, input_sequence, revision, checkpoint_json)
        VALUES (${request.threadId}, ${run}, ${update.checkpoint.originalSequence}, ${update.checkpoint.progress.revision}, ${checkpointJson})
        ON CONFLICT(thread_id, run_id) DO UPDATE SET revision=excluded.revision, checkpoint_json=excluded.checkpoint_json
        WHERE ${table("effect_agent_executions")}.revision=${update.previousRevision} RETURNING run_id`);

      if (changed.length !== 1) return yield* conflict(request, "tail");
      yield* query(sql`INSERT INTO ${table("effect_agent_execution_receipts")}
        (thread_id, run_id, batch_id, commit_digest, first_sequence, last_sequence)
        VALUES (${request.threadId}, ${run}, ${request.batchId}, ${request.commitDigest}, ${result.firstSequence}, ${result.lastSequence})`);
    }
  });

  return { read, replay, checkCommitIntent, validate, commit };
});

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-sql/RawReadRequest",
)({
  threadId: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-sql/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  throughSequence: CanonicalSequence,
}) {}

/** Construct journal operations over an already initialized database. */
export const makeSqlJournalKernel = Effect.fnUntraced(function* <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(options: SqlJournalOptions<S, C, W, F>) {
  const sql = yield* SqlClient.SqlClient;
  const { table: relation, execute } = yield* makeSqlQuery(options.namespace);
  const validateProgress = yield* makeProgressAppendValidation(options.namespace);
  const executions = yield* makeSqlExecutions(options.namespace);
  const physical = yield* makeCanonicalStorage(options.namespace);

  const lifecycle = yield* makeSqlLifecyclePublication(options.namespace).pipe(
    Effect.mapError((cause) =>
      options.errors.storage({
        operation: "initialize lifecycle publication",
        message: "Native publication storage unavailable",
        cause,
      }),
    ),
  );

  const failpoint = options.hitFailpoint;
  const { withReadTransaction, withWriteTransaction } = options.transactions;

  const archives = yield* makeSqlThreadArchiveRange(
    {
      read: (body) =>
        withReadTransaction("archive read")(body).pipe(
          Effect.mapError((cause) =>
            options.transactions.isTransactionFailure(cause)
              ? ThreadStoreError.make({ operation: "archive read", message: cause.message, cause })
              : cause,
          ),
        ),
      write: (body) =>
        withWriteTransaction("archive write")(body).pipe(
          Effect.mapError((cause) =>
            options.transactions.isTransactionFailure(cause)
              ? ThreadStoreError.make({ operation: "archive write", message: cause.message, cause })
              : cause,
          ),
        ),
    },
    options.namespace,
  );

  const recordJson = canonicalRecordJson(sql, options.namespace);
  const batchJson = canonicalBatchJson(sql, options.namespace);

  const work = yield* makeSqlThreadWork({
    ...(options.namespace === undefined ? {} : { namespace: options.namespace }),
    read: (body) =>
      withReadTransaction("work snapshot")(body).pipe(
        Effect.mapError((cause) =>
          options.transactions.isTransactionFailure(cause)
            ? ThreadStoreError.make({ operation: "work snapshot", message: cause.message, cause })
            : cause,
        ),
      ),
    write: (body) =>
      withWriteTransaction("work rebuild")(body).pipe(
        Effect.mapError((cause) =>
          options.transactions.isTransactionFailure(cause)
            ? ThreadStoreError.make({ operation: "work rebuild", message: cause.message, cause })
            : cause,
        ),
      ),
  });

  const workFailure = (cause: ThreadStoreError) =>
    options.errors.storage({ operation: cause.operation, message: cause.message, cause });

  const { decodeRows, decodeSingleRow } = makeRowDecoder(options.errors.corruption);
  const decodeThreadRows = decodeRows(Schema.Array(ThreadRow));
  const decodeThreadRow = decodeSingleRow(Schema.Array(ThreadRow));
  const decodeBatchRows = decodeRows(Schema.Array(BatchRow));

  const decodeRecordRows = decodeRows(Schema.Array(RecordRow));

  const decodeRecordIdentityRows = decodeRows(
    Schema.Array(Schema.Struct({ record_id: Schema.fromJsonString(BoundedIdentifier) })),
  );

  const decodeCheckpointRows = decodeRows(Schema.Array(CheckpointRow));
  const decodeCanonicalSequence = Schema.decodeEffect(CanonicalSequence);

  const isLifecyclePublicationFact = Schema.is(Schema.toType(LifecyclePublicationFact));

  const storageError =
    (operation: string) =>
    (error: SqlError): S =>
      options.errors.storage({ operation, message: error.message, cause: error });

  const materialize = Effect.fnUntraced(function* (
    threadId: ThreadId,
    createdAt: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<void, FenceRejected | C | S | W> {
    if (threadId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* options.errors.storage({
        operation: "materialize thread",
        message: "Thread identity exceeds the SQL storage bounds.",
      });
    }
    yield* withWriteTransaction("materialize transaction")(
      Effect.gen(function* () {
        const existingRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            producer_epoch
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${threadId}
        `.pipe(execute, Effect.mapError(storageError("read materialized thread")));

        const existing = yield* decodeThreadRows("effect_agent_threads", threadId, existingRows);

        if (existing.length > 1) {
          return yield* options.errors.corruption({
            table: "effect_agent_threads",
            rowKey: threadId,
            message: "A thread primary key returned more than one row.",
          });
        }
        if (existing.length === 0) {
          yield* sql`
            INSERT INTO ${relation("effect_agent_threads")} (
              thread_id,
              created_at,
              tail_sequence,
              producer_epoch
            ) VALUES (
              ${threadId},
              ${createdAt},
              0,
              ${producerEpoch}
            )
          `.pipe(execute, Effect.mapError(storageError("materialize thread")));

          yield* sql`INSERT INTO ${relation("effect_agent_transfer_state")}
            (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count, executions_count)
            VALUES (${threadId}, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO NOTHING`.pipe(
            execute,
            Effect.mapError(storageError("initialize transfer counters")),
          );

          yield* work.initialize(threadId).pipe(Effect.mapError(workFailure));

          return;
        }
        if (producerEpoch < existing[0].producer_epoch) {
          return yield* FenceRejected.make({
            threadId,
            attemptedEpoch: producerEpoch,
            actualEpoch: existing[0].producer_epoch,
          });
        }

        const counters = yield* sql`SELECT thread_id FROM ${relation("effect_agent_transfer_state")}
          WHERE thread_id=${threadId}`.pipe(
          execute,
          Effect.mapError(storageError("read transfer counters")),
        );

        if (counters.length !== 1)
          return yield* options.errors.corruption({
            table: "effect_agent_transfer_state",
            rowKey: threadId,
            message: "Materialized Thread has no transfer counter row.",
          });
        if (producerEpoch > existing[0].producer_epoch) {
          yield* sql`
            UPDATE ${relation("effect_agent_threads")}
            SET producer_epoch = ${producerEpoch}
            WHERE thread_id = ${threadId}
          `.pipe(execute, Effect.mapError(storageError("advance materialization epoch")));
        }
      }),
    );
  });

  const getThread = Effect.fnUntraced(function* (threadId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        created_at,
        tail_sequence,
        producer_epoch
      FROM ${relation("effect_agent_threads")}
      WHERE thread_id = ${threadId}
    `.pipe(execute, Effect.mapError(storageError("read thread")));

    return yield* decodeThreadRows("effect_agent_threads", threadId, rows);
  });

  const readAppendThread = Effect.fnUntraced(function* (threadId: string) {
    const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            producer_epoch
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${threadId}
        `.pipe(execute, Effect.mapError(storageError("read append tail")));

    return yield* decodeThreadRow("effect_agent_threads", threadId, threadRows);
  });

  const appendInTransaction = Effect.fnUntraced(function* (
    request: RawAppendRequest,
    readThread: Effect.Effect<ThreadRow, C | S>,
  ): Effect.fn.Return<
    RawAppendResult,
    AppendConflict | FenceRejected | ThreadStoreError | C | S | F | W
  > {
    if (
      request.threadId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchBytes > MAX_STORED_TEXT_BYTES ||
      request.executionWrites.some(
        (write) =>
          write.bytes > MAX_STORED_TEXT_BYTES ||
          write.update.checkpoint.progress.runId.length > MAX_IDENTIFIER_LENGTH,
      ) ||
      request.records.some(
        (record) =>
          record.recordId.length > MAX_IDENTIFIER_LENGTH ||
          record.recordBytes > MAX_STORED_TEXT_BYTES,
      )
    ) {
      return yield* options.errors.storage({
        operation: "append canonical batch",
        message: "Canonical identifiers or encoded JSON exceed the SQL storage bounds.",
      });
    }

    if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
      return yield* options.errors.storage({
        operation: "append canonical batch",
        message: "Prepared canonical append requires the journal writer transaction.",
      });

    const recordIds = request.records.map((record) => record.recordId);

    if (new Set(recordIds).size !== recordIds.length) {
      return yield* AppendConflict.make({
        threadId: request.threadId,
        batchId: request.batchId,
        reason: "record-identity",
      });
    }

    const thread = yield* readThread;

    if (request.producerEpoch !== thread.producer_epoch) {
      return yield* FenceRejected.make({
        threadId: request.threadId,
        attemptedEpoch: request.producerEpoch,
        actualEpoch: thread.producer_epoch,
      });
    }

    const replay = yield* executions.replay(request);

    if (replay !== undefined) return replay;

    const existing = yield* archives.replay(request.threadId, request.batchId);

    if (existing !== undefined) {
      if (existing.batch_json !== request.batchJson) {
        return yield* AppendConflict.make({
          threadId: request.threadId,
          batchId: request.batchId,
          reason: "batch-content",
        });
      }

      return RawAppendResult.make({
        firstSequence: existing.first_sequence,
        lastSequence: existing.last_sequence,
        replayed: true,
      });
    }

    if (request.expectedTailSequence !== thread.tail_sequence) {
      return yield* AppendConflict.make({
        threadId: request.threadId,
        batchId: request.batchId,
        reason: "tail",
        actualTailSequence: thread.tail_sequence,
      });
    }

    const existingRecordRows =
      recordIds.length === 0
        ? []
        : yield* sql<Record<string, unknown>>`
        SELECT record_id
        FROM ${relation("effect_agent_canonical_records")}
        WHERE thread_id = ${request.threadId}
          AND record_id IN ${sql.in(recordIds.map((id) => JSON.stringify(id)))}
        LIMIT 1
      `.pipe(execute, Effect.mapError(storageError("check canonical record identities")));

    const existingRecords = yield* decodeRecordIdentityRows(
      "effect_agent_canonical_records",
      `${request.threadId}/record_ids`,
      existingRecordRows,
    );

    if (existingRecords.length > 0) {
      return yield* AppendConflict.make({
        threadId: request.threadId,
        batchId: request.batchId,
        reason: "record-identity",
      });
    }

    const firstSequence = yield* decodeCanonicalSequence(
      thread.tail_sequence + (request.records.length === 0 ? 0 : 1),
    ).pipe(
      Effect.mapError((error) =>
        options.errors.storage({
          cause: error,
          operation: "append canonical batch",
          message: error.message,
        }),
      ),
    );

    const lastSequence = yield* decodeCanonicalSequence(
      thread.tail_sequence + request.records.length,
    ).pipe(
      Effect.mapError((error) =>
        options.errors.storage({
          cause: error,
          operation: "append canonical batch",
          message: error.message,
        }),
      ),
    );

    yield* executions.validate(request);
    yield* validateProgress(request);

    const result = RawAppendResult.make({
      firstSequence,
      lastSequence,
      replayed: false,
    });

    if (request.records.length > 0) {
      yield* archives.append(request, firstSequence, lastSequence);
      yield* sql`
        INSERT INTO ${relation("effect_agent_canonical_batches")} (
          thread_id,
          batch_id,
          first_sequence,
          last_sequence,
          producer_id_json
        ) VALUES (
          ${request.threadId},
          ${JSON.stringify(request.batchId)},
          ${firstSequence},
          ${lastSequence},
          ${request.producerIdJson}
        )
      `.pipe(execute, Effect.mapError(storageError("insert canonical batch")));
    }
    yield* failpoint("append:after-batch-insert");

    const physicalRecords = yield* physical.records(
      request.threadId,
      firstSequence,
      request.records,
    );

    // Five canonical rows (18 columns each) and 33 membership rows (three columns each)
    // stay below every adapter's parameter ceiling, including Durable Objects' limit of 100.
    for (let at = 0; at < request.records.length; at += 5) {
      const records = request.records.slice(at, at + 5);

      yield* sql`INSERT INTO ${relation("effect_agent_canonical_records")} ${sql.insert(
        physicalRecords.slice(at, at + 5).map((record) => record.columns),
      )}`.pipe(execute, Effect.mapError(storageError("insert canonical record")));

      const memberships = physicalRecords.slice(at, at + 5).flatMap((record) => record.memberships);

      for (let from = 0; from < memberships.length; from += 33)
        yield* sql`INSERT INTO ${relation("effect_agent_record_runs")} ${sql.insert(memberships.slice(from, from + 33))}`.pipe(
          execute,
          Effect.mapError(storageError("index canonical Run membership")),
        );

      for (const [index, record] of records.entries()) {
        const canonical = record.canonical;

        if (lifecycle !== undefined && isLifecyclePublicationFact(canonical.payload))
          yield* lifecycle
            .retain({
              id: JSON.stringify([request.threadId, "record", record.recordId]),
              ownerThreadId: request.threadId,
              canonicalSequence: yield* decodeCanonicalSequence(firstSequence + at + index).pipe(
                Effect.orDie,
              ),
              createdAt: canonical.createdAt,
              fact: canonical.payload,
            })
            .pipe(
              Effect.mapError((cause) =>
                options.errors.storage({
                  operation: "retain lifecycle publication",
                  message: "Native publication storage unavailable",
                  cause,
                }),
              ),
            );
        yield* failpoint("append:after-record-insert");
      }
    }

    if (request.records.length > 0) {
      yield* sql`
        UPDATE ${relation("effect_agent_threads")}
        SET
          tail_sequence = ${lastSequence},
          producer_epoch = ${request.producerEpoch}
        WHERE thread_id = ${request.threadId}
      `.pipe(execute, Effect.mapError(storageError("advance thread tail")));
      yield* work
        .apply(
          request.threadId,
          firstSequence,
          request.records.map((record) => record.canonical),
        )
        .pipe(Effect.mapError(workFailure));
    }
    yield* executions.commit(request, result);
    if (request.executionWrites.length > 0) yield* failpoint("append:after-record-insert");
    for (const { update } of request.executionWrites) {
      for (const fact of update.facts) {
        if (lifecycle !== undefined && isLifecyclePublicationFact(fact.payload))
          yield* lifecycle
            .retain({
              id: JSON.stringify([request.threadId, "record", fact.recordId]),
              ownerThreadId: request.threadId,
              canonicalSequence: lastSequence,
              createdAt: fact.createdAt,
              fact: fact.payload,
            })
            .pipe(
              Effect.mapError((cause) =>
                options.errors.storage({
                  operation: "retain operational lifecycle",
                  message: cause.message,
                  cause,
                }),
              ),
            );
      }
    }
    yield* failpoint("append:after-tail-update");

    return result;
  });

  const appendKernel = (request: RawAppendRequest, readThread: Effect.Effect<ThreadRow, C | S>) =>
    withWriteTransaction("append transaction")(appendInTransaction(request, readThread));

  const read = Effect.fnUntraced(function* (request: RawReadRequest) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        sequence,
        record_id,
        ${recordBatchId(sql, options.namespace)} AS batch_id,
        ${recordJson} AS record_json
      FROM ${relation("effect_agent_canonical_records")}
      WHERE thread_id = ${request.threadId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(execute, Effect.mapError(storageError("read canonical records")));

    return yield* decodeRecordRows(
      "effect_agent_canonical_records",
      `${request.threadId}>${request.fromSequenceExclusive}`,
      rows,
    );
  });

  const saveCheckpoint = Effect.fnUntraced(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointRejected | C | S | W> {
    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpoint.checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* options.errors.storage({
        operation: "save checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQL storage bounds.",
      });
    }
    yield* withWriteTransaction("checkpoint transaction")(
      Effect.gen(function* () {
        const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            producer_epoch
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${checkpoint.threadId}
        `.pipe(execute, Effect.mapError(storageError("read checkpoint tail")));

        const thread = yield* decodeThreadRow(
          "effect_agent_threads",
          checkpoint.threadId,
          threadRows,
        );

        if (checkpoint.throughSequence > thread.tail_sequence) {
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "ahead-of-tail",
          });
        }
        if (!(yield* isBatchBoundary(checkpoint.threadId, checkpoint.throughSequence))) {
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "not-batch-boundary",
          });
        }

        const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            through_sequence,
            checkpoint_json
          FROM ${relation("effect_agent_checkpoints")}
          WHERE thread_id = ${checkpoint.threadId}
            AND through_sequence = ${checkpoint.throughSequence}
        `.pipe(execute, Effect.mapError(storageError("read idempotent checkpoint")));

        const existing = yield* decodeCheckpointRows(
          "effect_agent_checkpoints",
          `${checkpoint.threadId}/${checkpoint.throughSequence}`,
          checkpointRows,
        );

        if (existing.length > 1) {
          return yield* options.errors.corruption({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.threadId}/${checkpoint.throughSequence}`,
            message: "A checkpoint primary key returned more than one row.",
          });
        }
        if (existing.length === 1) {
          if (existing[0].checkpoint_json !== checkpoint.checkpointJson) {
            return yield* CheckpointRejected.make({
              threadId: checkpoint.threadId,
              reason: "corrupt",
            });
          }

          return;
        }

        yield* sql`
          INSERT INTO ${relation("effect_agent_checkpoints")} (
            thread_id,
            through_sequence,
            checkpoint_json
          ) VALUES (
            ${checkpoint.threadId},
            ${checkpoint.throughSequence},
            ${checkpoint.checkpointJson}
          )
        `.pipe(execute, Effect.mapError(storageError("insert checkpoint")));
      }),
    );
  });

  const loadCheckpoint = Effect.fnUntraced(function* (
    threadId: string,
    atOrBeforeSequence: CanonicalSequence,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        through_sequence,
        checkpoint_json
      FROM ${relation("effect_agent_checkpoints")}
      WHERE thread_id = ${threadId}
        AND through_sequence <= ${atOrBeforeSequence}
      ORDER BY through_sequence DESC
      LIMIT 1
    `.pipe(execute, Effect.mapError(storageError("load checkpoint")));

    return yield* decodeCheckpointRows(
      "effect_agent_checkpoints",
      `${threadId}<=${atOrBeforeSequence}`,
      rows,
    );
  });

  const isBatchBoundary = Effect.fnUntraced(function* (
    threadId: string,
    sequence: CanonicalSequence,
  ) {
    if (sequence === 0) return true;

    // The identity row and batch-end predicate reject missing and interior sequences.
    const rows = yield* sql<Record<string, unknown>>`
      SELECT last_sequence
      FROM ${relation("effect_agent_canonical_batches")}
      WHERE thread_id = ${threadId}
        AND batch_id = (
          SELECT ${recordBatchId(sql, options.namespace)} FROM ${relation("effect_agent_canonical_records")}
          WHERE thread_id = ${threadId} AND sequence = ${sequence}
        )
        AND last_sequence = ${sequence}
    `.pipe(execute, Effect.mapError(storageError("read canonical batch boundary")));

    const batches = yield* decodeRows(
      Schema.Array(
        Schema.Struct({ last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)) }),
      ),
    )("effect_agent_canonical_batches", `${threadId}/${sequence}`, rows);

    return batches.length === 1;
  });

  const scanStoredPayloads = () =>
    withReadTransaction("startup scan transaction")(
      Effect.gen(function* () {
        const threads = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              producer_epoch
            FROM ${relation("effect_agent_threads")}
            ORDER BY thread_id
          `.pipe(execute, Effect.mapError(storageError("scan threads")));

        const batches = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              batch_id,
              first_sequence,
              last_sequence,
              ${batchJson} AS batch_json
            FROM ${relation("effect_agent_canonical_batches")}
            ORDER BY thread_id, first_sequence
          `.pipe(execute, Effect.mapError(storageError("scan canonical batches")));

        const records = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              ${recordBatchId(sql, options.namespace)} AS batch_id,
              ${recordJson} AS record_json
            FROM ${relation("effect_agent_canonical_records")}
            ORDER BY thread_id, sequence
          `.pipe(execute, Effect.mapError(storageError("scan canonical records")));

        const checkpoints = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              through_sequence,
              checkpoint_json
            FROM ${relation("effect_agent_checkpoints")}
            ORDER BY thread_id, through_sequence
          `.pipe(execute, Effect.mapError(storageError("scan checkpoints")));

        return {
          threads: yield* decodeThreadRows("effect_agent_threads", "startup_scan", threads),
          batches: yield* decodeBatchRows(
            "effect_agent_canonical_batches",
            "startup_scan",
            batches,
          ),
          records: yield* decodeRecordRows(
            "effect_agent_canonical_records",
            "startup_scan",
            records,
          ),
          checkpoints: yield* decodeCheckpointRows(
            "effect_agent_checkpoints",
            "startup_scan",
            checkpoints,
          ),
        };
      }),
    );

  const journal = {
    checkCommitIntent: executions.checkCommitIntent,
    archives: archives.storage,
    archiveRanges: archives,
    lifecycle,
    work,
    append: (request: RawAppendRequest) =>
      appendKernel(
        request,
        Effect.suspend(() => readAppendThread(request.threadId)),
      ),
    /** Adapter-private body; a co-owned publisher must already hold this journal's writer. */
    appendInTransaction: (request: RawAppendRequest) =>
      appendInTransaction(
        request,
        Effect.suspend(() => readAppendThread(request.threadId)),
      ),
    getThread,
    isBatchBoundary,
    loadCheckpoint,
    materialize,
    read,
    saveCheckpoint,
    scanStoredPayloads,
    withWriteTransaction,
    withReadTransaction,
    isTransactionFailure: options.transactions.isTransactionFailure,
  } as const;

  return {
    journal,
    appendWithThread: (request: RawAppendRequest, thread: ThreadRow) =>
      appendKernel(request, Effect.succeed(thread)),
    /** Exclusive Run owner only; the caller already holds this journal's writer. */
    appendWithThreadInTransaction: (request: RawAppendRequest, thread: ThreadRow) =>
      appendInTransaction(request, Effect.succeed(thread)),
  };
});

/** Ordinary journals retain transactionally read producer and tail fences. */
export const makeSqlJournal = <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(
  options: SqlJournalOptions<S, C, W, F>,
) => Effect.map(makeSqlJournalKernel(options), (kernel) => kernel.journal);

export type SqlJournal<
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
> = Effect.Success<ReturnType<typeof makeSqlJournal<S, C, W, F>>>;
