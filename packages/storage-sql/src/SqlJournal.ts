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
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

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
  tail_digest: BoundedStoredText,
  tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

export class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_digest: BoundedStoredText,
  batch_id: BoundedIdentifier,
  batch_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  tail_digest: BoundedStoredText,
}) {}

export class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: BoundedIdentifier,
  thread_id: BoundedIdentifier,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  tail_digest: BoundedStoredText,
  through_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

/**
 * Trusted adapter SPI produced by prepareSqlAppend, never a wire or caller-data boundary.
 * Canonical values, encoded strings and UTF-8 sizes remain paired; ThreadStore owns capture
 * and validation before entering this journal, whose caller already holds SQL storage authority.
 */
export interface RawAppendRequest {
  readonly batchDigest: Digest;
  readonly batchId: PreparedAppend["batch"]["batchId"];
  readonly batchJson: string;
  readonly batchHeaderJson: string;
  readonly batchBytes: number;
  readonly threadId: PreparedAppend["threadId"];
  readonly expectedTailDigest: Digest;
  readonly expectedTailSequence: CanonicalSequence;
  readonly producerEpoch: ProducerEpoch;
  readonly records: ReadonlyArray<
    PreparedAppend["records"][number] & {
      readonly readMetadata: CanonicalRecordMetadata;
    }
  >;
  readonly progress: PreparedAppend["progress"];
  readonly tailDigest: Digest;
}

export class RawAppendResult extends Schema.Class<RawAppendResult>(
  "@effect-agent/storage-sql/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

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
  tailDigest: BoundedStoredText,
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
    Schema.Array(Schema.Struct({ record_id: BoundedIdentifier })),
  );

  const decodeCheckpointRows = decodeRows(Schema.Array(CheckpointRow));
  const decodeCanonicalSequence = Schema.decodeEffect(CanonicalSequence);
  const isDigest = Schema.is(Digest);

  const isLifecyclePublicationFact = Schema.is(Schema.toType(LifecyclePublicationFact));

  const storageError =
    (operation: string) =>
    (error: SqlError): S =>
      options.errors.storage({ operation, message: error.message, cause: error });

  const materialize = Effect.fnUntraced(function* (
    threadId: ThreadId,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<void, FenceRejected | C | S | W> {
    if (
      threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(emptyTailDigest) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* options.errors.storage({
        operation: "materialize thread",
        message: "Thread identity or initial digest exceeds the SQL storage bounds.",
      });
    }
    yield* withWriteTransaction("materialize transaction")(
      Effect.gen(function* () {
        const existingRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
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
              tail_digest,
              producer_epoch
            ) VALUES (
              ${threadId},
              ${createdAt},
              0,
              ${emptyTailDigest},
              ${producerEpoch}
            )
          `.pipe(execute, Effect.mapError(storageError("materialize thread")));

          yield* sql`INSERT INTO ${relation("effect_agent_transfer_state")}
            (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count)
            VALUES (${threadId}, 0, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO NOTHING`.pipe(
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
        tail_digest,
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
            tail_digest,
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
      storedTextBytes(request.batchDigest) > MAX_STORED_TEXT_BYTES ||
      storedTextBytes(request.tailDigest) > MAX_STORED_TEXT_BYTES ||
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

    const batchRows = yield* sql<Record<string, unknown>>`
        SELECT
          thread_id,
          batch_id,
          first_sequence,
          last_sequence,
          batch_digest,
          tail_digest,
          '' AS batch_json
        FROM ${relation("effect_agent_canonical_batches")}
        WHERE thread_id = ${request.threadId}
          AND batch_id = ${request.batchId}
      `.pipe(execute, Effect.mapError(storageError("read idempotent batch")));

    const batches = yield* decodeBatchRows(
      "effect_agent_canonical_batches",
      `${request.threadId}/${request.batchId}`,
      batchRows,
    );

    if (batches.length > 1) {
      return yield* options.errors.corruption({
        table: "effect_agent_canonical_batches",
        rowKey: `${request.threadId}/${request.batchId}`,
        message: "A canonical batch primary key returned more than one row.",
      });
    }
    if (batches.length === 1) {
      const existing = batches[0];

      if (existing.batch_digest !== request.batchDigest) {
        return yield* AppendConflict.make({
          threadId: request.threadId,
          batchId: request.batchId,
          reason: "batch-digest",
        });
      }

      return RawAppendResult.make({
        firstSequence: existing.first_sequence,
        lastSequence: existing.last_sequence,
        replayed: true,
        tailDigest: existing.tail_digest,
      });
    }

    if (
      request.expectedTailSequence !== thread.tail_sequence ||
      request.expectedTailDigest !== thread.tail_digest
    ) {
      return yield* AppendConflict.make({
        threadId: request.threadId,
        batchId: request.batchId,
        reason: "tail",
        ...(isDigest(thread.tail_digest)
          ? { actualTailSequence: thread.tail_sequence, actualTailDigest: thread.tail_digest }
          : {}),
      });
    }

    const existingRecordRows = yield* sql<Record<string, unknown>>`
        SELECT record_id
        FROM ${relation("effect_agent_canonical_records")}
        WHERE thread_id = ${request.threadId}
          AND record_id IN ${sql.in(recordIds)}
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

    const firstSequence = yield* decodeCanonicalSequence(thread.tail_sequence + 1).pipe(
      Effect.mapError((error) =>
        options.errors.storage({
          cause: error,
          operation: "append canonical batch",
          message: error.message,
        }),
      ),
    );

    const lastSequence = yield* decodeCanonicalSequence(
      firstSequence + request.records.length - 1,
    ).pipe(
      Effect.mapError((error) =>
        options.errors.storage({
          cause: error,
          operation: "append canonical batch",
          message: error.message,
        }),
      ),
    );

    yield* validateProgress(request);
    yield* archives.append(request, firstSequence, lastSequence);
    yield* sql`
        INSERT INTO ${relation("effect_agent_canonical_batches")} (
          thread_id,
          batch_id,
          first_sequence,
          last_sequence,
          batch_digest,
          tail_digest,
          batch_header_json
        ) VALUES (
          ${request.threadId},
          ${request.batchId},
          ${firstSequence},
          ${lastSequence},
          ${request.batchDigest},
          ${request.tailDigest},
          ${request.batchHeaderJson}
        )
      `.pipe(execute, Effect.mapError(storageError("insert canonical batch")));
    yield* failpoint("append:after-batch-insert");

    // Five canonical rows (17 columns each) and 33 membership rows (three columns each)
    // stay below every adapter's parameter ceiling, including Durable Objects' limit of 100.
    for (let at = 0; at < request.records.length; at += 5) {
      const records = request.records.slice(at, at + 5);

      yield* sql`INSERT INTO ${relation("effect_agent_canonical_records")} ${sql.insert(
        records.map((record, index) => ({
          thread_id: request.threadId,
          sequence: firstSequence + at + index,
          record_id: record.recordId,
          batch_id: request.batchId,
          record_json: record.recordJson,
          ...record.readMetadata.columns,
        })),
      )}`.pipe(execute, Effect.mapError(storageError("insert canonical record")));

      const memberships = records.flatMap((record, index) =>
        record.readMetadata.runIds.map((runId) => ({
          thread_id: request.threadId,
          run_id: runId,
          sequence: firstSequence + at + index,
        })),
      );

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

    yield* sql`
        UPDATE ${relation("effect_agent_threads")}
        SET
          tail_sequence = ${lastSequence},
          tail_digest = ${request.tailDigest},
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
    yield* failpoint("append:after-tail-update");

    return RawAppendResult.make({
      firstSequence,
      lastSequence,
      replayed: false,
      tailDigest: request.tailDigest,
    });
  });

  const appendKernel = (request: RawAppendRequest, readThread: Effect.Effect<ThreadRow, C | S>) =>
    withWriteTransaction("append transaction")(appendInTransaction(request, readThread));

  const read = Effect.fnUntraced(function* (request: RawReadRequest) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        sequence,
        record_id,
        batch_id,
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
            tail_digest,
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
            reason: "digest-mismatch",
          });
        }

        const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            through_sequence,
            tail_digest,
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
          if (
            existing[0].tail_digest !== checkpoint.tailDigest ||
            existing[0].checkpoint_json !== checkpoint.checkpointJson
          ) {
            return yield* CheckpointRejected.make({
              threadId: checkpoint.threadId,
              reason: "digest-mismatch",
            });
          }

          return;
        }

        yield* sql`
          INSERT INTO ${relation("effect_agent_checkpoints")} (
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          ) VALUES (
            ${checkpoint.threadId},
            ${checkpoint.throughSequence},
            ${checkpoint.tailDigest},
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
        tail_digest,
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

  const getTailDigestAt = Effect.fnUntraced(function* (
    threadId: string,
    sequence: CanonicalSequence,
  ) {
    if (sequence === 0) {
      const threads = yield* getThread(threadId);

      return threads.length === 0
        ? []
        : [threads[0].tail_sequence === 0 ? threads[0].tail_digest : undefined].filter(
            (value): value is string => value !== undefined,
          );
    }

    // Resolve both primary keys; last_sequence alone scans the Thread's archived batches.
    // Retain the batch-end predicate so an interior record cannot certify a checkpoint.
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        batch_id,
        first_sequence,
        last_sequence,
        batch_digest,
        tail_digest,
        '' AS batch_json
      FROM ${relation("effect_agent_canonical_batches")}
      WHERE thread_id = ${threadId}
        AND batch_id = (
          SELECT batch_id FROM ${relation("effect_agent_canonical_records")}
          WHERE thread_id = ${threadId} AND sequence = ${sequence}
        )
        AND last_sequence = ${sequence}
    `.pipe(execute, Effect.mapError(storageError("read canonical digest at sequence")));

    const batches = yield* decodeBatchRows(
      "effect_agent_canonical_batches",
      `${threadId}/${sequence}`,
      rows,
    );

    return batches.map((batch) => batch.tail_digest);
  });

  const scanStoredPayloads = () =>
    withReadTransaction("startup scan transaction")(
      Effect.gen(function* () {
        const threads = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
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
              batch_digest,
              tail_digest,
              ${batchJson} AS batch_json
            FROM ${relation("effect_agent_canonical_batches")}
            ORDER BY thread_id, first_sequence
          `.pipe(execute, Effect.mapError(storageError("scan canonical batches")));

        const records = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              ${recordJson} AS record_json
            FROM ${relation("effect_agent_canonical_records")}
            ORDER BY thread_id, sequence
          `.pipe(execute, Effect.mapError(storageError("scan canonical records")));

        const checkpoints = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              through_sequence,
              tail_digest,
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
    getTailDigestAt,
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
