import {
  executionStates,
  executionStateKey,
  retainExecutionState,
  type ExecutionPredecessor,
} from "./internal/SqlExecutionState.ts";
import { sqlStorageOwner } from "./internal/SqlOwner.ts";
export { sqlStorageOwner } from "./internal/SqlOwner.ts";
import { type RunId, ThreadId } from "@yielded/agent/identifiers";
import { LifecyclePublicationFact } from "@yielded/agent/lifecycle-publication";
import type { MessageDeliveryRecord } from "@yielded/agent/message-delivery";
import type { Digest } from "@yielded/agent/records";
import { CanonicalSequence, RunContinuation } from "@yielded/agent/records";
import { ProgressAppendReader, validateProgressAppend } from "@yielded/agent/run-continuation";
import { SqlStorageOwner } from "@yielded/agent/sql-storage-owner";
import {
  AppendConflict,
  type ExecutionCheckpoint,
  type PreparedAppend,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { WORK_INDEX_VERSION } from "@yielded/agent/thread-work";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import type * as Statement from "effect/sql/Statement";

import {
  makeCanonicalStorage,
  recordBatchId,
  runKey,
  type CapturedRecordStorage,
} from "./SqlCanonicalStorage.ts";
import { makeSqlLifecyclePublication } from "./SqlLifecyclePublication.ts";
import { publishCanonicalDeliveries } from "./SqlMessageDeliveryStore.ts";
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
  makeInitialProgressRead,
  makeExecutionRead,
  StoredExecution,
  decodeStoredExecution,
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

export class ThreadRow extends Schema.Class<ThreadRow>("ThreadRow")({
  thread_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
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

const decodeReceipt = Schema.decodeUnknownEffect(
  Schema.Array(StoredExecution).check(Schema.isMaxLength(1)),
);

const ThreadRows = Schema.Array(ThreadRow);
const BatchRows = Schema.Array(BatchRow);
const RecordRows = Schema.Array(RecordRow);
const decodeCanonicalSequence = Schema.decodeEffect(CanonicalSequence);

const isLifecyclePublicationFact = Schema.is(Schema.toType(LifecyclePublicationFact));

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
  readonly records: ReadonlyArray<
    PreparedAppend["records"][number] & {
      readonly readMetadata: CanonicalRecordMetadata;
      readonly storage: CapturedRecordStorage;
    }
  >;
  readonly deliveries: ReadonlyArray<MessageDeliveryRecord>;
  readonly progress: PreparedAppend["progress"];
  readonly executionWrites: PreparedAppend["executionWrites"];
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

/** Shared native transaction body; callers own the local capability check and mutation transaction. */
export const makeSqlExecutions = Effect.fnUntraced(function* (namespace?: string) {
  const sql = yield* SqlClient.SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);
  const read = yield* makeExecutionRead(namespace);
  const readInitial = yield* makeInitialProgressRead(namespace);
  const owner = yield* SqlStorageOwner;
  const identity = owner?.identity ?? owner;
  let predecessors = identity === undefined ? undefined : executionStates.get(identity);

  if (owner !== undefined && identity !== undefined && predecessors === undefined) {
    predecessors = new Map();
    executionStates.set(identity, predecessors);
    const retained = predecessors;

    owner.invalidators.add(() => retained.clear());
  }
  const key = (threadId: string, runId: string) => executionStateKey(namespace, threadId, runId);

  const readPrevious = (
    threadId: ThreadId,
    runId: RunId,
    throughSequence: CanonicalSequence,
  ): Effect.Effect<Option.Option<ExecutionPredecessor>, ThreadStoreError> => {
    const retained = predecessors?.get(key(threadId, runId));

    return retained !== undefined &&
      (retained.checkpoint !== undefined || retained.throughSequence === throughSequence)
      ? Effect.succeed(Option.fromUndefinedOr(retained.checkpoint))
      : read({ threadId, runId });
  };

  const retain = (threadId: ThreadId, write: RawAppendRequest["executionWrites"][number]) => {
    if (predecessors === undefined) return;
    const id = key(threadId, write.update.checkpoint.progress.runId);

    predecessors.delete(id);
    if (!write.update.retire && write.bytes <= 4 * 1024 * 1024) {
      const { progress, originalSequence, operational } = write.update.checkpoint;

      // Detach the validator's small working set from the prepared append and its payloads.
      retainExecutionState(predecessors, id, {
        checkpoint: {
          originalSequence,
          operational: operational.map(({ recordId }) => ({ recordId })),
          progress: RunContinuation.make(
            {
              ...progress,
              accounting: { ...progress.accounting },
              originalInput: { ...progress.originalInput },
              lastFact: { ...progress.lastFact },
              ...(progress.savedContext === undefined
                ? {}
                : { savedContext: { ...progress.savedContext } }),
              ...(progress.latestResponse === undefined
                ? {}
                : { latestResponse: { ...progress.latestResponse } }),
              ...(progress.terminal === undefined ? {} : { terminal: { ...progress.terminal } }),
            },
            { disableChecks: true },
          ),
        },
        bytes: write.bytes,
      });
    }
  };

  const failed = (message: string, cause?: unknown) =>
    ThreadStoreError.make({
      operation: "commit active execution",
      message,
      ...(cause === undefined ? {} : { cause }),
    });

  const query = <A extends object>(statement: Statement.Statement<A>) =>
    execute(statement).pipe(
      Effect.mapError((cause) => failed("Active execution storage failed", cause)),
    );

  const conflict = (
    request: RawAppendRequest,
    reason: "batch-content" | "tail" | "stale-command",
  ) =>
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
      SELECT thread_id, run_id, input_sequence, revision, checkpoint_json, last_commit_id, last_commit_digest
      FROM ${table("effect_agent_executions")}
      WHERE thread_id=${request.threadId} AND last_commit_id=${request.batchId} LIMIT 1
    `),
    ).pipe(Effect.mapError((cause) => failed("Invalid execution receipt", cause)));

    if (receipt === undefined) return undefined;
    const checkpoint = yield* decodeStoredExecution(receipt);

    if (receipt.last_commit_digest !== request.commitDigest)
      return yield* conflict(request, "batch-content");

    return RawAppendResult.make({
      firstSequence: CanonicalSequence.make(
        checkpoint.throughSequence - Math.max(0, request.records.length - 1),
      ),
      lastSequence: checkpoint.throughSequence,
      replayed: true,
    });
  });

  /** Active intent conflicts fail; retired settlements still adopt their canonical terminal proof. */
  const checkCommitIntent = (request: RawAppendRequest) => replay(request).pipe(Effect.asVoid);

  const rejectSuperseded = Effect.fnUntraced(function* (request: RawAppendRequest) {
    if (request.records.length > 0) return;
    for (const { update } of request.executionWrites) {
      const prior = Option.getOrUndefined(
        yield* readPrevious(
          request.threadId,
          update.checkpoint.progress.runId,
          request.expectedTailSequence,
        ),
      );

      if (
        prior !== undefined &&
        update.previousRevision < prior.progress.revision &&
        update.facts.every((fact) =>
          prior.operational.some((known) => known.recordId === fact.recordId),
        )
      )
        return yield* conflict(request, "stale-command");
    }
  });

  const validate = Effect.fnUntraced(function* (request: RawAppendRequest) {
    // Both validators use the same owner-fenced predecessor for this transaction.
    const prior = new Map<RunId, ExecutionCheckpoint["progress"] | undefined>();

    for (const { update } of request.executionWrites) {
      const checkpoint = update.checkpoint;
      const next = checkpoint.progress;

      if (prior.has(next.runId)) return yield* failed("Duplicate execution update");

      const previous = Option.getOrUndefined(
        yield* readPrevious(request.threadId, next.runId, request.expectedTailSequence),
      );

      prior.set(next.runId, previous?.progress);

      if ((previous?.progress.revision ?? 0) !== update.previousRevision) {
        // Already retained operational facts cannot be charged again after their retry window.
        // A newly prepared command with new facts remains a CAS conflict for reprepare.
        const superseded =
          request.records.length === 0 &&
          previous !== undefined &&
          update.previousRevision < previous.progress.revision &&
          update.facts.every((fact) =>
            previous.operational.some((known) => known.recordId === fact.recordId),
          );

        return yield* conflict(request, superseded ? "stale-command" : "tail");
      }
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
    }
    yield* validateProgressAppend(
      request.progress,
      request.executionWrites.map(({ update }) => update),
    ).pipe(
      Effect.provideService(ProgressAppendReader, {
        previous: (runId) => Effect.succeed(prior.get(runId)),
        initial: (next) => {
          const retained = predecessors?.get(key(request.threadId, next.runId));

          // Fresh preparation proved absence under this same owner. A changed canonical
          // frontier, intervening execution write or invalidation requires the full read.
          return retained !== undefined &&
            retained.checkpoint === undefined &&
            retained.throughSequence === request.expectedTailSequence
            ? Effect.succeed([])
            : readInitial(request, next);
        },
      }),
    );
  });

  const prepareCommit = (request: RawAppendRequest) =>
    request.executionWrites.map((write) => {
      const { update, checkpointJson } = write;
      const run = JSON.stringify(update.checkpoint.progress.runId);

      const statement = update.retire
        ? sql`DELETE FROM ${table("effect_agent_executions")}
          WHERE thread_id=${request.threadId} AND run_id=${run} AND revision=${update.previousRevision} RETURNING run_id`
        : sql`INSERT INTO ${table("effect_agent_executions")}
        (thread_id, run_id, input_sequence, revision, checkpoint_json, last_commit_id, last_commit_digest)
        VALUES (${request.threadId}, ${run}, ${update.checkpoint.originalSequence}, ${update.checkpoint.progress.revision}, ${checkpointJson}, ${request.batchId}, ${request.commitDigest})
        ON CONFLICT(thread_id, run_id) DO UPDATE SET revision=excluded.revision, checkpoint_json=excluded.checkpoint_json,
          last_commit_id=excluded.last_commit_id, last_commit_digest=excluded.last_commit_digest
        WHERE ${table("effect_agent_executions")}.revision=${update.previousRevision} RETURNING run_id`;

      return {
        statement,
        accept: (changed: ReadonlyArray<unknown>) => {
          const expected = update.retire && update.previousRevision === 0 ? 0 : 1;

          if (changed.length !== expected) return Result.fail(conflict(request, "tail"));
          retain(request.threadId, write);

          return Result.succeed(undefined);
        },
      };
    });

  const commit = Effect.fnUntraced(function* (request: RawAppendRequest, _result: RawAppendResult) {
    for (const prepared of prepareCommit(request))
      yield* Effect.fromResult(prepared.accept(yield* query(prepared.statement)));
  });

  return { read, replay, checkCommitIntent, rejectSuperseded, validate, commit, prepareCommit };
});

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-sql/RawReadRequest",
)({
  threadId: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
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
  const owner = yield* sqlStorageOwner(sql, options.namespace);

  const publishDeliveries = (records: ReadonlyArray<MessageDeliveryRecord>) =>
    publishCanonicalDeliveries(owner, records).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "publish canonical deliveries",
          message: cause.message,
          deliveryReason: cause.reason,
          cause,
        }),
      ),
    );

  const withReadTransaction: SqlTransactions<S, W>["withReadTransaction"] = (operation) => (body) =>
    owner.withMutation(options.transactions.withReadTransaction(operation)(body));

  const withWriteTransaction: SqlTransactions<S, W>["withWriteTransaction"] =
    (operation) => (body) =>
      owner.withMutation(options.transactions.withWriteTransaction(operation)(body));

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
  const decodeThreadRows = decodeRows(ThreadRows);
  const decodeThreadRow = decodeSingleRow(ThreadRows);
  const decodeBatchRows = decodeRows(BatchRows);

  const decodeRecordRows = decodeRows(RecordRows);

  const storageError =
    (operation: string) =>
    (error: SqlError.SqlError): S =>
      options.errors.storage({ operation, message: error.message, cause: error });

  const materialize = Effect.fnUntraced(function* (
    threadId: ThreadId,
    createdAt: string,
  ): Effect.fn.Return<void, C | S | W> {
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
            tail_sequence
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${threadId} AND created_at IS NOT NULL
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
              work_version
            ) VALUES (
              ${threadId},
              ${createdAt},
              0,
              ${WORK_INDEX_VERSION}
            ) ON CONFLICT(thread_id) DO UPDATE SET created_at=excluded.created_at
            WHERE ${relation("effect_agent_threads")}.created_at IS NULL
          `.pipe(execute, Effect.mapError(storageError("materialize thread")));

          return;
        }
      }),
    );
  });

  const getThread = Effect.fnUntraced(function* (threadId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        created_at,
        tail_sequence
      FROM ${relation("effect_agent_threads")}
      WHERE thread_id = ${threadId} AND created_at IS NOT NULL
    `.pipe(execute, Effect.mapError(storageError("read thread")));

    return yield* decodeThreadRows("effect_agent_threads", threadId, rows);
  });

  const readAppendThread = Effect.fnUntraced(function* (threadId: string) {
    const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${threadId} AND created_at IS NOT NULL
        `.pipe(execute, Effect.mapError(storageError("read append tail")));

    return yield* decodeThreadRow("effect_agent_threads", threadId, threadRows);
  });

  const appendInTransaction = Effect.fnUntraced(function* (
    request: RawAppendRequest,
    readThread: Effect.Effect<ThreadRow, C | S>,
  ): Effect.fn.Return<RawAppendResult, AppendConflict | ThreadStoreError | C | S | F | W> {
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

    const replay = yield* executions.replay(request);

    if (replay !== undefined) {
      yield* publishDeliveries(request.deliveries);

      return replay;
    }

    const existing = yield* archives.replay(request.threadId, request.batchId);

    if (existing !== undefined) {
      if (existing.batch_json !== request.batchJson) {
        return yield* AppendConflict.make({
          threadId: request.threadId,
          batchId: request.batchId,
          reason: "batch-content",
        });
      }

      yield* publishDeliveries(request.deliveries);

      return RawAppendResult.make({
        firstSequence: existing.first_sequence,
        lastSequence: existing.last_sequence,
        replayed: true,
      });
    }

    yield* executions.rejectSuperseded(request);

    if (request.expectedTailSequence !== thread.tail_sequence) {
      return yield* AppendConflict.make({
        threadId: request.threadId,
        batchId: request.batchId,
        reason: "tail",
        actualTailSequence: thread.tail_sequence,
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

    const result = RawAppendResult.make({
      firstSequence,
      lastSequence,
      replayed: false,
    });

    if (request.records.length > 0) {
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

      const inserted =
        yield* sql`INSERT INTO ${relation("effect_agent_canonical_records")} ${sql.insert(
          physicalRecords.slice(at, at + 5).map((record) => record.columns),
        )} ON CONFLICT(thread_id, record_id) DO NOTHING RETURNING 1`.pipe(
          execute,
          Effect.mapError(storageError("insert canonical record")),
        );

      if (inserted.length !== records.length)
        return yield* AppendConflict.make({
          threadId: request.threadId,
          batchId: request.batchId,
          reason: "record-identity",
        });

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
      const workFields = yield* work
        .apply(
          request.threadId,
          firstSequence,
          request.records.map((record) => record.canonical),
        )
        .pipe(Effect.mapError(workFailure));

      yield* sql`UPDATE ${relation("effect_agent_threads")} SET ${sql.update({ tail_sequence: lastSequence, ...workFields })}
        WHERE thread_id=${request.threadId}`.pipe(
        execute,
        Effect.mapError(storageError("advance thread tail")),
      );
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
    yield* publishDeliveries(request.deliveries);
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

  const scanStoredPayloads = () =>
    withReadTransaction("startup scan transaction")(
      Effect.gen(function* () {
        const threads = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence
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
        };
      }),
    );

  const journal = {
    owner,
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
    materialize,
    read,
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

/** All journals use the physical owner gate and transactionally read tail CAS. */
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
