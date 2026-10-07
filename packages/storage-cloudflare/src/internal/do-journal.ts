import type { RawAppendRequest } from "@yielded/agent-storage-sql/sql-journal";
import {
  makeSqlLifecyclePublication,
  type SqlLifecycleRetainMany,
  SqlLifecycleSource,
  SqlLifecycleRetainer,
} from "@yielded/agent-storage-sql/sql-lifecycle-publication";
import { SqlStorageProgress } from "@yielded/agent-storage-sql/sql-storage-progress";
import {
  canonicalRecordJson,
  makeSqlThreadArchiveRange,
} from "@yielded/agent-storage-sql/sql-thread-archive-range";
import { makeProgressAppendValidation } from "@yielded/agent-storage-sql/sql-thread-native-reads";
import { makeSqlThreadWork } from "@yielded/agent-storage-sql/sql-thread-work";
import { ThreadId } from "@yielded/agent/identifiers";
import {
  LifecyclePublicationFact,
  LifecyclePublicationError,
} from "@yielded/agent/lifecycle-publication";
import { CanonicalRecord, CanonicalSequence, ProducerEpoch } from "@yielded/agent/records";
import { SqlStorageOwner } from "@yielded/agent/sql-memory-store";
import { ThreadStoreDiagnostic, ThreadStoreError } from "@yielded/agent/thread-store";
import { Cause, Clock, Effect, Option, Schema, Stream } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError } from "effect/sql/SqlError";

import {
  type DoStorageFailpointError,
  DoAppendConflict,
  DoCheckpointConflict,
  DoFenceRejected,
  DoStorageCorruptionError,
  DoStorageError,
  DoValueBoundExceeded,
  type DoStorageFailpointLocation,
} from "../DoStorageError.ts";
import { ensureDoStorageLayout } from "./migrations.ts";
import { ownedState, ownedRows, type OwnedState } from "./owned-state.ts";
import {
  isAppendContention,
  annotateStorageError,
  storageResult,
  withStorageSpan,
} from "./storage-span.ts";

/**
 * Static schema ceiling for stored text columns. Writes are bounded in BYTES by the
 * configured `maxStoredValueBytes` (always ≤ 2,000,000); UTF-8 byte length is never smaller
 * than UTF-16 string length, so any value that passed the byte bound also passes this
 * decode-side character ceiling.
 */
const BoundedStoredText = Schema.String.check(Schema.isMaxLength(2_000_000));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const MAX_IDENTIFIER_LENGTH = 1_024;
const MAX_READ_PAGE_JSON_BYTES = 4 * 1024 * 1024;
// A shared isolate budget permits multi-page hydration without multiplying its
// retained payload by the number of resident Durable Object instances.
const MAX_RECORD_CACHE_JSON_BYTES = 8 * 1024 * 1024;
const MAX_RECORD_CACHE_ENTRIES = 4_096;
/** Durable Object SQL storage allows at most 100 bound parameters per statement. */
const MAX_BOUND_PARAMETERS = 100;
const isSqlError = Schema.is(SqlError);

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

const chunked = <A>(values: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> => {
  const chunks: Array<ReadonlyArray<A>> = [];

  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }

  return chunks;
};

class ThreadRow extends Schema.Class<ThreadRow>("ThreadRow")({
  thread_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: ProducerEpoch,
  tail_digest: BoundedStoredText,
  tail_sequence: CanonicalSequence,
}) {}

const threadRows = ownedRows(
  ThreadRow,
  "effect_agent_threads",
  (row) => row.thread_id,
  "thread_id",
);

class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_digest: BoundedStoredText,
  batch_id: BoundedIdentifier,
  batch_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  first_sequence: CanonicalSequence,
  last_sequence: CanonicalSequence,
  tail_digest: BoundedStoredText,
}) {}

class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: BoundedIdentifier,
  thread_id: BoundedIdentifier,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  sequence: CanonicalSequence,
}) {}

interface RecordCacheEntry {
  readonly row: RecordRow;
  readonly bytes: number;
  readonly evict: () => void;
}

// Entries retain only their local row map, never a storage owner or incarnation.
const isolateRecords = new Map<RecordCacheEntry, true>();
let isolateRecordBytes = 0;

const releaseRecord = (entry: RecordCacheEntry) => {
  if (!isolateRecords.delete(entry)) return;
  isolateRecordBytes -= entry.bytes;
  entry.evict();
};

const recordCaches = new WeakMap<OwnedState, ReturnType<typeof makeRecordCache>>();

const makeRecordCache = () => {
  const records = new Map<string, RecordCacheEntry>();
  const key = (thread: string, sequence: number) => JSON.stringify([thread, sequence]);

  return {
    clear: () => {
      for (const entry of records.values()) releaseRecord(entry);
    },
    get: (thread: string, sequence: number) => records.get(key(thread, sequence))?.row,
    prefix: (thread: string, through: number): ReadonlyArray<RecordRow> | undefined => {
      const prefix: Array<RecordRow> = [];

      for (let sequence = 1; sequence <= through; sequence++) {
        const row = records.get(key(thread, sequence));

        if (row === undefined) return undefined;
        prefix.push(row.row);
      }

      return prefix;
    },
    put: (row: RecordRow) => {
      const id = key(row.thread_id, row.sequence);
      const prior = records.get(id);

      if (prior?.row === row) return;
      const size = storedTextBytes(row.record_json);

      if (prior !== undefined) releaseRecord(prior);

      const entry: RecordCacheEntry = {
        row,
        bytes: size,
        evict: () => records.delete(id),
      };

      records.set(id, entry);
      isolateRecords.set(entry, true);
      isolateRecordBytes += size;
      while (
        isolateRecordBytes > MAX_RECORD_CACHE_JSON_BYTES ||
        isolateRecords.size > MAX_RECORD_CACHE_ENTRIES
      ) {
        const oldest = isolateRecords.keys().next().value;

        if (oldest === undefined) break;
        releaseRecord(oldest);
      }
    },
  };
};

const ReadPlanRow = Schema.Struct({
  sequence: CanonicalSequence,
  record_json_bytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_READ_PAGE_JSON_BYTES)),
});

type ReadPage = [typeof ReadPlanRow.Type, ...Array<typeof ReadPlanRow.Type>];

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  tail_digest: BoundedStoredText,
  through_sequence: CanonicalSequence,
}) {}

const LifecycleCursorRow = Schema.Struct({
  thread_id: BoundedIdentifier,
  through_sequence: CanonicalSequence,
});

const lifecycleCursorRows = ownedRows(
  LifecycleCursorRow,
  "effect_agent_lifecycle_cursors",
  (row) => row.thread_id,
  "thread_id",
);

const initializedLifecycleSources = new WeakSet<OwnedState>();

export type { RawAppendRequest } from "@yielded/agent-storage-sql/sql-journal";

export class RawAppendResult extends Schema.Class<RawAppendResult>(
  "@effect-agent/storage-cloudflare/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-cloudflare/RawReadRequest",
)({
  threadId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-cloudflare/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

type AppendError =
  | ThreadStoreError
  | DoAppendConflict
  | DoFenceRejected
  | DoStorageCorruptionError
  | DoStorageError
  | DoStorageFailpointError
  | DoValueBoundExceeded;

type CheckpointError =
  | DoCheckpointConflict
  | DoStorageCorruptionError
  | DoStorageError
  | DoValueBoundExceeded;

type DoJournalFailpoint = (
  location: DoStorageFailpointLocation,
) => Effect.Effect<void, DoStorageFailpointError>;

const noFailpoint: DoJournalFailpoint = () => Effect.void;

const storageError =
  (operation: string) =>
  (error: SqlError): DoStorageError =>
    DoStorageError.make({
      cause: error,
      operation,
      message: error.message,
      diagnostic: ThreadStoreDiagnostic.make({
        causeTag: error._tag,
        operation,
        issueTag: error.reason._tag,
      }),
    });

/** Decode raw Durable Object SQLite rows against a Schema, reporting failures as typed corruption. */
export const decodeRows = <A, I>(
  schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
  table: string,
  rowKey: string,
  rows: unknown,
  decoder: string = table,
): Effect.Effect<ReadonlyArray<A>, DoStorageCorruptionError> =>
  Schema.decodeUnknownEffect(schema)(rows).pipe(
    Effect.mapError((error) =>
      DoStorageCorruptionError.make({
        table,
        rowKey,
        message: "Stored rows do not satisfy the storage schema",
        diagnostic: ThreadStoreDiagnostic.make({
          causeTag: error._tag,
          operation: "decode storage rows",
          decoder,
          issueTag: error.issue._tag,
        }),
      }),
    ),
    Effect.tapError((error) =>
      Effect.annotateCurrentSpan({
        "storage.failure.operation": error.diagnostic?.operation,
        "storage.failure.cause": error.diagnostic?.causeTag,
        "storage.failure.decoder": decoder,
        "storage.failure.issue": error.diagnostic?.issueTag,
      }),
    ),
  );

/** Decode exactly one raw row against a Schema, reporting failures as typed corruption. */
export const decodeSingleRow = <A, I>(
  schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
  table: string,
  rowKey: string,
  rows: unknown,
): Effect.Effect<A, DoStorageCorruptionError> =>
  decodeRows(schema, table, rowKey, rows).pipe(
    Effect.flatMap((decoded) =>
      decoded.length === 1
        ? Effect.succeed(decoded[0])
        : Effect.fail(
            DoStorageCorruptionError.make({
              table,
              rowKey,
              message: `Expected exactly one row but found ${decoded.length}.`,
            }),
          ),
    ),
  );

/** Open a validated layout and bind this Object's storage owner. */
const ensureCurrentStorage = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint = noFailpoint,
  maxStoredValueBytes: number,
) {
  yield* ensureDoStorageLayout().pipe(Effect.provideService(SqlClient.SqlClient, sql));

  const state = yield* ownedState(sql);
  const owner = (yield* SqlStorageOwner) ?? state;
  let journal: DoJournal | undefined;

  const lifecycle = yield* makeSqlLifecyclePublication(undefined, maxStoredValueBytes).pipe(
    Effect.provideService(SqlStorageOwner, owner),
    Effect.provideService(SqlLifecycleSource, {
      beforeRetain: (threadId) =>
        Effect.suspend(() =>
          journal === undefined
            ? Effect.fail(LifecyclePublicationError.make({ reason: "unavailable" }))
            : journal.flushCanonical(threadId),
        ),
      beforePending: Effect.suspend(() =>
        journal === undefined
          ? Effect.fail(LifecyclePublicationError.make({ reason: "unavailable" }))
          : journal.flushPublications(),
      ),
    }),
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) =>
      DoStorageError.make({
        operation: "initialize lifecycle publication",
        message: "Native publication storage unavailable",
        cause,
      }),
    ),
  );

  journal = yield* makeJournal(sql, failpoint, maxStoredValueBytes, lifecycle, state).pipe(
    Effect.provideService(SqlStorageOwner, owner),
  );
  if (lifecycle !== undefined) yield* journal.initializeLifecycleSource();

  return journal;
});

const makeJournal = (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint,
  maxStoredValueBytes: number,
  lifecycle: Effect.Success<ReturnType<typeof makeSqlLifecyclePublication>>,
  state: OwnedState,
) =>
  Effect.gen(function* () {
    const owner = (yield* SqlStorageOwner) ?? state;
    const progress = yield* SqlStorageProgress;

    const work = yield* makeSqlThreadWork({
      read: (body) => state.read(body),
      write: (body) =>
        owner.transaction(body.pipe(Effect.tap(() => state.invalidate))).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.catchTag("SqlError", (cause) =>
            ThreadStoreError.make({
              operation: "rebuild Thread work",
              message: "Thread work transaction failed",
              cause,
            }),
          ),
        ),
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));

    const workFailure = (cause: unknown) =>
      DoStorageError.make({
        operation: "publish Thread work",
        message: "Canonical work metadata could not be published",
        cause,
      });

    const validateProgress = yield* makeProgressAppendValidation().pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );

    const threads = threadRows(state, sql);
    const cursors = lifecycleCursorRows(state, sql);
    let records = recordCaches.get(state);

    if (records === undefined) {
      records = makeRecordCache();
      recordCaches.set(state, records);
      state.invalidators.add(records.clear);
    }
    const recordCache = records;

    const archives = yield* makeSqlThreadArchiveRange({
      read: (body) => state.read(body),
      write: (body) =>
        owner.transaction(body.pipe(Effect.tap(() => state.invalidate))).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.catchTag("SqlError", (cause) =>
            ThreadStoreError.make({
              operation: "archive write",
              message: "Archive transaction failed.",
              cause,
            }),
          ),
        ),
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));

    const recordJson = canonicalRecordJson(sql);

    /** Typed pre-write refusal for any single value over the configured byte bound. */
    const checkValueBound = (
      operation: string,
      value: string,
    ): Effect.Effect<void, DoValueBoundExceeded> => {
      const actualBytes = storedTextBytes(value);

      return actualBytes > maxStoredValueBytes
        ? Effect.fail(
            DoValueBoundExceeded.make({
              actualBytes,
              maxBytes: maxStoredValueBytes,
              operation,
            }),
          )
        : Effect.void;
    };

    /**
     * Runs one journal write transaction on the Durable Object storage-backed
     * `withTransaction` (`ctx.storage.transaction()` under the hood). Within one Durable
     * Object there is exactly ONE writer, so the Node `BEGIN IMMEDIATE` + busy-retry +
     * `SqliteWriteContention` machinery has no analogue here and is deliberately absent.
     * Ownership-token and epoch checks still run INSIDE the transaction, so fencing atomicity
     * (DUR-006) is preserved identically.
     *
     * Journal write transactions are always top level: the Durable Object client rejects
     * nested transactions, so new journal operations must not wrap this helper inside another
     * transaction. Expected outcomes may be captured only when every such refusal precedes
     * mutation; real faults still roll back. Successful preflight reads never authorize a write.
     */
    const withWriteTransaction =
      (operation: string, expected: (error: { readonly _tag: string }) => boolean = () => false) =>
      <A, E extends { readonly _tag: string }>(
        effect: Effect.Effect<A, E>,
      ): Effect.Effect<A, E | DoStorageError> =>
        owner.transaction(storageResult(effect, expected)).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError((error) => (isSqlError(error) ? storageError(operation)(error) : error)),
          Effect.tapError(annotateStorageError),
          Effect.withSpan("DoJournal.withWriteTransaction", { attributes: { operation } }),
          Effect.flatMap(Effect.fromResult),
        );

    const requireTransferState = Effect.fnUntraced(function* (threadId: string) {
      const rows = yield* state
        .read(sql`SELECT thread_id FROM effect_agent_transfer_state WHERE thread_id=${threadId}`)
        .pipe(Effect.mapError(storageError("read transfer counters")));

      if (rows.length !== 1)
        return yield* DoStorageCorruptionError.make({
          table: "effect_agent_transfer_state",
          rowKey: threadId,
          message: "Materialized Thread has no transfer counter row.",
        });
    });

    const materialize = Effect.fnUntraced(
      function* (
        threadId: string,
        createdAt: string,
        emptyTailDigest: string,
        producerEpoch: ProducerEpoch,
      ): Effect.fn.Return<
        void,
        DoFenceRejected | DoStorageCorruptionError | DoStorageError | DoValueBoundExceeded
      > {
        if (threadId.length > MAX_IDENTIFIER_LENGTH) {
          return yield* DoStorageError.make({
            operation: "materialize thread",
            message: "Thread identity exceeds the Durable Object storage bounds.",
          });
        }
        yield* checkValueBound("materialize thread", emptyTailDigest);
        // Refusals and idempotent materialization need no transaction. Recheck before writes.
        const current = yield* getThread(threadId);

        if (current.length === 1 && producerEpoch <= current[0].producer_epoch) {
          if (producerEpoch === current[0].producer_epoch) {
            yield* requireTransferState(threadId);

            return;
          }

          return yield* DoFenceRejected.make({
            producerEpoch,
            actualEpoch: current[0].producer_epoch,
            message: "The materialization producer epoch is stale.",
          });
        }
        yield* withWriteTransaction(
          "materialize transaction",
          (error) => error._tag === "DoFenceRejected",
        )(
          Effect.gen(function* () {
            const existingRows = yield* getThread(threadId);

            const existing = yield* decodeRows(
              Schema.Array(ThreadRow),
              "effect_agent_threads",
              threadId,
              existingRows,
            );

            if (existing.length > 1) {
              return yield* DoStorageCorruptionError.make({
                table: "effect_agent_threads",
                rowKey: threadId,
                message: "A thread primary key returned more than one row.",
              });
            }
            if (existing.length === 0) {
              yield* sql`
            INSERT INTO effect_agent_threads (
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
           RETURNING *`.pipe(
                threads.write,
                Effect.mapError((error) =>
                  error._tag === "SqlError" ? storageError("materialize thread")(error) : error,
                ),
              );

              yield* sql`INSERT INTO effect_agent_transfer_state
                (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count)
                VALUES (${threadId}, 0, 0, 0, 0, 0, 0) ON CONFLICT(thread_id) DO NOTHING`.pipe(
                Effect.mapError(storageError("initialize transfer counters")),
              );

              yield* work
                .initialize(
                  yield* Schema.decodeEffect(ThreadId)(threadId).pipe(Effect.mapError(workFailure)),
                )
                .pipe(Effect.mapError(workFailure));
              if (lifecycle !== undefined)
                yield* sql`INSERT INTO effect_agent_lifecycle_cursors (thread_id, through_sequence)
              VALUES (${threadId}, 0) RETURNING *`.pipe(
                  cursors.write,
                  Effect.mapError((error) =>
                    error._tag === "SqlError"
                      ? storageError("initialize lifecycle source")(error)
                      : error,
                  ),
                );

              return;
            }
            if (producerEpoch < existing[0].producer_epoch) {
              return yield* DoFenceRejected.make({
                producerEpoch,
                actualEpoch: existing[0].producer_epoch,
                message: `Producer epoch ${producerEpoch} is stale; current epoch is ${existing[0].producer_epoch}.`,
              });
            }
            yield* requireTransferState(threadId);
            if (producerEpoch > existing[0].producer_epoch) {
              yield* sql`
            UPDATE effect_agent_threads
            SET producer_epoch = ${producerEpoch}
            WHERE thread_id = ${threadId}
           RETURNING *`.pipe(
                threads.write,
                Effect.mapError((error) =>
                  error._tag === "SqlError"
                    ? storageError("advance materialization epoch")(error)
                    : error,
                ),
              );
            }
          }),
        );
      },
      withStorageSpan("DoJournal.materialize", (error) => error._tag === "DoFenceRejected"),
    );

    const getThread = Effect.fnUntraced(function* (threadId: string) {
      return (yield* threads
        .by("thread_id", threadId)
        .pipe(
          Effect.mapError((error) =>
            error._tag === "SqlError" ? storageError("read thread")(error) : error,
          ),
        )).filter((row) => row.thread_id === threadId);
    });

    const hasRecord = Effect.fnUntraced(function* (threadId: string, recordId: string) {
      const thread = (yield* getThread(threadId))[0];
      const prefix = recordCache.prefix(threadId, thread?.tail_sequence ?? 0);

      if (prefix !== undefined) return prefix.some((row) => row.record_id === recordId);

      const rows = yield* sql`SELECT record_id FROM effect_agent_canonical_records
      WHERE thread_id = ${threadId} AND record_id = ${recordId}`.pipe(
        Effect.mapError(storageError("read canonical record identity")),
      );

      return rows.length > 0;
    });

    const prepareAppend = Effect.fnUntraced(function* (request: RawAppendRequest) {
      if (
        request.threadId.length > MAX_IDENTIFIER_LENGTH ||
        request.batchId.length > MAX_IDENTIFIER_LENGTH ||
        request.records.some((record) => record.recordId.length > MAX_IDENTIFIER_LENGTH)
      ) {
        return yield* DoStorageError.make({
          operation: "append canonical batch",
          message: "Canonical identifiers exceed the Durable Object storage bounds.",
        });
      }
      // Archives store the full batch as one value, so their ceiling also bounds hot appends.
      yield* checkValueBound("append canonical batch", request.batchJson);
      yield* checkValueBound("append canonical batch", request.batchDigest);
      yield* checkValueBound("append canonical batch", request.tailDigest);
      yield* Effect.forEach(
        request.records,
        (record) => checkValueBound("append canonical record", record.recordJson),
        { discard: true },
      );

      const recordIds: Array<string> = request.records.map((record) => record.recordId);

      if (new Set(recordIds).size !== recordIds.length) {
        return yield* DoAppendConflict.make({
          message: `Batch ${request.batchId} contains duplicate canonical record IDs.`,
          reason: "record-identity",
        });
      }

      return request;
    });

    const appendPrepared = Effect.fnUntraced(function* (
      request: RawAppendRequest,
    ): Effect.fn.Return<RawAppendResult, AppendError> {
      const recordIds: Array<string> = request.records.map((record) => record.recordId);
      const threadRows = yield* getThread(request.threadId);

      const thread = yield* decodeSingleRow(
        Schema.Array(ThreadRow),
        "effect_agent_threads",
        request.threadId,
        threadRows,
      );

      if (request.producerEpoch !== thread.producer_epoch) {
        return yield* DoFenceRejected.make({
          producerEpoch: request.producerEpoch,
          actualEpoch: thread.producer_epoch,
          message: `Producer epoch ${request.producerEpoch} is not the current epoch ${thread.producer_epoch}.`,
        });
      }

      const prefix = recordCache.prefix(request.threadId, thread.tail_sequence);

      const batchRows =
        prefix !== undefined && !prefix.some((row) => row.batch_id === request.batchId)
          ? []
          : yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            '' AS batch_json
          FROM effect_agent_canonical_batches
          WHERE thread_id = ${request.threadId}
            AND batch_id = ${request.batchId}
        `.pipe(Effect.mapError(storageError("read idempotent batch")));

      const batches = yield* decodeRows(
        Schema.Array(BatchRow),
        "effect_agent_canonical_batches",
        `${request.threadId}/${request.batchId}`,
        batchRows,
      );

      if (batches.length > 1) {
        return yield* DoStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: `${request.threadId}/${request.batchId}`,
          message: "A canonical batch primary key returned more than one row.",
        });
      }
      if (batches.length === 1) {
        const existing = batches[0];

        if (existing.batch_digest !== request.batchDigest) {
          return yield* DoAppendConflict.make({
            message: `Batch ${request.batchId} already exists with different canonical content.`,
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
        return yield* DoAppendConflict.make({
          message:
            `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
            `but found ${thread.tail_sequence}/${thread.tail_digest}.`,
          reason: "tail",
          actualTailSequence: thread.tail_sequence,
          actualTailDigest: thread.tail_digest,
        });
      }

      // Chunked to respect the Durable Object platform's 100-bound-parameter statement
      // limit: a batch may carry up to 256 records.
      const existingRecords: Array<RecordRow> =
        prefix === undefined ? [] : prefix.filter((row) => recordIds.includes(row.record_id));

      for (const chunk of prefix === undefined
        ? chunked(recordIds, MAX_BOUND_PARAMETERS - 10)
        : []) {
        const existingRecordRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              ${recordJson} AS record_json
            FROM effect_agent_canonical_records
            WHERE thread_id = ${request.threadId}
              AND record_id IN ${sql.in([...chunk])}
          `.pipe(Effect.mapError(storageError("check canonical record identities")));

        existingRecords.push(
          ...(yield* decodeRows(
            Schema.Array(RecordRow),
            "effect_agent_canonical_records",
            `${request.threadId}/record_ids`,
            existingRecordRows,
          )),
        );
      }
      if (existingRecords.length > 0) {
        return yield* DoAppendConflict.make({
          message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
          reason: "record-identity",
        });
      }

      const firstSequence = yield* Schema.decodeEffect(CanonicalSequence)(
        thread.tail_sequence + 1,
      ).pipe(
        Effect.mapError((error) =>
          DoStorageError.make({
            cause: error,
            operation: "append canonical batch",
            message: error.message,
          }),
        ),
      );

      const lastSequence = yield* Schema.decodeEffect(CanonicalSequence)(
        firstSequence + request.records.length - 1,
      ).pipe(
        Effect.mapError((error) =>
          DoStorageError.make({
            cause: error,
            operation: "append canonical batch",
            message: error.message,
          }),
        ),
      );

      yield* validateProgress(request);
      yield* archives.append(request, firstSequence, lastSequence);
      yield* sql`
          INSERT INTO effect_agent_canonical_batches (
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
        `.pipe(Effect.mapError(storageError("insert canonical batch")));
      yield* failpoint("append:after-batch-insert");

      const records = request.records.map((record, index) => ({
        record,
        canonical: record.canonical,
        row: {
          thread_id: request.threadId,
          sequence: firstSequence + index,
          record_id: record.recordId,
          batch_id: request.batchId,
          record_json: record.recordJson,
          ...record.readMetadata.columns,
        },
      }));

      // Captured scalar locators avoid parsing canonical JSON under the writer.
      // Seventeen bound columns per row stay within workerd's statement limit.
      for (const group of chunked(records, Math.floor(MAX_BOUND_PARAMETERS / 17))) {
        yield* sql`INSERT INTO effect_agent_canonical_records ${sql.insert(group.map(({ row }) => row))}`.pipe(
          Effect.mapError(storageError("insert canonical records")),
        );

        const memberships = group.flatMap(({ record, row }) =>
          record.readMetadata.runIds.map((runId) => ({
            thread_id: request.threadId,
            run_id: runId,
            sequence: row.sequence,
          })),
        );

        for (const members of chunked(memberships, Math.floor(MAX_BOUND_PARAMETERS / 3)))
          yield* sql`INSERT INTO effect_agent_record_runs ${sql.insert(members)}`.pipe(
            Effect.mapError(storageError("index canonical Run membership")),
          );
        for (const { row } of group) {
          recordCache.put(
            RecordRow.make({
              ...row,
              sequence: Schema.decodeSync(CanonicalSequence)(row.sequence),
            }),
          );
          yield* failpoint("append:after-record-insert");
        }
      }

      yield* sql`
          UPDATE effect_agent_threads
          SET
            tail_sequence = ${lastSequence},
            tail_digest = ${request.tailDigest},
            producer_epoch = ${request.producerEpoch}
          WHERE thread_id = ${request.threadId}
         RETURNING *`.pipe(
        threads.write,
        Effect.mapError((error) =>
          error._tag === "SqlError" ? storageError("advance thread tail")(error) : error,
        ),
      );
      yield* work
        .apply(
          yield* Schema.decodeEffect(ThreadId)(request.threadId).pipe(Effect.mapError(workFailure)),
          firstSequence,
          request.records.map((record) => record.canonical),
        )
        .pipe(Effect.mapError(workFailure));
      yield* failpoint("append:after-tail-update");

      // Retain one start prefix atomically with its canonical proof, before model/tool
      // execution. Later facts stay journal-backed until the settlement publication wave.
      if (
        lifecycle !== undefined &&
        records.some(
          ({ canonical: { payload } }) =>
            payload._tag === "RunStarted" || payload._tag === "SubagentStarted",
        )
      )
        yield* flushCanonical(request.threadId).pipe(
          Effect.provideService(SqlLifecycleRetainer, { retainMany: lifecycle.retainMany }),
          Effect.mapError((cause) =>
            DoStorageError.make({
              operation: "retain lifecycle start prefix",
              message: "Lifecycle start intent could not be retained",
              cause,
            }),
          ),
        );

      yield* progress.committed("canonical").pipe(
        Effect.catchCause((cause) =>
          Effect.failCause(
            Cause.map(cause, (error) =>
              DoStorageError.make({
                operation: "enroll canonical progress",
                message: error.message,
                cause: error,
              }),
            ),
          ),
        ),
      );

      return RawAppendResult.make({
        firstSequence,
        lastSequence,
        replayed: false,
        tailDigest: request.tailDigest,
      });
    });

    const append = Effect.fnUntraced(
      function* (
        request: RawAppendRequest,
        observed: ThreadRow,
      ): Effect.fn.Return<RawAppendResult, AppendError> {
        yield* prepareAppend(request);
        // The facade already read this tail. Reject a known stale writer without opening a
        // transaction; a matching tail is still checked atomically below. Replays precede tail
        // conflicts, preserving retry identity even after the log has advanced.
        if (request.producerEpoch !== observed.producer_epoch) {
          return yield* DoFenceRejected.make({
            producerEpoch: request.producerEpoch,
            actualEpoch: observed.producer_epoch,
            message: "The append producer epoch is not current.",
          });
        }
        if (
          request.expectedTailSequence !== observed.tail_sequence ||
          request.expectedTailDigest !== observed.tail_digest
        ) {
          const batches = yield* sql`
          SELECT batch_id FROM effect_agent_canonical_batches
          WHERE thread_id = ${request.threadId} AND batch_id = ${request.batchId}
        `.pipe(Effect.mapError(storageError("preflight append replay")));

          // A possible replay still checks the current epoch and digest in the transaction.
          if (batches.length === 0) {
            return yield* DoAppendConflict.make({
              reason: "tail",
              message: "The canonical tail has advanced.",
              actualTailSequence: observed.tail_sequence,
              actualTailDigest: observed.tail_digest,
            });
          }
        }

        return yield* withWriteTransaction(
          "append transaction",
          isAppendContention,
        )(appendPrepared(request));
      },
      withStorageSpan("DoJournal.append", isAppendContention),
    );

    const read = Effect.fnUntraced(function* (request: RawReadRequest) {
      const thread = (yield* getThread(request.threadId))[0];

      const through = Math.min(
        thread?.tail_sequence ?? 0,
        request.fromSequenceExclusive + request.limit,
      );

      const cached: Array<RecordRow> = [];

      for (let sequence = request.fromSequenceExclusive + 1; sequence <= through; sequence++) {
        const record = recordCache.get(request.threadId, sequence);

        if (record === undefined) break;
        cached.push(record);
      }
      if (cached.length === Math.max(0, through - request.fromSequenceExclusive)) {
        return { count: cached.length, records: Stream.fromIterable(cached) };
      }

      // Capture membership without retaining payloads. Append-only sequences keep each later
      // payload query inside this snapshot, even when new records arrive during consumption.
      const planRows = yield* sql<Record<string, unknown>>`
      SELECT
        sequence,
        length(CAST(${recordJson} AS BLOB)) AS record_json_bytes
      FROM effect_agent_canonical_records
      WHERE thread_id = ${request.threadId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(Effect.mapError(storageError("read canonical records")));

      const plan = yield* decodeRows(
        Schema.Array(ReadPlanRow),
        "effect_agent_canonical_records",
        `${request.threadId}>${request.fromSequenceExclusive}`,
        planRows,
        "ReadPlanRow",
      );

      const mismatch = () =>
        DoStorageCorruptionError.make({
          table: "effect_agent_canonical_records",
          rowKey: `${request.threadId}>${request.fromSequenceExclusive}`,
          message:
            "Canonical read membership, sequence, or payload size changed from its read plan.",
        });

      if (plan.length > request.limit) return yield* mismatch();

      const pages: Array<ReadPage> = [];
      let page: ReadPage | undefined;
      let pageBytes = 0;
      let previousSequence = request.fromSequenceExclusive;

      for (const row of plan) {
        if (row.sequence !== previousSequence + 1) return yield* mismatch();
        previousSequence = row.sequence;
        if (page === undefined || pageBytes + row.record_json_bytes > MAX_READ_PAGE_JSON_BYTES) {
          page = [row];
          pages.push(page);
          pageBytes = row.record_json_bytes;
        } else {
          page.push(row);
          pageBytes += row.record_json_bytes;
        }
      }

      const readPage = Effect.fnUntraced(function* (page: ReadPage) {
        const rows = yield* sql<Record<string, unknown>>`
        SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
        FROM effect_agent_canonical_records
        WHERE thread_id = ${request.threadId}
          AND sequence >= ${page[0].sequence}
          AND sequence <= ${page[page.length - 1].sequence}
        ORDER BY sequence
      `.pipe(Effect.mapError(storageError("read canonical records")));

        const decoded = yield* decodeRows(
          Schema.Array(RecordRow),
          "effect_agent_canonical_records",
          `${request.threadId}/${page[0].sequence}`,
          rows,
          "RecordRow",
        );

        if (
          decoded.length !== page.length ||
          decoded.some(
            (row, index) =>
              row.thread_id !== request.threadId ||
              row.sequence !== page[index].sequence ||
              storedTextBytes(row.record_json) !== page[index].record_json_bytes,
          )
        ) {
          return yield* mismatch();
        }

        for (const row of decoded) recordCache.put(row);

        return decoded;
      });

      return {
        count: plan.length,
        records: Stream.fromIterable(pages).pipe(
          Stream.flatMap((page) => Stream.fromIterableEffect(state.read(readPage(page)))),
        ),
      };
    });

    const saveCheckpoint = Effect.fnUntraced(function* (
      checkpoint: RawCheckpoint,
    ): Effect.fn.Return<void, CheckpointError> {
      if (checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH) {
        return yield* DoStorageError.make({
          operation: "save checkpoint",
          message: "Checkpoint identity exceeds the Durable Object storage bounds.",
        });
      }
      yield* checkValueBound("save checkpoint", checkpoint.checkpointJson);
      yield* withWriteTransaction("checkpoint transaction")(
        Effect.gen(function* () {
          const threadRows = yield* getThread(checkpoint.threadId);

          const thread = yield* decodeSingleRow(
            Schema.Array(ThreadRow),
            "effect_agent_threads",
            checkpoint.threadId,
            threadRows,
          );

          if (checkpoint.throughSequence > thread.tail_sequence) {
            return yield* DoCheckpointConflict.make({
              message:
                `Checkpoint sequence ${checkpoint.throughSequence} is after canonical tail ` +
                `${thread.tail_sequence}.`,
            });
          }

          const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          FROM effect_agent_checkpoints
          WHERE thread_id = ${checkpoint.threadId}
            AND through_sequence = ${checkpoint.throughSequence}
        `.pipe(Effect.mapError(storageError("read idempotent checkpoint")));

          const existing = yield* decodeRows(
            Schema.Array(CheckpointRow),
            "effect_agent_checkpoints",
            `${checkpoint.threadId}/${checkpoint.throughSequence}`,
            checkpointRows,
          );

          if (existing.length > 1) {
            return yield* DoStorageCorruptionError.make({
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
              return yield* DoCheckpointConflict.make({
                message: "A different checkpoint already exists at this canonical sequence.",
              });
            }

            return;
          }

          yield* sql`
          INSERT INTO effect_agent_checkpoints (
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
        `.pipe(Effect.mapError(storageError("insert checkpoint")));
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
      FROM effect_agent_checkpoints
      WHERE thread_id = ${threadId}
        AND through_sequence <= ${atOrBeforeSequence}
      ORDER BY through_sequence DESC
      LIMIT 1
    `.pipe(Effect.mapError(storageError("load checkpoint")));

      return yield* decodeRows(
        Schema.Array(CheckpointRow),
        "effect_agent_checkpoints",
        `${threadId}<=${atOrBeforeSequence}`,
        rows,
      );
    });

    const getTailDigestAt = Effect.fnUntraced(function* (
      threadId: string,
      sequence: CanonicalSequence,
    ) {
      const thread = (yield* getThread(threadId))[0];

      if (thread !== undefined && sequence === thread.tail_sequence) return [thread.tail_digest];
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
      FROM effect_agent_canonical_batches
      WHERE thread_id = ${threadId}
        AND batch_id = (
          SELECT batch_id FROM effect_agent_canonical_records
          WHERE thread_id = ${threadId} AND sequence = ${sequence}
        )
        AND last_sequence = ${sequence}
    `.pipe(Effect.mapError(storageError("read canonical digest at sequence")));

      const batches = yield* decodeRows(
        Schema.Array(BatchRow),
        "effect_agent_canonical_batches",
        `${threadId}/${sequence}`,
        rows,
      );

      return batches.map((batch) => batch.tail_digest);
    });

    const sourceFailure = (cause: unknown) =>
      LifecyclePublicationError.make({ reason: "unavailable", cause });

    const sourceThreads = Effect.fnUntraced(function* () {
      const all = yield* threads.matching(
        "lifecycle-owners",
        () => true,
        sql`SELECT * FROM effect_agent_threads LIMIT 129`,
        128,
      );

      if (all.length <= 128) return all;

      // Large custom hosts hydrate only owners whose journal has an unmaterialized suffix.
      return yield* Schema.decodeUnknownEffect(Schema.Array(ThreadRow))(
        yield* sql`SELECT t.* FROM effect_agent_threads t LEFT JOIN effect_agent_lifecycle_cursors c ON c.thread_id=t.thread_id
        WHERE c.thread_id IS NULL OR t.tail_sequence > c.through_sequence ORDER BY t.thread_id LIMIT 128`,
      );
    });

    const initializeLifecycleSource = Effect.fnUntraced(function* () {
      if (initializedLifecycleSources.has(state)) return;
      yield* state
        .transaction(
          Effect.gen(function* () {
            const existing =
              yield* sql`SELECT name FROM sqlite_master WHERE name='effect_agent_lifecycle_cursors'`;

            if (existing.length === 0) {
              yield* sql`CREATE TABLE effect_agent_lifecycle_cursors (
        thread_id TEXT PRIMARY KEY, through_sequence INTEGER NOT NULL
      )`;
              // Existing inline writers already retained their facts. Enabling publication on an
              // old store starts here too; it does not publish historical facts retroactively.
            }
            // New Threads created with publication disabled have no cursor. Never advance
            // existing cursors: they can owe intent from an interrupted Attempt.
            yield* sql`INSERT INTO effect_agent_lifecycle_cursors(thread_id, through_sequence)
        SELECT thread_id, tail_sequence FROM effect_agent_threads WHERE true
        ON CONFLICT(thread_id) DO NOTHING`;
            yield* sourceThreads();
          }),
        )
        .pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError((cause) =>
            DoStorageError.make({
              operation: "initialize lifecycle source",
              message: "Lifecycle source cursor unavailable",
              cause,
            }),
          ),
        );
      initializedLifecycleSources.add(state);
    });

    /** The journal is durable intent; only the bounded publication wave materializes a write set. */
    const flushCanonical = Effect.fnUntraced(function* (threadId: string, maxRecords = Infinity) {
      const { retainMany } = yield* SqlLifecycleRetainer;
      const thread = (yield* getThread(threadId))[0];

      if (thread === undefined) return;
      const cursor = (yield* cursors.by("thread_id", threadId))[0];

      if (cursor === undefined || cursor.through_sequence > thread.tail_sequence)
        return yield* LifecyclePublicationError.make({ reason: "corrupt" });
      let through = cursor.through_sequence;
      const bound = Math.min(thread.tail_sequence, through + maxRecords);

      while (through < bound) {
        const page = yield* read(
          RawReadRequest.make({
            threadId,
            fromSequenceExclusive: through,
            limit: Math.min(128, bound - through),
          }),
        );

        const facts: Array<Parameters<SqlLifecycleRetainMany>[0][number]> = [];
        let bytes = 0;

        yield* Stream.runForEach(page.records, (row) =>
          Effect.gen(function* () {
            const record = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
              row.record_json,
            );

            if (record.recordId !== row.record_id || row.sequence !== through + 1)
              return yield* LifecyclePublicationError.make({ reason: "corrupt" });
            if (Schema.is(Schema.toType(LifecyclePublicationFact))(record.payload)) {
              if (
                bytes + storedTextBytes(row.record_json) > MAX_READ_PAGE_JSON_BYTES &&
                facts.length > 0
              ) {
                yield* retainMany(facts);
                facts.length = 0;
                bytes = 0;
              }
              facts.push({
                id: JSON.stringify([threadId, "record", record.recordId]),
                ownerThreadId: yield* Schema.decodeEffect(ThreadId)(threadId),
                canonicalSequence: row.sequence,
                createdAt: record.createdAt,
                fact: record.payload,
              });
              bytes += storedTextBytes(row.record_json);
            }
            through = row.sequence;
          }),
        );
        if (page.count === 0) return yield* LifecyclePublicationError.make({ reason: "corrupt" });
        yield* retainMany(facts);
        yield* sql`UPDATE effect_agent_lifecycle_cursors SET through_sequence=${through} WHERE thread_id=${threadId} RETURNING *`.pipe(
          cursors.write,
        );
      }
    }, Effect.mapError(sourceFailure));

    const flushPublications = Effect.fnUntraced(function* () {
      let owners = 0;

      for (const thread of yield* sourceThreads()) {
        const cursor = (yield* cursors.by("thread_id", thread.thread_id))[0];

        if (cursor !== undefined && cursor.through_sequence === thread.tail_sequence) continue;
        yield* flushCanonical(thread.thread_id, 1024);
        if (++owners === 4) break;
      }
    }, Effect.mapError(sourceFailure));

    const sourcePending = Effect.gen(function* () {
      for (const thread of yield* sourceThreads()) {
        const cursor = (yield* cursors.by("thread_id", thread.thread_id))[0];

        if (cursor === undefined || cursor.through_sequence > thread.tail_sequence)
          return yield* LifecyclePublicationError.make({ reason: "corrupt" });
        if (cursor.through_sequence < thread.tail_sequence) return true;
      }

      return false;
    }).pipe(Effect.mapError(sourceFailure));

    const ownedLifecycle =
      lifecycle === undefined
        ? undefined
        : {
            ...lifecycle,
            storage: {
              ...lifecycle.storage,
              retainedPendingDeadline: lifecycle.storage.pendingDeadline,
              pendingDeadline: state.read(
                Effect.gen(function* () {
                  const deadline = yield* lifecycle.storage.pendingDeadline;

                  if (!(yield* sourcePending)) return deadline;
                  const now = yield* Clock.currentTimeMillis;

                  return Option.some(Option.isSome(deadline) ? Math.min(now, deadline.value) : now);
                }),
              ),
            },
          };

    return {
      archives: archives.storage,
      archiveRanges: archives,
      work,
      state,
      owner,
      threads,
      lifecycle: ownedLifecycle,
      initializeLifecycleSource,
      flushCanonical,
      flushPublications,
      append,
      prepareAppend,
      appendPrepared,
      checkValueBound,
      getThread,
      hasRecord,
      getTailDigestAt,
      loadCheckpoint,
      materialize,
      read: (request: RawReadRequest) => state.read(read(request)),
      saveCheckpoint,
      withWriteTransaction,
    } as const;
  });

export type DoJournal = Effect.Success<ReturnType<typeof makeJournal>>;

export const initializeDoJournal = ensureCurrentStorage;
