import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import {
  makeCanonicalStorage,
  recordBatchId,
} from "@yielded/agent-storage-sql/sql-canonical-storage";
import type { RawAppendRequest } from "@yielded/agent-storage-sql/sql-journal";
import { makeSqlExecutions } from "@yielded/agent-storage-sql/sql-journal";
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
import { makeSqlThreadWork } from "@yielded/agent-storage-sql/sql-thread-work";
import { ThreadId } from "@yielded/agent/identifiers";
import {
  LifecyclePublicationFact,
  LifecyclePublicationError,
} from "@yielded/agent/lifecycle-publication";
import { BatchId, CanonicalRecord, CanonicalSequence } from "@yielded/agent/records";
import { SqlStorageOwner } from "@yielded/agent/sql-storage-owner";
import {
  type AppendConflict,
  AppendResult,
  ThreadStoreDiagnostic,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as References from "effect/References";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";

import { DoStorageConfig } from "../DoStorageConfig.ts";
import {
  type DoStorageFailpointError,
  DoAppendConflict,
  DoStorageCorruptionError,
  DoStorageError,
  DoValueBoundExceeded,
  type DoStorageFailpointLocation,
} from "../DoStorageError.ts";
import { DoStorageFailpoint, isDefaultDoStorageFailpoint } from "../DoStorageFailpoint.ts";
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
const isSqlError = Schema.is(SqlError.SqlError);

const isNativeClient = Schema.is(
  Schema.declare<SqliteClient.SqliteClient>((value): value is SqliteClient.SqliteClient =>
    Predicate.hasProperty(value, SqliteClient.TypeId),
  ),
);

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
  created_at: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(128))),
  tail_sequence: CanonicalSequence,
}) {}

const threadRows = ownedRows(
  ThreadRow,
  "effect_agent_threads",
  (row) => row.thread_id,
  "thread_id",
);

class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: Schema.fromJsonString(BatchId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH))),
  thread_id: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  record_id: Schema.fromJsonString(BoundedIdentifier),
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
    put: (row: RecordRow, capturedBytes?: number) => {
      const id = key(row.thread_id, row.sequence);
      const prior = records.get(id);

      if (prior?.row === row) return;
      const size = capturedBytes ?? storedTextBytes(row.record_json);

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

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-cloudflare/RawReadRequest",
)({
  threadId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

type AppendError =
  | ThreadStoreError
  | DoAppendConflict
  | DoStorageCorruptionError
  | DoStorageError
  | DoStorageFailpointError
  | DoValueBoundExceeded;

type DoJournalFailpoint = (
  location: DoStorageFailpointLocation,
) => Effect.Effect<void, DoStorageFailpointError>;

const noFailpoint: DoJournalFailpoint = () => Effect.void;

const storageError =
  (operation: string) =>
  (error: SqlError.SqlError): DoStorageError =>
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
  const state = yield* ownedState(sql);
  const owner = (yield* SqlStorageOwner) ?? state;

  yield* ensureDoStorageLayout().pipe(
    Effect.provideService(SqlStorageOwner, owner),
    Effect.provideService(SqlClient.SqlClient, sql),
  );

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

const makeJournal = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint,
  maxStoredValueBytes: number,
  lifecycle: Effect.Success<ReturnType<typeof makeSqlLifecyclePublication>>,
  state: OwnedState,
) {
  const owner = (yield* SqlStorageOwner) ?? state;
  const progress = yield* SqlStorageProgress;

  const nativeDb =
    isNativeClient(sql) &&
    sql.config.transformQueryNames === undefined &&
    sql.config.transformResultNames === undefined &&
    isDefaultDoStorageFailpoint(failpoint)
      ? sql.config.storage?.sql
      : undefined;

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

  yield* Effect.acquireRelease(
    Effect.sync(() => state.invalidators.add(work.invalidate)),
    () => Effect.sync(() => state.invalidators.delete(work.invalidate)),
  );

  const workFailure = (cause: unknown) =>
    DoStorageError.make({
      operation: "publish Thread work",
      message: "Canonical work metadata could not be published",
      cause,
    });

  const executions = yield* makeSqlExecutions().pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
  );

  const executionConflict = <A>(effect: Effect.Effect<A, AppendConflict | ThreadStoreError>) =>
    effect.pipe(
      Effect.catchTag("AppendConflict", (error) =>
        Effect.fail(
          DoAppendConflict.make({
            reason: error.reason,
            message: "Active execution conflicts with the captured commit",
            ...(error.actualTailSequence === undefined
              ? {}
              : { actualTailSequence: error.actualTailSequence }),
          }),
        ),
      ),
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

  const physical = yield* makeCanonicalStorage().pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
  );

  /** Typed pre-write refusal for any single value over the configured byte bound. */
  const checkValueBound = (
    operation: string,
    value: string,
    actualBytes = storedTextBytes(value),
  ): Effect.Effect<void, DoValueBoundExceeded> => {
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
   * The shared local owner gate covers the transaction and acknowledgement.
   *
   * Journal write transactions are always top level: the Durable Object client rejects
   * nested transactions, so new journal operations must not wrap this helper inside another
   * transaction. Expected outcomes may be captured only when every such refusal precedes
   * mutation; real faults still roll back. Successful preflight reads never authorize a write.
   */
  const withWriteTransaction = (
    operation: string,
    expected?: (error: { readonly _tag: string }) => boolean,
  ) => {
    const transaction = <A, E extends { readonly _tag: string }>(
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | DoStorageError> =>
      Effect.withFiber((fiber) => {
        const body = state.owner.withMutation(owner.transaction(effect)).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError((error) => (isSqlError(error) ? storageError(operation)(error) : error)),
        );

        const traced =
          fiber.getRef(References.TracerEnabled) &&
          fiber.getRef(Tracer.Tracer) !== Tracer.nativeTracer;

        return traced
          ? body.pipe(
              Effect.tapError(annotateStorageError),
              Effect.withSpan("DoJournal.withWriteTransaction", { attributes: { operation } }),
            )
          : body;
      });

    return <A, E extends { readonly _tag: string }>(effect: Effect.Effect<A, E>) =>
      expected === undefined
        ? transaction(effect)
        : transaction(storageResult(effect, expected)).pipe(Effect.flatMap(Effect.fromResult));
  };

  const materialize = Effect.fnUntraced(function* (
    threadId: string,
    createdAt: string,
  ): Effect.fn.Return<void, DoStorageCorruptionError | DoStorageError | DoValueBoundExceeded> {
    if (threadId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* DoStorageError.make({
        operation: "materialize thread",
        message: "Thread identity exceeds the Durable Object storage bounds.",
      });
    }
    // Refusals and idempotent materialization need no transaction. Recheck before writes.
    const current = yield* getThread(threadId);

    if (current.length === 1) {
      return;
    }
    yield* withWriteTransaction("materialize transaction")(
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
              tail_sequence
            ) VALUES (
              ${threadId},
              ${createdAt},
              0
            ) ON CONFLICT(thread_id) DO UPDATE SET created_at=excluded.created_at
              WHERE effect_agent_threads.created_at IS NULL
           RETURNING *`.pipe(
            threads.write,
            Effect.mapError((error) =>
              error._tag === "SqlError" ? storageError("materialize thread")(error) : error,
            ),
          );

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
      }),
    );
  }, withStorageSpan("DoJournal.materialize"));

  const getThread = Effect.fnUntraced(function* (threadId: string) {
    return (yield* threads
      .by("thread_id", threadId)
      .pipe(
        Effect.mapError((error) =>
          error._tag === "SqlError" ? storageError("read thread")(error) : error,
        ),
      )).filter((row) => row.thread_id === threadId && row.created_at !== null);
  });

  const hasRecord = Effect.fnUntraced(function* (threadId: string, recordId: string) {
    const thread = (yield* getThread(threadId))[0];
    const prefix = recordCache.prefix(threadId, thread?.tail_sequence ?? 0);

    if (prefix !== undefined) return prefix.some((row) => row.record_id === recordId);

    const rows = yield* sql`SELECT record_id FROM effect_agent_canonical_records
      WHERE thread_id = ${threadId} AND record_id = ${JSON.stringify(recordId)}`.pipe(
      Effect.mapError(storageError("read canonical record identity")),
    );

    return rows.length > 0;
  });

  const prepareAppend = Effect.fnUntraced(function* (request: RawAppendRequest) {
    if (
      request.threadId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchId.length > MAX_IDENTIFIER_LENGTH ||
      request.executionWrites.some(
        ({ update }) => update.checkpoint.progress.runId.length > MAX_IDENTIFIER_LENGTH,
      ) ||
      request.records.some((record) => record.recordId.length > MAX_IDENTIFIER_LENGTH)
    ) {
      return yield* DoStorageError.make({
        operation: "append canonical batch",
        message: "Canonical identifiers exceed the Durable Object storage bounds.",
      });
    }
    // Archives store the full batch as one value, so their ceiling also bounds hot appends.
    // The trusted append SPI pairs captured JSON with its exact UTF-8 width.
    yield* checkValueBound("append canonical batch", request.batchJson, request.batchBytes);
    for (const record of request.records) {
      if (record.recordBytes > maxStoredValueBytes)
        return yield* DoValueBoundExceeded.make({
          actualBytes: record.recordBytes,
          maxBytes: maxStoredValueBytes,
          operation: "append canonical record",
        });
    }
    for (const write of request.executionWrites)
      yield* checkValueBound("commit active execution", write.json, write.bytes);

    const recordIds: Array<string> = request.records.map((record) => record.recordId);

    if (new Set(recordIds).size !== recordIds.length) {
      return yield* DoAppendConflict.make({
        message: `Batch ${request.batchId} contains duplicate canonical record IDs.`,
        reason: "record-identity",
      });
    }

    return request;
  });

  // Replay and progress validation precede this body in the same owner transaction.
  // A miss makes no native change; the ordinary path reuses any tentative batch.
  // After advancing the frontier, failures must roll back rather than fall back.
  const appendNativeBody = Effect.fnUntraced(function* (
    request: RawAppendRequest,
    first: CanonicalSequence,
    last: CanonicalSequence,
    batchInserted: boolean,
  ) {
    const db = nativeDb;

    // Effectful statement transformers and exported SQL spans retain the ordinary path.
    const ordinaryStatement = yield* Effect.withFiber((fiber) =>
      Effect.succeed(
        fiber.getRef(Statement.CurrentTransformer) !== undefined ||
          (fiber.getRef(References.TracerEnabled) &&
            fiber.getRef(Tracer.Tracer) !== Tracer.nativeTracer),
      ),
    );

    if (ordinaryStatement) return false;
    if (
      db === undefined ||
      request.executionWrites.length !== 1 ||
      request.executionWrites[0]?.update.retire ||
      request.executionWrites[0]?.update.facts.length !== 0
    )
      return false;

    const workPlan = yield* work.prepareClosedPair(
      request.threadId,
      first,
      last,
      request.records.map((record) => record.canonical),
    );

    if (workPlan === undefined) return false;

    const native = yield* Effect.suspend(() => {
      const query = (statement: Statement.Statement<object>, operation: string) =>
        Result.try({
          try: () => {
            const [text, parameters] = statement.compile();

            return db.exec(text, ...parameters).toArray();
          },
          catch: (cause) =>
            storageError(operation)(
              SqlError.SqlError.make({
                reason: SqlError.classifySqliteError(cause, {
                  operation,
                  message: "Native canonical statement failed",
                }),
              }),
            ),
        });

      return Effect.fromResult(
        Result.gen(function* () {
          const captured = physical.cachedRecords(request.threadId, first, request.records);

          if (captured === undefined) return undefined;
          const physicalRecords = yield* captured;
          const rows = yield* query(workPlan.advance, "advance native work frontier");

          // A zero-row predicate miss has made no change. A successful update
          // must never fall back: every later refusal rolls back this transaction.
          if (rows.length === 0) return undefined;
          const executionPlans = executions.prepareCommit(request);

          if (!batchInserted)
            yield* query(
              sql`INSERT INTO effect_agent_canonical_batches
            (thread_id, batch_id, first_sequence, last_sequence, producer_id_json)
            VALUES (${request.threadId}, ${JSON.stringify(request.batchId)}, ${first}, ${last}, ${request.producerIdJson})`,
              "insert canonical batch",
            );

          const inserted = yield* query(
            sql`INSERT INTO effect_agent_canonical_records ${sql.insert(physicalRecords.map((row) => row.columns))}
            ON CONFLICT(thread_id, record_id) DO NOTHING RETURNING 1`,
            "insert canonical records",
          );

          if (inserted.length !== request.records.length)
            return yield* Result.fail(
              DoAppendConflict.make({
                message: "A canonical record ID already exists.",
                reason: "record-identity",
              }),
            );
          const memberships = physicalRecords.flatMap((row) => row.memberships);

          for (const group of chunked(memberships, Math.floor(MAX_BOUND_PARAMETERS / 3)))
            yield* query(
              sql`INSERT INTO effect_agent_record_runs ${sql.insert(group)}`,
              "index canonical Run membership",
            );
          for (const pointer of workPlan.pointers)
            yield* query(pointer, "index canonical pointers");

          for (const plan of executionPlans) {
            const changed = yield* query(plan.statement, "commit active execution");
            const accepted = plan.accept(changed);

            if (Result.isFailure(accepted))
              return yield* Result.fail(
                DoAppendConflict.make({
                  reason: accepted.failure.reason,
                  message: "Active execution conflicts with the captured commit",
                  ...(accepted.failure.actualTailSequence === undefined
                    ? {}
                    : { actualTailSequence: accepted.failure.actualTailSequence }),
                }),
              );
          }
          for (let index = 0; index < request.records.length; index++) {
            const record = request.records[index]!;

            recordCache.put(
              RecordRow.make({
                ...physicalRecords[index]!.columns,
                thread_id: request.threadId,
                record_id: record.recordId,
                batch_id: request.batchId,
                record_json: record.recordJson,
                sequence: CanonicalSequence.make(first + index),
              }),
              record.recordBytes,
            );
          }

          return rows;
        }),
      );
    });

    if (native === undefined) return false;
    yield* threads.write(Effect.succeed(native));

    return true;
  });

  const appendPrepared = Effect.fnUntraced(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<AppendResult, AppendError> {
    const threadRows = yield* getThread(request.threadId);

    const thread = threadRows.length === 1 ? threadRows[0] : undefined;

    if (thread === undefined)
      return yield* DoStorageCorruptionError.make({
        table: "effect_agent_threads",
        rowKey: request.threadId,
        message: `Expected exactly one row but found ${threadRows.length}.`,
      });

    // A fresh canonical batch key and absence of an active receipt prove the two
    // negative replay lookups in the same statement that retains the batch header.
    // Keep the read-first path for stale/overflowed coordinates so exact retries
    // still precede sequence and tail refusals.
    const canInsert =
      request.records.length > 0 &&
      request.expectedTailSequence === thread.tail_sequence &&
      Number.isSafeInteger(thread.tail_sequence + request.records.length);

    const inserted = canInsert
      ? yield* sql`INSERT INTO effect_agent_canonical_batches
            (thread_id, batch_id, first_sequence, last_sequence, producer_id_json)
            SELECT ${request.threadId}, ${JSON.stringify(request.batchId)},
              ${thread.tail_sequence + 1}, ${thread.tail_sequence + request.records.length},
              ${request.producerIdJson}
            WHERE NOT EXISTS (
              SELECT 1 FROM effect_agent_executions
              WHERE thread_id=${request.threadId} AND last_commit_id=${request.batchId}
            )
            ON CONFLICT(thread_id, batch_id) DO NOTHING RETURNING 1`.pipe(
          Effect.mapError(storageError("insert canonical batch")),
        )
      : [];

    if (inserted.length === 0) {
      const replay = yield* executionConflict(executions.replay(request));

      if (replay !== undefined) return AppendResult.make(replay);

      const prefix = recordCache.prefix(request.threadId, thread.tail_sequence);

      const existing =
        prefix !== undefined && !prefix.some((row) => row.batch_id === request.batchId)
          ? undefined
          : yield* archives.replay(request.threadId, request.batchId);

      if (existing !== undefined) {
        if (existing.batch_json !== request.batchJson) {
          return yield* DoAppendConflict.make({
            message: `Batch ${request.batchId} already exists with different canonical content.`,
            reason: "batch-content",
          });
        }

        return AppendResult.make({
          firstSequence: existing.first_sequence,
          lastSequence: existing.last_sequence,
          replayed: true,
        });
      }
      if (canInsert)
        return yield* DoStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: request.threadId,
          message: "Batch insertion conflicted without a durable replay receipt.",
        });
    }

    yield* executionConflict(executions.rejectSuperseded(request));

    if (request.expectedTailSequence !== thread.tail_sequence) {
      return yield* DoAppendConflict.make({
        message:
          `Expected tail ${request.expectedTailSequence} ` + `but found ${thread.tail_sequence}.`,
        reason: "tail",
        actualTailSequence: thread.tail_sequence,
      });
    }

    const result = yield* Schema.decodeEffect(AppendResult)({
      firstSequence: thread.tail_sequence + (request.records.length === 0 ? 0 : 1),
      lastSequence: thread.tail_sequence + request.records.length,
      replayed: false,
    }).pipe(
      Effect.mapError((error) =>
        DoStorageError.make({
          cause: error,
          operation: "append canonical batch",
          message: error.message,
        }),
      ),
    );

    const { firstSequence, lastSequence } = result;

    yield* executionConflict(executions.validate(request));

    if (yield* appendNativeBody(request, firstSequence, lastSequence, inserted.length > 0)) {
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

      return result;
    }

    if (request.records.length > 0 && inserted.length === 0) {
      yield* sql`
          INSERT INTO effect_agent_canonical_batches (
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
        `.pipe(Effect.mapError(storageError("insert canonical batch")));
    }
    yield* failpoint("append:after-batch-insert");

    const physicalRecords = yield* physical.records(
      request.threadId,
      firstSequence,
      request.records,
    );

    const records = physicalRecords.map(({ columns, memberships }, index) => ({
      record: request.records[index]!,
      canonical: request.records[index]!.canonical,
      row: columns,
      memberships,
    }));

    // Captured scalar locators avoid parsing canonical JSON under the writer.
    // Eighteen bound columns per row stay within workerd's statement limit.
    for (const group of chunked(records, Math.floor(MAX_BOUND_PARAMETERS / 18))) {
      const inserted =
        yield* sql`INSERT INTO effect_agent_canonical_records ${sql.insert(group.map(({ row }) => row))}
        ON CONFLICT(thread_id, record_id) DO NOTHING RETURNING 1`.pipe(
          Effect.mapError(storageError("insert canonical records")),
        );

      if (inserted.length !== group.length)
        return yield* DoAppendConflict.make({
          message: "A canonical record ID already exists.",
          reason: "record-identity",
        });

      const memberships = group.flatMap(({ memberships }) => memberships);

      for (const members of chunked(memberships, Math.floor(MAX_BOUND_PARAMETERS / 3)))
        yield* sql`INSERT INTO effect_agent_record_runs ${sql.insert(members)}`.pipe(
          Effect.mapError(storageError("index canonical Run membership")),
        );
      for (const { row, record } of group) {
        recordCache.put(
          RecordRow.make({
            ...row,
            thread_id: request.threadId,
            record_id: record.recordId,
            batch_id: request.batchId,
            record_json: record.recordJson,
            sequence: Schema.decodeSync(CanonicalSequence)(row.sequence),
          }),
          record.recordBytes,
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

      yield* sql`UPDATE effect_agent_threads SET ${sql.update({ tail_sequence: lastSequence, ...workFields })}
          WHERE thread_id=${request.threadId} RETURNING *`.pipe(
        threads.write,
        Effect.mapError((error) =>
          error._tag === "SqlError" ? storageError("advance thread tail")(error) : error,
        ),
      );
    }
    yield* executionConflict(executions.commit(request, result));
    if (request.executionWrites.length > 0) yield* failpoint("append:after-record-insert");
    if (
      lifecycle !== undefined &&
      request.executionWrites.some(({ update }) =>
        update.facts.some(({ payload }) => payload._tag === "RunStarted"),
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
    for (const { update } of request.executionWrites) {
      for (const fact of update.facts) {
        if (
          lifecycle !== undefined &&
          Schema.is(Schema.toType(LifecyclePublicationFact))(fact.payload)
        )
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
                DoStorageError.make({
                  operation: "retain operational lifecycle",
                  message: cause.message,
                  cause,
                }),
              ),
            );
      }
    }
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

    return result;
  });

  const append = Effect.fnUntraced(
    function* (
      request: RawAppendRequest,
      observed: ThreadRow,
    ): Effect.fn.Return<AppendResult, AppendError> {
      yield* prepareAppend(request);
      // The facade already read this tail. Reject a known stale writer without opening a
      // transaction; a matching tail is still checked atomically below. Replays precede tail
      // conflicts, preserving retry identity even after the log has advanced.
      if (request.records.length > 0 && request.expectedTailSequence !== observed.tail_sequence) {
        const batches = yield* sql`
          SELECT batch_id FROM effect_agent_canonical_batches
          WHERE thread_id = ${request.threadId} AND batch_id = ${JSON.stringify(request.batchId)}
          UNION ALL SELECT last_commit_id FROM effect_agent_executions
          WHERE thread_id = ${request.threadId} AND last_commit_id = ${request.batchId}
          LIMIT 1
        `.pipe(Effect.mapError(storageError("preflight append replay")));

        // A possible replay still checks exact content in the transaction.
        if (batches.length === 0) {
          return yield* DoAppendConflict.make({
            reason: "tail",
            message: "The canonical tail has advanced.",
            actualTailSequence: observed.tail_sequence,
          });
        }
      }

      // Receipt arbitration may have tentatively inserted a batch. Even typed
      // contention must roll back before leaving the owner mutation gate.
      return yield* withWriteTransaction("append transaction")(appendPrepared(request));
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
        message: "Canonical read membership, sequence, or payload size changed from its read plan.",
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
        SELECT thread_id, sequence, record_id, ${recordBatchId(sql)} AS batch_id, ${recordJson} AS record_json
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

  const sourceFailure = (cause: unknown) =>
    LifecyclePublicationError.make({ reason: "unavailable", cause });

  const sourceThreads = Effect.fnUntraced(function* () {
    const all = yield* threads.matching(
      "lifecycle-owners",
      (row) => row.created_at !== null,
      sql`SELECT * FROM effect_agent_threads WHERE created_at IS NOT NULL LIMIT 129`,
      128,
    );

    if (all.length <= 128) return all;

    // Large custom hosts hydrate only owners whose journal has an unmaterialized suffix.
    return yield* Schema.decodeUnknownEffect(Schema.Array(ThreadRow))(
      yield* sql`SELECT t.* FROM effect_agent_threads t LEFT JOIN effect_agent_lifecycle_cursors c ON c.thread_id=t.thread_id
        WHERE t.created_at IS NOT NULL AND (c.thread_id IS NULL OR t.tail_sequence > c.through_sequence) ORDER BY t.thread_id LIMIT 128`,
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
        SELECT thread_id, tail_sequence FROM effect_agent_threads WHERE created_at IS NOT NULL
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
    checkCommitIntent: (request: RawAppendRequest) =>
      executionConflict(executions.checkCommitIntent(request)),
    readExecution: executions.read,
    checkValueBound,
    getThread,
    hasRecord,
    materialize,
    read: (request: RawReadRequest) => state.read(read(request)),
    withWriteTransaction,
  } as const;
});

export type DoJournal = Effect.Success<ReturnType<typeof makeJournal>>;

const InitializedDoJournal = Context.Reference<DoJournal | undefined>(
  "@effect-agent/storage-cloudflare/InitializedDoJournal",
  { defaultValue: () => undefined },
);

/** Build once in the common owner, SQL, configuration, failpoint and lifecycle context. */
export const initializedDoJournalLayer = Layer.effect(InitializedDoJournal)(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const config = yield* DoStorageConfig;
    const failpoint = yield* DoStorageFailpoint;

    return yield* ensureCurrentStorage(sql, failpoint.hit, config.maxStoredValueBytes);
  }),
);

export const initializeDoJournal = (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint,
  maxStoredValueBytes: number,
) =>
  Effect.filterOrElse(InitializedDoJournal, Predicate.isNotUndefined, () =>
    ensureCurrentStorage(sql, failpoint, maxStoredValueBytes),
  );
