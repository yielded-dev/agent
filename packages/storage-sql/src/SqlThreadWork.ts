import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey } from "@yielded/agent/receipt";
import { ExportRecord } from "@yielded/agent/record-format";
import {
  BatchId,
  RecordId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  type CanonicalRecord,
} from "@yielded/agent/records";
import { SubmissionWorkItem } from "@yielded/agent/submission-ledger";
import { ThreadStoreError } from "@yielded/agent/thread-store";
import {
  WORK_INDEX_VERSION,
  MAX_WORK_ENTRY_BYTES,
  MAX_WORK_REBUILD_BYTES,
  MAX_WORK_REBUILD_RECORDS,
  CanonicalWorkEntry,
  ThreadWorkRequest,
  WorkThreadsRequest,
  WorkIndexRebuildRequest,
  WorkIndexProgress,
  WorkDiscoveryUnavailable,
  admissionWork,
  advanceWorkEntry,
  decodeWorkCursor,
  encodeWorkCursor,
  validateWorkPage,
  workId,
  workIndexChanges,
  type ThreadWorkStorage,
  type ThreadWorkEntry,
  type WorkerReportingMode,
} from "@yielded/agent/thread-work";
import { Crypto, Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

import { makeSqlSettlementIntervals } from "./internal/settlement-intervals.ts";
import { sqliteJsonText } from "./internal/sql-json.ts";
import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";
import { canonicalRecordJson } from "./SqlThreadArchiveRange.ts";
import { canonicalRecordMetadata, canonicalRecordPointers } from "./SqlThreadNativeReads.ts";

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Thread work storage is unavailable or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

/** The caller supplies the native snapshot/writer boundary; hooks never open transactions. */
export interface SqlThreadWorkOptions {
  readonly namespace?: string;
  readonly read?: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreError, R>;
  readonly write?: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreError, R>;
}

/** Disposable objects are separate from the authoritative layout compatibility contract. */
export const createSqlThreadWorkTables = Effect.fnUntraced(function* (namespace?: string) {
  const sql = yield* SqlClient.SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);
  const integer = sql.literal(sql.onDialectOrElse({ pg: () => "BIGINT", orElse: () => "INTEGER" }));

  const text = sql.literal(
    sql.onDialectOrElse({ pg: () => 'TEXT COLLATE "C"', orElse: () => "TEXT" }),
  );

  yield* sql`CREATE TABLE IF NOT EXISTS ${table("effect_agent_work_index")} (
    thread_id ${text} PRIMARY KEY NOT NULL, version ${integer} NOT NULL,
    state ${text} NOT NULL, through_sequence ${integer} NOT NULL,
    reporting ${integer} NOT NULL, entry_count ${integer} NOT NULL
  )`.pipe(
    execute,
    Effect.mapError((cause) => failure("create work index", cause)),
  );
  yield* sql`CREATE TABLE IF NOT EXISTS ${table("effect_agent_work_entries")} (
    thread_id ${text} NOT NULL, id ${text} NOT NULL, message_id ${text},
    entry_json ${text} NOT NULL, created_sequence ${integer} NOT NULL, owner_tag ${text} NOT NULL, worker_thread_id ${text}, worker_update ${integer}, handoff_kind ${text}, PRIMARY KEY (thread_id, id)
  )`.pipe(
    execute,
    Effect.mapError((cause) => failure("create work entries", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_message
    ON ${table("effect_agent_work_entries")} (thread_id, message_id) WHERE message_id IS NOT NULL`.pipe(
    execute,
    Effect.mapError((cause) => failure("create work lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_worker
    ON ${table("effect_agent_work_entries")} (thread_id, worker_thread_id, created_sequence) WHERE worker_thread_id IS NOT NULL`.pipe(
    execute,
    Effect.mapError((cause) => failure("create worker lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_worker_partition
    ON ${table("effect_agent_work_entries")} (thread_id, created_sequence) WHERE worker_thread_id IS NOT NULL`.pipe(
    execute,
    Effect.mapError((cause) => failure("create worker capacity lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_worker_capacity
    ON ${table("effect_agent_work_entries")} (thread_id, worker_thread_id, worker_update) WHERE worker_thread_id IS NOT NULL`.pipe(
    execute,
    Effect.mapError((cause) => failure("create exact worker capacity lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_peer
    ON ${table("effect_agent_work_entries")} (thread_id, message_id) WHERE handoff_kind = 'peer'`.pipe(
    execute,
    Effect.mapError((cause) => failure("create peer lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_foreign_child
    ON ${table("effect_agent_work_entries")} (thread_id, id) WHERE owner_tag='Child' OR handoff_kind IN ('reservation','child-accounting')`.pipe(
    execute,
    Effect.mapError((cause) => failure("create foreign child lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_foreign_worker
    ON ${table("effect_agent_work_entries")} (thread_id, id) WHERE owner_tag IN ('WorkerInput','WorkerEffects','Report') OR handoff_kind='worker-stop'`.pipe(
    execute,
    Effect.mapError((cause) => failure("create foreign worker lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_work_entries_foreign_delivery
    ON ${table("effect_agent_work_entries")} (thread_id, id) WHERE handoff_kind IN ('peer','update','report')`.pipe(
    execute,
    Effect.mapError((cause) => failure("create foreign delivery lookup", cause)),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_records_delivery_predecessor
    ON ${table("effect_agent_canonical_records")} (thread_id, record_tag, sequence) WHERE handoff = 1`.pipe(
    execute,
    Effect.mapError((cause) => failure("create delivery predecessor lookup", cause)),
  );
});

const Header = Schema.Struct({
  version: SqlInteger,
  state: Schema.Literals(["ready", "rebuilding"]),
  through_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  reporting: SqlInteger.pipe(Schema.decodeTo(Schema.Literals([0, 1, 2]))),
  entry_count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
  actual_count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
});

const decodeHeader = Schema.decodeUnknownEffect(Schema.Array(Header));

const workerReportingMode = (reporting: typeof Header.Type.reporting): WorkerReportingMode =>
  reporting === 1 ? "standard" : reporting === 2 ? "private" : "none";

const Count = Schema.Struct({ count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)) });
const CountRow = Schema.Tuple([Count]);
const EntryRow = Schema.Struct({ id: Schema.String, entry_json: Schema.NullOr(Schema.String) });
const EntryRows = Schema.Array(EntryRow);
const WorkEntryJson = Schema.fromJsonString(CanonicalWorkEntry);
const decodeWorkEntry = Schema.decodeEffect(WorkEntryJson);
const encodeWorkEntry = Schema.encodeEffect(WorkEntryJson);
const Tail = Schema.Struct({ tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)) });

const RebuildRow = Schema.Struct({
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  record_id: RecordId,
  batch_id: CanonicalRecordEnvelope.fields.batchId,
  record_bytes: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
});

const DeliveryRow = Schema.Struct({
  message_id: IdempotencyKey,
  version: SqlInteger.pipe(Schema.decodeTo(Schema.Int.check(Schema.isGreaterThan(0)))),
  state: Schema.Literals(["pending", "accepted", "parked"]),
  partition: Schema.Literals(["ordinary", "update"]),
  deadline_at_millis: Schema.NullOr(SqlInteger.pipe(Schema.decodeTo(Schema.Natural))),
});

export const makeSqlThreadWork = Effect.fnUntraced(function* (options: SqlThreadWorkOptions = {}) {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const { table, execute } = yield* makeSqlQuery(options.namespace);
  const recordJson = canonicalRecordJson(sql, options.namespace);

  const admissionIndex = sql.onDialectOrElse({
    pg: () => sql``,
    orElse: () => sql`INDEXED BY effect_agent_submissions_nonterminal`,
  });

  const intervals = yield* makeSqlSettlementIntervals(options.namespace);

  const deliveryIndex = sql.onDialectOrElse({
    pg: () => sql``,
    orElse: () => sql`INDEXED BY effect_agent_message_deliveries_pending`,
  });

  const query = <A extends object>(statement: ReturnType<typeof sql<A>>) =>
    execute(statement).pipe(Effect.mapError((cause) => failure("query work index", cause)));

  // Do not materialize an oversized damaged derivative in the application process.
  const boundedEntryJson = sql`CASE WHEN ${sql.onDialectOrElse({
    pg: () => sql`octet_length(entry_json)`,
    orElse: () => sql`length(CAST(entry_json AS BLOB))`,
  })} <= ${MAX_WORK_ENTRY_BYTES} THEN entry_json ELSE NULL END AS entry_json`;

  const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown) =>
    Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.mapError((cause) => failure("decode work metadata", cause)),
    );

  const read =
    options.read ??
    (<A, E, R>(body: Effect.Effect<A, E, R>) =>
      sql
        .withTransaction(body)
        .pipe(Effect.catchTag("SqlError", (cause) => failure("read work snapshot", cause))));

  const write =
    options.write ??
    (<A, E, R>(body: Effect.Effect<A, E, R>) =>
      sql
        .withTransaction(body)
        .pipe(Effect.catchTag("SqlError", (cause) => failure("write work index", cause))));

  const unavailable = (threadId: ThreadId, reason: WorkDiscoveryUnavailable["reason"]) =>
    WorkDiscoveryUnavailable.make({ threadId, reason });

  // Probe before referencing missing relations: a failed statement aborts PostgreSQL transactions.
  const present = Effect.fnUntraced(function* () {
    const rows = yield* query(
      sql.onDialectOrElse({
        pg: () => sql`SELECT COUNT(*) AS count FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=${options.namespace ?? "public"} AND c.relkind='r'
          AND c.relname IN ('effect_agent_work_index', 'effect_agent_work_entries')`,
        orElse: () => sql`SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table'
        AND name IN ('effect_agent_work_index', 'effect_agent_work_entries')`,
      }),
    );

    return (yield* decode(CountRow, rows))[0].count === 2;
  });

  const header = Effect.fnUntraced(function* (threadId: ThreadId) {
    if (!(yield* present())) return undefined;

    const rows = yield* query(sql`SELECT version, state, through_sequence, reporting, entry_count,
      (SELECT COUNT(*) FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId}) AS actual_count
      FROM ${table("effect_agent_work_index")} WHERE thread_id=${threadId}`);

    const decoded = yield* decodeHeader(rows).pipe(Effect.orElseSucceed(() => undefined));

    const value = decoded?.length === 1 ? decoded[0] : undefined;

    if (
      value === undefined ||
      value.version !== WORK_INDEX_VERSION ||
      value.entry_count !== value.actual_count
    )
      return undefined;

    return value;
  });

  const tail = Effect.fnUntraced(function* (threadId: ThreadId) {
    const rows = yield* decode(
      Schema.Array(Tail),
      yield* query(
        sql`SELECT tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
      ),
    );

    return rows[0]?.tail_sequence ?? CanonicalSequence.make(0);
  });

  const requireReady = Effect.fnUntraced(function* (threadId: ThreadId) {
    const current = yield* header(threadId);

    if (current === undefined) return yield* unavailable(threadId, "missing-index");
    if (current.state !== "ready" || current.through_sequence !== (yield* tail(threadId)))
      return yield* unavailable(threadId, "incomplete-rebuild");

    return current;
  });

  const decodeEntry = Effect.fnUntraced(function* (row: typeof EntryRow.Type) {
    if (
      row.entry_json === null ||
      new TextEncoder().encode(row.entry_json).byteLength > MAX_WORK_ENTRY_BYTES
    )
      return yield* failure("work entry byte bound");

    const entry = yield* decodeWorkEntry(row.entry_json).pipe(
      Effect.mapError((cause) => failure("decode canonical work", cause)),
    );

    if (entry.id !== row.id) return yield* failure("canonical work identity");

    return entry;
  });

  const getEntry = Effect.fnUntraced(function* (threadId: ThreadId, id: string) {
    const rows = yield* decode(
      EntryRows,
      yield* query(
        sql`SELECT id, ${boundedEntryJson} FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId} AND id=${id}`,
      ),
    );

    return rows[0] === undefined ? undefined : yield* decodeEntry(rows[0]);
  });

  const indexCanonicalPointers = Effect.fnUntraced(function* (
    threadId: ThreadId,
    sequence: CanonicalSequence,
    record: CanonicalRecord,
    rebuild = false,
  ) {
    yield* intervals.apply(threadId, sequence, record);
    const pointers = canonicalRecordPointers(record);

    // Fresh canonical sequences have no old pointers. Bounded rebuild repairs their derivatives.
    if (rebuild) {
      yield* query(
        sql`DELETE FROM ${table("effect_agent_tool_declarations")} WHERE thread_id=${threadId} AND sequence=${sequence}`,
      );
      yield* query(
        sql`DELETE FROM ${table("effect_agent_record_refusals")} WHERE thread_id=${threadId} AND sequence=${sequence}`,
      );
    }
    for (const settlement of pointers.toolSettlements)
      yield* query(sql`INSERT INTO ${table("effect_agent_tool_declarations")} (thread_id, settlement_record_id, sequence)
        VALUES (${threadId}, ${settlement}, ${sequence})`);
    if (pointers.refusalReservation !== undefined)
      yield* query(sql`INSERT INTO ${table("effect_agent_record_refusals")} (thread_id, reservation_record_id, sequence)
        VALUES (${threadId}, ${pointers.refusalReservation}, ${sequence})`);
  });

  const fold = Effect.fnUntraced(function* (
    threadId: ThreadId,
    sequence: CanonicalSequence,
    record: CanonicalRecord,
    workerMode: WorkerReportingMode,
  ) {
    // Append facts are privately captured; rebuild facts have crossed the persisted Schema.
    const envelope = new CanonicalRecordEnvelope(
      {
        threadId,
        sequence,
        record,
        batchId: BatchId.make("work-index"),
        offset: ObservationOffset.make(`work-index:${sequence}`),
      },
      { disableChecks: true },
    );

    const changes = yield* workIndexChanges(envelope, workerMode).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
    );

    for (const change of changes) {
      if (change._tag === "WorkerMode") {
        workerMode = change.mode;
        continue;
      }
      if (change._tag === "Remove") {
        if (change.stateRecordId !== undefined) {
          const previous = yield* getEntry(threadId, change.id);

          if (
            previous?.stateReference._tag !== "Canonical" ||
            previous.stateReference.recordId !== change.stateRecordId
          )
            continue;
        }
        // apply/rebuild already hold the native writer, so this comparison and delete are atomic.
        yield* query(
          sql`DELETE FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId} AND id=${change.id}`,
        );
        continue;
      }

      const messageId =
        change.entry.owner._tag === "Handoff" ? change.entry.owner.messageId : undefined;

      if (messageId !== undefined) {
        const rows = yield* query(
          sql`SELECT 1 FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${threadId} AND message_id=${messageId}`,
        );

        if (rows.length > 0) continue;
      }
      const entry = advanceWorkEntry(yield* getEntry(threadId, change.entry.id), change.entry);

      const encoded = yield* encodeWorkEntry(entry).pipe(
        Effect.mapError((cause) => failure("encode work entry", cause)),
      );

      if (new TextEncoder().encode(encoded).byteLength > MAX_WORK_ENTRY_BYTES)
        return yield* failure("work entry byte bound");
      const worker = entry.owner._tag === "WorkerInput" ? entry.owner : undefined;
      const handoff = entry.owner._tag === "Handoff" ? entry.owner.kind : null;

      yield* query(sql`INSERT INTO ${table("effect_agent_work_entries")} (thread_id, id, message_id, entry_json, created_sequence, owner_tag, worker_thread_id, worker_update, handoff_kind)
        VALUES (${threadId}, ${entry.id}, ${messageId === undefined ? null : JSON.stringify(messageId)}, ${encoded}, ${entry.createdSequence}, ${entry.owner._tag}, ${worker === undefined ? null : JSON.stringify(worker.workerThreadId)}, ${worker === undefined ? null : worker.update ? 1 : 0}, ${handoff})
        ON CONFLICT (thread_id, id) DO UPDATE SET message_id=excluded.message_id, entry_json=excluded.entry_json,
          created_sequence=excluded.created_sequence, owner_tag=excluded.owner_tag, worker_thread_id=excluded.worker_thread_id, worker_update=excluded.worker_update, handoff_kind=excluded.handoff_kind`);
    }

    return workerMode;
  });

  const saveHeader = Effect.fnUntraced(function* (
    threadId: ThreadId,
    through: CanonicalSequence,
    workerMode: WorkerReportingMode,
    state: "ready" | "rebuilding",
  ) {
    const reporting = workerMode === "standard" ? 1 : workerMode === "private" ? 2 : 0;

    yield* decode(
      CountRow,
      yield* query(sql`INSERT INTO ${table("effect_agent_work_index")} (thread_id, version, state, through_sequence, reporting, entry_count)
        VALUES (${threadId}, ${WORK_INDEX_VERSION}, ${state}, ${through}, ${reporting},
          (SELECT COUNT(*) FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId}))
        ON CONFLICT (thread_id) DO UPDATE SET version=excluded.version, state=excluded.state,
          through_sequence=excluded.through_sequence, reporting=excluded.reporting, entry_count=excluded.entry_count
        RETURNING entry_count AS count`),
    );
  });

  /** Only for newly materialized/imported Threads, inside their publication transaction. */
  const initialize = Effect.fnUntraced(function* (threadId: ThreadId) {
    if (!(yield* present())) return;
    yield* query(
      sql`DELETE FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId}`,
    );
    yield* saveHeader(threadId, CanonicalSequence.make(0), "none", "ready");
  });

  /** Called with captured canonical records; a rebuilding/missing catalogue stays unavailable. */
  const apply = Effect.fnUntraced(function* (
    threadId: ThreadId,
    firstSequence: CanonicalSequence,
    records: ReadonlyArray<CanonicalRecord>,
  ) {
    for (const [index, record] of records.entries())
      yield* indexCanonicalPointers(
        threadId,
        CanonicalSequence.make(firstSequence + index),
        record,
      );
    const current = yield* header(threadId);

    if (
      current === undefined ||
      current.state !== "ready" ||
      current.through_sequence !== firstSequence - 1
    )
      return;
    let workerMode = workerReportingMode(current.reporting);
    let through = current.through_sequence;

    for (const record of records) {
      through = CanonicalSequence.make(through + 1);
      workerMode = yield* fold(threadId, through, record, workerMode);
    }
    yield* saveHeader(threadId, through, workerMode, "ready");
  });

  /** Source insert hook, including deduplication; seed only an absent, unmaterialized owner. */
  const transferDelivery = Effect.fnUntraced(function* (
    threadId: ThreadId,
    messageId: IdempotencyKey,
  ) {
    const current = yield* header(threadId);

    if (current === undefined) {
      if (!(yield* present())) return;

      const retained = yield* query(sql`SELECT 1 AS retained WHERE
        EXISTS (SELECT 1 FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId})
        OR EXISTS (SELECT 1 FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${threadId})
        OR EXISTS (SELECT 1 FROM ${table("effect_agent_work_index")} WHERE thread_id=${threadId})
        OR EXISTS (SELECT 1 FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId})`);

      if (retained.length === 0) yield* initialize(threadId);

      return;
    }
    yield* query(
      sql`DELETE FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId} AND message_id=${JSON.stringify(messageId)}`,
    );
    yield* saveHeader(
      threadId,
      current.through_sequence,
      workerReportingMode(current.reporting),
      current.state,
    );
  });

  const storage: ThreadWorkStorage = {
    page: Effect.fnUntraced(function* (input) {
      const request = yield* decode(ThreadWorkRequest, input);

      return yield* read(
        Effect.gen(function* () {
          yield* requireReady(request.threadId);
          const cursor = yield* decodeWorkCursor(request);
          const after = cursor.after;
          let entries: ReadonlyArray<ThreadWorkEntry>;
          let next: string | undefined;

          if (cursor.source === "admissions") {
            const sequence =
              after === undefined
                ? 0
                : yield* decode(
                    Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.Natural)),
                    after,
                  );

            const rows = yield* decode(
              Schema.Array(
                Schema.Struct({
                  ...SubmissionWorkItem.fields,
                  queueSequence: SqlInteger.pipe(
                    Schema.decodeTo(SubmissionWorkItem.fields.queueSequence),
                  ),
                }),
              ),
              yield* query(sql`SELECT submission_id AS "submissionId", thread_id AS "threadId", queue_sequence AS "queueSequence",
            principal, idempotency_key AS "idempotencyKey", deployment_id AS "deploymentId", receipt_id AS "receiptId", state
            FROM ${table("effect_agent_submissions")} ${admissionIndex} WHERE thread_id=${request.threadId} AND state <> 'settled'
              AND queue_sequence>${sequence} ORDER BY queue_sequence LIMIT ${request.limit + 1}`),
            );

            const more = rows.length > request.limit ? rows[request.limit - 1] : undefined;

            entries = rows.slice(0, request.limit).map(admissionWork);
            next = encodeWorkCursor({
              version: WORK_INDEX_VERSION,
              threadId: request.threadId,
              source: more === undefined ? "canonical" : "admissions",
              ...(more === undefined ? {} : { after: String(more.queueSequence) }),
            });
          } else if (cursor.source === "canonical") {
            const rows = yield* decode(
              Schema.Array(EntryRow),
              yield* query(sql`SELECT id, ${boundedEntryJson} FROM ${table("effect_agent_work_entries")}
            WHERE thread_id=${request.threadId} ${after === undefined ? sql`` : sql`AND id>${after}`} ORDER BY id LIMIT ${request.limit + 1}`),
            );

            const more = rows.length > request.limit ? rows[request.limit - 1] : undefined;

            entries = yield* Effect.forEach(rows.slice(0, request.limit), decodeEntry);
            next = encodeWorkCursor({
              version: WORK_INDEX_VERSION,
              threadId: request.threadId,
              source: more === undefined ? "deliveries" : "canonical",
              ...(more === undefined ? {} : { after: more.id }),
            });
          } else {
            const rows = yield* decode(
              Schema.Array(DeliveryRow),
              yield* query(sql`SELECT message_id, version, state, deadline_at_millis,
              ${sql.onDialectOrElse({
                pg: () =>
                  sql`CASE (read_metadata ->> 'workerUpdate') WHEN 'true' THEN 'update' WHEN 'false' THEN 'ordinary' ELSE NULL END`,
                orElse: () =>
                  sql`CASE WHEN ${sqliteJsonText(sql, "record_json", ["envelope", "messageAdmission", "_tag"])} = 'WorkerUpdate' THEN 'update' ELSE 'ordinary' END`,
              })} AS partition
            FROM ${table("effect_agent_message_deliveries")} ${deliveryIndex} WHERE owner_thread_id=${request.threadId} AND state NOT IN ('processed', 'refused')
            ${after === undefined ? sql`` : sql`AND message_id>${after}`} ORDER BY message_id LIMIT ${request.limit + 1}`),
            );

            const more = rows.length > request.limit ? rows[request.limit - 1] : undefined;

            entries = rows.slice(0, request.limit).map((row): ThreadWorkEntry => ({
              id: workId("delivery", row.message_id),
              owner: { _tag: "Delivery", messageId: row.message_id },
              stateReference: { _tag: "Delivery", messageId: row.message_id, version: row.version },
              state: row.state === "pending" ? "ready" : "waiting",
              partition: row.partition,
              ...(row.state === "parked"
                ? { wait: "parked" }
                : row.state === "accepted"
                  ? { wait: "destination" }
                  : {}),
              ...(row.deadline_at_millis === null
                ? {}
                : { notBeforeMillis: row.deadline_at_millis }),
            }));
            if (more !== undefined)
              next = encodeWorkCursor({
                version: WORK_INDEX_VERSION,
                threadId: request.threadId,
                source: "deliveries",
                after: more.message_id,
              });
          }

          return yield* validateWorkPage(
            { entries, ...(next === undefined ? {} : { cursor: next }) },
            request.limit,
          );
        }),
      );
    }),
    threads: Effect.fnUntraced(function* (input) {
      const request = yield* decode(WorkThreadsRequest, input);

      return yield* read(
        Effect.gen(function* () {
          if (!(yield* present())) return yield* failure("discover work owners without catalogue");

          // Keep incomplete owners discoverable; healthy empty Threads need no runtime sweep.
          const after = request.afterThreadId;
          const limit = request.limit + 1;

          const rows = yield* decode(
            Schema.Array(Schema.Struct({ thread_id: ThreadId })),
            yield* query(sql`
          SELECT thread_id FROM (
            SELECT thread_id FROM (
              SELECT canonical.thread_id FROM ${table("effect_agent_threads")} AS canonical
              LEFT JOIN ${table("effect_agent_work_index")} AS work ON work.thread_id=canonical.thread_id
              WHERE (work.thread_id IS NULL OR work.version IS NULL OR work.version<>${WORK_INDEX_VERSION}
                OR work.state IS NULL OR work.state<>'ready'
                OR work.through_sequence IS NULL OR work.through_sequence<>canonical.tail_sequence
                OR work.entry_count IS NULL OR work.entry_count<>0
                OR work.reporting IS NULL OR work.reporting NOT IN (0, 1, 2))
              ${after === undefined ? sql`` : sql`AND canonical.thread_id>${after}`}
              ORDER BY canonical.thread_id LIMIT ${limit}
            ) AS canonical_owners
            UNION SELECT thread_id FROM (SELECT DISTINCT thread_id FROM ${table("effect_agent_submissions")} ${admissionIndex}
              WHERE state <> 'settled' ${after === undefined ? sql`` : sql`AND thread_id>${after}`}
              ORDER BY thread_id LIMIT ${limit}) AS admission_owners
            UNION SELECT thread_id FROM (SELECT DISTINCT owner_thread_id AS thread_id FROM ${table("effect_agent_message_deliveries")} ${deliveryIndex}
              WHERE state NOT IN ('processed', 'refused') ${after === undefined ? sql`` : sql`AND owner_thread_id>${after}`}
              ORDER BY owner_thread_id LIMIT ${limit}) AS delivery_owners
            UNION SELECT thread_id FROM (SELECT DISTINCT thread_id FROM ${table("effect_agent_work_entries")}
              ${after === undefined ? sql`` : sql`WHERE thread_id>${after}`}
              ORDER BY thread_id LIMIT ${limit}) AS canonical_work_owners
          ) AS owners ORDER BY thread_id LIMIT ${limit}`),
          );

          const threadIds = rows.slice(0, request.limit).map((row) => row.thread_id);
          const more = rows.length > request.limit ? rows[request.limit - 1] : undefined;

          return {
            threadIds,
            ...(more === undefined ? {} : { afterThreadId: more.thread_id }),
          };
        }),
      );
    }),
    rebuild: Effect.fnUntraced(function* (input) {
      const request = yield* decode(WorkIndexRebuildRequest, input);

      return yield* write(
        Effect.gen(function* () {
          yield* createSqlThreadWorkTables(options.namespace).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          const observedTail = yield* tail(request.threadId);
          let current = yield* header(request.threadId);

          if (request.restart || current === undefined) {
            yield* query(
              sql`DELETE FROM ${table("effect_agent_work_entries")} WHERE thread_id=${request.threadId}`,
            );
            yield* saveHeader(request.threadId, CanonicalSequence.make(0), "none", "rebuilding");
            current = {
              version: WORK_INDEX_VERSION,
              state: "rebuilding",
              through_sequence: CanonicalSequence.make(0),
              reporting: 0,
              entry_count: 0,
              actual_count: 0,
            };
          }
          if (current.through_sequence > observedTail)
            return yield* unavailable(request.threadId, "incomplete-rebuild");

          const plan = yield* decode(
            Schema.Array(RebuildRow),
            yield* query(sql`SELECT sequence, record_id, batch_id,
          ${sql.onDialectOrElse({ pg: () => sql`octet_length(${recordJson})`, orElse: () => sql`length(CAST(${recordJson} AS BLOB))` })} AS record_bytes
          FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${request.threadId} AND sequence>${current.through_sequence}
            AND sequence<=${observedTail} ORDER BY sequence LIMIT ${request.limit ?? MAX_WORK_REBUILD_RECORDS}`),
          );

          let through = current.through_sequence;
          let workerMode = workerReportingMode(current.reporting);
          let processedBytes = 0;
          let processedRecords = 0;

          for (const row of plan) {
            if (row.sequence !== through + 1 || row.record_bytes > MAX_WORK_REBUILD_BYTES)
              return yield* failure("rebuild canonical coverage");
            if (processedBytes + row.record_bytes > MAX_WORK_REBUILD_BYTES) break;

            const [raw] = yield* decode(
              Schema.Tuple([Schema.Struct({ record_json: Schema.String })]),
              yield* query(sql`
            SELECT ${recordJson} AS record_json FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${request.threadId} AND sequence=${row.sequence}`),
            );

            if (new TextEncoder().encode(raw.record_json).byteLength !== row.record_bytes)
              return yield* failure("rebuild canonical bytes");

            const record = yield* Schema.decodeEffect(Schema.fromJsonString(ExportRecord))(
              raw.record_json,
            ).pipe(Effect.mapError((cause) => failure("decode rebuild record", cause)));

            if (record.recordId !== row.record_id)
              return yield* failure("rebuild canonical identity");

            const wire = yield* Schema.encodeEffect(ExportRecord)(record).pipe(
              Effect.mapError((cause) => failure("encode rebuild metadata", cause)),
            );

            const metadata = canonicalRecordMetadata({ canonical: record, wire });

            yield* query(sql`UPDATE ${table("effect_agent_canonical_records")} SET ${sql.update(metadata.columns)}
            WHERE thread_id=${request.threadId} AND sequence=${row.sequence}`);
            yield* query(
              sql`DELETE FROM ${table("effect_agent_record_runs")} WHERE thread_id=${request.threadId} AND sequence=${row.sequence}`,
            );
            for (const runId of metadata.runIds)
              yield* query(sql`INSERT INTO ${table("effect_agent_record_runs")} (thread_id, run_id, sequence)
            VALUES (${request.threadId}, ${runId}, ${row.sequence})`);
            yield* indexCanonicalPointers(request.threadId, row.sequence, record, true);
            workerMode = yield* fold(request.threadId, row.sequence, record, workerMode);
            through = row.sequence;
            processedBytes += row.record_bytes;
            processedRecords++;
          }
          const latestTail = yield* tail(request.threadId);

          if (processedRecords === 0 && through < latestTail)
            return yield* failure("rebuild missing canonical records");
          const state = through === latestTail ? "ready" : "rebuilding";

          yield* saveHeader(request.threadId, through, workerMode, state);

          return WorkIndexProgress.make({
            version: WORK_INDEX_VERSION,
            threadId: request.threadId,
            state,
            throughSequence: through,
            tailSequence: latestTail,
            processedRecords,
            processedBytes,
          });
        }),
      );
    }),
  };

  return { storage, initialize, apply, transferDelivery };
});
