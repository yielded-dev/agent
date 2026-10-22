/**
 * Canonical archive range reads and publication over adapter-owned SQL tables.
 *
 * @since 0.1.0
 */
import { canonicalJson, utf8ByteLength } from "@yielded/agent/digest";
import { ExportBatch, ExportRecord } from "@yielded/agent/record-format";
import { CanonicalSequence } from "@yielded/agent/records";
import {
  MAX_ARCHIVE_RANGE_BYTES,
  MAX_ARCHIVE_RANGE_PAGE,
  MAX_ARCHIVE_RANGE_RECORDS,
  MAX_CANONICAL_BATCH_BYTES,
  ThreadArchiveRange,
  ThreadArchiveRangeRead,
  ThreadArchiveRangeSeal,
  ThreadArchiveRangeRequest,
  ThreadArchiveRangePublish,
  type ThreadArchiveStorage,
} from "@yielded/agent/thread-archive-range";
import {
  ThreadNotMaterialized,
  ThreadStoreError,
  type ThreadExportBatch,
} from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { hotRecordJson, recordBatchId, storedBatchHeader } from "./SqlCanonicalStorage.ts";
import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";

const bytes = utf8ByteLength;
const maxVerificationPageBytes = 1024 * 1024;

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Canonical archive range is incomplete, corrupt, or exceeds its bounds.",
    ...(cause === undefined ? {} : { cause }),
  });

/**
 * Keep indexed selectors on the identity table, including SQLite's INDEXED BY hints. A scalar
 * locator probe prevents the planner from expanding archive ownership into a Thread-wide join.
 *
 * @category utilities
 * @since 0.1.0
 */
export const canonicalRecordJson = (
  sql: SqlClient.SqlClient,
  namespace?: string,
  alias?: string,
) => {
  const table = (name: string) => sql(namespace === undefined ? name : `${namespace}.${name}`);
  const owner = sql(alias ?? "effect_agent_canonical_records");

  return sql`COALESCE(${hotRecordJson(sql, namespace, alias)}, (
    SELECT a.record_json FROM ${table("effect_agent_archive_records")} a
    WHERE a.thread_id=${owner}.thread_id AND a.sequence=${owner}.sequence
      AND (SELECT r.state='archived' AND r.locator IS NOT NULL
        FROM ${table("effect_agent_journal_ranges")} r
        WHERE r.thread_id=a.thread_id AND r.first_sequence=a.range_first_sequence)
  ))`;
};

/**
 * Exact canonical metadata; records remain the only hot payload copy.
 *
 * @internal
 */
export const canonicalBatchHeaderJson = (batch: typeof ThreadExportBatch.Type): string =>
  canonicalJson({ batchId: batch.batchId, producerId: batch.producerId });

const textBytes = (sql: SqlClient.SqlClient, text: ReturnType<typeof sql.literal>) =>
  sql.onDialectOrElse({
    pg: () => sql`octet_length(${text})`,
    orElse: () => sql`length(CAST(${text} AS BLOB))`,
  });

const canonicalBatchValue = (
  sql: SqlClient.SqlClient,
  namespace: string | undefined,
  alias: string | undefined,
  sizeOnly: boolean,
) => {
  const table = (name: string) => sql(namespace === undefined ? name : `${namespace}.${name}`);
  const owner = sql(alias ?? "effect_agent_canonical_batches");
  const header = storedBatchHeader(sql, alias);
  const recordBytes = textBytes(sql, sql`batch_records.record_json`);

  const contents = sql.onDialectOrElse({
    pg: () => sql`string_agg(batch_records.record_json, ',' ORDER BY batch_records.sequence)`,
    orElse: () => sql`group_concat(batch_records.record_json, ',')`,
  });

  // Preserve wire escaping and order. The records member adds 13 bytes plus n-1 commas.
  const hot = sql`(SELECT CASE WHEN COUNT(*)=${owner}.last_sequence-${owner}.first_sequence+1
      AND COUNT(batch_records.record_json)=COUNT(*) THEN ${
        sizeOnly
          ? sql`${textBytes(sql, header)} + 12 + COUNT(*) + SUM(${recordBytes})`
          : sql`substr(${header}, 1, length(${header}) - 1) || ',"records":[' || ${contents} || ']}'`
      }
      END FROM (SELECT sequence, ${hotRecordJson(sql, namespace, "hot")} AS record_json FROM ${table("effect_agent_canonical_records")} hot
        WHERE thread_id=${owner}.thread_id
          AND sequence BETWEEN ${owner}.first_sequence AND ${owner}.last_sequence
        ORDER BY sequence) batch_records)`;

  const archived = sizeOnly ? textBytes(sql, sql`a.batch_json`) : sql`a.batch_json`;

  // Hot history needs no range metadata. Once archived, only the validated locator selects bytes.
  return sql`CASE WHEN EXISTS (SELECT 1 FROM ${table("effect_agent_journal_ranges")} placement
    WHERE placement.thread_id=${owner}.thread_id AND placement.first_sequence<=${owner}.first_sequence
      AND placement.last_sequence>=${owner}.last_sequence AND placement.state='archived') THEN (
    SELECT ${archived} FROM ${table("effect_agent_archive_batches")} a
      JOIN ${table("effect_agent_journal_ranges")} placement ON placement.thread_id=a.thread_id AND placement.first_sequence=a.range_first_sequence
    WHERE a.thread_id=${owner}.thread_id AND a.batch_id=${owner}.batch_id
      AND placement.state='archived' AND placement.locator IS NOT NULL
      AND placement.first_sequence<=${owner}.first_sequence AND placement.last_sequence>=${owner}.last_sequence
  ) ELSE ${hot} END`;
};

/**
 * Byte-identical to PreparedAppend and canonical import serialization.
 *
 * @internal
 */
export const canonicalBatchJson = (sql: SqlClient.SqlClient, namespace?: string, alias?: string) =>
  canonicalBatchValue(sql, namespace, alias, false);

/**
 * Inspect sizes without allocating a reconstructed batch payload.
 *
 * @internal
 */
export const canonicalBatchBytes = (sql: SqlClient.SqlClient, namespace?: string, alias?: string) =>
  canonicalBatchValue(sql, namespace, alias, true);

const RangeRow = Schema.Struct({
  thread_id: ThreadArchiveRange.fields.threadId,
  first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  record_count: SqlInteger.pipe(Schema.decodeTo(Schema.Int)),
  batch_count: SqlInteger.pipe(Schema.decodeTo(Schema.Int)),
  byte_count: SqlInteger.pipe(Schema.decodeTo(Schema.Int)),
  state: Schema.Literals(["sealed", "archived"]),
  locator: Schema.NullOr(Schema.String),
});

type RangeRow = typeof RangeRow.Type;

const decodeRanges = Schema.decodeUnknownEffect(Schema.Array(RangeRow));

const decodeReplayPlan = Schema.decodeUnknownEffect(
  Schema.Array(
    Schema.Struct({
      first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
      last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
      byte_count: SqlInteger.pipe(
        Schema.decodeTo(
          Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_CANONICAL_BATCH_BYTES })),
        ),
      ),
    }),
  ).check(Schema.isMaxLength(1)),
);

/**
 * Adapter-provided snapshot and atomic write boundaries for canonical archives.
 *
 * @category models
 * @since 0.1.0
 */
export interface SqlArchiveTransactions {
  readonly read: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreError, R>;
  readonly write: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreError, R>;
}

/**
 * Adapter layout DDL owns these authoritative tables; this module never initializes them.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeSqlThreadArchiveRange = Effect.fnUntraced(function* (
  transactions: SqlArchiveTransactions,
  namespace?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);

  const query = <A extends object>(statement: Parameters<typeof execute<A>>[0]) =>
    execute(statement).pipe(Effect.mapError((cause) => failure("archive SQL", cause)));

  /** Retired operational intent is deliberately absent; historical replay proves canonical bytes. */
  const replay = Effect.fnUntraced(function* (threadId: string, batchId: string) {
    const [plan] = yield* decodeReplayPlan(
      yield* query(sql`
      SELECT first_sequence, last_sequence, ${canonicalBatchBytes(sql, namespace)} AS byte_count
      FROM ${table("effect_agent_canonical_batches")} WHERE thread_id=${threadId} AND batch_id=${JSON.stringify(batchId)}`),
    ).pipe(Effect.mapError((cause) => failure("replay batch plan", cause)));

    if (plan === undefined) return undefined;
    if (
      plan.first_sequence < 1 ||
      plan.last_sequence < plan.first_sequence ||
      plan.last_sequence - plan.first_sequence >= 256
    )
      return yield* failure("replay batch span");

    const [payload] = yield* Schema.decodeUnknownEffect(
      Schema.Tuple([Schema.Struct({ batch_json: Schema.String })]),
    )(
      yield* query(sql`SELECT ${canonicalBatchJson(sql, namespace)} AS batch_json
      FROM ${table("effect_agent_canonical_batches")} WHERE thread_id=${threadId} AND batch_id=${JSON.stringify(batchId)}`),
    ).pipe(Effect.mapError((cause) => failure("replay batch payload", cause)));

    if (bytes(payload.batch_json) !== plan.byte_count) return yield* failure("replay batch size");

    const batch = yield* Schema.decodeEffect(Schema.fromJsonString(ExportBatch))(
      payload.batch_json,
    ).pipe(Effect.mapError((cause) => failure("replay batch wire", cause)));

    if (
      batch.batchId !== batchId ||
      batch.records.length !== plan.last_sequence - plan.first_sequence + 1
    )
      return yield* failure("replay batch identity");

    return { ...plan, batch_json: payload.batch_json };
  });

  const descriptor = Effect.fnUntraced(function* (row: RangeRow) {
    if (
      (row.state === "archived") !== (row.locator !== null) ||
      row.last_sequence - row.first_sequence + 1 !== row.record_count ||
      row.batch_count > row.record_count
    )
      return yield* failure("archive descriptor identity");

    return yield* ThreadArchiveRange.makeEffect({
      format: "effect-agent/thread-range@2",
      threadId: row.thread_id,
      firstSequence: row.first_sequence,
      lastSequence: row.last_sequence,
      recordCount: row.record_count,
      batchCount: row.batch_count,
      byteCount: row.byte_count,
      state: row.state === "archived" ? "archived" : "sealed",
      ...(row.locator === null ? {} : { locator: row.locator }),
    }).pipe(Effect.mapError((cause) => failure("archive descriptor", cause)));
  });

  const requireThread = Effect.fnUntraced(function* (threadId: ThreadArchiveRange["threadId"]) {
    const rows = yield* Schema.decodeUnknownEffect(
      Schema.Array(
        Schema.Struct({
          tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
        }),
      ),
    )(
      yield* query(
        sql`SELECT tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId} AND created_at IS NOT NULL`,
      ),
    ).pipe(Effect.mapError((cause) => failure("archive owner", cause)));

    if (rows.length === 0) return yield* ThreadNotMaterialized.make({ threadId });
    if (rows.length !== 1) return yield* failure("archive owner");
    const [row] = rows;

    if (row === undefined) return yield* failure("archive owner");

    return row.tail_sequence;
  });

  const range = Effect.fnUntraced(function* (
    threadId: ThreadArchiveRange["threadId"],
    firstSequence: CanonicalSequence,
  ) {
    const rows = yield* decodeRanges(
      yield* query(sql`SELECT * FROM ${table("effect_agent_journal_ranges")}
      WHERE thread_id=${threadId} AND first_sequence=${firstSequence}`),
    ).pipe(Effect.mapError((cause) => failure("archive range", cause)));

    if (rows.length !== 1 || rows[0] === undefined) return yield* failure("archive range missing");
    yield* descriptor(rows[0]);

    return rows[0];
  });

  /** Scalar bounded planning occurs only for explicit seal/verification, never append. */
  const planHotRange = Effect.fnUntraced(function* (
    threadId: ThreadArchiveRange["threadId"],
    after: number,
    tail: number,
  ) {
    if (after === tail) return undefined;

    const plan = yield* Schema.decodeUnknownEffect(
      Schema.Array(
        Schema.Struct({
          first_sequence: SqlInteger,
          last_sequence: SqlInteger,
          batch_bytes: SqlInteger,
          record_bytes: SqlInteger,
        }),
      ),
    )(
      yield* query(sql`SELECT b.first_sequence, b.last_sequence, ${canonicalBatchBytes(sql, namespace, "b")} AS batch_bytes,
      (SELECT SUM(${textBytes(sql, hotRecordJson(sql, namespace, "r"))}) FROM ${table("effect_agent_canonical_records")} r
        WHERE r.thread_id=b.thread_id AND r.sequence BETWEEN b.first_sequence AND b.last_sequence) AS record_bytes
      FROM ${table("effect_agent_canonical_batches")} b WHERE b.thread_id=${threadId} AND b.first_sequence>${after}
      ORDER BY b.first_sequence LIMIT ${MAX_ARCHIVE_RANGE_RECORDS}`),
    ).pipe(Effect.mapError((cause) => failure("plan hot range", cause)));

    let last = after,
      count = 0,
      batches = 0,
      size = 0;

    for (const batch of plan) {
      if (
        batch.first_sequence !== last + 1 ||
        batch.last_sequence > tail ||
        batch.last_sequence < batch.first_sequence ||
        batch.last_sequence - batch.first_sequence >= 256 ||
        batch.batch_bytes < 1 ||
        batch.batch_bytes > MAX_CANONICAL_BATCH_BYTES ||
        batch.record_bytes < 1
      )
        return yield* failure("plan hot range coverage");
      const records = batch.last_sequence - batch.first_sequence + 1;
      const bytes = batch.batch_bytes + batch.record_bytes;

      if (count + records > MAX_ARCHIVE_RANGE_RECORDS || size + bytes > MAX_ARCHIVE_RANGE_BYTES)
        break;
      last = batch.last_sequence;
      count += records;
      batches++;
      size += bytes;
    }
    if (count === 0) return yield* failure("plan empty or oversized hot range");

    return {
      thread_id: threadId,
      first_sequence: CanonicalSequence.make(after + 1),
      last_sequence: CanonicalSequence.make(last),
      record_count: count,
      batch_count: batches,
      byte_count: size,
      state: "sealed",
      locator: null,
    } satisfies RangeRow;
  });

  const verifyInTransaction = Effect.fnUntraced(function* (row: RangeRow, copied = false) {
    const result = yield* descriptor(row);

    if (row.state === "archived") {
      const locator = `sql-range:${row.first_sequence}:${row.last_sequence}`;

      if (row.locator !== locator) return yield* failure("archive locator identity");
    }

    if (row.last_sequence - row.first_sequence + 1 !== row.record_count)
      return yield* failure("archive range coverage");
    const archive = copied || row.state === "archived";

    if (row.state === "archived" && row.locator === null)
      return yield* failure("archive locator missing");
    // Plan scalar sizes before fetching any payloads, so corrupt contents cannot allocate an unbounded range.
    const batchText = archive ? sql`a.batch_json` : canonicalBatchJson(sql, namespace, "c");
    const recordText = archive ? sql.literal("a.record_json") : hotRecordJson(sql, namespace, "c");
    const hotRecordText = archive ? hotRecordJson(sql, namespace, "c") : sql`NULL`;

    const size = (text: ReturnType<typeof sql.literal>) => textBytes(sql, text);
    const batchSize = archive ? size(batchText) : canonicalBatchBytes(sql, namespace, "c");

    const BatchPlan = Schema.Struct({
      batch_id: Schema.fromJsonString(Schema.String),
      first_sequence: SqlInteger,
      last_sequence: SqlInteger,
      byte_count: SqlInteger,
      header_byte_count: SqlInteger,
    });

    const RecordPlan = Schema.Struct({
      sequence: SqlInteger,
      record_id: Schema.fromJsonString(Schema.String),
      batch_id: Schema.fromJsonString(Schema.String),
      byte_count: SqlInteger,
      hot_byte_count: Schema.NullOr(SqlInteger),
    });

    const RecordContent = Schema.Struct({
      sequence: SqlInteger,
      record_json: Schema.String,
      hot_record_json: Schema.NullOr(Schema.String),
    });

    const decodeRecordContents = Schema.decodeUnknownEffect(
      Schema.Array(RecordContent).check(Schema.isMaxLength(MAX_ARCHIVE_RANGE_PAGE + 1)),
    );

    const batchJoin = archive
      ? sql`LEFT JOIN ${table("effect_agent_archive_batches")} a ON a.thread_id=c.thread_id AND a.batch_id=c.batch_id AND a.range_first_sequence=${row.first_sequence}`
      : sql``;

    const recordJoin = archive
      ? sql`LEFT JOIN ${table("effect_agent_archive_records")} a ON a.thread_id=c.thread_id AND a.sequence=c.sequence AND a.range_first_sequence=${row.first_sequence}`
      : sql``;

    const batches = yield* Schema.decodeUnknownEffect(Schema.Array(BatchPlan))(
      yield* query(sql`
      SELECT c.batch_id, c.first_sequence, c.last_sequence, ${batchSize} AS byte_count, ${size(storedBatchHeader(sql, "c"))} AS header_byte_count
      FROM ${table("effect_agent_canonical_batches")} c ${batchJoin}
      WHERE c.thread_id=${row.thread_id} AND c.first_sequence>=${row.first_sequence} AND c.first_sequence<=${row.last_sequence}
      ORDER BY c.first_sequence LIMIT ${MAX_ARCHIVE_RANGE_RECORDS + 1}`),
    ).pipe(Effect.mapError((cause) => failure("archive batch plan", cause)));

    const records = yield* Schema.decodeUnknownEffect(Schema.Array(RecordPlan))(
      yield* query(sql`
      SELECT c.sequence, c.record_id, ${recordBatchId(sql, namespace, "c")} AS batch_id, ${size(recordText)} AS byte_count, ${size(hotRecordText)} AS hot_byte_count FROM ${table("effect_agent_canonical_records")} c ${recordJoin}
      WHERE c.thread_id=${row.thread_id} AND c.sequence>=${row.first_sequence} AND c.sequence<=${row.last_sequence}
      ORDER BY c.sequence LIMIT ${MAX_ARCHIVE_RANGE_RECORDS + 1}`),
    ).pipe(Effect.mapError((cause) => failure("archive record plan", cause)));

    if (
      batches.length !== row.batch_count ||
      records.length !== row.record_count ||
      batches.some(
        (batch) =>
          batch.byte_count < 1 ||
          batch.byte_count > MAX_CANONICAL_BATCH_BYTES ||
          batch.header_byte_count < 1 ||
          batch.header_byte_count > MAX_CANONICAL_BATCH_BYTES,
      ) ||
      records.some(
        (record) => record.byte_count < 1 || record.byte_count > MAX_CANONICAL_BATCH_BYTES,
      ) ||
      batches.reduce((total, batch) => total + batch.byte_count, 0) +
        records.reduce((total, record) => total + record.byte_count, 0) !==
        row.byte_count
    )
      return yield* failure("archive size or count");
    if (
      records.some(
        (record) =>
          record.hot_byte_count !== null &&
          (record.hot_byte_count < 1 || record.hot_byte_count > MAX_CANONICAL_BATCH_BYTES),
      )
    )
      return yield* failure("archive record wire");
    let sequence = row.first_sequence;
    let recordIndex = 0;

    for (const batch of batches) {
      const [payload] = yield* Schema.decodeUnknownEffect(
        Schema.Tuple([
          Schema.Struct({
            batch_json: Schema.String,
            batch_header_json: Schema.String,
          }),
        ]),
      )(
        yield* query(sql`
        SELECT ${batchText} AS batch_json, ${storedBatchHeader(sql, "c")} AS batch_header_json FROM ${table("effect_agent_canonical_batches")} c ${batchJoin}
        WHERE c.thread_id=${row.thread_id} AND c.batch_id=${JSON.stringify(batch.batch_id)}`),
      ).pipe(Effect.mapError((cause) => failure("archive batch content", cause)));

      if (bytes(payload.batch_json) !== batch.byte_count || batch.first_sequence !== sequence)
        return yield* failure("archive batch frontier");

      const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ExportBatch))(
        payload.batch_json,
      ).pipe(Effect.mapError((cause) => failure("archive batch decode", cause)));

      if (
        decoded.batchId !== batch.batch_id ||
        canonicalBatchHeaderJson(decoded) !== payload.batch_header_json ||
        batch.last_sequence !== sequence + decoded.records.length - 1
      )
        return yield* failure("archive batch identity");
      let rawRecords: ReadonlyArray<typeof RecordContent.Type> = [];
      let rawIndex = 0;

      for (const record of decoded.records) {
        const plan = records[recordIndex++];

        if (
          plan === undefined ||
          plan.sequence !== sequence ||
          plan.batch_id !== batch.batch_id ||
          plan.record_id !== record.recordId
        )
          return yield* failure("archive record identity");

        if (rawIndex === rawRecords.length) {
          // The scalar plan and payload reads share the caller's transaction. Charge both
          // copies before hydration; one larger valid record runs alone. Never prefetch
          // another batch, and release the consumed page before acquiring its successor.
          rawRecords = [];
          rawIndex = 0;
          let last = plan;
          let count = 1;
          let pageBytes = plan.byte_count + (plan.hot_byte_count ?? 0);

          for (let at = recordIndex; at < records.length && count < MAX_ARCHIVE_RANGE_PAGE; at++) {
            const next = records[at];

            if (
              next === undefined ||
              next.sequence > batch.last_sequence ||
              pageBytes + next.byte_count + (next.hot_byte_count ?? 0) > maxVerificationPageBytes
            )
              break;
            if (next.sequence !== sequence + count || next.batch_id !== batch.batch_id)
              return yield* failure("archive record identity");
            pageBytes += next.byte_count + (next.hot_byte_count ?? 0);
            last = next;
            count++;
          }

          rawRecords = yield* decodeRecordContents(
            yield* query(sql`
            SELECT c.sequence, ${recordText} AS record_json, ${hotRecordText} AS hot_record_json FROM ${table("effect_agent_canonical_records")} c ${recordJoin}
            WHERE c.thread_id=${row.thread_id} AND c.sequence BETWEEN ${sequence} AND ${last.sequence}
            ORDER BY c.sequence LIMIT ${count + 1}`),
          ).pipe(Effect.mapError((cause) => failure("archive record content", cause)));

          if (rawRecords.length !== count) return yield* failure("archive record content");
        }
        const raw = rawRecords[rawIndex++];

        if (raw === undefined || raw.sequence !== sequence)
          return yield* failure("archive record content");

        const encoded = yield* Schema.encodeEffect(ExportRecord)(record).pipe(
          Effect.mapError((cause) => failure("archive record encode", cause)),
        );

        if (
          bytes(raw.record_json) !== plan.byte_count ||
          canonicalJson(encoded) !== raw.record_json ||
          (raw.hot_record_json !== null && raw.hot_record_json !== raw.record_json)
        )
          return yield* failure("archive record wire");
        sequence = CanonicalSequence.make(sequence + 1);
      }
    }
    if (sequence !== row.last_sequence + 1 || recordIndex !== row.record_count)
      return yield* failure("archive final frontier");

    return result;
  });

  const storage: ThreadArchiveStorage = {
    page: (input) =>
      transactions.read(
        Effect.gen(function* () {
          const request = yield* Schema.decodeEffect(ThreadArchiveRangeRead)(input).pipe(
            Effect.mapError((cause) => failure("archive page request", cause)),
          );

          yield* requireThread(request.threadId);
          const limit = request.limit ?? MAX_ARCHIVE_RANGE_PAGE;

          const rows = yield* decodeRanges(
            yield* query(sql`SELECT * FROM ${table("effect_agent_journal_ranges")}
        WHERE thread_id=${request.threadId} AND first_sequence>${request.afterSequence ?? 0}
        ORDER BY first_sequence LIMIT ${limit + 1}`),
          ).pipe(Effect.mapError((cause) => failure("archive page", cause)));

          const ranges = yield* Effect.forEach(rows.slice(0, limit), descriptor);

          return {
            ranges,
            ...(rows.length > limit ? { afterSequence: rows[limit - 1]?.first_sequence } : {}),
          };
        }),
      ),
    seal: (input) =>
      transactions.write(
        Effect.gen(function* () {
          const request = yield* Schema.decodeEffect(ThreadArchiveRangeSeal)(input).pipe(
            Effect.mapError((cause) => failure("archive seal request", cause)),
          );

          const tail = yield* requireThread(request.threadId);

          const previous = yield* decodeRanges(
            yield* query(sql`SELECT * FROM ${table("effect_agent_journal_ranges")}
            WHERE thread_id=${request.threadId} ORDER BY first_sequence DESC LIMIT 1`),
          ).pipe(Effect.mapError((cause) => failure("seal predecessor", cause)));

          if (previous[0] !== undefined) yield* verifyInTransaction(previous[0]);
          const row = yield* planHotRange(request.threadId, previous[0]?.last_sequence ?? 0, tail);

          if (row === undefined) return Option.none();
          const verified = yield* verifyInTransaction(row);

          yield* query(sql`INSERT INTO ${table("effect_agent_journal_ranges")} ${sql.insert(row)}`);

          return Option.some(verified);
        }),
      ),
    archive: (input) =>
      transactions.write(
        Effect.gen(function* () {
          const request = yield* Schema.decodeEffect(ThreadArchiveRangePublish)(input).pipe(
            Effect.mapError((cause) => failure("archive publish request", cause)),
          );

          yield* requireThread(request.threadId);
          const row = yield* range(request.threadId, request.firstSequence);

          if (row.state === "archived") return yield* verifyInTransaction(row);
          yield* verifyInTransaction(row);
          yield* query(sql`INSERT INTO ${table("effect_agent_archive_batches")} (thread_id, batch_id, range_first_sequence, batch_json)
        SELECT c.thread_id, c.batch_id, ${row.first_sequence}, ${canonicalBatchJson(sql, namespace, "c")} FROM ${table("effect_agent_canonical_batches")} c
        WHERE c.thread_id=${row.thread_id} AND c.first_sequence>=${row.first_sequence} AND c.last_sequence<=${row.last_sequence}`);
          yield* query(sql`INSERT INTO ${table("effect_agent_archive_records")} (thread_id, sequence, range_first_sequence, record_json)
        SELECT thread_id, sequence, ${row.first_sequence}, ${hotRecordJson(sql, namespace)} FROM ${table("effect_agent_canonical_records")}
        WHERE thread_id=${row.thread_id} AND sequence>=${row.first_sequence} AND sequence<=${row.last_sequence}`);
          yield* verifyInTransaction(row, true);

          const locator = `sql-range:${row.first_sequence}:${row.last_sequence}`;

          yield* query(sql`UPDATE ${table("effect_agent_journal_ranges")} SET state='archived', locator=${locator}
        WHERE thread_id=${row.thread_id} AND first_sequence=${row.first_sequence}`);
          yield* query(sql`UPDATE ${table("effect_agent_canonical_records")} SET payload_json=NULL
        WHERE thread_id=${row.thread_id} AND sequence>=${row.first_sequence} AND sequence<=${row.last_sequence}`);

          return yield* descriptor({ ...row, state: "archived", locator });
        }),
      ),
    verify: (input) =>
      transactions.read(
        Effect.gen(function* () {
          const request = yield* Schema.decodeEffect(ThreadArchiveRangeRequest)(input).pipe(
            Effect.mapError((cause) => failure("archive verify request", cause)),
          );

          yield* requireThread(request.threadId);
          const row = yield* range(request.threadId, request.firstSequence);

          return yield* verifyInTransaction(row);
        }),
      ),
  };

  /**
   * Reject authoritative rows excluded by owner/range traversal. Each probe returns at most
   * one scalar, including for orphan payloads; content validation belongs to verifyThread.
   */
  const verifyCoverage = transactions.read(
    Effect.gen(function* () {
      const checks = [
        [
          "effect_agent_canonical_records",
          sql`NOT EXISTS (SELECT 1 FROM ${table("effect_agent_canonical_batches")} b
            WHERE b.thread_id=c.thread_id AND c.sequence BETWEEN b.first_sequence AND b.last_sequence)`,
        ],
        ["effect_agent_canonical_batches", sql`FALSE`],
        ["effect_agent_journal_ranges", sql`FALSE`],
        [
          "effect_agent_archive_records",
          sql`NOT EXISTS (SELECT 1 FROM ${table("effect_agent_canonical_records")} b
            JOIN ${table("effect_agent_journal_ranges")} r
              ON r.thread_id=b.thread_id AND r.first_sequence=c.range_first_sequence
            WHERE b.thread_id=c.thread_id AND b.sequence=c.sequence
              AND r.state='archived' AND c.sequence BETWEEN r.first_sequence AND r.last_sequence)`,
        ],
        [
          "effect_agent_archive_batches",
          sql`NOT EXISTS (SELECT 1 FROM ${table("effect_agent_canonical_batches")} b
            JOIN ${table("effect_agent_journal_ranges")} r
              ON r.thread_id=b.thread_id AND r.first_sequence=c.range_first_sequence
            WHERE b.thread_id=c.thread_id AND b.batch_id=c.batch_id
              AND r.state='archived' AND r.locator IS NOT NULL AND b.first_sequence>=r.first_sequence
              AND b.last_sequence<=r.last_sequence)`,
        ],
      ] as const;

      for (const [name, excluded] of checks) {
        const rows = yield* query(sql`SELECT 1 FROM ${table(name)} c
          WHERE NOT EXISTS (SELECT 1 FROM ${table("effect_agent_threads")} t WHERE t.thread_id=c.thread_id)
            OR ${excluded} LIMIT 1`);

        if (rows.length !== 0) return yield* failure(`archive verification coverage: ${name}`);
      }
    }),
  );

  /** Bounded header discovery for the integrator's startup verification; never returns payloads. */
  const threadPage = Effect.fnUntraced(function* (afterThreadId?: string) {
    return yield* transactions.read(
      query(sql`
      SELECT thread_id FROM ${table("effect_agent_threads")}
      WHERE created_at IS NOT NULL ${afterThreadId === undefined ? sql`` : sql`AND thread_id>${afterThreadId}`}
      ORDER BY thread_id LIMIT ${MAX_ARCHIVE_RANGE_PAGE}`).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.Array(Schema.Struct({ thread_id: ThreadArchiveRange.fields.threadId })),
          ),
        ),
        Effect.mapError((cause) => failure("archive verification owners", cause)),
      ),
    );
  });

  /** A snapshot audit with bounded range plans and at most one batch decoded at a time. */
  const verifyThread = (threadId: ThreadArchiveRange["threadId"]) =>
    transactions.read(
      Effect.gen(function* () {
        const [header] = yield* Schema.decodeUnknownEffect(
          Schema.Tuple([Schema.Struct({ tail_sequence: SqlInteger })]),
        )(
          yield* query(sql`
      SELECT tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`),
        ).pipe(Effect.mapError((cause) => failure("archive verification tail", cause)));

        let after = 0;
        let rangeAfter: CanonicalSequence | undefined;

        while (true) {
          const rows = yield* decodeRanges(
            yield* query(sql`SELECT * FROM ${table("effect_agent_journal_ranges")}
        WHERE thread_id=${threadId} ${rangeAfter === undefined ? sql`` : sql`AND first_sequence>${rangeAfter}`}
        ORDER BY first_sequence LIMIT ${MAX_ARCHIVE_RANGE_PAGE}`),
          ).pipe(Effect.mapError((cause) => failure("archive verification ranges", cause)));

          for (const row of rows) {
            if (row.first_sequence !== after + 1)
              return yield* failure("archive verification order");
            yield* verifyInTransaction(row);
            rangeAfter = row.first_sequence;
            after = row.last_sequence;
          }
          if (rows.length < MAX_ARCHIVE_RANGE_PAGE) break;
        }
        if (after > header.tail_sequence) return yield* failure("archive verification coverage");
        while (after < header.tail_sequence) {
          const hot = yield* planHotRange(threadId, after, header.tail_sequence);

          if (hot === undefined) return yield* failure("archive verification missing hot suffix");
          yield* verifyInTransaction(hot);
          after = hot.last_sequence;
        }
      }),
    );

  return {
    storage,
    replay,
    verifyInTransaction,
    range,
    threadPage,
    verifyCoverage,
    verifyThread,
  };
});
