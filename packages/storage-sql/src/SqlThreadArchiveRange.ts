import {
  canonicalJson,
  digestCanonicalBatchJson,
  digestJson,
  EMPTY_TAIL_DIGEST,
  utf8ByteLength,
} from "@yielded/agent/digest";
import { ExportBatch, ExportRecord } from "@yielded/agent/record-format";
import { CanonicalSequence, Digest, ProducerEpoch } from "@yielded/agent/records";
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
  FenceRejected,
  ThreadNotMaterialized,
  ThreadStoreError,
  ThreadCheckpoint,
} from "@yielded/agent/thread-store";
import { Crypto, Effect, Option, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

import type { RawAppendRequest } from "./SqlJournal.ts";
import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";

const bytes = utf8ByteLength;

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Canonical archive range is incomplete, corrupt, or exceeds its bounds.",
    ...(cause === undefined ? {} : { cause }),
  });

/**
 * Keep indexed selectors on the identity table, including SQLite's INDEXED BY hints. A scalar
 * locator probe prevents the planner from expanding archive ownership into a Thread-wide join.
 */
export const canonicalRecordJson = (
  sql: SqlClient.SqlClient,
  namespace?: string,
  alias?: string,
) => {
  const table = (name: string) => sql(namespace === undefined ? name : `${namespace}.${name}`);
  const owner = sql(alias ?? "effect_agent_canonical_records");

  return sql`COALESCE(${owner}.record_json, (
    SELECT a.record_json FROM ${table("effect_agent_archive_records")} a
    WHERE a.thread_id=${owner}.thread_id AND a.sequence=${owner}.sequence
      AND (SELECT r.state='archived' AND r.locator IS NOT NULL
        FROM ${table("effect_agent_journal_ranges")} r
        WHERE r.thread_id=a.thread_id AND r.first_sequence=a.range_first_sequence)
  ))`;
};

export const canonicalBatchJson = (
  sql: SqlClient.SqlClient,
  namespace?: string,
  alias?: string,
) => {
  const table = (name: string) => sql(namespace === undefined ? name : `${namespace}.${name}`);
  const owner = sql(alias ?? "effect_agent_canonical_batches");

  return sql`COALESCE(${owner}.batch_json, (
    SELECT a.batch_json FROM ${table("effect_agent_archive_batches")} a
    WHERE a.thread_id=${owner}.thread_id AND a.batch_id=${owner}.batch_id
      AND (SELECT r.state='archived' AND r.locator IS NOT NULL
        FROM ${table("effect_agent_journal_ranges")} r
        WHERE r.thread_id=a.thread_id AND r.first_sequence=a.range_first_sequence)
  ))`;
};

const RangeRow = Schema.Struct({
  thread_id: ThreadArchiveRange.fields.threadId,
  first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  previous_tail_digest: Digest,
  tail_digest: Digest,
  record_count: SqlInteger.pipe(Schema.decodeTo(Schema.Int)),
  batch_count: SqlInteger.pipe(Schema.decodeTo(Schema.Int)),
  byte_count: SqlInteger.pipe(Schema.decodeTo(Schema.Int)),
  state: Schema.Literals(["open", "sealed", "archived"]),
  locator: Schema.NullOr(Schema.String),
});

type RangeRow = typeof RangeRow.Type;

export interface SqlArchiveTransactions {
  readonly read: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreError, R>;
  readonly write: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ThreadStoreError, R>;
}

/** Adapter layout DDL owns these authoritative tables; this module never initializes them. */
export const makeSqlThreadArchiveRange = Effect.fnUntraced(function* (
  transactions: SqlArchiveTransactions,
  namespace?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const { table, execute } = yield* makeSqlQuery(namespace);

  const query = <A extends object>(statement: Parameters<typeof execute<A>>[0]) =>
    execute(statement).pipe(Effect.mapError((cause) => failure("archive SQL", cause)));

  const decodeRanges = Schema.decodeUnknownEffect(Schema.Array(RangeRow));

  const descriptor = Effect.fnUntraced(function* (row: RangeRow) {
    if (
      (row.state === "archived") !== (row.locator !== null) ||
      row.last_sequence - row.first_sequence + 1 !== row.record_count ||
      row.batch_count > row.record_count
    )
      return yield* failure("archive descriptor identity");

    return yield* ThreadArchiveRange.makeEffect({
      format: "effect-agent/thread-range@1",
      threadId: row.thread_id,
      firstSequence: row.first_sequence,
      lastSequence: row.last_sequence,
      previousTailDigest: row.previous_tail_digest,
      tailDigest: row.tail_digest,
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
          producer_epoch: SqlInteger.pipe(Schema.decodeTo(ProducerEpoch)),
        }),
      ),
    )(
      yield* query(
        sql`SELECT producer_epoch FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
      ),
    ).pipe(Effect.mapError((cause) => failure("archive owner", cause)));

    if (rows.length === 0) return yield* ThreadNotMaterialized.make({ threadId });
    if (rows.length !== 1) return yield* failure("archive owner");
    const [row] = rows;

    if (row === undefined) return yield* failure("archive owner");

    return row.producer_epoch;
  });

  const requireFence = Effect.fnUntraced(function* (
    threadId: ThreadArchiveRange["threadId"],
    epoch: ProducerEpoch,
  ) {
    const actualEpoch = yield* requireThread(threadId);

    if (actualEpoch !== epoch)
      return yield* FenceRejected.make({ threadId, attemptedEpoch: epoch, actualEpoch });
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

  /** Called only after append fences/deduplication pass, in that append's writer transaction. */
  const append = Effect.fnUntraced(function* (
    request: RawAppendRequest,
    first: CanonicalSequence,
    last: CanonicalSequence,
  ) {
    const batchBytes = request.batchBytes;

    const byteCount =
      batchBytes + request.records.reduce((total, record) => total + record.recordBytes, 0);

    if (
      batchBytes > MAX_CANONICAL_BATCH_BYTES ||
      byteCount > MAX_ARCHIVE_RANGE_BYTES ||
      request.records.length > MAX_ARCHIVE_RANGE_RECORDS
    )
      return yield* failure("append archive range bound");

    // Advance the usual open range in one indexed statement. Its predecessor frontier and
    // remaining capacity are checked by the writer; decoding the returned descriptor still
    // rejects malformed metadata and rolls back this entire append transaction.
    if (request.expectedTailSequence > 0) {
      const advanced = yield* decodeRanges(
        yield* query(sql`UPDATE ${table("effect_agent_journal_ranges")}
          SET last_sequence=${last}, tail_digest=${request.tailDigest},
            record_count=record_count+${request.records.length}, batch_count=batch_count+1,
            byte_count=byte_count+${byteCount}
          WHERE thread_id=${request.threadId} AND state='open' AND locator IS NULL
            AND last_sequence=${first - 1} AND tail_digest=${request.expectedTailDigest}
            AND first_sequence>0 AND record_count=last_sequence-first_sequence+1
            AND record_count BETWEEN 1 AND ${MAX_ARCHIVE_RANGE_RECORDS - request.records.length}
            AND batch_count BETWEEN 1 AND record_count
            AND byte_count BETWEEN 1 AND ${MAX_ARCHIVE_RANGE_BYTES - byteCount}
          RETURNING *`),
      ).pipe(Effect.mapError((cause) => failure("advance archive range", cause)));

      if (advanced.length > 1) return yield* failure("ambiguous open archive range");
      if (advanced[0] !== undefined) {
        yield* descriptor(advanced[0]);

        return;
      }
    }

    const rows = yield* decodeRanges(
      yield* query(sql`SELECT * FROM ${table("effect_agent_journal_ranges")}
      WHERE thread_id=${request.threadId} AND state='open'`),
    ).pipe(Effect.mapError((cause) => failure("open archive range", cause)));

    if (rows.length > 1) return yield* failure("ambiguous open archive range");
    const current = rows[0];

    if (current !== undefined) {
      yield* descriptor(current);
      if (current.last_sequence !== first - 1 || current.tail_digest !== request.expectedTailDigest)
        return yield* failure("archive range append frontier");
      if (
        current.record_count + request.records.length <= MAX_ARCHIVE_RANGE_RECORDS &&
        current.byte_count + byteCount <= MAX_ARCHIVE_RANGE_BYTES
      )
        return yield* failure("archive range append frontier");
      yield* query(sql`UPDATE ${table("effect_agent_journal_ranges")} SET state='sealed'
        WHERE thread_id=${request.threadId} AND first_sequence=${current.first_sequence}`);
    }
    yield* query(sql`INSERT INTO ${table("effect_agent_journal_ranges")}
        (thread_id, first_sequence, last_sequence, previous_tail_digest, tail_digest, record_count, batch_count, byte_count, state, locator)
        VALUES (${request.threadId}, ${first}, ${last}, ${request.expectedTailDigest}, ${request.tailDigest}, ${request.records.length}, 1, ${byteCount}, 'open', NULL)`);
  });

  const verifyInTransaction = Effect.fnUntraced(function* (row: RangeRow, copied = false) {
    const result = yield* descriptor(row);

    if (row.state === "archived") {
      const locator = `sql-range:${yield* digestJson([
        row.thread_id,
        row.first_sequence,
        row.last_sequence,
        row.tail_digest,
      ]).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((cause) => failure("archive locator digest", cause)),
      )}`;

      if (row.locator !== locator) return yield* failure("archive locator identity");
    }

    if (row.last_sequence - row.first_sequence + 1 !== row.record_count)
      return yield* failure("archive range coverage");
    const archive = copied || row.state === "archived";

    if (row.state === "archived" && row.locator === null)
      return yield* failure("archive locator missing");
    // Plan scalar sizes before fetching any payloads, so corrupt contents cannot allocate an unbounded range.
    const batchText = archive ? sql.literal("a.batch_json") : sql.literal("c.batch_json");
    const recordText = archive ? sql.literal("a.record_json") : sql.literal("c.record_json");

    const size = (text: ReturnType<typeof sql.literal>) =>
      sql.onDialectOrElse({
        pg: () => sql`octet_length(${text})`,
        orElse: () => sql`length(CAST(${text} AS BLOB))`,
      });

    const BatchPlan = Schema.Struct({
      batch_id: Schema.String,
      first_sequence: SqlInteger,
      last_sequence: SqlInteger,
      batch_digest: Digest,
      tail_digest: Digest,
      byte_count: SqlInteger,
    });

    const RecordPlan = Schema.Struct({
      sequence: SqlInteger,
      record_id: Schema.String,
      batch_id: Schema.String,
      byte_count: SqlInteger,
    });

    const batchJoin = archive
      ? sql`LEFT JOIN ${table("effect_agent_archive_batches")} a ON a.thread_id=c.thread_id AND a.batch_id=c.batch_id AND a.range_first_sequence=${row.first_sequence}`
      : sql``;

    const recordJoin = archive
      ? sql`LEFT JOIN ${table("effect_agent_archive_records")} a ON a.thread_id=c.thread_id AND a.sequence=c.sequence AND a.range_first_sequence=${row.first_sequence}`
      : sql``;

    const batches = yield* Schema.decodeUnknownEffect(Schema.Array(BatchPlan))(
      yield* query(sql`
      SELECT c.batch_id, c.first_sequence, c.last_sequence, c.batch_digest, c.tail_digest, ${size(batchText)} AS byte_count
      FROM ${table("effect_agent_canonical_batches")} c ${batchJoin}
      WHERE c.thread_id=${row.thread_id} AND c.first_sequence>=${row.first_sequence} AND c.first_sequence<=${row.last_sequence}
      ORDER BY c.first_sequence LIMIT ${MAX_ARCHIVE_RANGE_RECORDS + 1}`),
    ).pipe(Effect.mapError((cause) => failure("archive batch plan", cause)));

    const records = yield* Schema.decodeUnknownEffect(Schema.Array(RecordPlan))(
      yield* query(sql`
      SELECT c.sequence, c.record_id, c.batch_id, ${size(recordText)} AS byte_count FROM ${table("effect_agent_canonical_records")} c ${recordJoin}
      WHERE c.thread_id=${row.thread_id} AND c.sequence>=${row.first_sequence} AND c.sequence<=${row.last_sequence}
      ORDER BY c.sequence LIMIT ${MAX_ARCHIVE_RANGE_RECORDS + 1}`),
    ).pipe(Effect.mapError((cause) => failure("archive record plan", cause)));

    if (
      batches.length !== row.batch_count ||
      records.length !== row.record_count ||
      batches.some(
        (batch) => batch.byte_count < 1 || batch.byte_count > MAX_CANONICAL_BATCH_BYTES,
      ) ||
      records.some(
        (record) => record.byte_count < 1 || record.byte_count > MAX_CANONICAL_BATCH_BYTES,
      ) ||
      batches.reduce((total, batch) => total + batch.byte_count, 0) +
        records.reduce((total, record) => total + record.byte_count, 0) !==
        row.byte_count
    )
      return yield* failure("archive size or count");
    let digest = row.previous_tail_digest;
    let sequence = row.first_sequence;
    let recordIndex = 0;

    for (const batch of batches) {
      const [payload] = yield* Schema.decodeUnknownEffect(
        Schema.Tuple([Schema.Struct({ batch_json: Schema.String })]),
      )(
        yield* query(sql`
        SELECT ${batchText} AS batch_json FROM ${table("effect_agent_canonical_batches")} c ${batchJoin}
        WHERE c.thread_id=${row.thread_id} AND c.batch_id=${batch.batch_id}`),
      ).pipe(Effect.mapError((cause) => failure("archive batch content", cause)));

      if (bytes(payload.batch_json) !== batch.byte_count || batch.first_sequence !== sequence)
        return yield* failure("archive batch frontier");

      const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ExportBatch))(
        payload.batch_json,
      ).pipe(Effect.mapError((cause) => failure("archive batch decode", cause)));

      if (
        decoded.batchId !== batch.batch_id ||
        batch.last_sequence !== sequence + decoded.records.length - 1
      )
        return yield* failure("archive batch identity");
      digest = yield* digestCanonicalBatchJson(digest, payload.batch_json).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((cause) => failure("archive digest", cause)),
      );
      if (digest !== batch.batch_digest || digest !== batch.tail_digest)
        return yield* failure("archive batch digest");
      for (const record of decoded.records) {
        const plan = records[recordIndex++];

        if (
          plan === undefined ||
          plan.sequence !== sequence ||
          plan.batch_id !== batch.batch_id ||
          plan.record_id !== record.recordId
        )
          return yield* failure("archive record identity");

        const [raw] = yield* Schema.decodeUnknownEffect(
          Schema.Tuple([Schema.Struct({ record_json: Schema.String })]),
        )(
          yield* query(sql`
          SELECT ${recordText} AS record_json FROM ${table("effect_agent_canonical_records")} c ${recordJoin}
          WHERE c.thread_id=${row.thread_id} AND c.sequence=${sequence}`),
        ).pipe(Effect.mapError((cause) => failure("archive record content", cause)));

        const encoded = yield* Schema.encodeEffect(ExportRecord)(record).pipe(
          Effect.mapError((cause) => failure("archive record encode", cause)),
        );

        if (
          bytes(raw.record_json) !== plan.byte_count ||
          canonicalJson(encoded) !== raw.record_json
        )
          return yield* failure("archive record wire");
        sequence = CanonicalSequence.make(sequence + 1);
      }
    }
    if (
      sequence !== row.last_sequence + 1 ||
      recordIndex !== row.record_count ||
      digest !== row.tail_digest
    )
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
        WHERE thread_id=${request.threadId} AND state<>'open' AND first_sequence>${request.afterSequence ?? 0}
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

          yield* requireFence(request.threadId, request.producerEpoch);

          const rows = yield* decodeRanges(
            yield* query(
              sql`SELECT * FROM ${table("effect_agent_journal_ranges")} WHERE thread_id=${request.threadId} AND state='open'`,
            ),
          ).pipe(Effect.mapError((cause) => failure("archive seal", cause)));

          if (rows.length === 0) return Option.none();
          if (rows.length !== 1 || rows[0] === undefined) return yield* failure("archive seal");
          const verified = yield* verifyInTransaction(rows[0]);

          yield* query(
            sql`UPDATE ${table("effect_agent_journal_ranges")} SET state='sealed' WHERE thread_id=${request.threadId} AND first_sequence=${rows[0].first_sequence}`,
          );

          return Option.some(verified);
        }),
      ),
    archive: (input) =>
      transactions.write(
        Effect.gen(function* () {
          const request = yield* Schema.decodeEffect(ThreadArchiveRangePublish)(input).pipe(
            Effect.mapError((cause) => failure("archive publish request", cause)),
          );

          yield* requireFence(request.threadId, request.producerEpoch);
          const row = yield* range(request.threadId, request.firstSequence);

          if (row.state === "open") return yield* failure("archive unsealed range");
          if (row.state === "archived") return yield* verifyInTransaction(row);
          yield* verifyInTransaction(row);
          yield* query(sql`INSERT INTO ${table("effect_agent_archive_batches")} (thread_id, batch_id, range_first_sequence, batch_json)
        SELECT thread_id, batch_id, ${row.first_sequence}, batch_json FROM ${table("effect_agent_canonical_batches")}
        WHERE thread_id=${row.thread_id} AND first_sequence>=${row.first_sequence} AND last_sequence<=${row.last_sequence}`);
          yield* query(sql`INSERT INTO ${table("effect_agent_archive_records")} (thread_id, sequence, range_first_sequence, record_json)
        SELECT thread_id, sequence, ${row.first_sequence}, record_json FROM ${table("effect_agent_canonical_records")}
        WHERE thread_id=${row.thread_id} AND sequence>=${row.first_sequence} AND sequence<=${row.last_sequence}`);
          yield* verifyInTransaction(row, true);

          const locator = `sql-range:${yield* digestJson([
            row.thread_id,
            row.first_sequence,
            row.last_sequence,
            row.tail_digest,
          ]).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError((cause) => failure("archive locator digest", cause)),
          )}`;

          yield* query(sql`UPDATE ${table("effect_agent_journal_ranges")} SET state='archived', locator=${locator}
        WHERE thread_id=${row.thread_id} AND first_sequence=${row.first_sequence}`);
          yield* query(sql`UPDATE ${table("effect_agent_canonical_records")} SET record_json=NULL
        WHERE thread_id=${row.thread_id} AND sequence>=${row.first_sequence} AND sequence<=${row.last_sequence}`);
          yield* query(sql`UPDATE ${table("effect_agent_canonical_batches")} SET batch_json=NULL
        WHERE thread_id=${row.thread_id} AND first_sequence>=${row.first_sequence} AND last_sequence<=${row.last_sequence}`);

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

          if (row.state === "open") return yield* failure("verify unsealed archive range");

          return yield* verifyInTransaction(row);
        }),
      ),
  };

  /** Bounded header discovery for the integrator's startup verification; never returns payloads. */
  const threadPage = Effect.fnUntraced(function* (afterThreadId?: string) {
    return yield* transactions.read(
      query(sql`
      SELECT thread_id FROM ${table("effect_agent_threads")} WHERE thread_id>${afterThreadId ?? ""}
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
          Schema.Tuple([Schema.Struct({ tail_sequence: SqlInteger, tail_digest: Digest })]),
        )(
          yield* query(sql`
      SELECT tail_sequence, tail_digest FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`),
        ).pipe(Effect.mapError((cause) => failure("archive verification tail", cause)));

        let after = 0;
        let previous = EMPTY_TAIL_DIGEST;

        while (true) {
          const rows = yield* decodeRanges(
            yield* query(sql`SELECT * FROM ${table("effect_agent_journal_ranges")}
        WHERE thread_id=${threadId} AND first_sequence>${after} ORDER BY first_sequence LIMIT ${MAX_ARCHIVE_RANGE_PAGE}`),
          ).pipe(Effect.mapError((cause) => failure("archive verification ranges", cause)));

          for (const row of rows) {
            if (row.first_sequence !== after + 1 || row.previous_tail_digest !== previous)
              return yield* failure("archive verification chain");
            yield* verifyInTransaction(row);
            after = row.last_sequence;
            previous = row.tail_digest;
          }
          if (rows.length < MAX_ARCHIVE_RANGE_PAGE) break;
        }
        if (after !== header.tail_sequence || previous !== header.tail_digest)
          return yield* failure("archive verification coverage");
        let checkpointAfter = -1;

        while (true) {
          const plans = yield* query<{ through_sequence: number; byte_count: number }>(sql`
            SELECT through_sequence, ${sql.onDialectOrElse({ pg: () => sql`octet_length(checkpoint_json)`, orElse: () => sql`length(CAST(checkpoint_json AS BLOB))` })} AS byte_count
            FROM ${table("effect_agent_checkpoints")} WHERE thread_id=${threadId} AND through_sequence>${checkpointAfter}
            ORDER BY through_sequence LIMIT ${MAX_ARCHIVE_RANGE_PAGE}`);

          for (const plan of plans) {
            if (Number(plan.byte_count) > MAX_CANONICAL_BATCH_BYTES)
              return yield* failure("checkpoint verification bound");

            const [payload] = yield* query<{ checkpoint_json: string; tail_digest: string }>(sql`
              SELECT checkpoint_json, tail_digest FROM ${table("effect_agent_checkpoints")}
              WHERE thread_id=${threadId} AND through_sequence=${plan.through_sequence}`);

            if (payload === undefined) return yield* failure("checkpoint verification missing");

            const checkpoint = yield* Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(
              payload.checkpoint_json,
            ).pipe(Effect.mapError((cause) => failure("checkpoint verification decode", cause)));

            const sequence = Number(plan.through_sequence);

            const anchor =
              sequence === 0
                ? EMPTY_TAIL_DIGEST
                : (yield* query<{ tail_digest: string }>(sql`
              SELECT tail_digest FROM ${table("effect_agent_canonical_batches")}
              WHERE thread_id=${threadId} AND last_sequence=${sequence}`))[0]?.tail_digest;

            if (
              checkpoint.threadId !== threadId ||
              checkpoint.throughSequence !== sequence ||
              checkpoint.tailDigest !== payload.tail_digest ||
              checkpoint.tailDigest !== anchor
            )
              return yield* failure("checkpoint verification anchor");
            checkpointAfter = sequence;
          }
          if (plans.length < MAX_ARCHIVE_RANGE_PAGE) break;
        }
      }),
    );

  return { storage, append, verifyInTransaction, range, threadPage, verifyThread };
});
