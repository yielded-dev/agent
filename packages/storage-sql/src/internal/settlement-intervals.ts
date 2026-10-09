import { SubmissionId, type ThreadId } from "@yielded/agent/identifiers";
import { CanonicalSequence, type CanonicalRecord } from "@yielded/agent/records";
import {
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import type { ThreadSettlementPredecessorRequest } from "@yielded/agent/thread-import";
import { ThreadStoreError } from "@yielded/agent/thread-store";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { SqlClient } from "effect/sql/SqlClient";
import type { Fragment } from "effect/sql/Statement";
import * as Stream from "effect/Stream";

import { makeSqlQuery, SqlInteger } from "../SqlStorage.ts";

// Disjoint dyadic spans cover [input + 1, settlement). A point belongs to exactly one span
// per closed input. At most 106 scalar rows describe an interval in the safe-integer space.
const spans = (input: number, settlement: number) => {
  const result: Array<{ height: number; slot: number }> = [];
  let at = input + 1;

  while (at < settlement) {
    let height = Math.floor(Math.log2(settlement - at));

    while (2 ** height > settlement - at || at % 2 ** height !== 0) height--;
    const width = 2 ** height;

    result.push({ height, slot: at / width });
    at += width;
  }

  return result;
};

const invalid = (message: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation: "settlement interval index",
    message,
    ...(cause === undefined ? {} : { cause }),
  });

/** Native scalar derivative: publication/import/rebuild share the writer; verification only reads. */
export const makeSqlSettlementIntervals = Effect.fnUntraced(function* (namespace?: string) {
  const sql = yield* SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);

  const query = <A extends object>(statement: ReturnType<typeof sql<A>>) =>
    execute(statement).pipe(
      Effect.mapError((cause) => invalid("Interval storage is unavailable", cause)),
    );

  const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown) =>
    Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.mapError((cause) => invalid("Interval metadata is malformed", cause)),
    );

  const boundary = Effect.fnUntraced(function* (threadId: ThreadId, recordId: string, tag: string) {
    const rows =
      yield* query(sql`SELECT sequence, record_tag FROM ${table("effect_agent_canonical_records")}
      WHERE thread_id=${threadId} AND record_id=${recordId}`).pipe(
        Effect.flatMap((r) =>
          decode(
            Schema.Array(
              Schema.Struct({
                sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
                record_tag: Schema.String,
              }),
            ),
            r,
          ),
        ),
      );

    const row = rows[0];

    if (rows.length > 1 || (row !== undefined && row.record_tag !== tag))
      return yield* invalid("Interval boundary has conflicting canonical identity");

    return row?.sequence;
  });

  const publish = Effect.fnUntraced(function* (
    threadId: ThreadId,
    input: number,
    settlement: number | null,
  ) {
    if (settlement !== null && input >= settlement)
      return yield* invalid("Canonical settlement does not follow its input");
    yield* query(sql`INSERT INTO ${table("effect_agent_input_intervals")} (thread_id, sequence, settlement_sequence)
      VALUES (${threadId}, ${input}, ${settlement}) ON CONFLICT (thread_id, sequence)
      DO UPDATE SET settlement_sequence=excluded.settlement_sequence`);
    yield* query(
      sql`DELETE FROM ${table("effect_agent_settlement_spans")} WHERE thread_id=${threadId} AND input_sequence=${input}`,
    );
    if (settlement === null) return;
    const cover = spans(input, settlement);

    // Five bound columns per row; every adapter, including DO's 100-parameter limit, fits.
    for (let at = 0; at < cover.length; at += 20)
      yield* query(
        sql`INSERT INTO ${table("effect_agent_settlement_spans")} ${sql.insert(
          cover.slice(at, at + 20).map((span) => ({
            thread_id: threadId,
            ...span,
            settlement_sequence: settlement,
            input_sequence: input,
          })),
        )}`,
      );
  });

  const apply = Effect.fnUntraced(function* (
    threadId: ThreadId,
    sequence: CanonicalSequence,
    record: CanonicalRecord,
  ) {
    const payload = record.payload;

    if (payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) {
      const end = yield* boundary(
        threadId,
        submissionSettlementRecordId(payload.submissionId),
        "SubmissionSettled",
      );

      yield* publish(threadId, sequence, end ?? null);
    } else if (payload._tag === "SubmissionSettled") {
      const input = yield* boundary(
        threadId,
        submissionInputRecordId(payload.submissionId),
        "UserInputRecorded",
      );

      if (input !== undefined) yield* publish(threadId, input, sequence);
    }
  });

  // Validate every canonical input's directory and complete span cover during the existing
  // streaming audit, so damaged/missing derivatives cannot silently hide a FIFO predecessor.
  const verify = Effect.fnUntraced(function* (
    threadId: ThreadId,
    sequence: CanonicalSequence,
    record: CanonicalRecord,
  ) {
    const payload = record.payload;

    if (payload._tag !== "UserInputRecorded" || payload.submissionId === undefined) return;

    const end = yield* boundary(
      threadId,
      submissionSettlementRecordId(payload.submissionId),
      "SubmissionSettled",
    );

    const metadata =
      yield* query(sql`SELECT settlement_sequence FROM ${table("effect_agent_input_intervals")}
      WHERE thread_id=${threadId} AND sequence=${sequence}`).pipe(
        Effect.flatMap((r) =>
          decode(
            Schema.Array(Schema.Struct({ settlement_sequence: Schema.NullOr(SqlInteger) })),
            r,
          ),
        ),
      );

    if (
      metadata.length !== 1 ||
      metadata[0]?.settlement_sequence !== (end ?? null) ||
      (end !== undefined && end <= sequence)
    )
      return yield* invalid("Input interval differs from canonical boundaries");
    if (end === undefined) return;
    const cover = spans(sequence, end);

    for (let at = 0; at < cover.length; at += 40) {
      const group = cover.slice(at, at + 40);

      const values = sql.join(
        ", ",
        false,
      )(group.map((span) => sql`(${span.height}, ${span.slot})`));

      const found = yield* query(sql`WITH expected(height, slot) AS (VALUES ${values})
        SELECT s.input_sequence FROM expected LEFT JOIN ${table("effect_agent_settlement_spans")} s
          ON s.thread_id=${threadId} AND s.height=expected.height AND s.slot=expected.slot
          AND s.settlement_sequence=${end} AND s.input_sequence=${sequence}`).pipe(
        Effect.flatMap((r) =>
          decode(Schema.Array(Schema.Struct({ input_sequence: Schema.NullOr(SqlInteger) })), r),
        ),
      );

      if (found.length !== group.length || found.some((row) => row.input_sequence !== sequence))
        return yield* invalid("Canonical input has an incomplete settlement span cover");
    }
  });

  const candidate = Effect.fnUntraced(function* (threadId: ThreadId, sequence: number) {
    const rows =
      yield* query(sql`SELECT record_tag, submission_id FROM ${table("effect_agent_canonical_records")}
      WHERE thread_id=${threadId} AND sequence=${sequence}`).pipe(
        Effect.flatMap((r) =>
          decode(
            Schema.Array(
              Schema.Struct({
                record_tag: Schema.String,
                submission_id: Schema.NullOr(Schema.String),
              }),
            ),
            r,
          ),
        ),
      );

    const row = rows[0];

    if (rows.length !== 1 || row?.record_tag !== "UserInputRecorded" || row.submission_id === null)
      return yield* invalid("Indexed FIFO predecessor has no canonical input identity");

    return { submissionId: yield* decode(Schema.fromJsonString(SubmissionId), row.submission_id) };
  });

  const predecessors = (threadId: ThreadId, request: ThreadSettlementPredecessorRequest) => {
    const open = Stream.paginate(0, (after) =>
      Effect.gen(function* () {
        const rows = yield* query(sql`SELECT sequence FROM ${table("effect_agent_input_intervals")}
        ${sql.onDialectOrElse({ pg: () => sql``, orElse: () => sql`INDEXED BY effect_agent_input_intervals_open` })}
        WHERE thread_id=${threadId} AND settlement_sequence IS NULL AND sequence>${after}
          AND sequence<${request.inputSequence} ORDER BY sequence LIMIT 1`).pipe(
          Effect.flatMap((r) => decode(Schema.Array(Schema.Struct({ sequence: SqlInteger })), r)),
        );

        const row = rows[0];

        return [
          row === undefined ? [] : [yield* candidate(threadId, row.sequence)],
          row === undefined ? Option.none<number>() : Option.some(row.sequence),
        ] as const;
      }),
    );

    const closed = Stream.paginate<
      readonly [number, number] | undefined,
      { submissionId: SubmissionId },
      ThreadStoreError
    >(undefined, (after) =>
      Effect.gen(function* () {
        // Each branch seeks one covering bucket's end frontier and returns at most one scalar.
        // A single statement merges at most 53 candidates; 58 parameters fit DO without
        // making 53 database round trips for every ordinary completed submission.
        const join = sql.join(" UNION ALL ", false);

        let branches: Array<Fragment> = Array.from(
          { length: 53 },
          (_, height) => sql`SELECT * FROM (
          SELECT settlement_sequence, input_sequence FROM ${table("effect_agent_settlement_spans")}
          WHERE thread_id=(SELECT thread_id FROM request) AND height=${sql.literal(String(height))}
            AND slot=${Math.floor(request.inputSequence / 2 ** height)}
            AND settlement_sequence>(SELECT settlement_sequence FROM request)
            AND (settlement_sequence, input_sequence)>((SELECT after_end FROM request), (SELECT after_input FROM request))
          ORDER BY settlement_sequence, input_sequence LIMIT 1
        ) AS span_branch`,
        );

        // Workerd limits compound SELECTs to a small number of terms. Reduce bounded
        // frontiers in groups of four; each level retains the same earliest candidate.
        while (branches.length > 4) {
          const groups: Array<Fragment> = [];

          for (let at = 0; at < branches.length; at += 4)
            groups.push(sql`SELECT * FROM (${join(branches.slice(at, at + 4))}
              ORDER BY settlement_sequence, input_sequence LIMIT 1) AS bounded_spans`);
          branches = groups;
        }
        const candidates = join(branches);

        const rows = yield* query(sql`WITH request AS (
          SELECT ${threadId} AS thread_id, CAST(${request.settlementSequence} AS BIGINT) AS settlement_sequence,
            CAST(${after?.[0] ?? request.settlementSequence} AS BIGINT) AS after_end,
            CAST(${after?.[1] ?? 0} AS BIGINT) AS after_input
        ) SELECT * FROM (${candidates}) AS candidates ORDER BY settlement_sequence, input_sequence LIMIT 1`).pipe(
          Effect.flatMap((r) =>
            decode(
              Schema.Array(
                Schema.Struct({ settlement_sequence: SqlInteger, input_sequence: SqlInteger }),
              ),
              r,
            ),
          ),
        );

        const row = rows[0];

        if (row !== undefined && row.input_sequence >= request.inputSequence)
          return yield* invalid("A settlement span points beyond its requested input");

        return [
          row === undefined ? [] : [yield* candidate(threadId, row.input_sequence)],
          row === undefined
            ? Option.none<readonly [number, number] | undefined>()
            : Option.some([row.settlement_sequence, row.input_sequence] as const),
        ] as const;
      }),
    );

    return Stream.concat(open, closed);
  };

  return { apply, verify, predecessors };
});
