import type { CanonicalRecord } from "@yielded/agent/records";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
  RecordId,
} from "@yielded/agent/records";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
} from "@yielded/agent/run-journal";
import type { ThreadStore } from "@yielded/agent/thread-store";
import {
  SelectedThreadRead,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { Context, Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import { sqliteJsonText, nullSafeEquals, queryIdentifier } from "./internal/sql-json.ts";
import { makeSqlQuery, SqlInteger, makeSqlTransaction } from "./SqlStorage.ts";

const canonicalPaths = {
  tag: ["payload", "_tag"],
  runId: ["payload", "runId"],
  toolCallId: ["payload", "toolCallId"],
  kind: ["payload", "kind"],
  sourceSubmissionId: ["payload", "sourceSubmissionId"],
  messageId: ["payload", "admission", "messageId"],
} as const;

const canonicalField = (sql: SqlClient.SqlClient, field: keyof typeof canonicalPaths) =>
  sql.onDialectOrElse({
    orElse: () => sqliteJsonText(sql, "record_json", canonicalPaths[field]),
    pg: () => sql.literal(`(read_metadata ->> '${field}')`),
  });

const encodeMetadata = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      tag: Schema.String,
      runId: Schema.NullOr(Schema.String),
      toolCallId: Schema.NullOr(Schema.String),
      kind: Schema.NullOr(Schema.String),
      sourceSubmissionId: Schema.NullOr(Schema.String),
      messageId: Schema.NullOr(Schema.String),
    }),
  ),
);

/**
 * Postgres jsonb rejects NUL and lone surrogates in JSON strings. Index only these decoded
 * fields, escaping identifiers as JSON strings, and leave canonical payload/digest bytes intact.
 */
export const canonicalRecordMetadata = (record: CanonicalRecord): string => {
  const payload = record.payload;

  if (payload._tag === "UnknownRecord") {
    const value = payload.value;

    return encodeMetadata({
      tag: value._tag,
      runId: typeof value.runId === "string" ? JSON.stringify(value.runId) : null,
      toolCallId: typeof value.toolCallId === "string" ? JSON.stringify(value.toolCallId) : null,
      kind: typeof value.kind === "string" ? value.kind : null,
      sourceSubmissionId:
        typeof value.sourceSubmissionId === "string"
          ? JSON.stringify(value.sourceSubmissionId)
          : null,
      messageId: null,
    });
  }

  return encodeMetadata({
    tag: payload._tag,
    runId: "runId" in payload ? JSON.stringify(payload.runId) : null,
    toolCallId: "toolCallId" in payload ? JSON.stringify(payload.toolCallId) : null,
    kind: "kind" in payload ? payload.kind : null,
    sourceSubmissionId:
      "sourceSubmissionId" in payload && payload.sourceSubmissionId !== undefined
        ? JSON.stringify(payload.sourceSubmissionId)
        : null,
    messageId:
      payload._tag === "WorkerInputRequested" ? JSON.stringify(payload.admission.messageId) : null,
  });
};

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

/** Adapter-owned metadata on canonical rows, never a second copy of execution records. */
export const createNativeReadIndexes = (namespace?: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { table: relation, execute } = yield* makeSqlQuery(namespace);

    yield* sql.onDialectOrElse({
      orElse: () => Effect.void,
      pg: () =>
        sql`ALTER TABLE ${relation("effect_agent_canonical_records")} ADD COLUMN read_metadata JSONB NOT NULL`.pipe(
          execute,
        ),
    });
    yield* sql`CREATE INDEX effect_agent_records_call ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "tag")}, ${canonicalField(sql, "runId")}, ${canonicalField(sql, "toolCallId")})`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_run_input ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "runId")}) WHERE ${canonicalField(sql, "tag")} = 'UserInputRecorded' AND ${canonicalField(sql, "kind")} = 'user'`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_subtree ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "sourceSubmissionId")}, sequence) WHERE ${canonicalField(sql, "tag")} = 'SubtreeBudgetReserved'`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_worker_input ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "messageId")}) WHERE ${canonicalField(sql, "tag")} = 'WorkerInputRequested'`.pipe(
      execute,
    );
    yield* sql.onDialectOrElse({
      orElse: () => Effect.void,
      pg: () =>
        sql`CREATE INDEX effect_agent_worker_execution ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "tag")}, sequence) WHERE ${canonicalField(sql, "runId")} IS NOT NULL`.pipe(
          execute,
        ),
    });
  });

const Row = Schema.Struct({
  thread_id: SelectedThreadRead.fields.threadId,
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  record_id: RecordId,
  batch_id: CanonicalRecordEnvelope.fields.batchId,
  record_json: Schema.String,
});

/** Exclusive-owner snapshot and header reuse for indexed canonical reads. */
export interface SelectedReadOwner {
  readonly snapshot: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly tail: (threadId: SelectedThreadRead["threadId"]) => Effect.Effect<
    | {
        readonly tail_sequence: CanonicalSequence;
        readonly tail_digest: Digest;
        readonly producer_epoch: ProducerEpoch;
      }
    | undefined,
    ThreadStoreError
  >;
}

export const SelectedReadOwner = Context.Reference<SelectedReadOwner | undefined>(
  "@effect-agent/storage-sql/SelectedReadOwner",
  { defaultValue: () => undefined },
);

export const makeSelectedReads = Effect.fnUntraced(function* (
  envelope: (row: typeof Row.Type) => Effect.Effect<CanonicalRecordEnvelope, ThreadStoreError>,
  namespace?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const owner = yield* SelectedReadOwner;
  const { table: relation, execute } = yield* makeSqlQuery(namespace);
  const decodeRows = Schema.decodeUnknownEffect(Schema.Array(Row));

  const decodeTailRows = Schema.decodeUnknownEffect(
    Schema.Array(
      Schema.Struct({
        tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
        tail_digest: Digest,
        producer_epoch: SqlInteger.pipe(Schema.decodeTo(ProducerEpoch)),
      }),
    ),
  );

  const sqlSnapshot = sql.onDialectOrElse({
    orElse: () => sql.withTransaction,
    pg: () => makeSqlTransaction(sql, { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" }),
  });

  const snapshot = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | SqlError, R> =>
    owner === undefined ? sqlSnapshot(effect) : owner.snapshot(effect);

  const requireThread = Effect.fnUntraced(function* (threadId: SelectedThreadRead["threadId"]) {
    if (owner !== undefined) {
      const tail = yield* owner.tail(threadId);

      if (tail === undefined) return yield* ThreadNotMaterialized.make({ threadId });

      return tail;
    }

    const rows =
      yield* sql`SELECT tail_sequence, tail_digest, producer_epoch FROM ${relation("effect_agent_threads")} WHERE thread_id = ${threadId}`.pipe(
        execute,
      );

    if (rows.length === 0) return yield* ThreadNotMaterialized.make({ threadId });

    const decoded = yield* decodeTailRows(rows);

    if (decoded.length !== 1 || decoded[0] === undefined)
      return yield* failure("native thread tail");

    return decoded[0];
  });

  const read = Effect.fnUntraced(
    function* (request: SelectedThreadRead) {
      return yield* snapshot(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);
          const selection = request.selection;

          if (
            "expectedTailSequence" in selection &&
            (tail.tail_sequence !== selection.expectedTailSequence ||
              tail.tail_digest !== selection.expectedTailDigest)
          )
            return yield* failure("selected read tail changed");
          const after = request.page.afterSequence ?? 0;
          let rows: unknown;

          switch (selection._tag) {
            case "RecordId":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${selection.recordId} AND sequence > ${after}`.pipe(
                  execute,
                );
              break;
            case "RunInput":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = 'UserInputRecorded' AND ${canonicalField(sql, "kind")} = 'user' AND ${canonicalField(sql, "runId")} = ${queryIdentifier(sql, selection.runId)} LIMIT 2`.pipe(
                  execute,
                );
              break;
            case "WorkerExecution":
              rows = (yield* Effect.forEach(["UserInputRecorded", "RunStarted"], (tag) =>
                sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
                  WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = ${tag}
                    AND ${canonicalField(sql, "runId")} IS NOT NULL
                  ORDER BY sequence DESC LIMIT 1`.pipe(execute),
              )).flat();
              break;
            case "WorkerState": {
              const runId =
                selection.sourceSubmissionId === undefined
                  ? null
                  : runIdForSubmission(selection.sourceSubmissionId);

              rows = yield* sql`
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${canonicalField(sql, "tag")} IN ('ThreadCreated', 'WorkerOriginRecorded', 'SubagentLineageRecorded', 'WorkerInputRequested', 'WorkerInputCompleted', 'WorkerStopRequested')
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${canonicalField(sql, "tag")} = 'SubtreeBudgetReserved' AND ${nullSafeEquals(sql, canonicalField(sql, "sourceSubmissionId"), queryIdentifier(sql, selection.sourceSubmissionId ?? null))}
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${canonicalField(sql, "tag")} = 'SubagentJoined' AND ${canonicalField(sql, "runId")} = ${queryIdentifier(sql, runId)}
            ORDER BY sequence LIMIT ${request.page.limit}`.pipe(execute);
              break;
            }
          }
          const decoded = yield* decodeRows(rows);

          if (selection._tag === "RunInput" && decoded.length > 1)
            return yield* failure("ambiguous original Run input");

          const remaining = decoded.filter((row) => row.sequence > after);

          return yield* Effect.forEach(
            selection._tag === "WorkerExecution"
              ? remaining.sort((a, b) => a.sequence - b.sequence).slice(0, request.page.limit)
              : remaining,
            (row) =>
              Effect.gen(function* () {
                const value = yield* envelope(row);

                if (value.record.recordId !== row.record_id || row.thread_id !== request.threadId)
                  return yield* failure("selected record incomplete or corrupt");

                return value;
              }),
          );
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("selected read", cause),
    ),
  );

  const readIdentity: ThreadStore["Service"]["readIdentity"] = Effect.fnUntraced(
    function* (request) {
      yield* Schema.decodeEffect(Schema.toType(ThreadIdentityRequest))(request);

      return yield* snapshot(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);
          const origin = workerOriginRecordId(request.threadId);
          const lineage = subagentLineageRecordId(request.threadId);

          const rows =
            yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM (
            SELECT thread_id, sequence, record_id, batch_id, record_json, 0 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND sequence = 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json, 1 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${origin} AND sequence <> 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json, 2 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${lineage} AND sequence <> 1
          ) ORDER BY identity_order`.pipe(execute);

          const records = yield* decodeRows(rows).pipe(
            Effect.flatMap(
              Effect.forEach((row) =>
                envelope(row).pipe(
                  Effect.filterOrFail(
                    (value) => value.record.recordId === row.record_id,
                    () => failure("identity record locator"),
                  ),
                ),
              ),
            ),
          );

          return yield* ThreadIdentity.makeEffect({
            threadId: request.threadId,
            tailSequence: tail.tail_sequence,
            tailDigest: tail.tail_digest,
            producerEpoch: tail.producer_epoch,
            records,
          });
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("readIdentity", cause),
    ),
  );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(
      function* (request) {
        yield* Schema.decodeEffect(ThreadPeerCountRequest)(request);
        yield* requireThread(request.threadId);

        const rows =
          yield* sql`SELECT 1 FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = 'PeerMessagePrepared' LIMIT ${request.limit}`.pipe(
            execute,
          );

        return rows.length;
      },
      Effect.mapError((cause) =>
        cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
          ? cause
          : failure("countPeerMessages", cause),
      ),
    );

  return { read, countPeerMessages, readIdentity };
});
