import { decodeExportRecord, ExportRecord } from "@yielded/agent/record-format";
import type { CanonicalRecord } from "@yielded/agent/records";
import {
  CURRENT_RECORD_FORMAT,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
  MAX_RUN_CONTINUATION_BYTES,
  RecordJson,
  ObservationOffset,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
  RecordId,
} from "@yielded/agent/records";
import {
  canonicalRunIds,
  canonicalRecordBytes,
  isWorkHandoff,
  validateProgressAppend,
} from "@yielded/agent/run-continuation";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
} from "@yielded/agent/run-journal";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import type { PreparedAppend, ThreadStore } from "@yielded/agent/thread-store";
import {
  SelectedThreadRead,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { Context, Effect, Predicate, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type { Fragment } from "effect/sql/Statement";

import { nullSafeEquals } from "./internal/sql-json.ts";
import type { RawAppendRequest } from "./SqlJournal.ts";
import { makeSqlQuery, SqlInteger, makeSqlTransaction } from "./SqlStorage.ts";

const canonicalColumns = {
  tag: "record_tag",
  runId: "run_id",
  toolCallId: "tool_call_id",
  kind: "input_kind",
  sourceSubmissionId: "source_submission_id",
  messageId: "message_id",
} as const;

const canonicalField = (sql: SqlClient.SqlClient, field: keyof typeof canonicalColumns) =>
  sql.literal(canonicalColumns[field]);

/** Scalar keys preserve opaque identifiers, including Postgres-inadmissible NUL/surrogates. */
const canonicalIdentifier = (value: string | null): string | null =>
  value === null ? null : JSON.stringify(value);

/** Recovery must seek its owner range even on fresh SQLite without planner statistics. */
const recoveryIndex = (sql: SqlClient.SqlClient, name: string | undefined) =>
  name === undefined
    ? sql.literal("")
    : sql.onDialectOrElse({
        orElse: () => sql`INDEXED BY ${sql(name)}`,
        pg: () => sql.literal(""),
      });

const wireField = (wire: RecordJson, path: ReadonlyArray<string>): unknown => {
  let value: unknown = wire;

  for (const key of path) {
    if (!Predicate.isObject(value) || Array.isArray(value)) return undefined;
    value = Reflect.get(value, key);
  }

  return value;
};

/**
 * Derive disposable locators from privately captured wire before the writer can suspend.
 * Membership is canonical read ownership, not progress accounting or execution authority:
 * a joined SubmissionSettled retains both its host and its own Submission's Run.
 */
export const canonicalRecordMetadata = (
  record: Pick<PreparedAppend["records"][number], "wire" | "canonical">,
) => {
  const text = (...path: ReadonlyArray<string>) => {
    const value = wireField(record.wire, ["payload", ...path]);

    return typeof value === "string" ? value : null;
  };

  const tag = record.canonical.payload._tag;

  return Object.freeze({
    columns: Object.freeze({
      record_tag: tag,
      run_id: canonicalIdentifier(text("runId")),
      tool_call_id: canonicalIdentifier(text("toolCallId")),
      input_kind: text("kind"),
      source_submission_id: canonicalIdentifier(text("sourceSubmissionId")),
      message_id: canonicalIdentifier(text("admission", "messageId")),
      handoff: isWorkHandoff(record.canonical) ? 1 : 0,
    }),
    runIds: Object.freeze(
      tag === "RunContinuation"
        ? []
        : canonicalRunIds(record.canonical).map((runId) => JSON.stringify(runId)),
    ),
  });
};

export type CanonicalRecordMetadata = ReturnType<typeof canonicalRecordMetadata>;

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

/** Keep every compound below workerd's limit while retaining one bounded indexed statement. */
const boundedCandidates = (
  sql: SqlClient.SqlClient,
  branches: ReadonlyArray<Fragment>,
  limit: number,
  distinct: boolean,
) => {
  const join = sql.join(distinct ? " UNION " : " UNION ALL ", false);
  const groups: Array<Fragment> = [];

  for (let start = 0; start < branches.length; start += 4) {
    const group = join(branches.slice(start, start + 4));

    groups.push(sql`SELECT * FROM (${group} ORDER BY sequence LIMIT ${limit}) AS bounded_group`);
  }

  return join(groups);
};

/** Caller owns the existing append transaction; no snapshot or nested transaction here. */
export const makeProgressAppendValidation = Effect.fnUntraced(function* (namespace?: string) {
  const sql = yield* SqlClient.SqlClient;
  const { table, execute } = yield* makeSqlQuery(namespace);

  const decode = Schema.decodeUnknownEffect(
    Schema.Array(
      Schema.Struct({
        record_id: RecordId,
        record_json: Schema.String,
      }),
    ),
  );

  return Effect.fnUntraced(function* (request: RawAppendRequest) {
    if (!request.progress.some((record) => record.continuation !== undefined)) return;
    yield* validateProgressAppend(
      request.progress,
      (runId) =>
        Effect.gen(function* () {
          const rows =
            yield* sql`SELECT record_id, record_json FROM ${table("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_continuation")}
        WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "runId")} = ${canonicalIdentifier(runId)}
          AND ${canonicalField(sql, "tag")} = 'RunContinuation'
        ORDER BY sequence DESC LIMIT 1`.pipe(execute);

          const [row] = yield* decode(rows);

          if (row === undefined) return undefined;

          const record = yield* Schema.decodeEffect(Schema.fromJsonString(ExportRecord))(
            row.record_json,
          );

          if (
            record.recordId !== row.record_id ||
            record.payload._tag !== "RunContinuation" ||
            record.payload.runId !== runId ||
            canonicalRecordBytes(record) > MAX_RUN_CONTINUATION_BYTES
          )
            return yield* failure("invalid newest Run continuation");

          return record.payload;
        }).pipe(Effect.mapError((cause) => failure("read canonical Run continuation", cause))),
      (next) =>
        Effect.gen(function* () {
          const selected = yield* makeSelectedReads(
            (row) =>
              Effect.gen(function* () {
                const wire = yield* Schema.decodeEffect(Schema.fromJsonString(RecordJson))(
                  row.record_json,
                );

                const record = yield* decodeExportRecord(CURRENT_RECORD_FORMAT, wire);

                return yield* CanonicalRecordEnvelope.makeEffect({
                  threadId: row.thread_id,
                  sequence: row.sequence,
                  batchId: row.batch_id,
                  // This private selection consumes only records, never an observation cursor.
                  offset: ObservationOffset.make(`progress-validation:${row.sequence}`),
                  record,
                });
              }).pipe(Effect.mapError((cause) => failure("decode initial preparation", cause))),
            namespace,
          ).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.provideService(SelectedReadOwner, {
              snapshot: (effect) => effect,
              tail: () =>
                Effect.succeed({
                  tail_sequence: request.expectedTailSequence,
                  tail_digest: request.expectedTailDigest,
                  producer_epoch: request.producerEpoch,
                }),
            }),
          );

          const records: Array<CanonicalRecord> = [];
          let afterSequence = CanonicalSequence.make(0);
          let bytes = 0;

          while (true) {
            const page = yield* selected.read({
              threadId: request.threadId,
              selection: {
                _tag: "RunEvidence",
                runId: next.runId,
                submissionId: next.submissionId,
                throughSequence: request.expectedTailSequence,
              },
              page: { limit: 8, afterSequence },
            });

            for (const entry of page) {
              records.push(entry.record);
              bytes += canonicalRecordBytes(entry.record);
              if (
                records.length > MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
                bytes > MAX_RUN_RECOVERY_SUFFIX_BYTES
              )
                return yield* failure("Initial Run preparation exceeds its recovery bound");
              afterSequence = entry.sequence;
            }
            if (page.length < 8) return records;
          }
        }).pipe(Effect.mapError((cause) => failure("read initial preparation", cause))),
    );
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
          if ("throughSequence" in selection && selection.throughSequence > tail.tail_sequence)
            return yield* failure("selected read is ahead of canonical tail");
          const after = request.page.afterSequence ?? 0;
          let rows: unknown;

          switch (selection._tag) {
            case "LastAgentUpdate":
              rows = yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_tag")}
                WHERE thread_id=${request.threadId} AND record_tag='AgentUpdateEmitted'
                  AND sequence<=${selection.throughSequence}
                ORDER BY sequence DESC LIMIT 1`.pipe(execute);
              break;
            case "DeliveryPredecessor": {
              const candidates = sql.join(
                " UNION ALL ",
                false,
              )(
                ["WorkerReportPrepared", "AgentUpdateEmitted"].map(
                  (tag) => sql`SELECT * FROM (
                  SELECT thread_id, sequence, record_id, batch_id, record_json
                  FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_delivery_predecessor")}
                  WHERE thread_id=${request.threadId} AND record_tag=${tag} AND handoff=1
                    AND sequence<=${selection.throughSequence}
                  ORDER BY sequence DESC LIMIT 1
                ) AS predecessor_branch`,
                ),
              );

              rows =
                yield* sql`SELECT * FROM (${candidates}) AS predecessors ORDER BY sequence DESC LIMIT 1`.pipe(
                  execute,
                );
              break;
            }
            case "RunContinuation":
              rows = yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_continuation")}
                WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "runId")} = ${canonicalIdentifier(selection.runId)}
                  AND ${canonicalField(sql, "tag")} = 'RunContinuation' AND sequence <= ${selection.throughSequence}
                ORDER BY sequence DESC LIMIT 1`.pipe(execute);
              break;
            case "RunEvidence": {
              const controls = [
                submissionInputRecordId(selection.submissionId),
                submissionAbortRecordId(selection.submissionId),
                submissionSettlementRecordId(selection.submissionId),
              ];

              const candidates = boundedCandidates(
                sql,
                [
                  sql`SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, canonical.record_json
                    FROM (
                      SELECT sequence FROM ${relation("effect_agent_record_runs")}
                      WHERE thread_id = ${request.threadId} AND run_id = ${canonicalIdentifier(selection.runId)}
                        AND sequence > ${after} AND sequence <= ${selection.throughSequence}
                      ORDER BY sequence LIMIT ${request.page.limit}
                    ) AS membership
                    JOIN ${relation("effect_agent_canonical_records")} AS canonical
                      ON canonical.thread_id = ${request.threadId} AND canonical.sequence = membership.sequence`,
                  ...controls.map(
                    (recordId) => sql`SELECT thread_id, sequence, record_id, batch_id, record_json
                    FROM ${relation("effect_agent_canonical_records")}
                    WHERE thread_id = ${request.threadId} AND record_id = ${recordId}
                      AND sequence > ${after} AND sequence <= ${selection.throughSequence}`,
                  ),
                ],
                request.page.limit,
                true,
              );

              rows = yield* sql`${candidates} ORDER BY sequence LIMIT ${request.page.limit}`.pipe(
                execute,
              );
              break;
            }
            case "WorkHandoffs":
              rows = yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_handoff")}
                WHERE thread_id = ${request.threadId} AND sequence > ${after} AND sequence <= ${selection.throughSequence}
                  AND handoff = 1
                ORDER BY sequence LIMIT ${request.page.limit}`.pipe(execute);
              break;
            case "OperationEvidence": {
              // Scalar locators store JSON-escaped identifiers; escape each key before the array.
              const encodedApprovalIds = JSON.stringify(
                selection.approvalToolCallIds.map(canonicalIdentifier),
              );

              const approvalCalls = sql.onDialectOrElse({
                pg: () => sql`SELECT value AS tool_call_id
                  FROM jsonb_array_elements_text(${encodedApprovalIds}::jsonb) AS approval_ids(value)`,
                orElse: () =>
                  sql`SELECT value AS tool_call_id FROM json_each(${encodedApprovalIds})`,
              });

              const tags = [
                "ToolCallSettled",
                "ToolCallUnknown",
                "ToolCallResolved",
                "ToolStepSettled",
                "ToolApprovalRequested",
                "ToolApprovalDecided",
                "SubagentRequested",
                "SubagentStarted",
                "SubagentJoined",
              ];

              const candidates = boundedCandidates(
                sql,
                [
                  sql`SELECT thread_id, sequence, record_id, batch_id, record_json
                  FROM ${relation("effect_agent_canonical_records")}
                  WHERE thread_id=${request.threadId} AND record_id=${selection.originRecordId}
                    AND sequence>${after} AND sequence<=${selection.throughSequence}`,
                  ...tags.map(
                    (tag) => sql`SELECT * FROM (
                  SELECT thread_id, sequence, record_id, batch_id, record_json
                  FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_call")}
                  WHERE thread_id=${request.threadId} AND record_tag=${tag}
                    AND run_id=${canonicalIdentifier(selection.runId)}
                    AND ${
                      tag === "ToolApprovalRequested" || tag === "ToolApprovalDecided"
                        ? sql`tool_call_id IN (SELECT tool_call_id FROM approval_calls)`
                        : sql`tool_call_id=${canonicalIdentifier(selection.toolCallId)}`
                    }
                    AND sequence>${after} AND sequence<=${selection.throughSequence}
                  ORDER BY sequence LIMIT ${request.page.limit}
                ) AS operation_branch`,
                  ),
                ],
                request.page.limit,
                true,
              );

              rows = yield* sql`WITH approval_calls AS (${approvalCalls})
                ${candidates} ORDER BY sequence LIMIT ${request.page.limit}`.pipe(execute);
              break;
            }
            case "RecordId":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${selection.recordId} AND sequence > ${after}`.pipe(
                  execute,
                );
              break;
            case "RunInput":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = 'UserInputRecorded' AND ${canonicalField(sql, "kind")} = 'user' AND ${canonicalField(sql, "runId")} = ${canonicalIdentifier(selection.runId)} LIMIT 2`.pipe(
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

              const predicates = [
                ...[
                  "ThreadCreated",
                  "WorkerOriginRecorded",
                  "SubagentLineageRecorded",
                  "WorkerInputRequested",
                  "WorkerInputCompleted",
                  "WorkerStopRequested",
                ].map((tag) => ({
                  index: "effect_agent_records_tag",
                  predicate: sql`record_tag = ${tag}`,
                })),
                {
                  index: "effect_agent_records_subtree",
                  predicate: sql`record_tag = 'SubtreeBudgetReserved' AND ${nullSafeEquals(sql, canonicalField(sql, "sourceSubmissionId"), canonicalIdentifier(selection.sourceSubmissionId ?? null))}`,
                },
                {
                  index: "effect_agent_records_call",
                  predicate: sql`record_tag = 'SubagentJoined' AND run_id = ${canonicalIdentifier(runId)}`,
                },
              ];

              const candidates = boundedCandidates(
                sql,
                predicates.map(
                  ({ index, predicate }) => sql`SELECT * FROM (
                SELECT thread_id, sequence, record_id, batch_id, record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, index)}
                WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${predicate}
                ORDER BY sequence LIMIT ${request.page.limit}
              ) AS worker_branch`,
                ),
                request.page.limit,
                false,
              );

              rows = yield* sql`${candidates} ORDER BY sequence LIMIT ${request.page.limit}`.pipe(
                execute,
              );
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
                if (
                  selection._tag === "RunContinuation" &&
                  (value.record.payload._tag !== "RunContinuation" ||
                    value.record.payload.runId !== selection.runId)
                )
                  return yield* failure("invalid newest Run continuation");
                if (
                  selection._tag === "RunEvidence" &&
                  (value.record.payload._tag === "RunContinuation" ||
                    (!canonicalRunIds(value.record).includes(selection.runId) &&
                      ![
                        submissionInputRecordId(selection.submissionId),
                        submissionAbortRecordId(selection.submissionId),
                        submissionSettlementRecordId(selection.submissionId),
                      ].includes(value.record.recordId)))
                )
                  return yield* failure("invalid Run evidence membership");
                if (selection._tag === "WorkHandoffs" && !isWorkHandoff(value.record))
                  return yield* failure("invalid canonical handoff membership");
                if (
                  selection._tag === "LastAgentUpdate" &&
                  value.record.payload._tag !== "AgentUpdateEmitted"
                )
                  return yield* failure("invalid newest update membership");
                if (
                  selection._tag === "DeliveryPredecessor" &&
                  value.record.payload._tag !== "WorkerReportPrepared" &&
                  !(
                    value.record.payload._tag === "AgentUpdateEmitted" &&
                    value.record.payload.delivery !== undefined
                  )
                )
                  return yield* failure("invalid delivery predecessor membership");
                if (selection._tag === "OperationEvidence") {
                  const payload = value.record.payload;
                  const origin = value.record.recordId === selection.originRecordId;

                  if (
                    !("runId" in payload) ||
                    payload.runId !== selection.runId ||
                    (origin && payload._tag === "ModelResponseRecorded"
                      ? !payload.toolOperations.some(
                          (call) => call.toolCallId === selection.toolCallId,
                        )
                      : !("toolCallId" in payload) ||
                        (payload._tag === "ToolApprovalRequested" ||
                        payload._tag === "ToolApprovalDecided"
                          ? !selection.approvalToolCallIds.includes(payload.toolCallId)
                          : payload.toolCallId !== selection.toolCallId))
                  )
                    return yield* failure("invalid operation evidence membership");
                }

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
