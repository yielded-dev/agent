import { decodeExportRecord, ExportRecord } from "@yielded/agent/record-format";
import {
  type CanonicalRecord,
  CURRENT_RECORD_FORMAT,
  MAX_CANONICAL_RECORD_BYTES,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
  MAX_RUN_CONTINUATION_BYTES,
  RecordJson,
  ObservationOffset,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
  PromptRecord,
  type PromptRecordEnvelope,
  RecordId,
} from "@yielded/agent/records";
import {
  canonicalRunIds,
  canonicalRecordBytes,
  isWorkHandoff,
  ProgressAppendReader,
  validateProgressAppend,
} from "@yielded/agent/run-continuation";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
  toolCallSettledRecordId,
} from "@yielded/agent/run-journal";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import type { PreparedAppend, ThreadStore } from "@yielded/agent/thread-store";
import {
  SelectedThreadRead,
  ThreadPromptRead,
  ThreadPeerCountRequest,
  ThreadWorkerCapacityRequest,
  ThreadWorkerCapacity,
  PROMPT_EVIDENCE_TAGS,
  ThreadIdentity,
  ThreadIdentityRequest,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { WORK_INDEX_VERSION } from "@yielded/agent/thread-work";
import { Context, Effect, Predicate, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type { Fragment } from "effect/sql/Statement";

import type { RawAppendRequest } from "./SqlJournal.ts";
import { makeSqlQuery, SqlInteger, makeSqlTransaction } from "./SqlStorage.ts";
import { canonicalRecordJson } from "./SqlThreadArchiveRange.ts";

/** Closed library tags must match SQLite partial-index predicates at prepare time. */
export const SQL_PROMPT_PREDICATE = `record_tag IN (${PROMPT_EVIDENCE_TAGS.map((tag) => `'${tag}'`).join(", ")})`;

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
      source_submission_id: canonicalIdentifier(
        tag === "WorkerInputRequested"
          ? text("admission", "sourceSubmissionId")
          : text("sourceSubmissionId"),
      ),
      message_id: canonicalIdentifier(
        tag === "SubtreeBudgetReserved"
          ? text("reservationId")
          : (text("admission", "messageId") ?? text("messageId")),
      ),
      submission_id: canonicalIdentifier(text("submissionId")),
      application_input:
        tag === "UserInputRecorded" &&
        text("messageAdmission", "_tag") !== "WorkerCompletion" &&
        text("messageAdmission", "_tag") !== "WorkerUpdate"
          ? 1
          : 0,
      context_through:
        record.canonical.payload._tag === "RunContextRecorded"
          ? record.canonical.payload.historyThrough
          : record.canonical.payload._tag === "CompactionCreated"
            ? record.canonical.payload.coversThrough
            : null,
      context_kind: tag === "CompactionCreated" ? text("kind") : null,
      worker_thread_id: canonicalIdentifier(
        tag === "WorkerInputRequested"
          ? text("admission", "origin", "worker", "threadId")
          : tag === "WorkerStopRequested"
            ? text("command", "worker", "threadId")
            : text("workerThreadId"),
      ),
      handoff: isWorkHandoff(record.canonical) ? 1 : 0,
    }),
    runIds: Object.freeze(
      tag === "RunContinuation"
        ? []
        : canonicalRunIds(record.canonical).map((runId) => JSON.stringify(runId)),
    ),
  });
};

/** Exact opaque identities. Callers publish these persistent pointers with their canonical rows. */
export const canonicalRecordPointers = (record: CanonicalRecord) => {
  const payload = record.payload;

  return {
    toolSettlements:
      payload._tag === "ModelResponseRecorded"
        ? payload.toolOperations.map((operation) =>
            JSON.stringify(
              toolCallSettledRecordId(payload.runId, payload.turn, operation.toolCallId),
            ),
          )
        : [],
    refusalReservation:
      payload._tag === "WorkerInputRefused"
        ? JSON.stringify(payload.reservation.recordId)
        : undefined,
  };
};

export type CanonicalRecordMetadata = ReturnType<typeof canonicalRecordMetadata>;

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

const decodeRecordJson = Schema.decodeEffect(Schema.fromJsonString(ExportRecord));
const decodeRecordWire = Schema.decodeEffect(Schema.fromJsonString(RecordJson));
const decodeIdentityRequest = Schema.decodeEffect(Schema.toType(ThreadIdentityRequest));

const decodeIdentityFacts = Schema.decodeUnknownEffect(
  Schema.Tuple([
    Schema.Struct({ admissions_count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)) }),
  ]),
);

const decodeReadyWork = Schema.decodeUnknownEffect(
  Schema.Tuple([
    Schema.Struct({
      version: SqlInteger,
      state: Schema.Literal("ready"),
      through_sequence: SqlInteger,
      entry_count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
      actual_count: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
    }),
  ]),
);

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
  const recordJson = canonicalRecordJson(sql, namespace);

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
    yield* validateProgressAppend(request.progress).pipe(
      Effect.provideService(ProgressAppendReader, {
        previous: (runId) =>
          Effect.gen(function* () {
            const rows =
              yield* sql`SELECT record_id, ${recordJson} AS record_json FROM ${table("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_continuation")}
        WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "runId")} = ${canonicalIdentifier(runId)}
          AND ${canonicalField(sql, "tag")} = 'RunContinuation'
        ORDER BY sequence DESC LIMIT 1`.pipe(execute);

            const [row] = yield* decode(rows);

            if (row === undefined) return undefined;

            const record = yield* decodeRecordJson(row.record_json);

            if (
              record.recordId !== row.record_id ||
              record.payload._tag !== "RunContinuation" ||
              record.payload.runId !== runId ||
              canonicalRecordBytes(record) > MAX_RUN_CONTINUATION_BYTES
            )
              return yield* failure("invalid newest Run continuation");

            return record.payload;
          }).pipe(Effect.mapError((cause) => failure("read canonical Run continuation", cause))),
        initial: (next) =>
          Effect.gen(function* () {
            const selected = yield* makeSelectedReads(
              (row) =>
                Effect.gen(function* () {
                  const wire = yield* decodeRecordWire(row.record_json);

                  const record = yield* decodeExportRecord(CURRENT_RECORD_FORMAT, wire);

                  return new CanonicalRecordEnvelope(
                    {
                      threadId: row.thread_id,
                      sequence: row.sequence,
                      batchId: row.batch_id,
                      // This private selection consumes only records, never an observation cursor.
                      offset: ObservationOffset.make(`progress-validation:${row.sequence}`),
                      record,
                    },
                    { disableChecks: true },
                  );
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
      }),
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

const MAX_PROMPT_PAGE_JSON_BYTES = 4 * 1024 * 1024;

const PromptReadPlanRow = Schema.Struct({
  sequence: Row.fields.sequence,
  record_json_bytes: SqlInteger.pipe(
    Schema.decodeTo(
      Schema.Natural.check(
        Schema.isGreaterThan(0),
        Schema.isLessThanOrEqualTo(MAX_CANONICAL_RECORD_BYTES),
      ),
    ),
  ),
});

type PromptReadPage = [typeof PromptReadPlanRow.Type, ...Array<typeof PromptReadPlanRow.Type>];

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
  const recordJson = canonicalRecordJson(sql, namespace);
  const membershipJson = canonicalRecordJson(sql, namespace, "canonical");
  const decodeRows = Schema.decodeUnknownEffect(Schema.Array(Row));

  const decodePromptPlan = Schema.decodeUnknownEffect(Schema.Array(PromptReadPlanRow));

  const promptRecordBytes = sql.onDialectOrElse({
    pg: () => sql`octet_length(${recordJson})`,
    orElse: () => sql`length(CAST(${recordJson} AS BLOB))`,
  });

  const decodePromptRows = Schema.decodeUnknownEffect(
    Schema.Array(
      Schema.Struct({
        thread_id: Row.fields.thread_id,
        sequence: Row.fields.sequence,
        record_id: Row.fields.record_id,
        record_json: Schema.fromJsonString(PromptRecord),
      }),
    ),
  );

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

  /** Disposable membership must cover this captured tail; historical liveness is unavailable. */
  const requireReadyWork = Effect.fnUntraced(function* (
    threadId: SelectedThreadRead["threadId"],
    through: number,
  ) {
    const rows = yield* sql`SELECT version, state, through_sequence, entry_count,
      (SELECT COUNT(*) FROM ${relation("effect_agent_work_entries")} WHERE thread_id=${threadId}) AS actual_count
      FROM ${relation("effect_agent_work_index")} WHERE thread_id=${threadId}`.pipe(execute);

    const [header] = yield* decodeReadyWork(rows);

    if (
      header.version !== WORK_INDEX_VERSION ||
      header.through_sequence !== through ||
      header.entry_count !== header.actual_count
    )
      return yield* failure("native work membership is incomplete");
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
            case "LatestApplicationInput":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_application_input")}
                WHERE thread_id=${request.threadId} AND application_input=1 AND sequence<=${selection.throughSequence}
                ORDER BY sequence DESC LIMIT 1`.pipe(execute);
              break;
            case "LatestRunContext":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_context")}
                WHERE thread_id=${request.threadId} AND record_tag='RunContextRecorded'
                  AND context_through<=${selection.throughSequence} AND sequence<=${selection.throughSequence}
                ORDER BY context_through DESC, sequence DESC LIMIT 1`.pipe(execute);
              break;
            case "PromptEvidence":
              // SQLite proves forced partial-index predicates at prepare time. These are closed
              // library tags matching the fresh layout, never interpolated caller SQL.
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_prompt")}
                WHERE thread_id=${request.threadId} AND ${sql.literal(SQL_PROMPT_PREDICATE)}
                  AND sequence>${after} AND sequence<=${selection.throughSequence}
                ORDER BY sequence LIMIT ${request.page.limit}`.pipe(execute);
              break;
            case "LatestModelCompleted":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_tag")}
                WHERE thread_id=${request.threadId} AND record_tag='ModelCompleted' AND sequence<=${selection.throughSequence}
                ORDER BY sequence DESC LIMIT 1`.pipe(execute);
              break;
            case "DurableHistoryOwner": {
              const candidates = boundedCandidates(
                sql,
                [
                  sql`SELECT * FROM (SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                  FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_admitted_input")}
                  WHERE thread_id=${request.threadId} AND record_tag='UserInputRecorded' AND submission_id IS NOT NULL
                    AND sequence<=${selection.throughSequence} ORDER BY sequence LIMIT 1) AS admitted_owner`,
                  ...[
                    "RunStarted",
                    "SubmissionSettled",
                    "AbortRequested",
                    "WorkerOriginRecorded",
                    "SubagentLineageRecorded",
                  ].map(
                    (tag) => sql`SELECT * FROM (
                  SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                  FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_tag")}
                  WHERE thread_id=${request.threadId} AND record_tag=${tag} AND sequence<=${selection.throughSequence}
                  ORDER BY sequence LIMIT 1) AS durable_owner`,
                  ),
                ],
                1,
                false,
              );

              rows =
                yield* sql`SELECT * FROM (${candidates}) AS owners ORDER BY sequence LIMIT 1`.pipe(
                  execute,
                );
              break;
            }
            case "ContextWindowBoundary": {
              // Cover [0, atSequence) with disjoint power-of-two buckets. Each native
              // prefix index seeks its newest visible sequence, even for nonmonotone
              // coverage. At most 53 scalar candidates precede one payload lookup.
              let candidates: Array<Fragment> = [];
              let start = 0;

              while (start < selection.atSequence) {
                const remaining = selection.atSequence - start;
                let height = Math.floor(Math.log2(remaining));

                while (2 ** height > remaining) height--;
                const width = 2 ** height;

                const prefix =
                  height === 0
                    ? sql`context_through`
                    : sql`(context_through >> ${sql.literal(String(height))})`;

                candidates.push(sql`COALESCE((
                  SELECT sequence FROM ${relation("effect_agent_canonical_records")}
                  ${recoveryIndex(sql, height === 0 ? "effect_agent_records_rollover" : `effect_agent_records_rollover_bucket_${height}`)}
                  WHERE thread_id=(SELECT thread_id FROM boundary_request)
                    AND record_tag='CompactionCreated' AND context_kind='rollover'
                    AND ${prefix}=${start / width}
                    AND sequence<=(SELECT through_sequence FROM boundary_request)
                  ORDER BY sequence DESC LIMIT 1
                ), 0)`);
                start += width;
              }
              // Binary scalar merges avoid workerd's compound-SELECT and function-arity
              // limits. Materialize the winning sequence once before hydrating its record.
              while (candidates.length > 1) {
                const merged: Array<Fragment> = [];

                for (let at = 0; at < candidates.length; at += 2) {
                  const left = candidates[at];
                  const right = candidates[at + 1];

                  if (left !== undefined)
                    merged.push(
                      right === undefined
                        ? left
                        : sql.onDialectOrElse({
                            pg: () => sql`GREATEST(${left}, ${right})`,
                            orElse: () => sql`MAX(${left}, ${right})`,
                          }),
                    );
                }
                candidates = merged;
              }
              const winner = candidates[0];

              if (winner === undefined) {
                rows = [];
                break;
              }

              rows = yield* sql`WITH boundary_request AS (
                  SELECT ${request.threadId} AS thread_id, CAST(${selection.throughSequence} AS BIGINT) AS through_sequence
                ), boundary AS MATERIALIZED (SELECT ${winner} AS sequence)
                SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${membershipJson} AS record_json
                FROM boundary
                JOIN ${relation("effect_agent_canonical_records")} AS canonical
                  ON canonical.thread_id=${request.threadId} AND canonical.sequence=boundary.sequence`.pipe(
                execute,
              );
              break;
            }
            case "LiveWorkerInputs": {
              if (selection.throughSequence !== tail.tail_sequence)
                return yield* failure("live worker membership requires current canonical tail");
              yield* requireReadyWork(request.threadId, tail.tail_sequence);
              rows =
                yield* sql`SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${membershipJson} AS record_json
                FROM (SELECT created_sequence FROM ${relation("effect_agent_work_entries")}
                  ${recoveryIndex(sql, selection.workerThreadId === undefined ? "effect_agent_work_entries_worker_partition" : "effect_agent_work_entries_worker")}
                  WHERE thread_id=${request.threadId} AND worker_thread_id IS NOT NULL
                    ${selection.workerThreadId === undefined ? sql`` : sql`AND worker_thread_id=${canonicalIdentifier(selection.workerThreadId)}`}
                    AND created_sequence>${after} AND created_sequence<=${selection.throughSequence}
                  ORDER BY created_sequence LIMIT ${request.page.limit}) AS membership
                LEFT JOIN ${relation("effect_agent_canonical_records")} AS canonical
                  ON canonical.thread_id=${request.threadId} AND canonical.sequence=membership.created_sequence`.pipe(
                  execute,
                );
              break;
            }
            case "WorkerStop":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_worker_stop")}
                WHERE thread_id=${request.threadId} AND record_tag='WorkerStopRequested'
                  AND worker_thread_id=${canonicalIdentifier(selection.workerThreadId)} AND sequence<=${selection.throughSequence}
                ORDER BY sequence DESC LIMIT 1`.pipe(execute);
              break;
            case "LastAgentUpdate":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
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
                  SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
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
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
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
                  sql`SELECT * FROM (
                      SELECT sequence FROM ${relation("effect_agent_record_runs")}
                      WHERE thread_id = ${request.threadId} AND run_id = ${canonicalIdentifier(selection.runId)}
                        AND sequence > ${after} AND sequence <= ${selection.throughSequence}
                      ORDER BY sequence LIMIT ${request.page.limit}
                    ) AS membership`,
                  ...controls.map(
                    (recordId) => sql`SELECT sequence
                    FROM ${relation("effect_agent_canonical_records")}
                    WHERE thread_id = ${request.threadId} AND record_id = ${recordId}
                      AND sequence > ${after} AND sequence <= ${selection.throughSequence}`,
                  ),
                ],
                request.page.limit,
                true,
              );

              // Deduplicate and page scalar identities before loading hot or archived wire.
              // The materialized page keeps every payload probe on its exact primary key.
              rows = yield* sql`WITH evidence AS MATERIALIZED (
                  ${candidates} ORDER BY sequence LIMIT ${request.page.limit}
                )
                SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${membershipJson} AS record_json
                FROM evidence
                LEFT JOIN ${relation("effect_agent_canonical_records")} AS canonical
                  ON canonical.thread_id=${request.threadId} AND canonical.sequence=evidence.sequence
                ORDER BY evidence.sequence`.pipe(execute);
              break;
            }
            case "WorkHandoffs":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
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
                  sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                  FROM ${relation("effect_agent_canonical_records")}
                  WHERE thread_id=${request.threadId} AND record_id=${selection.originRecordId}
                    AND sequence>${after} AND sequence<=${selection.throughSequence}`,
                  ...tags.map(
                    (tag) => sql`SELECT * FROM (
                  SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
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
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${selection.recordId} AND sequence > ${after}`.pipe(
                  execute,
                );
              break;
            case "ToolDeclaration":
              rows =
                yield* sql`SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${membershipJson} AS record_json
                FROM (SELECT sequence FROM ${relation("effect_agent_tool_declarations")}
                  WHERE thread_id=${request.threadId} AND settlement_record_id=${canonicalIdentifier(selection.settlementRecordId)}
                    AND sequence<=${selection.throughSequence} ORDER BY sequence LIMIT 2) AS declaration
                LEFT JOIN ${relation("effect_agent_canonical_records")} AS canonical
                  ON canonical.thread_id=${request.threadId} AND canonical.sequence=declaration.sequence`.pipe(
                  execute,
                );
              break;
            case "RunInput":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = 'UserInputRecorded' AND ${canonicalField(sql, "kind")} = 'user' AND ${canonicalField(sql, "runId")} = ${canonicalIdentifier(selection.runId)} LIMIT 2`.pipe(
                  execute,
                );
              break;
            case "WorkerExecution":
              rows = (yield* Effect.forEach(["UserInputRecorded", "RunStarted"], (tag) =>
                sql`SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_worker_execution")}
                  WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = ${tag}
                    AND ${canonicalField(sql, "runId")} IS NOT NULL
                  ORDER BY sequence DESC LIMIT 1`.pipe(execute),
              )).flat();
              break;
            case "WorkerState": {
              const funding = selection.sourceSubmissionId;

              // Omission is identity-only. Null-funded programmatic lifetime history is not a Run.
              const branches = [
                "ThreadCreated",
                "WorkerOriginRecorded",
                "SubagentLineageRecorded",
              ].map(
                (tag) => sql`SELECT * FROM (
                SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_tag")}
                WHERE thread_id=${request.threadId} AND record_tag=${tag} AND sequence<=${selection.expectedTailSequence}
                ORDER BY sequence LIMIT 1) AS worker_identity WHERE sequence>${after}`,
              );

              if (funding !== undefined) {
                for (const [tag, index] of [
                  ["WorkerInputRequested", "effect_agent_records_worker_funding"],
                  ["SubtreeBudgetReserved", "effect_agent_records_subtree"],
                ]) {
                  const tagLiteral = sql.literal(
                    tag === "WorkerInputRequested"
                      ? "'WorkerInputRequested'"
                      : "'SubtreeBudgetReserved'",
                  );

                  branches.push(sql`SELECT * FROM (
                    SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                    FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, index)}
                    WHERE thread_id=${request.threadId} AND record_tag=${tagLiteral} AND source_submission_id=${canonicalIdentifier(funding)}
                      AND sequence>${after} AND sequence<=${selection.expectedTailSequence}
                    ORDER BY sequence LIMIT ${request.page.limit}) AS funded_worker`);
                  // Refusal closes the exact preparation identity, including a reservation which
                  // never produced WorkerInputRequested. Reused message IDs cannot select another Run.
                  branches.push(sql`SELECT * FROM (
                    SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${membershipJson} AS record_json
                    FROM ${relation("effect_agent_canonical_records")} AS funded ${recoveryIndex(sql, index)}
                    JOIN ${relation("effect_agent_record_refusals")} AS refusal
                      ON refusal.thread_id=funded.thread_id AND refusal.reservation_record_id=${sql.onDialectOrElse({ pg: () => sql`to_json(funded.record_id)::text`, orElse: () => sql`json_quote(funded.record_id)` })}
                    LEFT JOIN ${relation("effect_agent_canonical_records")} AS canonical
                      ON canonical.thread_id=refusal.thread_id AND canonical.sequence=refusal.sequence
                    WHERE funded.thread_id=${request.threadId} AND funded.record_tag=${tagLiteral} AND funded.source_submission_id=${canonicalIdentifier(funding)}
                      AND refusal.sequence>${after} AND refusal.sequence<=${selection.expectedTailSequence}
                    ORDER BY refusal.sequence LIMIT ${request.page.limit}) AS refused_worker`);
                }
                branches.push(sql`SELECT * FROM (
                  SELECT canonical.thread_id, canonical.sequence, canonical.record_id, canonical.batch_id, ${membershipJson} AS record_json
                  FROM ${relation("effect_agent_canonical_records")} AS funded ${recoveryIndex(sql, "effect_agent_records_worker_funding")}
                  JOIN ${relation("effect_agent_canonical_records")} AS canonical ${recoveryIndex(sql, "effect_agent_records_worker_completed")}
                    ON canonical.thread_id=funded.thread_id AND canonical.message_id=funded.message_id AND canonical.record_tag='WorkerInputCompleted'
                  WHERE funded.thread_id=${request.threadId} AND funded.record_tag='WorkerInputRequested' AND funded.source_submission_id=${canonicalIdentifier(funding)}
                    AND canonical.sequence>${after} AND canonical.sequence<=${selection.expectedTailSequence}
                  ORDER BY canonical.sequence LIMIT ${request.page.limit}) AS completed_worker`);
                branches.push(sql`SELECT * FROM (
                  SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json
                  FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_call")}
                  WHERE thread_id=${request.threadId} AND record_tag='SubagentJoined' AND run_id=${canonicalIdentifier(runIdForSubmission(funding))}
                    AND sequence>${after} AND sequence<=${selection.expectedTailSequence}
                  ORDER BY sequence LIMIT ${request.page.limit}) AS joined_worker`);
              }
              const candidates = boundedCandidates(sql, branches, request.page.limit, true);

              rows =
                yield* sql`SELECT * FROM (${candidates}) AS worker_state ORDER BY sequence LIMIT ${request.page.limit}`.pipe(
                  execute,
                );
              break;
            }
          }
          const decoded = yield* decodeRows(rows);

          if (selection._tag === "ToolDeclaration" && decoded.length > 1)
            return yield* failure("ambiguous original Tool declaration");

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
                const payload = value.record.payload;

                if (
                  selection._tag === "LatestApplicationInput" &&
                  (payload._tag !== "UserInputRecorded" ||
                    (payload.messageAdmission !== undefined &&
                      "_tag" in payload.messageAdmission &&
                      (payload.messageAdmission._tag === "WorkerCompletion" ||
                        payload.messageAdmission._tag === "WorkerUpdate")))
                )
                  return yield* failure("invalid newest application input membership");
                if (
                  selection._tag === "ToolDeclaration" &&
                  (payload._tag !== "ModelResponseRecorded" ||
                    !payload.toolOperations.some(
                      (operation) =>
                        toolCallSettledRecordId(
                          payload.runId,
                          payload.turn,
                          operation.toolCallId,
                        ) === selection.settlementRecordId,
                    ))
                )
                  return yield* failure("invalid original Tool declaration pointer");
                if (
                  selection._tag === "WorkerState" &&
                  (payload._tag === "WorkerInputRequested"
                    ? payload.admission.sourceSubmissionId !== selection.sourceSubmissionId
                    : payload._tag === "SubtreeBudgetReserved"
                      ? payload.sourceSubmissionId !== selection.sourceSubmissionId
                      : payload._tag === "SubagentJoined"
                        ? selection.sourceSubmissionId === undefined ||
                          payload.runId !== runIdForSubmission(selection.sourceSubmissionId)
                        : ![
                            "ThreadCreated",
                            "WorkerOriginRecorded",
                            "SubagentLineageRecorded",
                            "WorkerInputCompleted",
                            "WorkerInputRefused",
                          ].includes(payload._tag))
                )
                  return yield* failure("invalid Worker funding membership");
                if (
                  selection._tag === "LatestRunContext" &&
                  (payload._tag !== "RunContextRecorded" ||
                    payload.historyThrough > selection.throughSequence)
                )
                  return yield* failure("invalid Run context membership");
                if (
                  selection._tag === "PromptEvidence" &&
                  !PROMPT_EVIDENCE_TAGS.some((tag) => tag === payload._tag)
                )
                  return yield* failure("invalid prompt evidence membership");
                if (selection._tag === "LatestModelCompleted" && payload._tag !== "ModelCompleted")
                  return yield* failure("invalid newest model membership");
                if (
                  selection._tag === "ContextWindowBoundary" &&
                  (payload._tag !== "CompactionCreated" ||
                    payload.kind !== "rollover" ||
                    payload.coversThrough >= selection.atSequence)
                )
                  return yield* failure("invalid context boundary membership");
                if (
                  selection._tag === "DurableHistoryOwner" &&
                  !(payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) &&
                  ![
                    "RunStarted",
                    "SubmissionSettled",
                    "AbortRequested",
                    "WorkerOriginRecorded",
                    "SubagentLineageRecorded",
                  ].includes(payload._tag)
                )
                  return yield* failure("invalid durable owner membership");
                if (
                  selection._tag === "LiveWorkerInputs" &&
                  (payload._tag !== "WorkerInputRequested" ||
                    (selection.workerThreadId !== undefined &&
                      payload.admission.origin.worker.threadId !== selection.workerThreadId))
                )
                  return yield* failure("invalid live worker membership");
                if (
                  selection._tag === "WorkerStop" &&
                  (payload._tag !== "WorkerStopRequested" ||
                    payload.command.worker.threadId !== selection.workerThreadId)
                )
                  return yield* failure("invalid worker stop membership");
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

  const readPrompt = Effect.fnUntraced(
    function* (input: ThreadPromptRead) {
      const request = yield* Schema.decodeEffect(ThreadPromptRead)(input);

      return yield* snapshot(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);

          if (request.throughSequence > tail.tail_sequence)
            return yield* failure("prompt read is ahead of canonical tail");

          // Bound raw hydration before the narrow decoder discards large non-prompt fields.
          const plan = yield* decodePromptPlan(
            yield* sql`SELECT sequence, ${promptRecordBytes} AS record_json_bytes
            FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_prompt")}
            WHERE thread_id=${request.threadId} AND ${sql.literal(SQL_PROMPT_PREDICATE)}
              AND sequence>${request.afterSequence ?? 0} AND sequence<=${request.throughSequence}
            ORDER BY sequence LIMIT ${request.limit}`.pipe(execute),
          );

          if (plan.length > request.limit) return yield* failure("prompt read membership");
          const pages: Array<PromptReadPage> = [];
          let page: PromptReadPage | undefined;
          let pageBytes = 0;
          let previousSequence = request.afterSequence ?? 0;

          for (const row of plan) {
            if (row.sequence <= previousSequence || row.sequence > request.throughSequence)
              return yield* failure("prompt read ordering");
            previousSequence = row.sequence;
            if (
              page === undefined ||
              pageBytes + row.record_json_bytes > MAX_PROMPT_PAGE_JSON_BYTES
            ) {
              page = [row];
              pages.push(page);
              pageBytes = row.record_json_bytes;
            } else {
              page.push(row);
              pageBytes += row.record_json_bytes;
            }
          }

          const records: Array<PromptRecordEnvelope> = [];

          for (const page of pages) {
            const rows = yield* decodePromptRows(
              yield* sql`SELECT thread_id, sequence, record_id, ${recordJson} AS record_json
              FROM ${relation("effect_agent_canonical_records")} ${recoveryIndex(sql, "effect_agent_records_prompt")}
              WHERE thread_id=${request.threadId} AND ${sql.literal(SQL_PROMPT_PREDICATE)}
                AND sequence>=${page[0].sequence} AND sequence<=${page[page.length - 1].sequence}
              ORDER BY sequence`.pipe(execute),
            );

            if (
              rows.length !== page.length ||
              rows.some(
                (row, index) =>
                  row.sequence !== page[index].sequence ||
                  row.record_json.recordId !== row.record_id ||
                  row.thread_id !== request.threadId,
              )
            )
              return yield* failure("prompt read membership or identity");
            for (const row of rows)
              records.push({
                threadId: row.thread_id,
                sequence: row.sequence,
                record: row.record_json,
              });
          }

          return records;
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("readPrompt", cause),
    ),
  );

  const readIdentity: ThreadStore["Service"]["readIdentity"] = Effect.fnUntraced(
    function* (request) {
      yield* decodeIdentityRequest(request);

      return yield* snapshot(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);

          const [facts] = yield* decodeIdentityFacts(
            yield* sql`SELECT admissions_count FROM ${relation("effect_agent_transfer_state")} WHERE thread_id=${request.threadId}`.pipe(
              execute,
            ),
          );

          const origin = workerOriginRecordId(request.threadId);
          const lineage = subagentLineageRecordId(request.threadId);

          const rows =
            yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM (
            SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json, 0 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND sequence = 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json, 1 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${origin} AND sequence <> 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, ${recordJson} AS record_json, 2 AS identity_order
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
            admissions: facts.admissions_count,
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

  const readWorkerCapacity: NonNullable<ThreadStore["Service"]["readWorkerCapacity"]> =
    Effect.fnUntraced(
      function* (request) {
        yield* Schema.decodeEffect(ThreadWorkerCapacityRequest)(request);

        return yield* snapshot(
          Effect.gen(function* () {
            const tail = yield* requireThread(request.threadId);

            if (
              tail.tail_sequence !== request.expectedTailSequence ||
              tail.tail_digest !== request.expectedTailDigest
            )
              return yield* failure("worker capacity tail changed");
            yield* requireReadyWork(request.threadId, tail.tail_sequence);

            const active =
              yield* sql`SELECT DISTINCT worker_thread_id FROM ${relation("effect_agent_work_entries")} ${recoveryIndex(sql, "effect_agent_work_entries_worker")}
          WHERE thread_id=${request.threadId} AND worker_thread_id IS NOT NULL LIMIT ${request.activeLimit + 1}`.pipe(
                execute,
              );

            const worker =
              yield* sql`SELECT 1 FROM ${relation("effect_agent_work_entries")} ${recoveryIndex(sql, "effect_agent_work_entries_worker")}
          WHERE thread_id=${request.threadId} AND worker_thread_id IS NOT NULL AND worker_thread_id=${canonicalIdentifier(request.workerThreadId)} LIMIT 1`.pipe(
                execute,
              );

            const pending =
              yield* sql`SELECT 1 FROM ${relation("effect_agent_work_entries")} ${recoveryIndex(sql, "effect_agent_work_entries_worker_capacity")}
          WHERE thread_id=${request.threadId} AND worker_thread_id IS NOT NULL AND worker_thread_id=${canonicalIdentifier(request.workerThreadId)}
            AND worker_update=${request.update ? 1 : 0} LIMIT ${request.pendingLimit + 1}`.pipe(
                execute,
              );

            return yield* ThreadWorkerCapacity.makeEffect({
              threadId: request.threadId,
              tailSequence: tail.tail_sequence,
              tailDigest: tail.tail_digest,
              producerEpoch: tail.producer_epoch,
              activeWorkers: active.length,
              workerActive: worker.length === 1,
              pendingInputs: pending.length,
            });
          }),
        );
      },
      Effect.mapError((cause) =>
        cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
          ? cause
          : failure("readWorkerCapacity", cause),
      ),
    );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(
      function* (request) {
        yield* Schema.decodeEffect(ThreadPeerCountRequest)(request);

        return yield* snapshot(
          Effect.gen(function* () {
            const tail = yield* requireThread(request.threadId);

            yield* requireReadyWork(request.threadId, tail.tail_sequence);

            // Canonical handoffs disappear on transfer; every retained delivery identity suppresses
            // its old handoff, including terminal identities. Both candidates contain only live rows.
            const peer = sql.onDialectOrElse({
              pg: () => sql`(read_metadata ->> 'peer') = 'true'`,
              orElse: () =>
                sql`json_extract(record_json, '$.envelope.messageAdmission.schemaVersion') = 1`,
            });

            const rows = yield* sql`SELECT message_id FROM (
          SELECT message_id FROM (SELECT message_id FROM ${relation("effect_agent_work_entries")} AS work ${recoveryIndex(sql, "effect_agent_work_entries_peer")}
            WHERE thread_id=${request.threadId} AND handoff_kind='peer' AND message_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM ${relation("effect_agent_message_deliveries")} AS delivery ${recoveryIndex(sql, "effect_agent_message_deliveries_identity")}
                WHERE delivery.owner_thread_id=work.thread_id AND ${sql.onDialectOrElse({ pg: () => sql`(delivery.read_metadata ->> 'messageId')`, orElse: () => sql`json_quote(delivery.message_id)` })}=work.message_id)
            ORDER BY message_id LIMIT ${request.limit}) AS handoffs
          UNION SELECT message_id FROM (SELECT ${sql.onDialectOrElse({ pg: () => sql`(read_metadata ->> 'messageId')`, orElse: () => sql`json_quote(message_id)` })} AS message_id
            FROM ${relation("effect_agent_message_deliveries")} ${recoveryIndex(sql, "effect_agent_message_deliveries_peer")}
            WHERE owner_thread_id=${request.threadId} AND state NOT IN ('processed', 'refused') AND ${peer}
            ORDER BY message_id LIMIT ${request.limit}) AS deliveries
          ) AS live_peers LIMIT ${request.limit}`.pipe(execute);

            return rows.length;
          }),
        );
      },
      Effect.mapError((cause) =>
        cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
          ? cause
          : failure("countPeerMessages", cause),
      ),
    );

  return { read, readPrompt, countPeerMessages, readIdentity, readWorkerCapacity };
});
