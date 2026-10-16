import { canonicalJson, utf8ByteLength } from "@yielded/agent/digest";
import { RunId, SubmissionId, ThreadId } from "@yielded/agent/identifiers";
import {
  MessageDeliveryRecord,
  messageDeliveryDeadline,
  peerMessageRetentionDeadline,
  defaultMessageDeliveryStoreLimits,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import { decodeExportRecordJson, ExportedRecord } from "@yielded/agent/record-format";
import type { RecordId } from "@yielded/agent/records";
import {
  CURRENT_RECORD_FORMAT,
  SettlementOutcome,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  RecordJson,
  MAX_RUN_EVIDENCE_RECORDS,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_TERMINAL_BYTES,
  RUN_TERMINAL_RESERVE_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
} from "@yielded/agent/records";
import { isOperationalRecord, verifyRunContinuations } from "@yielded/agent/run-continuation";
import type { StorageOwner } from "@yielded/agent/run-storage";
import { validateCanonicalSettlement } from "@yielded/agent/settlement-publisher";
import {
  ApprovalDecisionIntent,
  AbortIntent,
  SubmissionSnapshot,
  SubmissionState,
  UnknownResolutionIntent,
  UnknownResolutionKind,
  unknownResolutionKind,
  Settlement,
  settlementFailureFromRecord,
  submissionInputRecordId,
  submissionSettlementRecordId,
  submissionSettlementId,
  workerTerminalFromRecord,
} from "@yielded/agent/submission-ledger";
import { PreparedInput } from "@yielded/agent/subscription";
import {
  ThreadImport,
  ThreadImportRejected,
  makeThreadImportProgress,
  captureImportSource,
  prepareImportPage,
  finishThreadImport,
  rebuildClosedSubmission,
  validateClosedRun,
  verifyImportedReferences,
  verifyImportedInputOrder,
  validateImportedRunExecution,
  invalidThreadArchive,
  type ThreadArchive,
  ThreadImportReader,
  type ThreadImportResult,
  type PreparedImportPage,
} from "@yielded/agent/thread-import";
import { verifyThreadInvariants } from "@yielded/agent/thread-invariants";
import type { ThreadExportRequest, ThreadCommands } from "@yielded/agent/thread-store";
import {
  ThreadAdmission,
  ThreadReadRequest,
  ThreadExportBatch,
  ThreadExportRecord,
  ThreadNotMaterialized,
  ThreadExportRejected,
  ThreadStoreError,
  ThreadCheckpoint,
  ExecutionCheckpoint,
  streamExport,
  ThreadExportSource,
  canonicalBatchFitsTransfer,
} from "@yielded/agent/thread-store";
import {
  exportThreadPage,
  ThreadExporterReader,
  checkExportSource,
  sameTransferManifest,
  type TransferManifest,
  type TransferSection,
  type TransferFacts,
} from "@yielded/agent/thread-transfer";
import { WORK_INDEX_VERSION } from "@yielded/agent/thread-work";
import { AssignmentTerminal } from "@yielded/agent/worker";
import { Clock, Crypto, DateTime, Effect, Schema, Stream, Struct, Option } from "effect";
import { SqlClient } from "effect/sql/SqlClient";
import type { Fragment } from "effect/sql/Statement";

import {
  decodeAdmissionFact,
  decodeAbortFact,
  decodeApprovalFact,
  decodeResolutionFact,
} from "./SqlAdmissionFacts.ts";
import {
  captureRecordStorage,
  definitionJson,
  makeCanonicalStorage,
  recordBatchId,
  runIdentity,
  runKey,
  storedBatchHeader,
} from "./SqlCanonicalStorage.ts";
import { BatchRow, RecordRow, ThreadRow } from "./SqlJournal.ts";
import { messageDeliveryMetadata } from "./SqlMessageDeliveryStore.ts";
import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";
import {
  canonicalBatchJson,
  canonicalBatchBytes,
  canonicalBatchHeaderJson,
  canonicalRecordJson,
  makeSqlThreadArchiveRange,
} from "./SqlThreadArchiveRange.ts";
import {
  canonicalRecordMetadata,
  makeSelectedReads,
  SelectedReadOwner,
} from "./SqlThreadNativeReads.ts";
import { makeSqlThreadWork } from "./SqlThreadWork.ts";

export interface SqlThreadImportOptions<E extends { readonly message: string }> {
  readonly namespace?: string;
  readonly owner: StorageOwner["Service"];
  readonly offsetPrefix: string;
  readonly maxValueBytes?: number;
  readonly deliveryLimits?: MessageDeliveryStoreLimits;
  readonly afterThreadRead?: Effect.Effect<void, E>;
  readonly afterPage?: (page: PreparedImportPage) => Effect.Effect<void, E | ThreadStoreError>;
  readonly afterImport?: (summary: {
    readonly result: ThreadImportResult;
  }) => Effect.Effect<void, E | ThreadStoreError>;
  readonly read: <A, E2, R>(
    body: Effect.Effect<A, E2, R>,
  ) => Effect.Effect<A, E2 | ThreadStoreError, R>;
  readonly write: <A, E2, R>(
    body: Effect.Effect<A, E2, R>,
  ) => Effect.Effect<A, E2 | ThreadStoreError, R>;
}

const failure = (operation: string, cause: unknown) =>
  ThreadStoreError.make({
    operation,
    message: typeof cause === "string" ? cause : `Unable to ${operation}`,
    cause,
  });

// Share archive row schemas after the first import, export, or verification operation.
const makeImportSchemas = () => {
  const Header = Schema.Struct({
    ...Struct.pick(ThreadRow.fields, ["thread_id", "tail_sequence"]),
  });

  const Row = Schema.Struct({
    ...Struct.pick(RecordRow.fields, ["batch_id", "sequence", "record_json", "record_id"]),
    thread_id: ThreadId,
  });

  const WireBatch = Schema.Struct({
    ...ThreadExportBatch.fields,
    records: Schema.NonEmptyArray(RecordJson).check(Schema.isMaxLength(256)),
  });

  const ExecutionRow = Schema.Struct({
    thread_id: ThreadId,
    run_id: Schema.fromJsonString(RunId),
    input_sequence: SqlInteger.pipe(
      Schema.decodeTo(CanonicalSequence.check(Schema.isGreaterThan(0))),
    ),
    revision: SqlInteger.check(Schema.isGreaterThan(0)),
    checkpoint_json: Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024)),
  });

  const decodeStoredIdentifiers = Schema.decodeEffect(Schema.Array(ThreadRow.fields.thread_id));

  const AdmissionRow = Schema.Struct({
    submission_id: Schema.String,
    thread_id: Schema.String,
    receipt_id: Schema.String,
    queue_sequence: SqlInteger,
    principal: Schema.String,
    idempotency_key: Schema.String,
    agent_id: Schema.String,
    agent_digests_json: Schema.String,
    deployment_id: Schema.String,
    input_json: Schema.String,
    input_digest: Schema.String,
    created_at: Schema.String,
    finalized_at: Schema.NullOr(Schema.String),
    parent_submission_id: Schema.NullOr(Schema.String),
    parent_tool_call_id: Schema.NullOr(Schema.String),
    worker_admission_json: Schema.NullOr(Schema.String),
    message_admission_json: Schema.NullOr(Schema.String),
    admission_group: Schema.NullOr(Schema.String),
    admission_fence_json: Schema.NullOr(Schema.String),
  });

  const AbortRow = Schema.Struct({
    queue_sequence: SqlInteger,
    submission_id: Schema.String,
    author: Schema.String,
    reason: Schema.String,
    requested_at: Schema.String,
  });

  const ApprovalRow = Schema.Struct({
    queue_sequence: SqlInteger,
    submission_id: Schema.String,
    tool_call_id: Schema.String,
    decision: Schema.String,
    resolver: Schema.String,
    reason: Schema.String,
    decided_at: Schema.String,
  });

  const ResolutionRow = Schema.Struct({
    queue_sequence: SqlInteger,
    submission_id: Schema.String,
    tool_call_id: Schema.String,
    resolution_kind: UnknownResolutionKind,
    author: Schema.String,
    reason: Schema.String,
    resolution_json: Schema.String,
    resolved_at: Schema.String,
  });

  return {
    Header,
    Row,
    WireBatch,
    ExecutionRow,
    decodeStoredIdentifiers,
    AdmissionRow,
    AbortRow,
    ApprovalRow,
    ResolutionRow,
  };
};

let importSchemas: ReturnType<typeof makeImportSchemas> | undefined;
const getImportSchemas = () => (importSchemas ??= makeImportSchemas());

const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((cause) => failure("decode archive storage", cause)),
  );

const json = <A, I>(schema: Schema.Codec<A, I>, value: string) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError((cause) => failure("decode archive JSON", cause)),
  );

const encode = <A, I>(schema: Schema.Codec<A, I>, value: A) =>
  Schema.encodeEffect(schema)(value).pipe(
    Effect.mapError((cause) => failure("encode imported facts", cause)),
  );

/** A page read transaction and one offline atomic import transaction; no visible prefix writes. */
export const makeSqlThreadImport = Effect.fnUntraced(function* <
  E extends { readonly message: string },
>(options: SqlThreadImportOptions<E>) {
  const sql = yield* SqlClient;
  const crypto = yield* Crypto.Crypto;
  const deliveryLimits = options.deliveryLimits ?? defaultMessageDeliveryStoreLimits;
  const { table, execute } = yield* makeSqlQuery(options.namespace);
  const physical = yield* makeCanonicalStorage(options.namespace);

  const owner = options.owner;

  const seekIndex = (name: string) =>
    sql.onDialectOrElse({
      pg: () => sql.literal(""),
      orElse: () => sql`INDEXED BY ${sql(name)}`,
    });

  const work = yield* makeSqlThreadWork(
    options.namespace === undefined ? {} : { namespace: options.namespace },
  );

  // Import and verification already own the snapshot used by physical range checks.
  const ranges = yield* makeSqlThreadArchiveRange(
    { read: (body) => body, write: options.write },
    options.namespace,
  );

  const query = <A extends object>(statement: ReturnType<typeof sql<A>>) =>
    execute(statement).pipe(
      Effect.mapError((cause) => failure("read or write transfer storage", cause)),
    );

  const recordJson = canonicalRecordJson(sql, options.namespace, "r");
  const batchJson = canonicalBatchJson(sql, options.namespace, "b");

  const rows = (threadId: ThreadId, predicate: Fragment, limit: number) =>
    query(
      sql`SELECT r.thread_id, ${recordBatchId(sql, options.namespace, "r")} AS batch_id, r.sequence, r.record_id, ${recordJson} AS record_json
      FROM ${table("effect_agent_canonical_records")} r WHERE r.thread_id=${threadId} AND ${predicate}
      ORDER BY r.sequence LIMIT ${limit}`,
    ).pipe(Effect.flatMap((value) => decode(Schema.Array(getImportSchemas().Row), value)));

  const envelope = Effect.fnUntraced(function* (
    row: ReturnType<typeof makeImportSchemas>["Row"]["Type"],
  ) {
    const record = yield* decodeExportRecordJson(row.record_json).pipe(
      Effect.mapError((e) => failure("decode transfer record", e)),
    );

    if (record.recordId !== row.record_id)
      return yield* failure("record identity", "Indexed record identity differs from its wire");

    return CanonicalRecordEnvelope.make({
      threadId: row.thread_id,
      batchId: yield* decode(ThreadExportBatch.fields.batchId, row.batch_id),
      sequence: row.sequence,
      offset: yield* decode(
        ObservationOffset,
        `${options.offsetPrefix}${encodeURIComponent(row.thread_id)}:${row.sequence}`,
      ),
      record,
    });
  });

  const snapshotReads = yield* makeSelectedReads(envelope, options.namespace).pipe(
    Effect.provideService(SelectedReadOwner, {
      snapshot: (body) => body,
      tail: (threadId) =>
        query(
          sql`SELECT thread_id, tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId} AND created_at IS NOT NULL`,
        ).pipe(
          Effect.flatMap((value) => decode(Schema.Array(getImportSchemas().Header), value)),
          Effect.map((headers) => headers[0]),
        ),
    }),
  );

  const admissionRows = (threadId: ThreadId, predicate: Fragment, limit: number) =>
    query(
      sql`SELECT s.*, ${definitionJson(sql, options.namespace, "s")} AS agent_digests_json FROM ${table("effect_agent_submissions")} s WHERE s.thread_id=${threadId} AND ${predicate}
      ORDER BY s.queue_sequence LIMIT ${limit}`,
    ).pipe(Effect.flatMap((value) => decode(Schema.Array(getImportSchemas().AdmissionRow), value)));

  const admission = Effect.fnUntraced(function* (
    row: ReturnType<typeof makeImportSchemas>["AdmissionRow"]["Type"],
  ) {
    const fact = yield* decodeAdmissionFact(row).pipe(
      Effect.mapError((e) => failure("decode transfer admission", e)),
    );

    return ThreadAdmission.make({
      ...fact,
      ...(row.finalized_at === null
        ? {}
        : { settledAt: yield* decode(Schema.DateTimeUtcFromString, row.finalized_at) }),
    });
  });

  const executions = Effect.fnUntraced(function* (threadId: ThreadId, predicate: Fragment) {
    const selected =
      yield* query(sql`SELECT e.thread_id, e.run_id, e.input_sequence, e.revision, e.checkpoint_json
      FROM ${table("effect_agent_executions")} e WHERE e.thread_id=${threadId} AND ${predicate}
      ORDER BY e.input_sequence LIMIT 1`).pipe(
        Effect.flatMap((value) =>
          decode(Schema.Array(getImportSchemas().ExecutionRow).check(Schema.isMaxLength(1)), value),
        ),
      );

    return yield* Effect.forEach(selected, (row) =>
      Effect.gen(function* () {
        const checkpoint = yield* json(ExecutionCheckpoint, row.checkpoint_json);

        if (
          row.thread_id !== threadId ||
          checkpoint.progress.runId !== row.run_id ||
          checkpoint.progress.revision !== row.revision ||
          checkpoint.originalSequence !== row.input_sequence
        )
          return yield* failure(
            "read transfer execution",
            "Active execution differs from its stored identity or revision",
          );

        return checkpoint;
      }),
    );
  });

  const commandFacts = Effect.fnUntraced(function* (
    threadId: ThreadId,
    section: "aborts" | "approvals" | "resolutions",
    predicate: Fragment,
    limit: number,
  ) {
    const factTable =
      section === "aborts"
        ? "effect_agent_abort_intents"
        : section === "approvals"
          ? "effect_agent_approval_decisions"
          : "effect_agent_unknown_resolutions";

    const selected = sql`SELECT * FROM (
      SELECT s.queue_sequence, fact.*
        ${section === "aborts" ? sql`, '' AS tool_call_id` : sql``}
        ${section !== "resolutions" ? sql`, '' AS resolution_kind` : sql``}
      FROM ${table("effect_agent_submissions")} s JOIN ${table(factTable)} fact ON fact.submission_id=s.submission_id
      WHERE s.thread_id=${threadId}
    ) a WHERE ${predicate} ORDER BY a.queue_sequence, a.tool_call_id, a.resolution_kind LIMIT ${limit}`;

    if (section === "aborts") {
      const rows = yield* query(selected).pipe(
        Effect.flatMap((r) => decode(Schema.Array(getImportSchemas().AbortRow), r)),
      );

      return {
        commands: {
          aborts: yield* Effect.forEach(rows, (r) =>
            decodeAbortFact(r).pipe(Effect.mapError((e) => failure("decode transfer abort", e))),
          ),
          approvals: [],
          resolutions: [],
        },
        after:
          rows.at(-1) === undefined
            ? undefined
            : JSON.stringify([rows.at(-1)!.queue_sequence, "", ""]),
      };
    }
    if (section === "approvals") {
      const rows = yield* query(selected).pipe(
        Effect.flatMap((r) => decode(Schema.Array(getImportSchemas().ApprovalRow), r)),
      );

      return {
        commands: {
          aborts: [],
          approvals: yield* Effect.forEach(rows, (r) =>
            decodeApprovalFact(r).pipe(
              Effect.mapError((e) => failure("decode transfer approval", e)),
            ),
          ),
          resolutions: [],
        },
        after:
          rows.at(-1) === undefined
            ? undefined
            : JSON.stringify([rows.at(-1)!.queue_sequence, rows.at(-1)!.tool_call_id, ""]),
      };
    }

    const rows = yield* query(selected).pipe(
      Effect.flatMap((r) => decode(Schema.Array(getImportSchemas().ResolutionRow), r)),
    );

    return {
      commands: {
        aborts: [],
        approvals: [],
        resolutions: yield* Effect.forEach(rows, (r) =>
          decodeResolutionFact(r).pipe(
            Effect.mapError((e) => failure("decode transfer resolution", e)),
          ),
        ),
      },
      after:
        rows.at(-1) === undefined
          ? undefined
          : JSON.stringify([
              rows.at(-1)!.queue_sequence,
              rows.at(-1)!.tool_call_id,
              rows.at(-1)!.resolution_kind,
            ]),
    };
  });

  const deliveries = (threadId: ThreadId, after?: string) =>
    query(sql`SELECT record_json FROM ${table("effect_agent_message_deliveries")}
    WHERE owner_thread_id=${threadId} ${after === undefined ? sql`` : sql`AND message_id>${after}`} ORDER BY message_id LIMIT 1`).pipe(
      Effect.flatMap((r) => decode(Schema.Array(Schema.Struct({ record_json: Schema.String })), r)),
      Effect.flatMap((r) => Effect.forEach(r, (r) => json(MessageDeliveryRecord, r.record_json))),
    );

  const countFacts = (threadId: ThreadId) =>
    query(sql`SELECT
    (SELECT COUNT(*) FROM ${table("effect_agent_submissions")} WHERE thread_id=${threadId}) AS admissions,
    (SELECT COUNT(*) FROM ${table("effect_agent_abort_intents")} c JOIN ${table("effect_agent_submissions")} s ON s.submission_id=c.submission_id WHERE s.thread_id=${threadId}) AS aborts,
    (SELECT COUNT(*) FROM ${table("effect_agent_approval_decisions")} c JOIN ${table("effect_agent_submissions")} s ON s.submission_id=c.submission_id WHERE s.thread_id=${threadId}) AS approvals,
    (SELECT COUNT(*) FROM ${table("effect_agent_unknown_resolutions")} c JOIN ${table("effect_agent_submissions")} s ON s.submission_id=c.submission_id WHERE s.thread_id=${threadId}) AS resolutions,
    (SELECT COUNT(*) FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${threadId}) AS deliveries`).pipe(
      Effect.flatMap((r) =>
        decode(
          Schema.Tuple([
            Schema.Struct({
              admissions: SqlInteger,
              aborts: SqlInteger,
              approvals: SqlInteger,
              resolutions: SqlInteger,
              deliveries: SqlInteger,
            }),
          ]),
          r,
        ),
      ),
      Effect.map((r) => r[0]),
    );

  const checkIdle = Effect.fnUntraced(function* (threadId: ThreadId) {
    if (owner.hasOwner(threadId)) return false;

    const [header] =
      yield* query(sql`SELECT work_version, work_state, work_through_sequence, work_entry_count, tail_sequence
      FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`).pipe(
        Effect.flatMap((r) =>
          decode(
            Schema.Array(
              Schema.Struct({
                work_version: SqlInteger,
                work_state: Schema.String,
                work_through_sequence: SqlInteger,
                work_entry_count: SqlInteger,
                tail_sequence: SqlInteger,
              }),
            ),
            r,
          ),
        ),
      );

    if (
      header === undefined ||
      header.work_version !== WORK_INDEX_VERSION ||
      header.work_state !== "ready" ||
      header.work_through_sequence !== header.tail_sequence
    )
      return yield* failure("check idle work", "Missing or incomplete native work index");

    const occupied = yield* query(sql`SELECT 1 AS occupied WHERE
      EXISTS (SELECT 1 FROM ${table("effect_agent_work_entries")} WHERE thread_id=${threadId}) OR
      EXISTS (SELECT 1 FROM ${table("effect_agent_executions")} WHERE thread_id=${threadId}) OR
      EXISTS (SELECT 1 FROM ${table("effect_agent_submissions")} WHERE thread_id=${threadId} AND state<>'settled') OR
      EXISTS (SELECT 1 FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${threadId} AND state NOT IN ('processed','refused')) OR
      EXISTS (SELECT 1 FROM ${table("effect_agent_live_child_reservations")} WHERE thread_id=${threadId})`);

    return header.work_entry_count === 0 && occupied.length === 0;
  });

  const makeExportReader = (idle: boolean) =>
    ThreadExporterReader.of({
      snapshot: Effect.fnUntraced(function* (threadId: ThreadId, captured?: TransferManifest) {
        // A delivery owner can retain closed receipts before its conversation is materialized.
        const headers = yield* query(
          sql`SELECT thread_id, tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
        ).pipe(Effect.flatMap((r) => decode(Schema.Array(getImportSchemas().Header), r)));

        const header = headers[0];

        if (header === undefined) return yield* ThreadNotMaterialized.make({ threadId });
        if (idle && !(yield* checkIdle(threadId)))
          return yield* ThreadExportRejected.make({ threadId, reason: "non-idle" });
        if (idle && captured === undefined)
          yield* validateIdleHistory(threadId, header.tail_sequence);
        const state = captured?.snapshot ?? (yield* countFacts(threadId));

        if (options.afterThreadRead !== undefined)
          yield* options.afterThreadRead.pipe(
            Effect.mapError((e) => failure("capture transfer snapshot", e)),
          );
        // The persisted 256-record batch bound makes the first-sequence index sufficient
        // to detect every batch extending beyond this tail, including overlapping batches.
        if (
          (yield* query(
            sql`SELECT sequence FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${threadId} AND sequence>${header.tail_sequence} LIMIT 1`,
          )).length > 0 ||
          (yield* query(
            sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} ${seekIndex("effect_agent_canonical_batches_sequence")}
          WHERE thread_id=${threadId} AND first_sequence<1 LIMIT 1`,
          )).length > 0 ||
          (yield* query(
            sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} ${seekIndex("effect_agent_canonical_batches_sequence")}
          WHERE thread_id=${threadId} AND first_sequence>${header.tail_sequence - 255}
            AND last_sequence>${header.tail_sequence} LIMIT 1`,
          )).length > 0
        )
          return yield* failure(
            "capture canonical tail",
            "Canonical rows extend beyond the captured thread tail",
          );

        const seals = yield* query(
          sql`SELECT terminal FROM ${table("effect_agent_worker_stops")} WHERE thread_id=${threadId} LIMIT 2`,
        ).pipe(
          Effect.flatMap((r) =>
            decode(Schema.Array(Schema.Struct({ terminal: Schema.NullOr(AssignmentTerminal) })), r),
          ),
        );

        if (seals.length > 1)
          return yield* failure("capture worker seal", "Ambiguous native worker seal");

        return {
          format: CURRENT_RECORD_FORMAT,
          threadId,
          tailSequence: header.tail_sequence,
          snapshot: state,
          ...(seals[0] === undefined
            ? {}
            : { workerSeal: seals[0].terminal === null ? {} : { terminal: seals[0].terminal } }),
        };
      }),
      batch: Effect.fnUntraced(function* (threadId: ThreadId, fromSequence: number) {
        // Inspect original string sizes before reconstructing or hydrating canonical batches.
        const batchLength = canonicalBatchBytes(sql, options.namespace, "b");

        const headerLength = sql.onDialectOrElse({
          pg: () => sql`octet_length(${storedBatchHeader(sql, "b")})`,
          orElse: () => sql`length(CAST(${storedBatchHeader(sql, "b")} AS BLOB))`,
        });

        const recordLength = sql.onDialectOrElse({
          pg: () => sql`octet_length(${recordJson})`,
          orElse: () => sql`length(CAST(${recordJson} AS BLOB))`,
        });

        const sizes =
          yield* query(sql`SELECT b.last_sequence, ${batchLength} AS batch_bytes, ${headerLength} AS header_bytes,
        (SELECT SUM(${recordLength}) FROM ${table("effect_agent_canonical_records")} r WHERE r.thread_id=b.thread_id AND r.sequence BETWEEN b.first_sequence AND b.last_sequence) AS record_bytes
        FROM ${table("effect_agent_canonical_batches")} b WHERE b.thread_id=${threadId} AND b.first_sequence=${fromSequence} LIMIT 2`).pipe(
            Effect.flatMap((r) =>
              decode(
                Schema.Array(
                  Schema.Struct({
                    last_sequence: SqlInteger,
                    batch_bytes: Schema.NullOr(SqlInteger),
                    header_bytes: SqlInteger,
                    record_bytes: Schema.NullOr(SqlInteger),
                  }),
                ),
                r,
              ),
            ),
          );

        const size = sizes[0];

        if (
          sizes.length !== 1 ||
          size === undefined ||
          size.batch_bytes === null ||
          size.record_bytes === null ||
          size.batch_bytes > 16 * 1024 * 1024 ||
          size.header_bytes > 16 * 1024 * 1024 ||
          size.record_bytes > 16 * 1024 * 1024 ||
          size.last_sequence - fromSequence >= 256
        )
          return yield* failure(
            "export batch",
            "Missing or oversized canonical wire before hydration",
          );

        const stored =
          yield* query(sql`SELECT b.batch_id, b.first_sequence, b.last_sequence, ${storedBatchHeader(sql, "b")} AS batch_header_json, ${batchJson} AS batch_json
        FROM ${table("effect_agent_canonical_batches")} b WHERE b.thread_id=${threadId} AND b.first_sequence=${fromSequence} LIMIT 2`).pipe(
            Effect.flatMap((r) =>
              decode(
                Schema.Array(
                  Schema.Struct({
                    ...Struct.omit(BatchRow.fields, ["thread_id"]),
                    batch_header_json: Schema.String,
                  }),
                ),
                r,
              ),
            ),
          );

        const row = stored[0];

        if (
          stored.length !== 1 ||
          row === undefined ||
          row.last_sequence - row.first_sequence >= 256
        )
          return yield* failure("export batch", "Missing or oversized canonical batch");
        if (
          (yield* query(sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} ${seekIndex("effect_agent_canonical_batches_sequence")}
        WHERE thread_id=${threadId} AND batch_id<>${JSON.stringify(row.batch_id)}
        AND first_sequence BETWEEN ${Math.max(1, row.first_sequence - 255)} AND ${row.last_sequence}
        AND last_sequence>=${row.first_sequence} LIMIT 1`)).length > 0
        )
          return yield* failure("export batch", "Overlapping canonical batch ranges");
        const batch = yield* json(getImportSchemas().WireBatch, row.batch_json);

        const selected = yield* rows(
          threadId,
          sql`r.sequence>=${row.first_sequence} AND r.sequence<=${row.last_sequence}`,
          257,
        );

        if (
          batch.batchId !== row.batch_id ||
          canonicalBatchHeaderJson(batch) !== row.batch_header_json ||
          batch.records.length !== selected.length ||
          selected.length !== row.last_sequence - row.first_sequence + 1
        )
          return yield* failure("export batch", "Batch identity or count differs from its index");
        const records: Array<typeof ThreadExportRecord.Type> = [];

        for (const [index, item] of selected.entries()) {
          if (item.batch_id !== batch.batchId || item.sequence !== fromSequence + index)
            return yield* failure(
              "export batch",
              "Canonical batch differs from its indexed records",
            );
          const entry = yield* envelope(item);

          if (!(entry.record instanceof ExportedRecord))
            return yield* failure("export batch", "Canonical wire is missing");
          if (canonicalJson(entry.record.wire) !== canonicalJson(batch.records[index]!))
            return yield* failure(
              "export batch",
              "Canonical batch differs from its indexed records",
            );

          // PersistentHistory retains RunCompleted as its canonical terminal fact.
          if (
            (isOperationalRecord(entry.record) && entry.record.payload._tag !== "RunCompleted") ||
            entry.record.payload._tag === "RunContinuation"
          )
            return yield* failure("export batch", "Execution state is not canonical history");
          records.push(ThreadExportRecord.make({ ...entry, record: entry.record }));
        }

        return {
          records,
          batches: [
            ThreadExportBatch.make({ batchId: batch.batchId, producerId: batch.producerId }),
          ],
        };
      }),
      facts: Effect.fnUntraced(function* (
        threadId: ThreadId,
        section: TransferSection,
        after: string | undefined,
      ) {
        const facts: TransferFacts = {
          admissions: [],
          commands: { aborts: [], approvals: [], resolutions: [] },
          deliveries: [],
        };

        if (section === "admissions") {
          const queue =
            after === undefined
              ? -1
              : yield* decode(Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.Natural)), after);

          const selected = yield* admissionRows(threadId, sql`s.queue_sequence>${queue}`, 1);
          const item = selected[0];

          return {
            facts: { ...facts, admissions: yield* Effect.forEach(selected, admission) },
            after: String(item?.queue_sequence ?? queue),
          };
        }
        if (section === "deliveries") {
          const selected = yield* deliveries(threadId, after);

          return {
            facts: {
              ...facts,
              deliveries: selected.map((r) =>
                MessageDeliveryRecord.make({ ...r, leaseUntilMillis: null }),
              ),
            },
            after: selected[0]?.key.messageId ?? after ?? "",
          };
        }

        const position =
          after === undefined
            ? undefined
            : yield* json(Schema.Tuple([Schema.Natural, Schema.String, Schema.String]), after);

        const page = yield* commandFacts(
          threadId,
          section,
          position === undefined
            ? sql`1=1`
            : sql`(a.queue_sequence,a.tool_call_id,a.resolution_kind)>(${position[0]},${position[1]},${position[2]})`,
          1,
        );

        return {
          facts: { ...facts, commands: page.commands },
          after: page.after ?? after ?? "",
        };
      }),
    });

  const verificationReader = makeExportReader(false);
  const idleReader = makeExportReader(true);

  const readPage = (request: ThreadExportRequest, reader: ThreadExporterReader["Service"]) =>
    exportThreadPage(request).pipe(Effect.provideService(ThreadExporterReader, reader));

  const verificationPage = (request: ThreadExportRequest) =>
    readPage(request, verificationReader).pipe(
      Effect.catchTag("ThreadExportRejected", (cause) => failure("verify captured Thread", cause)),
    );

  const exportThread = (request: ThreadExportRequest) =>
    checkExportSource(request.threadId).pipe(
      Effect.andThen(options.read(readPage(request, idleReader))),
    );

  const readerLayer = (
    id: ThreadId,
    tailSequence: CanonicalSequence,
  ): ReturnType<typeof ThreadImportReader.layer> => {
    const readRecord = (recordId: RecordId) =>
      rows(
        id,
        sql`r.record_id=${JSON.stringify(recordId)} AND r.sequence<=${tailSequence}`,
        1,
      ).pipe(
        Effect.flatMap((r) => (r[0] === undefined ? Effect.succeed(undefined) : envelope(r[0]))),
      );

    const readAdmission = (sid: SubmissionId) =>
      admissionRows(id, sql`s.submission_id=${sid}`, 1).pipe(
        Effect.flatMap((r) => (r[0] === undefined ? Effect.succeed(undefined) : admission(r[0]))),
      );

    const maxRunRecords =
      2 * (MAX_RUN_EVIDENCE_RECORDS + RUN_TERMINAL_RESERVE_RECORDS) +
      MAX_RUN_RECOVERY_SUFFIX_RECORDS;

    return ThreadImportReader.layer({
      execution: (runId) =>
        executions(id, sql`e.run_id=${JSON.stringify(runId)}`).pipe(
          Effect.map((selected) => selected[0]),
        ),
      read: (input) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const request = yield* decode(Schema.toType(ThreadReadRequest), input);

            if (request.threadId !== id)
              return yield* failure("read snapshot", "Thread differs from the captured snapshot");
            if ("selection" in request) {
              const selection = request.selection;

              if (
                ("throughSequence" in selection && selection.throughSequence > tailSequence) ||
                ("expectedTailSequence" in selection &&
                  selection.expectedTailSequence !== tailSequence)
              )
                return yield* failure("read snapshot", "Selection escaped the captured tail");

              return Stream.fromIterable(
                yield* snapshotReads
                  .read(request)
                  .pipe(
                    Effect.catchTag("ThreadNotMaterialized", (cause) =>
                      failure("read snapshot", cause),
                    ),
                  ),
              );
            }

            return Stream.fromIterable(
              yield* Effect.forEach(
                yield* rows(
                  id,
                  sql`r.sequence>${request.afterSequence ?? 0} AND r.sequence<=${tailSequence}`,
                  request.limit,
                ),
                envelope,
              ),
            );
          }),
        ),
      delivery: (messageId) =>
        query(
          sql`SELECT record_json FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${id} AND message_id=${messageId} LIMIT 1`,
        ).pipe(
          Effect.flatMap((r) =>
            decode(Schema.Array(Schema.Struct({ record_json: Schema.String })), r),
          ),
          Effect.flatMap((r) =>
            r[0] === undefined
              ? Effect.succeed(undefined)
              : json(MessageDeliveryRecord, r[0].record_json),
          ),
        ),
      record: readRecord,
      admission: readAdmission,
      runOwner: (runId) => {
        const sid = runId.startsWith("run:") ? runId.slice(4) : "";

        return decode(SubmissionId, sid).pipe(Effect.flatMap(readAdmission));
      },
      runRecords: Effect.fnUntraced(function* (runId: RunId) {
        const entries: Array<CanonicalRecordEnvelope> = [];

        let after = 0,
          facts = 0,
          bytes = 0;

        while (true) {
          const selected =
            yield* query(sql`SELECT r.thread_id, ${recordBatchId(sql, options.namespace, "r")} AS batch_id, r.sequence, r.record_id, ${recordJson} AS record_json
            FROM ${table("effect_agent_record_runs")} rr
            LEFT JOIN ${table("effect_agent_canonical_records")} r ON r.thread_id=rr.thread_id AND r.sequence=rr.sequence
            WHERE rr.thread_id=${id} AND rr.run_id=${runKey(sql, sql`${id}`, sql`${JSON.stringify(runId)}`, options.namespace)} AND rr.sequence>${after} AND rr.sequence<=${tailSequence}
            ORDER BY rr.sequence LIMIT 1`).pipe(
              Effect.flatMap((value) => decode(Schema.Array(getImportSchemas().Row), value)),
            );

          const row = selected[0];

          if (row === undefined) return entries;
          const entry = yield* envelope(row);
          const size = utf8ByteLength(row.record_json);

          facts++;
          bytes += size;
          if (
            facts >
              MAX_RUN_EVIDENCE_RECORDS +
                RUN_TERMINAL_RESERVE_RECORDS +
                MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
            bytes >
              MAX_RUN_EVIDENCE_BYTES + MAX_RUN_TERMINAL_BYTES + MAX_RUN_RECOVERY_SUFFIX_BYTES ||
            entries.length >= maxRunRecords
          )
            return yield* failure("read bounded Run", "Run evidence exceeds its bound");
          entries.push(entry);
          after = row.sequence;
        }
      }),
      commands: Effect.fnUntraced(function* (sid: SubmissionId) {
        const result: typeof ThreadCommands.Type = { aborts: [], approvals: [], resolutions: [] };
        const aborts = yield* commandFacts(id, "aborts", sql`a.submission_id=${sid}`, 2);

        if (aborts.commands.aborts.length > 1)
          return yield* failure("read staged commands", "Duplicate accepted abort");
        let bytes = 0;
        const approvals: Array<ApprovalDecisionIntent> = [];
        const resolutions: Array<UnknownResolutionIntent> = [];

        for (const command of aborts.commands.aborts)
          bytes += utf8ByteLength(canonicalJson(yield* encode(AbortIntent, command)));
        for (const section of ["approvals", "resolutions"] as const) {
          let after: readonly [string, string] | undefined;

          while (true) {
            const page = yield* commandFacts(
              id,
              section,
              sql`a.submission_id=${sid} ${after === undefined ? sql`` : sql`AND (a.tool_call_id, a.resolution_kind)>(${after[0]}, ${after[1]})`}`,
              1,
            );

            if (section === "approvals") {
              const command = page.commands.approvals[0];

              if (command === undefined) break;
              bytes += utf8ByteLength(
                canonicalJson(yield* encode(ApprovalDecisionIntent, command)),
              );
              if (bytes > MAX_RUN_EVIDENCE_BYTES || approvals.length >= MAX_RUN_EVIDENCE_RECORDS)
                return yield* failure(
                  "read staged commands",
                  "Accepted command evidence exceeds Run bounds",
                );
              approvals.push(command);
              after = [command.toolCallId, ""];
            } else {
              const command = page.commands.resolutions[0];

              if (command === undefined) break;
              bytes += utf8ByteLength(
                canonicalJson(yield* encode(UnknownResolutionIntent, command)),
              );
              if (bytes > MAX_RUN_EVIDENCE_BYTES || resolutions.length >= MAX_RUN_EVIDENCE_RECORDS)
                return yield* failure(
                  "read staged commands",
                  "Accepted command evidence exceeds Run bounds",
                );
              resolutions.push(command);
              after = [command.toolCallId, unknownResolutionKind(command.resolution)];
            }
          }
        }

        return { ...result, aborts: aborts.commands.aborts, approvals, resolutions };
      }),
      hasAgent: (agentId) =>
        query(sql`SELECT record_id FROM ${table("effect_agent_canonical_records")} r WHERE r.thread_id=${id} AND r.record_tag=23
        AND ${sql.onDialectOrElse({ pg: () => sql`(${recordJson})::jsonb -> 'payload' ->> 'agentId'`, orElse: () => sql`json_extract(${recordJson}, '$.payload.agentId')` })}=${agentId} LIMIT 1`).pipe(
          Effect.map((r) => r.length === 1),
        ),
    });
  };

  const validateIdleHistory = (threadId: ThreadId, tailSequence: CanonicalSequence) =>
    Effect.gen(function* () {
      let after = 0;

      while (true) {
        const [next] = yield* query(sql`SELECT run_id FROM ${table("effect_agent_record_runs")}
          WHERE thread_id=${threadId} AND run_id>${after} GROUP BY run_id ORDER BY run_id LIMIT 1`).pipe(
          Effect.flatMap((r) => decode(Schema.Array(Schema.Struct({ run_id: SqlInteger })), r)),
        );

        if (next === undefined) break;

        const [identity] = yield* query(
          sql`SELECT run_id FROM ${table("effect_agent_run_keys")} WHERE thread_id=${threadId} AND run_key=${next.run_id}`,
        ).pipe(
          Effect.flatMap((r) =>
            decode(Schema.Tuple([Schema.Struct({ run_id: Schema.fromJsonString(RunId) })]), r),
          ),
        );

        yield* validateClosedRun(threadId, identity.run_id).pipe(
          Effect.mapError((e) =>
            Schema.is(ThreadImportRejected)(e)
              ? ThreadExportRejected.make({ threadId, reason: "non-idle" })
              : e,
          ),
        );
        after = next.run_id;
      }
      let queue = -1;

      while (true) {
        const [row] = yield* admissionRows(threadId, sql`s.queue_sequence>${queue}`, 1);

        if (row === undefined) break;
        yield* rebuildClosedSubmission(yield* admission(row)).pipe(
          Effect.mapError((e) =>
            Schema.is(ThreadImportRejected)(e)
              ? ThreadExportRejected.make({ threadId, reason: "non-idle" })
              : e,
          ),
        );
        queue = row.queue_sequence;
      }
    }).pipe(Effect.provide(readerLayer(threadId, tailSequence)));

  const importThread = <E2, R>(pages: Stream.Stream<ThreadArchive, E2, R>) =>
    captureImportSource(pages, (captured) =>
      options.write(
        Effect.gen(function* () {
          const progress = makeThreadImportProgress();
          let threadId: ThreadId | undefined;
          const maxBytes = options.maxValueBytes ?? 16 * 1024 * 1024;

          const reject = (reason: ThreadImportRejected["reason"], message: string) =>
            ThreadImportRejected.make({
              ...(threadId === undefined ? {} : { threadId }),
              reason,
              message,
            });

          const checkValues = (values: ReadonlyArray<string | undefined | null>) =>
            values.some((v) => v !== undefined && v !== null && utf8ByteLength(v) > maxBytes)
              ? Effect.fail(
                  reject(
                    "unsupported-capacity",
                    "Imported value exceeds the destination storage bound",
                  ),
                )
              : Effect.void;

          const checkIdentifiers = (values: ReadonlyArray<string | undefined>) =>
            getImportSchemas()
              .decodeStoredIdentifiers(values.filter((v) => v !== undefined))
              .pipe(
                Effect.mapError(() =>
                  reject(
                    "unsupported-capacity",
                    "Imported identifier exceeds the destination row schema",
                  ),
                ),
                Effect.andThen(checkValues(values)),
              );

          yield* Stream.runForEach(captured, (input) =>
            Effect.gen(function* () {
              const page = yield* prepareImportPage(progress, input);

              if (threadId === undefined) {
                threadId = page.archive.threadId;
                yield* checkIdentifiers([threadId]);

                const headers = yield* query(
                  sql`SELECT thread_id, tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
                ).pipe(Effect.flatMap((r) => decode(Schema.Array(getImportSchemas().Header), r)));

                for (const name of [
                  "effect_agent_submissions",
                  "effect_agent_canonical_records",
                  "effect_agent_record_runs",
                  "effect_agent_canonical_batches",
                  "effect_agent_executions",
                  "effect_agent_checkpoints",
                  "effect_agent_worker_stops",
                  "effect_agent_run_keys",
                  "effect_agent_work_entries",
                  "effect_agent_journal_ranges",
                  "effect_agent_import_delivery_visits",
                  "effect_agent_rollover_buckets",
                ])
                  if (
                    (yield* query(
                      sql`SELECT thread_id FROM ${table(name)} WHERE thread_id=${threadId} LIMIT 1`,
                    )).length > 0
                  )
                    return yield* reject(
                      "target-not-empty",
                      "Destination Thread contains retained facts or derivatives",
                    );
                if (
                  (yield* query(
                    sql`SELECT owner_thread_id FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${threadId} LIMIT 1`,
                  )).length > 0 ||
                  headers.some((h) => h.tail_sequence !== 0)
                )
                  return yield* reject("target-not-empty", "Destination Thread is not empty");

                const catalogue = yield* query(
                  sql`SELECT work_state AS state, work_through_sequence AS through_sequence, work_entry_count AS entry_count, work_reporting AS reporting FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
                ).pipe(
                  Effect.flatMap((r) =>
                    decode(
                      Schema.Array(
                        Schema.Struct({
                          state: Schema.String,
                          through_sequence: SqlInteger,
                          entry_count: SqlInteger,
                          reporting: SqlInteger,
                        }),
                      ),
                      r,
                    ),
                  ),
                );

                if (
                  catalogue.length > 1 ||
                  catalogue.some(
                    (c) =>
                      c.state !== "ready" ||
                      c.through_sequence !== 0 ||
                      c.entry_count !== 0 ||
                      c.reporting !== 0,
                  )
                )
                  return yield* reject(
                    "target-not-empty",
                    "Destination retains transfer facts or nonempty work state",
                  );

                const now = DateTime.formatIso(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));

                yield* query(
                  sql`INSERT INTO ${table("effect_agent_threads")} (thread_id, created_at, tail_sequence) VALUES (${threadId}, ${now}, 0)
                  ON CONFLICT(thread_id) DO UPDATE SET created_at=excluded.created_at WHERE ${table("effect_agent_threads")}.created_at IS NULL`,
                );
              }
              for (const batch of page.batches) {
                if (
                  batch.batch.records.some(
                    (record) =>
                      (isOperationalRecord(record) && record.payload._tag !== "RunCompleted") ||
                      record.payload._tag === "RunContinuation",
                  )
                )
                  return yield* invalidThreadArchive(
                    "Execution state is not canonical history",
                    threadId,
                  );
                if (
                  !canonicalBatchFitsTransfer(
                    threadId,
                    batch.batch,
                    utf8ByteLength(batch.batchJson),
                    utf8ByteLength(JSON.stringify(options.offsetPrefix)) +
                      3 * utf8ByteLength(threadId) +
                      25,
                  )
                )
                  return yield* ThreadImportRejected.make({
                    threadId,
                    reason: "unsupported-capacity",
                    message: "Canonical batch exceeds its bounded transfer representation",
                  });

                if (
                  (yield* query(
                    sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} WHERE thread_id=${threadId} AND batch_id=${JSON.stringify(batch.batch.batchId)} LIMIT 1`,
                  )).length > 0
                )
                  return yield* invalidThreadArchive(
                    "Duplicate canonical batch identity",
                    threadId,
                  );
                yield* checkIdentifiers([
                  batch.batch.batchId,
                  batch.batch.producerId,
                  ...batch.batch.records.map((r) => r.recordId),
                ]);
                yield* checkValues([batch.batchJson, ...batch.recordJson]);
                yield* query(sql`INSERT INTO ${table("effect_agent_canonical_batches")} (thread_id, batch_id, first_sequence, last_sequence, producer_id_json)
          VALUES (${threadId}, ${JSON.stringify(batch.batch.batchId)}, ${batch.firstSequence}, ${batch.lastSequence}, ${JSON.stringify(batch.batch.producerId)})`);
                const captured = [];

                for (const [index, record] of batch.batch.records.entries()) {
                  if (!(record instanceof ExportedRecord))
                    return yield* invalidThreadArchive(
                      "Imported record has no owned wire",
                      threadId,
                    );
                  if (
                    (yield* query(
                      sql`SELECT record_id FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${threadId} AND record_id=${JSON.stringify(record.recordId)} LIMIT 1`,
                    )).length > 0
                  )
                    return yield* invalidThreadArchive(
                      "Duplicate canonical record identity",
                      threadId,
                    );

                  const metadata = canonicalRecordMetadata({
                    canonical: record,
                    wire: record.wire,
                  });

                  const recordJson = batch.recordJson[index];

                  if (recordJson === undefined)
                    return yield* invalidThreadArchive(
                      "Imported record capture is missing",
                      threadId,
                    );
                  const storage = yield* captureRecordStorage(record.wire, recordJson);

                  const [stored] = yield* physical.records(threadId, batch.firstSequence + index, [
                    { storage, readMetadata: metadata },
                  ]);

                  if (stored === undefined)
                    return yield* invalidThreadArchive(
                      "Imported record capture is missing",
                      threadId,
                    );
                  yield* query(
                    sql`INSERT INTO ${table("effect_agent_canonical_records")} ${sql.insert(stored.columns)}`,
                  );
                  for (const member of stored.memberships)
                    yield* query(
                      sql`INSERT INTO ${table("effect_agent_record_runs")} ${sql.insert(member)}`,
                    );
                  captured.push({
                    storage,
                    recordId: record.recordId,
                    recordBytes: utf8ByteLength(recordJson),
                    canonical: record,
                    wire: record.wire,
                    recordJson,
                    readMetadata: metadata,
                  });
                }

                const workFields = yield* work.apply(
                  threadId,
                  batch.firstSequence,
                  batch.batch.records,
                );

                yield* query(
                  sql`UPDATE ${table("effect_agent_threads")} SET ${sql.update({ tail_sequence: batch.lastSequence, ...workFields })} WHERE thread_id=${threadId}`,
                );
              }
              for (const fact of page.archive.admissions) {
                const a = yield* encode(ThreadAdmission, fact);

                yield* checkIdentifiers([
                  a.submissionId,
                  a.receiptId,
                  a.principal,
                  a.idempotencyKey,
                  a.agentId,
                  a.deploymentId,
                  a.parentLinkage?.parentSubmissionId,
                  a.parentLinkage?.parentToolCallId,
                ]);
                yield* checkValues([
                  canonicalJson(a.inputPayload),
                  canonicalJson(a.agentDigests),
                  a.admissionGroup,
                  a.admissionFence === undefined ? undefined : canonicalJson(a.admissionFence),
                  a.workerAdmission === undefined ? undefined : canonicalJson(a.workerAdmission),
                  a.messageAdmission === undefined ? undefined : canonicalJson(a.messageAdmission),
                ]);
                if (
                  (yield* query(sql`SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE submission_id=${a.submissionId} OR receipt_id=${a.receiptId}
          OR (thread_id=${threadId} AND ((principal=${a.principal} AND idempotency_key=${a.idempotencyKey}) OR queue_sequence=${a.queueSequence})) LIMIT 1`))
                    .length > 0
                )
                  return yield* reject(
                    "destination-conflict",
                    "Duplicate admission, receipt, replay key or queue position",
                  );

                const terminalRows = yield* rows(
                  threadId,
                  sql`r.record_id=${JSON.stringify(submissionSettlementRecordId(fact.submissionId))}`,
                  1,
                );

                const terminal =
                  terminalRows[0] === undefined ? undefined : yield* envelope(terminalRows[0]);

                const settled = terminal?.record.payload;

                if (settled?._tag !== "SubmissionSettled" || a.settledAt === undefined)
                  return yield* invalidThreadArchive("Transfer admission is not closed", threadId);
                yield* query(
                  sql`INSERT INTO ${table("effect_agent_submissions")} ${sql.insert({
                    submission_id: a.submissionId,
                    thread_id: threadId,
                    queue_sequence: a.queueSequence,
                    principal: a.principal,
                    idempotency_key: a.idempotencyKey,
                    agent_id: a.agentId,
                    definition_key: yield* physical.definition(canonicalJson(a.agentDigests)),
                    deployment_id: a.deploymentId,
                    input_json: canonicalJson(a.inputPayload),
                    input_digest: a.inputDigest,
                    receipt_id: a.receiptId,
                    state: settled?._tag === "SubmissionSettled" ? "settled" : "admitted",
                    settled_outcome: settled?._tag === "SubmissionSettled" ? settled.outcome : null,
                    settled_record_id:
                      settled?._tag === "SubmissionSettled"
                        ? (terminal?.record.recordId ?? null)
                        : null,
                    finalized_at: a.settledAt ?? null,
                    created_at: a.createdAt,
                    parent_submission_id: a.parentLinkage?.parentSubmissionId ?? null,
                    parent_tool_call_id: a.parentLinkage?.parentToolCallId ?? null,
                    admission_group: a.admissionGroup ?? null,
                    admission_fence_json:
                      a.admissionFence === undefined ? null : canonicalJson(a.admissionFence),
                    worker_admission_json:
                      a.workerAdmission === undefined ? null : canonicalJson(a.workerAdmission),
                    message_admission_json:
                      a.messageAdmission === undefined ? null : canonicalJson(a.messageAdmission),
                  })}`,
                );
              }
              for (const fact of page.archive.commands.aborts) {
                if (
                  (yield* query(
                    sql`SELECT submission_id FROM ${table("effect_agent_abort_intents")} WHERE submission_id=${fact.submissionId}  LIMIT 1`,
                  )).length > 0
                )
                  return yield* invalidThreadArchive("Duplicate accepted command", threadId);
                if (
                  (yield* query(
                    sql`SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE thread_id=${threadId} AND submission_id=${fact.submissionId} LIMIT 1`,
                  )).length !== 1
                )
                  return yield* invalidThreadArchive(
                    "Accepted command has no admission in this Thread",
                    threadId,
                  );
                const a = yield* encode(AbortIntent, AbortIntent.make(fact));

                yield* checkIdentifiers([a.submissionId, a.author]);
                yield* checkValues([a.reason]);
                yield* query(
                  sql`INSERT INTO ${table("effect_agent_abort_intents")} (submission_id, author, reason, requested_at) VALUES (${a.submissionId}, ${a.author}, ${a.reason}, ${a.requestedAt})`,
                );
              }
              for (const fact of page.archive.commands.approvals) {
                if (
                  (yield* query(
                    sql`SELECT submission_id FROM ${table("effect_agent_approval_decisions")} WHERE submission_id=${fact.submissionId} AND tool_call_id=${fact.toolCallId} LIMIT 1`,
                  )).length > 0
                )
                  return yield* invalidThreadArchive("Duplicate accepted command", threadId);
                if (
                  (yield* query(
                    sql`SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE thread_id=${threadId} AND submission_id=${fact.submissionId} LIMIT 1`,
                  )).length !== 1
                )
                  return yield* invalidThreadArchive(
                    "Accepted command has no admission in this Thread",
                    threadId,
                  );

                const a = yield* encode(ApprovalDecisionIntent, ApprovalDecisionIntent.make(fact));

                yield* checkIdentifiers([a.submissionId, a.toolCallId, a.resolver]);
                yield* checkValues([a.reason]);
                yield* query(sql`INSERT INTO ${table("effect_agent_approval_decisions")} (submission_id, tool_call_id, decision, resolver, reason, decided_at)
          VALUES (${a.submissionId}, ${a.toolCallId}, ${a.decision}, ${a.resolver}, ${a.reason}, ${a.decidedAt})`);
              }
              for (const fact of page.archive.commands.resolutions) {
                if (
                  (yield* query(
                    sql`SELECT submission_id FROM ${table("effect_agent_unknown_resolutions")} WHERE submission_id=${fact.submissionId} AND tool_call_id=${fact.toolCallId} AND resolution_kind=${unknownResolutionKind(fact.resolution)} LIMIT 1`,
                  )).length > 0
                )
                  return yield* invalidThreadArchive("Duplicate accepted command", threadId);
                if (
                  (yield* query(
                    sql`SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE thread_id=${threadId} AND submission_id=${fact.submissionId} LIMIT 1`,
                  )).length !== 1
                )
                  return yield* invalidThreadArchive(
                    "Accepted command has no admission in this Thread",
                    threadId,
                  );

                const a = yield* encode(
                  UnknownResolutionIntent,
                  UnknownResolutionIntent.make(fact),
                );

                yield* checkIdentifiers([a.submissionId, a.toolCallId, a.author]);
                yield* checkValues([a.reason, canonicalJson(a.resolution)]);
                yield* query(sql`INSERT INTO ${table("effect_agent_unknown_resolutions")} (submission_id, tool_call_id, resolution_kind, author, reason, resolution_json, resolved_at)
          VALUES (${a.submissionId}, ${a.toolCallId}, ${unknownResolutionKind(a.resolution)}, ${a.author}, ${a.reason}, ${canonicalJson(a.resolution)}, ${a.resolvedAt})`);
              }
              for (const delivery of page.archive.deliveries) {
                if (
                  (yield* query(
                    sql`SELECT message_id FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${threadId} AND message_id=${delivery.key.messageId} LIMIT 1`,
                  )).length > 0
                )
                  return yield* invalidThreadArchive("Duplicate delivery identity", threadId);

                const text = yield* Schema.encodeEffect(
                  Schema.fromJsonString(MessageDeliveryRecord),
                )(delivery).pipe(Effect.mapError((e) => failure("encode delivery", e)));

                yield* checkIdentifiers([
                  delivery.key.messageId,
                  delivery.key.ownerThreadId,
                  delivery.envelope.threadId,
                  delivery.envelope.agentId,
                ]);
                yield* checkValues([text]);
                yield* query(sql`INSERT INTO ${table("effect_agent_message_deliveries")} (owner_thread_id, message_id, version, state, deadline_at_millis, peer_retained_until_millis, record_json ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, read_metadata` })})
          VALUES (${threadId}, ${delivery.key.messageId}, ${delivery.version}, ${delivery.status}, ${messageDeliveryDeadline(delivery)}, ${peerMessageRetentionDeadline(delivery)}, ${text} ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, ${messageDeliveryMetadata(delivery)}::jsonb` })})`);
                yield* work.transferDelivery(threadId, delivery.key.messageId);
              }
              if (options.afterPage !== undefined)
                yield* options
                  .afterPage(page)
                  .pipe(Effect.mapError((e) => failure("rebuild imported publication", e)));
            }),
          );
          const result = yield* finishThreadImport(progress);

          return yield* Effect.gen(function* () {
            const id = result.threadId;
            const workerSeal = progress.manifest?.workerSeal;
            let terminalEvidence = workerSeal?.terminal === undefined;

            yield* query(
              sql`UPDATE ${table("effect_agent_threads")} SET tail_sequence=${result.tailSequence} WHERE thread_id=${id}`,
            );
            const reader = yield* ThreadImportReader;
            const readAdmission = reader.admission;
            // Check missing active state for every retained Run, including terminal history counts.
            let afterRun: number | undefined;

            while (true) {
              const found =
                yield* query(sql`SELECT r.run_id AS run_key, ${runIdentity(sql, sql`r.run_id`, options.namespace)} AS run_id FROM ${table("effect_agent_record_runs")} r
        WHERE thread_id=${id} AND run_id IS NOT NULL
        ${afterRun === undefined ? sql`` : sql`AND run_id>${afterRun}`} GROUP BY r.run_id ORDER BY r.run_id LIMIT 1`).pipe(
                  Effect.flatMap((r) =>
                    decode(
                      Schema.Array(Schema.Struct({ run_key: SqlInteger, run_id: Schema.String })),
                      r,
                    ),
                  ),
                );

              const next = found[0];

              if (next === undefined) break;
              const runId = yield* json(RunId, next.run_id);

              yield* verifyRunContinuations(yield* reader.runRecords(runId), {
                verifyContext: false,
              }).pipe(Effect.mapError((e) => invalidThreadArchive(e.message, id)));
              yield* validateClosedRun(id, runId);
              yield* validateImportedRunExecution(id, runId, result.tailSequence);
              afterRun = next.run_key;
            }
            let afterQueue = -1;

            while (true) {
              const selected = yield* admissionRows(id, sql`s.queue_sequence>${afterQueue}`, 1);
              const row = selected[0];

              if (row === undefined) break;
              const a = yield* admission(row);
              const rebuilt = yield* rebuildClosedSubmission(a);

              if (
                rebuilt.settlement !== undefined &&
                workerTerminalFromRecord(
                  SubmissionSnapshot.make({ ...a, state: rebuilt.state }),
                  rebuilt.settlement,
                ) === workerSeal?.terminal
              )
                terminalEvidence = true;
              const settled = rebuilt.settlement;

              if (settled?.payload._tag !== "SubmissionSettled" || a.settledAt === undefined)
                return yield* invalidThreadArchive("Closed admission lost its settlement", id);
              yield* query(
                sql`UPDATE ${table("effect_agent_submissions")} SET ${sql.update({
                  state: "settled",
                  settled_outcome: settled.payload.outcome,
                  settled_record_id: settled.recordId,
                  finalized_at: DateTime.formatIso(a.settledAt),
                  input_applied_record_id: rebuilt.inputApplied?.recordId ?? null,
                  input_applied_sequence: rebuilt.inputApplied?.sequence ?? null,
                  joined_host_submission_id: rebuilt.joinedHostSubmissionId ?? null,
                })} WHERE submission_id=${a.submissionId}`,
              );
              if (rebuilt.abort?.canonicalRecordId !== undefined)
                yield* query(
                  sql`UPDATE ${table("effect_agent_abort_intents")} SET canonical_record_id=${rebuilt.abort.canonicalRecordId} WHERE submission_id=${a.submissionId}`,
                );
              afterQueue = a.queueSequence;
            }
            if (!terminalEvidence)
              return yield* invalidThreadArchive(
                "Worker seal terminal has no exact completed input evidence",
                id,
              );

            // Each command insert already required its exact staged admission in this Thread.
            let afterSequence = 0;
            let lastInputQueue = -1;

            while (true) {
              const selected = yield* rows(id, sql`r.sequence>${afterSequence}`, 1);
              const row = selected[0];

              if (row === undefined) break;
              const entry = yield* envelope(row);
              const payload = entry.record.payload;

              yield* verifyImportedReferences(entry);
              if (
                payload._tag !== "WorkerInputCompleted" &&
                "submissionId" in payload &&
                payload.submissionId !== undefined &&
                (yield* readAdmission(payload.submissionId)) === undefined
              )
                return yield* invalidThreadArchive(
                  "Canonical submission reference has no immutable admission",
                  id,
                );
              if (
                payload._tag === "UserInputRecorded" &&
                payload.submissionId !== undefined &&
                entry.record.recordId !== submissionInputRecordId(payload.submissionId)
              )
                return yield* invalidThreadArchive("Canonical input identity is ambiguous", id);
              if (
                payload._tag === "SubmissionSettled" &&
                entry.record.recordId !== submissionSettlementRecordId(payload.submissionId)
              )
                return yield* invalidThreadArchive(
                  "Canonical settlement identity is ambiguous",
                  id,
                );
              if (payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) {
                const admitted = yield* readAdmission(payload.submissionId);

                if (admitted === undefined || admitted.queueSequence <= lastInputQueue)
                  return yield* invalidThreadArchive("Canonical inputs violate admission FIFO", id);
                lastInputQueue = admitted.queueSequence;
              }
              if (payload._tag === "SubmissionSettled") {
                const admitted = yield* readAdmission(payload.submissionId);

                if (admitted === undefined)
                  return yield* invalidThreadArchive("Settlement has no immutable admission", id);
                // Every executed settlement must follow its own immutable canonical input.
                yield* verifyImportedInputOrder(admitted);
              }
              afterSequence = row.sequence;
            }
            let afterDelivery: string | undefined;

            const readDelivery = Effect.fnUntraced(function* (
              messageId: MessageDeliveryRecord["key"]["messageId"],
            ) {
              const found = yield* query(
                sql`SELECT record_json FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${id} AND message_id=${messageId} LIMIT 1`,
              ).pipe(
                Effect.flatMap((r) =>
                  decode(Schema.Array(Schema.Struct({ record_json: Schema.String })), r),
                ),
              );

              if (found[0] === undefined)
                return yield* invalidThreadArchive("Delivery predecessor is missing", id);
              const value = yield* json(MessageDeliveryRecord, found[0].record_json);

              if (value.key.ownerThreadId !== id || value.key.messageId !== messageId)
                return yield* invalidThreadArchive(
                  "Delivery index differs from its exact wire",
                  id,
                );

              return value;
            });

            const visitDelivery = Effect.fnUntraced(function* (root: MessageDeliveryRecord) {
              let current = root;

              while (true) {
                const visits =
                  yield* query(sql`SELECT path_id, completed FROM ${table("effect_agent_import_delivery_visits")}
                  WHERE thread_id=${id} AND message_id=${current.key.messageId}`).pipe(
                    Effect.flatMap((r) =>
                      decode(
                        Schema.Array(
                          Schema.Struct({ path_id: Schema.String, completed: SqlInteger }),
                        ),
                        r,
                      ),
                    ),
                  );

                const prior = visits[0];

                if (prior !== undefined) {
                  if (prior.completed === 1) break;
                  if (prior.completed !== 0 || prior.path_id !== root.key.messageId)
                    return yield* invalidThreadArchive("Delivery visitation is inconsistent", id);

                  return yield* invalidThreadArchive("Delivery predecessor cycle", id);
                }
                yield* query(sql`INSERT INTO ${table("effect_agent_import_delivery_visits")}
                  (thread_id, message_id, path_id, completed) VALUES (${id}, ${current.key.messageId}, ${root.key.messageId}, 0)`);
                if (current.predecessor === undefined) break;
                const predecessor = yield* readDelivery(current.predecessor);

                if (predecessor.createdAtMillis > current.createdAtMillis)
                  return yield* invalidThreadArchive(
                    "Delivery predecessor has a later creation boundary",
                    id,
                  );
                current = predecessor;
              }
              yield* query(sql`UPDATE ${table("effect_agent_import_delivery_visits")} SET completed=1
                WHERE thread_id=${id} AND path_id=${root.key.messageId} AND completed=0`);
            });

            while (true) {
              const selected = yield* deliveries(id, afterDelivery);
              const d = selected[0];

              if (d === undefined) break;

              const encoded = yield* Schema.encodeEffect(MessageDeliveryRecord)(d).pipe(
                Effect.mapError((e) => failure("validate restored delivery", e)),
              );

              if (utf8ByteLength(canonicalJson(encoded.envelope)) > deliveryLimits.maxEnvelopeBytes)
                return yield* reject(
                  "unsupported-capacity",
                  "Delivery envelope exceeds destination capacity",
                );
              yield* visitDelivery(d);
              if (d.receipt !== null) {
                if (
                  d.settlement !== null &&
                  d.settlement.settlementId !== submissionSettlementId(d.receipt.submissionId)
                )
                  return yield* invalidThreadArchive(
                    "Delivery settlement identity is noncanonical",
                    id,
                  );
                const foreign = d.receipt.threadId !== id;

                if (foreign && d.status !== "processed")
                  return yield* reject(
                    "unsupported-obligations",
                    "Live foreign delivery receipt cannot be restored",
                  );

                const evidence =
                  yield* query(sql`SELECT s.*, ${definitionJson(sql, options.namespace, "s")} AS agent_digests_json FROM ${table("effect_agent_submissions")} s WHERE thread_id=${d.receipt.threadId}
          AND submission_id=${d.receipt.submissionId} LIMIT 1`).pipe(
                    Effect.flatMap((r) => decode(Schema.Array(getImportSchemas().AdmissionRow), r)),
                  );

                const destinationRow = evidence[0];

                // Closed foreign facts do not require importing the receiver's execution authority.
                if (destinationRow === undefined) {
                  if (foreign && d.status === "processed") {
                    afterDelivery = d.key.messageId;
                    continue;
                  }

                  return yield* invalidThreadArchive(
                    "Delivery destination receipt evidence is missing",
                    id,
                  );
                }
                const destination = yield* admission(destinationRow);

                const expected = PreparedInput.make({
                  schemaVersion: 1,
                  authorization: d.envelope.authorization,
                  threadId: destination.threadId,
                  deliveryPrincipal: destination.principal,
                  admissionKey: destination.idempotencyKey,
                  agentId: destination.agentId,
                  definitions: destination.agentDigests,
                  input: destination.inputPayload,
                  inputDigest: destination.inputDigest,
                  ...(destination.admissionGroup === undefined
                    ? {}
                    : { admissionGroup: destination.admissionGroup }),
                  ...(destination.admissionFence === undefined
                    ? {}
                    : { admissionFence: destination.admissionFence }),
                  ...(destination.workerAdmission === undefined
                    ? {}
                    : { workerAdmission: destination.workerAdmission }),
                  ...(destination.messageAdmission === undefined
                    ? {}
                    : { messageAdmission: destination.messageAdmission }),
                });

                if (
                  d.receipt.receiptId !== destination.receiptId ||
                  d.receipt.queueSequence !== destination.queueSequence ||
                  !Schema.toEquivalence(PreparedInput)(d.envelope, expected)
                )
                  return yield* invalidThreadArchive(
                    "Delivery envelope disagrees with destination admission",
                    id,
                  );
                if (d.settlement !== null) {
                  const found = yield* rows(
                    destination.threadId,
                    sql`r.record_id=${JSON.stringify(submissionSettlementRecordId(destination.submissionId))}`,
                    1,
                  );

                  if (found[0] === undefined) {
                    if (foreign) {
                      afterDelivery = d.key.messageId;
                      continue;
                    }

                    return yield* invalidThreadArchive(
                      "Processed delivery has no exact destination settlement",
                      id,
                    );
                  }
                  const record = (yield* envelope(found[0])).record;

                  const settled = yield* validateCanonicalSettlement(record, destination).pipe(
                    Effect.mapError(() =>
                      invalidThreadArchive("Delivery destination settlement is inconsistent", id),
                    ),
                  );

                  // Preserve the frozen receipt time; destination finalization caches are rebuilt.
                  const failureDiagnostic = settlementFailureFromRecord(record);

                  const expectedSettlement = Settlement.make({
                    submissionId: settled.submissionId,
                    receiptId: settled.receiptId,
                    settlementId: settled.settlementId,
                    outcome: settled.outcome,
                    settledAt: d.settlement.settledAt,
                    ...(failureDiagnostic === undefined ? {} : { failure: failureDiagnostic }),
                    ...(d.settlement.runDisposition === undefined ||
                    settled.runDisposition === undefined
                      ? {}
                      : { runDisposition: settled.runDisposition }),
                    ...(d.settlement.usageSummary === undefined ||
                    settled.usageSummary === undefined
                      ? {}
                      : { usageSummary: settled.usageSummary }),
                  });

                  if (!Schema.toEquivalence(Settlement)(d.settlement, expectedSettlement))
                    return yield* invalidThreadArchive(
                      "Delivery settlement disagrees with exact destination evidence",
                      id,
                    );
                }
              }
              afterDelivery = d.key.messageId;
            }
            const staged = yield* countFacts(id);

            if (canonicalJson(staged) !== canonicalJson(progress.counts))
              return yield* invalidThreadArchive(
                "Staged native fact counts differ from the captured manifest",
                id,
              );
            if (!(yield* checkIdle(id)))
              return yield* reject(
                "unsupported-obligations",
                "Staged Thread retains live obligations",
              );

            if (workerSeal !== undefined)
              yield* query(
                sql`INSERT INTO ${table("effect_agent_worker_stops")} (thread_id, terminal) VALUES (${id}, ${workerSeal.terminal ?? null})`,
              );
            yield* query(
              sql`DELETE FROM ${table("effect_agent_import_delivery_visits")} WHERE thread_id=${id}`,
            );
            yield* query(
              sql`UPDATE ${table("effect_agent_threads")} SET tail_sequence=${result.tailSequence} WHERE thread_id=${id}`,
            );
            if (options.afterImport !== undefined)
              yield* options
                .afterImport({ result })
                .pipe(Effect.mapError((e) => failure("publish imported Thread", e)));

            return result;
          }).pipe(Effect.provide(readerLayer(result.threadId, result.tailSequence)));
        }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
      ),
    );

  const verify = Effect.fnUntraced(function* (request: {
    readonly threadId: ThreadId;
    readonly requireAllSettled?: boolean;
  }) {
    return yield* options
      .read(
        Effect.gen(function* () {
          const first = yield* verificationPage({ threadId: request.threadId });
          const id = first.threadId;

          yield* ranges.verifyThread(id);
          yield* work.verifyPointers(id);

          const LedgerRow = Schema.Struct({
            ...getImportSchemas().AdmissionRow.fields,
            state: SubmissionState,
            settled_outcome: Schema.NullOr(SettlementOutcome),
            ready_at: Schema.NullOr(Schema.String),
          });

          const snapshot = Effect.fnUntraced(function* (row: typeof LedgerRow.Type) {
            const fact = yield* admission(row);

            return SubmissionSnapshot.make({
              ...fact,
              state: row.state,
              ...(row.settled_outcome === null ? {} : { settledOutcome: row.settled_outcome }),
              ...(row.ready_at === null
                ? {}
                : { readyAt: yield* decode(Schema.DateTimeUtcFromString, row.ready_at) }),
            });
          });

          const scan = (predicate: (after: number) => Fragment) =>
            Stream.paginate(-1, (after) =>
              Effect.gen(function* () {
                const rows = yield* query(
                  sql`SELECT s.*, ${definitionJson(sql, options.namespace, "s")} AS agent_digests_json FROM ${table("effect_agent_submissions")} s WHERE s.thread_id=${id} AND ${predicate(after)} ORDER BY s.queue_sequence LIMIT 1`,
                ).pipe(Effect.flatMap((r) => decode(Schema.Array(LedgerRow), r)));

                const row = rows[0];

                return [
                  row === undefined ? [] : [yield* snapshot(row)],
                  row === undefined ? Option.none<number>() : Option.some(row.queue_sequence),
                ] as const;
              }),
            );

          const submissions = scan((after) => sql`s.queue_sequence>${after}`);

          const runs = Stream.paginate<number | undefined, RunId, ThreadStoreError>(
            undefined,
            (after) =>
              Effect.gen(function* () {
                const selected =
                  yield* query(sql`SELECT r.run_id AS run_key, ${runIdentity(sql, sql`r.run_id`, options.namespace)} AS run_id FROM ${table("effect_agent_record_runs")} r
          WHERE thread_id=${id} AND sequence<=${first.tailSequence} AND run_id IS NOT NULL
          ${after === undefined ? sql`` : sql`AND run_id>${after}`} GROUP BY r.run_id ORDER BY r.run_id LIMIT 1`).pipe(
                    Effect.flatMap((r) =>
                      decode(
                        Schema.Array(Schema.Struct({ run_key: SqlInteger, run_id: Schema.String })),
                        r,
                      ),
                    ),
                  );

                const row = selected[0];

                return [
                  row === undefined ? [] : [yield* json(RunId, row.run_id)],
                  row === undefined ? Option.none<number | undefined>() : Option.some(row.run_key),
                ] as const;
              }),
          );

          const checkpoints = yield* query(
            sql`SELECT checkpoint_json FROM ${table("effect_agent_checkpoints")} WHERE thread_id=${id} AND through_sequence<=${first.tailSequence} ORDER BY through_sequence DESC LIMIT 1`,
          ).pipe(
            Effect.flatMap((r) =>
              decode(Schema.Array(Schema.Struct({ checkpoint_json: Schema.String })), r),
            ),
          );

          const checkpoint =
            checkpoints[0] === undefined
              ? undefined
              : yield* json(ThreadCheckpoint, checkpoints[0].checkpoint_json);

          const pages = streamExport({ threadId: id }).pipe(
            Stream.provideService(ThreadExportSource, { export: verificationPage }),
          );

          // The sequence keyset must visit every active row, including malformed/orphan state.
          if (
            (yield* query(sql`SELECT input_sequence FROM ${table("effect_agent_executions")}
          WHERE thread_id=${id} GROUP BY input_sequence
          HAVING input_sequence<1 OR COUNT(*)<>1 LIMIT 1`)).length > 0
          )
            return yield* failure(
              "verify active execution directory",
              "Invalid or duplicate original input sequence",
            );

          const active = Stream.paginate(0, (after) =>
            executions(id, sql`e.input_sequence>${after}`).pipe(
              Effect.map(
                (rows) =>
                  [
                    rows,
                    rows[0] === undefined
                      ? Option.none<number>()
                      : Option.some(rows[0].originalSequence),
                  ] as const,
              ),
            ),
          );

          const result = yield* verifyThreadInvariants({
            threadId: id,
            pages,
            submissions,
            runs,
            executions: active,
            ...(checkpoint === undefined ? {} : { checkpoint }),
            checkpointsSupported: true,
            ...(request.requireAllSettled === undefined
              ? {}
              : { requireAllSettled: request.requireAllSettled }),
          }).pipe(Effect.provide(readerLayer(id, first.tailSequence)));

          const last = yield* verificationPage({ threadId: id });

          if (!sameTransferManifest(last, first))
            return yield* failure("verify snapshot", "Thread changed during verification");

          return result;
        }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
      )
      .pipe(
        Effect.catchTag("ThreadExportRejected", (cause) =>
          failure("verify captured Thread", cause),
        ),
      );
  });

  return {
    export: exportThread,
    verificationPage,
    verifyPointers: work.verifyPointers,
    importer: ThreadImport.of({ import: importThread }),
    verification: { verify },
  };
});
