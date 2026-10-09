import { EMPTY_TAIL_DIGEST, canonicalJson, utf8ByteLength } from "@yielded/agent/digest";
import { RunId, SubmissionId, ThreadId } from "@yielded/agent/identifiers";
import {
  MessageDeliveryRecord,
  messageDeliveryDeadline,
  isWorkerUpdateDelivery,
  defaultMessageDeliveryStoreLimits,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import { decodeExportRecord, ExportedRecord } from "@yielded/agent/record-format";
import type { RecordId } from "@yielded/agent/records";
import {
  CURRENT_RECORD_FORMAT,
  SettlementOutcome,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
  ProducerEpoch,
  RecordJson,
  MAX_RUN_EVIDENCE_RECORDS,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_TERMINAL_BYTES,
  RUN_TERMINAL_RESERVE_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
  MAX_RUN_CONTINUATION_BYTES,
} from "@yielded/agent/records";
import { verifyRunContinuations } from "@yielded/agent/run-continuation";
import { validateCanonicalSettlement } from "@yielded/agent/settlement-publisher";
import {
  ApprovalDecisionIntent,
  AbortIntent,
  SuspensionSnapshot,
  SubmissionSnapshot,
  SubmissionState,
  SubmissionAdmissionFence,
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
  rebuildImportedSubmission,
  verifyImportedReferences,
  verifyImportedSettlementOrder,
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
  ThreadStoreError,
  ThreadCheckpoint,
  streamExport,
  ThreadExportSource,
  canonicalBatchFitsTransfer,
} from "@yielded/agent/thread-store";
import {
  exportThreadPage,
  ThreadExporterReader,
  type TransferSection,
  type TransferFacts,
} from "@yielded/agent/thread-transfer";
import { WORK_INDEX_VERSION } from "@yielded/agent/thread-work";
import { AssignmentTerminal } from "@yielded/agent/worker";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { SqlClient } from "effect/sql/SqlClient";
import type { Fragment } from "effect/sql/Statement";
import * as Stream from "effect/Stream";
import * as Struct from "effect/Struct";

import { makeSqlSettlementIntervals } from "./internal/settlement-intervals.ts";
import {
  decodeAdmissionFact,
  decodeAbortFact,
  decodeApprovalFact,
  decodeResolutionFact,
} from "./SqlAdmissionFacts.ts";
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

const Header = Schema.Struct({
  ...Struct.pick(ThreadRow.fields, ["thread_id", "tail_sequence", "producer_epoch"]),
  tail_digest: Digest,
});

const Row = Schema.Struct({
  ...Struct.pick(RecordRow.fields, ["batch_id", "sequence", "record_json", "record_id"]),
  thread_id: ThreadId,
});

const WireBatch = Schema.Struct({
  ...ThreadExportBatch.fields,
  records: Schema.NonEmptyArray(RecordJson).check(Schema.isMaxLength(256)),
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
  parent_submission_id: Schema.NullOr(Schema.String),
  parent_tool_call_id: Schema.NullOr(Schema.String),
  worker_admission_json: Schema.NullOr(Schema.String),
  message_admission_json: Schema.NullOr(Schema.String),
  admission_group: Schema.NullOr(Schema.String),
  admission_fence_json: Schema.NullOr(Schema.String),
});

const AbortRow = Schema.Struct({
  command_sequence: SqlInteger.check(Schema.isGreaterThan(0)),
  submission_id: Schema.String,
  author: Schema.String,
  reason: Schema.String,
  requested_at: Schema.String,
});

const ApprovalRow = Schema.Struct({
  command_sequence: SqlInteger.check(Schema.isGreaterThan(0)),
  submission_id: Schema.String,
  tool_call_id: Schema.String,
  decision: Schema.String,
  resolver: Schema.String,
  reason: Schema.String,
  decided_at: Schema.String,
});

const ResolutionRow = Schema.Struct({
  command_sequence: SqlInteger.check(Schema.isGreaterThan(0)),
  submission_id: Schema.String,
  tool_call_id: Schema.String,
  resolution_kind: UnknownResolutionKind,
  author: Schema.String,
  reason: Schema.String,
  resolution_json: Schema.String,
  resolved_at: Schema.String,
});

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
  const admissionFence = yield* SubmissionAdmissionFence;
  const deliveryLimits = options.deliveryLimits ?? defaultMessageDeliveryStoreLimits;
  const { table, execute } = yield* makeSqlQuery(options.namespace);

  const intervals = yield* makeSqlSettlementIntervals(options.namespace);

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
      sql`SELECT r.thread_id, r.batch_id, r.sequence, r.record_id, ${recordJson} AS record_json
      FROM ${table("effect_agent_canonical_records")} r WHERE r.thread_id=${threadId} AND ${predicate}
      ORDER BY r.sequence LIMIT ${limit}`,
    ).pipe(Effect.flatMap((value) => decode(Schema.Array(Row), value)));

  const envelope = Effect.fnUntraced(function* (row: typeof Row.Type) {
    const wire = yield* json(RecordJson, row.record_json);

    const record = yield* decodeExportRecord(CURRENT_RECORD_FORMAT, wire).pipe(
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
          sql`SELECT thread_id, tail_sequence, tail_digest, producer_epoch FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
        ).pipe(
          Effect.flatMap((value) => decode(Schema.Array(Header), value)),
          Effect.map((headers) => headers[0]),
        ),
    }),
  );

  const admissionRows = (threadId: ThreadId, predicate: Fragment, limit: number) =>
    query(
      sql`SELECT s.* FROM ${table("effect_agent_submissions")} s WHERE s.thread_id=${threadId} AND ${predicate}
      ORDER BY s.queue_sequence LIMIT ${limit}`,
    ).pipe(Effect.flatMap((value) => decode(Schema.Array(AdmissionRow), value)));

  const admission = (row: typeof AdmissionRow.Type) =>
    decodeAdmissionFact(row).pipe(Effect.mapError((e) => failure("decode transfer admission", e)));

  const commandFacts = Effect.fnUntraced(function* (
    threadId: ThreadId,
    section: "aborts" | "approvals" | "resolutions",
    predicate: Fragment,
    limit: number,
    order: "identity" | "sequence" = "identity",
  ) {
    const kind = section;

    const orderBy =
      order === "sequence"
        ? sql`a.command_sequence`
        : sql`a.submission_id, a.tool_call_id, a.resolution_kind`;

    const selected = sql`SELECT a.command_sequence, a.submission_id, a.tool_call_id, a.resolution_kind
      FROM ${table("effect_agent_transfer_commands")} a
      WHERE a.thread_id=${threadId} AND a.command_kind=${kind} AND ${predicate}
      ORDER BY ${orderBy} LIMIT ${limit}`;

    if (section === "aborts") {
      const rows =
        yield* query(sql`SELECT selected.command_sequence, fact.* FROM (${selected}) selected
          LEFT JOIN ${table("effect_agent_abort_intents")} fact ON fact.submission_id=selected.submission_id`).pipe(
          Effect.flatMap((r) => decode(Schema.Array(AbortRow), r)),
        );

      return {
        commands: {
          aborts: yield* Effect.forEach(rows, (r) =>
            decodeAbortFact(r).pipe(Effect.mapError((e) => failure("decode transfer abort", e))),
          ),
          approvals: [],
          resolutions: [],
        },
        after: rows.at(-1)?.command_sequence,
      };
    }
    if (section === "approvals") {
      const rows =
        yield* query(sql`SELECT selected.command_sequence, fact.* FROM (${selected}) selected
          LEFT JOIN ${table("effect_agent_approval_decisions")} fact
            ON fact.submission_id=selected.submission_id AND fact.tool_call_id=selected.tool_call_id`).pipe(
          Effect.flatMap((r) => decode(Schema.Array(ApprovalRow), r)),
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
        after: rows.at(-1)?.command_sequence,
      };
    }

    const rows =
      yield* query(sql`SELECT selected.command_sequence, fact.* FROM (${selected}) selected
        LEFT JOIN ${table("effect_agent_unknown_resolutions")} fact
          ON fact.submission_id=selected.submission_id AND fact.tool_call_id=selected.tool_call_id
          AND fact.resolution_kind=selected.resolution_kind`).pipe(
        Effect.flatMap((r) => decode(Schema.Array(ResolutionRow), r)),
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
      after: rows.at(-1)?.command_sequence,
    };
  });

  const deliveries = (threadId: ThreadId, after?: string) =>
    query(sql`SELECT record_json FROM ${table("effect_agent_message_deliveries")}
    WHERE owner_thread_id=${threadId} ${after === undefined ? sql`` : sql`AND message_id>${after}`} ORDER BY message_id LIMIT 1`).pipe(
      Effect.flatMap((r) => decode(Schema.Array(Schema.Struct({ record_json: Schema.String })), r)),
      Effect.flatMap((r) => Effect.forEach(r, (r) => json(MessageDeliveryRecord, r.record_json))),
    );

  const checkForeignWork = Effect.fnUntraced(function* (threadId: ThreadId) {
    const live = new Set<"child" | "worker" | "message-delivery">();

    if (
      (yield* query(sql`SELECT reservation_id FROM ${table("effect_agent_live_child_reservations")}
      ${seekIndex("effect_agent_live_child_reservations_thread")} WHERE thread_id=${threadId} LIMIT 1`))
        .length > 0
    )
      live.add("child");
    if (
      (yield* query(
        sql`SELECT submission_id FROM ${table("effect_agent_submissions")} ${seekIndex("effect_agent_submissions_active_worker")}
        WHERE thread_id=${threadId} AND state<>'settled' AND worker_admission_json IS NOT NULL LIMIT 1`,
      )).length > 0
    )
      live.add("worker");
    if (
      (yield* query(sql`SELECT thread_id FROM ${table("effect_agent_worker_stops")} WHERE thread_id=${threadId}
        AND EXISTS (SELECT 1 FROM ${table("effect_agent_submissions")} ${seekIndex("effect_agent_submissions_nonterminal")}
          WHERE thread_id=${threadId} AND state<>'settled' LIMIT 1) LIMIT 1`)).length > 0
    )
      live.add("worker");
    if (
      (yield* query(
        sql`SELECT submission_id FROM ${table("effect_agent_submissions")} ${seekIndex("effect_agent_submissions_active_parent")}
        WHERE thread_id=${threadId} AND state<>'settled' AND parent_submission_id IS NOT NULL LIMIT 1`,
      )).length > 0
    )
      live.add("child");

    const indexed = yield* query(
      sql`SELECT version, state, through_sequence, entry_count FROM ${table("effect_agent_work_index")} WHERE thread_id=${threadId} LIMIT 2`,
    ).pipe(
      Effect.flatMap((r) =>
        decode(
          Schema.Array(
            Schema.Struct({
              version: SqlInteger,
              state: Schema.String,
              through_sequence: SqlInteger,
              entry_count: SqlInteger,
            }),
          ),
          r,
        ),
      ),
    );

    const tail = yield* query(
      sql`SELECT tail_sequence FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId} LIMIT 1`,
    ).pipe(
      Effect.flatMap((r) => decode(Schema.Array(Schema.Struct({ tail_sequence: SqlInteger })), r)),
    );

    if (
      indexed.length !== 1 ||
      indexed[0]!.version !== WORK_INDEX_VERSION ||
      indexed[0]!.state !== "ready" ||
      indexed[0]!.through_sequence !== (tail[0]?.tail_sequence ?? 0)
    )
      return yield* failure("read transfer work index", "Missing or incomplete native work index");
    if (
      (yield* query(sql`SELECT id FROM ${table("effect_agent_work_entries")} w ${seekIndex("effect_agent_work_entries_foreign_child")} WHERE thread_id=${threadId}
      AND (owner_tag='Child' OR handoff_kind IN ('reservation','child-accounting')) LIMIT 1`))
        .length > 0
    )
      live.add("child");
    if (
      (yield* query(sql`SELECT id FROM ${table("effect_agent_work_entries")} w ${seekIndex("effect_agent_work_entries_foreign_worker")} WHERE thread_id=${threadId}
      AND (owner_tag IN ('WorkerInput','WorkerEffects','Report') OR handoff_kind='worker-stop') LIMIT 1`))
        .length > 0
    )
      live.add("worker");
    if (
      (yield* query(sql`SELECT id FROM ${table("effect_agent_work_entries")} w ${seekIndex("effect_agent_work_entries_foreign_delivery")} WHERE thread_id=${threadId}
      AND handoff_kind IN ('peer','update','report') LIMIT 1`)).length > 0
    )
      live.add("message-delivery");

    return [...live];
  });

  const exportReader = ThreadExporterReader.of({
    snapshot: Effect.fnUntraced(function* (threadId: ThreadId) {
      const headers = yield* query(
        sql`SELECT thread_id, tail_sequence, tail_digest, producer_epoch FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
      ).pipe(Effect.flatMap((r) => decode(Schema.Array(Header), r)));

      const state =
        yield* query(sql`SELECT revision, admissions_count AS admissions, aborts_count AS aborts,
        approvals_count AS approvals, resolutions_count AS resolutions, deliveries_count AS deliveries
        FROM ${table("effect_agent_transfer_state")} WHERE thread_id=${threadId}`).pipe(
          Effect.flatMap((r) =>
            decode(
              Schema.Array(
                Schema.Struct({
                  revision: SqlInteger,
                  admissions: SqlInteger,
                  aborts: SqlInteger,
                  approvals: SqlInteger,
                  resolutions: SqlInteger,
                  deliveries: SqlInteger,
                }),
              ),
              r,
            ),
          ),
        );

      if (headers.length === 0 && state.length === 0)
        return yield* ThreadNotMaterialized.make({ threadId });
      if (
        state.length !== 1 ||
        headers.length > 1 ||
        Object.values(state[0]!).some((v) => !Number.isSafeInteger(v) || v < 0)
      )
        return yield* failure(
          "capture transfer counters",
          "Missing, ambiguous or invalid native transfer counters",
        );
      if (options.afterThreadRead !== undefined)
        yield* options.afterThreadRead.pipe(
          Effect.mapError((e) => failure("capture transfer snapshot", e)),
        );
      if (
        (yield* query(
          sql`SELECT sequence FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${threadId} AND sequence>${headers[0]?.tail_sequence ?? 0} LIMIT 1`,
        )).length > 0 ||
        (yield* query(
          sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} ${seekIndex("effect_agent_canonical_batches_sequence")}
          WHERE thread_id=${threadId} AND first_sequence<1 LIMIT 1`,
        )).length > 0 ||
        (yield* query(
          sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} ${seekIndex("effect_agent_canonical_batches_last")}
          WHERE thread_id=${threadId} AND last_sequence>${headers[0]?.tail_sequence ?? 0} LIMIT 1`,
        )).length > 0
      )
        return yield* failure(
          "capture canonical tail",
          "Canonical rows extend beyond the captured thread tail",
        );
      const externalObligations = yield* checkForeignWork(threadId);

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
        tailSequence: headers[0]?.tail_sequence ?? CanonicalSequence.make(0),
        tailDigest: headers[0]?.tail_digest ?? EMPTY_TAIL_DIGEST,
        snapshot: state[0]!,
        ...(seals[0] === undefined
          ? {}
          : { workerSeal: seals[0].terminal === null ? {} : { terminal: seals[0].terminal } }),
        ...(externalObligations.length === 0 ? {} : { externalObligations }),
      };
    }),
    batch: Effect.fnUntraced(function* (threadId: ThreadId, fromSequence: number) {
      // Inspect original string sizes before reconstructing or hydrating canonical batches.
      const batchLength = canonicalBatchBytes(sql, options.namespace, "b");

      const headerLength = sql.onDialectOrElse({
        pg: () => sql`octet_length(b.batch_header_json)`,
        orElse: () => sql`length(CAST(b.batch_header_json AS BLOB))`,
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
        yield* query(sql`SELECT b.batch_id, b.first_sequence, b.last_sequence, b.batch_digest, b.tail_digest, b.batch_header_json, ${batchJson} AS batch_json
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

      if (stored.length !== 1 || row === undefined || row.last_sequence - row.first_sequence >= 256)
        return yield* failure("export batch", "Missing or oversized canonical batch");
      if (
        (yield* query(sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} ${seekIndex("effect_agent_canonical_batches_sequence")}
        WHERE thread_id=${threadId} AND batch_id<>${row.batch_id}
        AND first_sequence BETWEEN ${Math.max(1, row.first_sequence - 255)} AND ${row.last_sequence}
        AND last_sequence>=${row.first_sequence} LIMIT 1`)).length > 0
      )
        return yield* failure("export batch", "Overlapping canonical batch ranges");
      const batch = yield* json(WireBatch, row.batch_json);

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
        if (
          item.batch_id !== batch.batchId ||
          item.sequence !== fromSequence + index ||
          canonicalJson(yield* json(RecordJson, item.record_json)) !==
            canonicalJson(batch.records[index]!)
        )
          return yield* failure("export batch", "Canonical batch differs from its indexed records");
        const entry = yield* envelope(item);

        if (!(entry.record instanceof ExportedRecord))
          return yield* failure("export batch", "Canonical wire is missing");
        records.push(ThreadExportRecord.make({ ...entry, record: entry.record }));
      }

      return {
        records,
        batches: [ThreadExportBatch.make({ batchId: batch.batchId, producerId: batch.producerId })],
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

      const sequence =
        after === undefined
          ? 0
          : yield* decode(Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.Natural)), after);

      const page = yield* commandFacts(
        threadId,
        section,
        sql`a.command_sequence>${sequence}`,
        1,
        "sequence",
      );

      return {
        facts: { ...facts, commands: page.commands },
        after: String(page.after ?? sequence),
      };
    }),
  });

  const exportPage = (request: ThreadExportRequest) =>
    exportThreadPage(request).pipe(
      Effect.provideService(ThreadExporterReader, exportReader),
      Effect.provideService(Crypto.Crypto, crypto),
    );

  const exportThread = (request: ThreadExportRequest) => options.read(exportPage(request));

  const readerLayer = (
    id: ThreadId,
    tailSequence: CanonicalSequence,
    mode: "import" | "verify",
  ): ReturnType<typeof ThreadImportReader.layer> => {
    const readRecord = (recordId: RecordId) =>
      rows(id, sql`r.record_id=${recordId} AND r.sequence<=${tailSequence}`, 1).pipe(
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
          continuations = 0,
          bytes = 0;

        while (true) {
          const selected = yield* query(sql`SELECT * FROM (
            SELECT * FROM (SELECT r.thread_id, r.batch_id, r.sequence, r.record_id, ${recordJson} AS record_json
              FROM ${table("effect_agent_record_runs")} rr
              LEFT JOIN ${table("effect_agent_canonical_records")} r ON r.thread_id=rr.thread_id AND r.sequence=rr.sequence
              WHERE rr.thread_id=${id} AND rr.run_id=${JSON.stringify(runId)} AND rr.sequence>${after} AND rr.sequence<=${tailSequence}
              ORDER BY rr.sequence LIMIT 1) AS run_facts
            UNION ALL
            SELECT * FROM (SELECT r.thread_id, r.batch_id, r.sequence, r.record_id, ${recordJson} AS record_json
              FROM ${table("effect_agent_canonical_records")} r ${seekIndex("effect_agent_records_continuation")}
              WHERE r.thread_id=${id} AND r.record_tag='RunContinuation' AND r.run_id=${JSON.stringify(runId)}
                AND r.sequence>${after} AND r.sequence<=${tailSequence}
              ORDER BY r.sequence LIMIT 1) AS run_progress
            ) AS run_membership ORDER BY sequence LIMIT 1`).pipe(
            Effect.flatMap((value) => decode(Schema.Array(Row), value)),
          );

          const row = selected[0];

          if (row === undefined) return entries;
          const entry = yield* envelope(row);
          const size = utf8ByteLength(row.record_json);

          if (entry.record.payload._tag === "RunContinuation") {
            continuations++;
            if (
              size > MAX_RUN_CONTINUATION_BYTES ||
              continuations > MAX_RUN_EVIDENCE_RECORDS + RUN_TERMINAL_RESERVE_RECORDS
            )
              return yield* failure("read bounded Run", "Continuation evidence exceeds its bound");
          } else {
            facts++;
            bytes += size;
          }
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
        query(sql`SELECT record_id FROM ${table("effect_agent_canonical_records")} r WHERE r.thread_id=${id} AND r.record_tag='ThreadCreated'
        AND ${sql.onDialectOrElse({ pg: () => sql`(${recordJson})::jsonb -> 'payload' ->> 'agentId'`, orElse: () => sql`json_extract(${recordJson}, '$.payload.agentId')` })}=${agentId} LIMIT 1`).pipe(
          Effect.map((r) => r.length === 1),
        ),
      settlementPredecessors: (request) =>
        mode === "verify"
          ? intervals.predecessors(id, request)
          : Stream.paginate<number, Pick<SubmissionSnapshot, "submissionId">, ThreadStoreError>(
              -1,
              (after) =>
                Effect.gen(function* () {
                  const candidates = yield* query(sql`SELECT submission_id, queue_sequence
                    FROM ${table("effect_agent_import_fifo")}
                    ${seekIndex("effect_agent_import_fifo_active")}
                    WHERE thread_id=${id} AND queue_sequence>${after} AND queue_sequence<${request.queueSequence}
                    ORDER BY queue_sequence LIMIT 1`).pipe(
                    Effect.flatMap((r) =>
                      decode(
                        Schema.Array(
                          Schema.Struct({
                            submission_id: SubmissionId,
                            queue_sequence: SqlInteger,
                          }),
                        ),
                        r,
                      ),
                    ),
                  );

                  const prior = candidates[0];

                  return [
                    prior === undefined ? [] : [{ submissionId: prior.submission_id }],
                    prior === undefined ? Option.none<number>() : Option.some(prior.queue_sequence),
                  ] as const;
                }),
            ),
    });
  };

  const importThread = <E2, R>(pages: Stream.Stream<ThreadArchive, E2, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const captured = yield* captureImportSource(pages);

        return yield* options.write(
          Effect.gen(function* () {
            const progress = makeThreadImportProgress();
            let threadId: ThreadId | undefined;
            let destinationEpoch = 0;
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
              decodeStoredIdentifiers(values.filter((v) => v !== undefined)).pipe(
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
                    sql`SELECT thread_id, tail_sequence, tail_digest, producer_epoch FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
                  ).pipe(Effect.flatMap((r) => decode(Schema.Array(Header), r)));

                  for (const name of [
                    "effect_agent_submissions",
                    "effect_agent_canonical_records",
                    "effect_agent_record_runs",
                    "effect_agent_canonical_batches",
                    "effect_agent_checkpoints",
                    "effect_agent_worker_stops",
                    "effect_agent_attempts",
                    "effect_agent_work_entries",
                    "effect_agent_journal_ranges",
                    "effect_agent_import_fifo",
                    "effect_agent_input_intervals",
                    "effect_agent_settlement_spans",
                    "effect_agent_import_delivery_visits",
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
                    headers.some(
                      (h) => h.tail_sequence !== 0 || h.tail_digest !== EMPTY_TAIL_DIGEST,
                    )
                  )
                    return yield* reject("target-not-empty", "Destination Thread is not empty");

                  const counters = yield* query(
                    sql`SELECT revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count FROM ${table("effect_agent_transfer_state")} WHERE thread_id=${threadId}`,
                  ).pipe(
                    Effect.flatMap((r) =>
                      decode(
                        Schema.Array(
                          Schema.Struct({
                            revision: SqlInteger,
                            admissions_count: SqlInteger,
                            aborts_count: SqlInteger,
                            approvals_count: SqlInteger,
                            resolutions_count: SqlInteger,
                            deliveries_count: SqlInteger,
                          }),
                        ),
                        r,
                      ),
                    ),
                  );

                  const catalogue = yield* query(
                    sql`SELECT state, through_sequence, entry_count, reporting FROM ${table("effect_agent_work_index")} WHERE thread_id=${threadId}`,
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
                    counters.length > 1 ||
                    counters.some((c) => Object.values(c).some((v) => v !== 0)) ||
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
                  destinationEpoch = (headers[0]?.producer_epoch ?? -1) + 1;

                  const now = DateTime.formatIso(
                    DateTime.makeUnsafe(yield* Clock.currentTimeMillis),
                  );

                  if (headers.length === 0)
                    yield* query(
                      sql`INSERT INTO ${table("effect_agent_threads")} (thread_id, created_at, tail_sequence, tail_digest, producer_epoch) VALUES (${threadId}, ${now}, 0, ${EMPTY_TAIL_DIGEST}, ${destinationEpoch})`,
                    );
                  yield* query(sql`INSERT INTO ${table("effect_agent_transfer_state")} (thread_id, revision, admissions_count, aborts_count, approvals_count, resolutions_count, deliveries_count)
          VALUES (${threadId}, 0, 0, 0, 0, 0, 0) ON CONFLICT DO NOTHING`);
                  yield* work.initialize(threadId);
                }
                for (const batch of page.batches) {
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
                      sql`SELECT batch_id FROM ${table("effect_agent_canonical_batches")} WHERE thread_id=${threadId} AND batch_id=${batch.batch.batchId} LIMIT 1`,
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
                  const batchHeaderJson = canonicalBatchHeaderJson(batch.batch);

                  yield* query(sql`INSERT INTO ${table("effect_agent_canonical_batches")} (thread_id, batch_id, first_sequence, last_sequence, batch_digest, tail_digest, batch_header_json)
          VALUES (${threadId}, ${batch.batch.batchId}, ${batch.firstSequence}, ${batch.lastSequence}, ${batch.tailDigest}, ${batch.tailDigest}, ${batchHeaderJson})`);
                  const captured = [];

                  for (const [index, record] of batch.batch.records.entries()) {
                    if (!(record instanceof ExportedRecord))
                      return yield* invalidThreadArchive(
                        "Imported record has no owned wire",
                        threadId,
                      );
                    if (
                      (yield* query(
                        sql`SELECT record_id FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${threadId} AND record_id=${record.recordId} LIMIT 1`,
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

                    yield* query(
                      sql`INSERT INTO ${table("effect_agent_canonical_records")} ${sql.insert({
                        thread_id: threadId,
                        sequence: batch.firstSequence + index,
                        record_id: record.recordId,
                        batch_id: batch.batch.batchId,
                        record_json: batch.recordJson[index],
                        ...metadata.columns,
                      })}`,
                    );
                    for (const runId of metadata.runIds)
                      yield* query(
                        sql`INSERT INTO ${table("effect_agent_record_runs")} (thread_id, run_id, sequence) VALUES (${threadId}, ${runId}, ${batch.firstSequence + index})`,
                      );
                    captured.push({
                      recordId: record.recordId,
                      recordBytes: utf8ByteLength(batch.recordJson[index]!),
                      canonical: record,
                      wire: record.wire,
                      recordJson: batch.recordJson[index]!,
                      readMetadata: metadata,
                    });
                  }
                  yield* ranges.append(
                    {
                      threadId,
                      batchId: batch.batch.batchId,
                      batchJson: batch.batchJson,
                      batchHeaderJson,
                      batchBytes: utf8ByteLength(batch.batchJson),
                      batchDigest: batch.tailDigest,
                      expectedTailSequence: CanonicalSequence.make(batch.firstSequence - 1),
                      expectedTailDigest: batch.previousTailDigest,
                      tailDigest: batch.tailDigest,
                      producerEpoch: ProducerEpoch.make(destinationEpoch),
                      records: captured,
                      progress: [],
                    },
                    batch.firstSequence,
                    batch.lastSequence,
                  );
                  yield* work.apply(threadId, batch.firstSequence, batch.batch.records);
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
                    a.messageAdmission === undefined
                      ? undefined
                      : canonicalJson(a.messageAdmission),
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
                    sql`r.record_id=${submissionSettlementRecordId(fact.submissionId)}`,
                    1,
                  );

                  const terminal =
                    terminalRows[0] === undefined ? undefined : yield* envelope(terminalRows[0]);

                  const settled = terminal?.record.payload;

                  if (settled?._tag !== "SubmissionSettled") {
                    if (fact.parentLinkage !== undefined || fact.workerAdmission !== undefined)
                      return yield* reject(
                        "unsupported-obligations",
                        "Live foreign linked admission needs its owning store",
                      );
                    yield* admissionFence
                      .check(fact)
                      .pipe(
                        Effect.mapError((e) =>
                          reject(
                            e.reason === "occupied"
                              ? "admission-policy-conflict"
                              : "admission-policy-unavailable",
                            `Destination admission policy ${e.code} ${e.reason}`,
                          ),
                        ),
                      );
                    if (
                      fact.admissionGroup !== undefined &&
                      (yield* query(
                        sql`SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE thread_id=${threadId} AND state<>'settled' AND admission_group=${fact.admissionGroup} LIMIT 1`,
                      )).length > 0
                    )
                      return yield* reject(
                        "admission-policy-conflict",
                        "Active admission group conflicts at destination",
                      );
                  }
                  yield* query(
                    sql`INSERT INTO ${table("effect_agent_submissions")} ${sql.insert({
                      submission_id: a.submissionId,
                      thread_id: threadId,
                      queue_sequence: a.queueSequence,
                      principal: a.principal,
                      idempotency_key: a.idempotencyKey,
                      agent_id: a.agentId,
                      agent_digests_json: canonicalJson(a.agentDigests),
                      deployment_id: a.deploymentId,
                      input_json: canonicalJson(a.inputPayload),
                      input_digest: a.inputDigest,
                      receipt_id: a.receiptId,
                      state: settled?._tag === "SubmissionSettled" ? "settled" : "admitted",
                      settled_outcome:
                        settled?._tag === "SubmissionSettled" ? settled.outcome : null,
                      settled_record_id:
                        settled?._tag === "SubmissionSettled"
                          ? (terminal?.record.recordId ?? null)
                          : null,
                      finalized_at:
                        settled?._tag === "SubmissionSettled" && terminal !== undefined
                          ? DateTime.formatIso(terminal.record.createdAt)
                          : null,
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

                  const a = yield* encode(
                    ApprovalDecisionIntent,
                    ApprovalDecisionIntent.make(fact),
                  );

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
                  yield* query(sql`INSERT INTO ${table("effect_agent_message_deliveries")} (owner_thread_id, message_id, version, state, deadline_at_millis, record_json ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, read_metadata` })})
          VALUES (${threadId}, ${delivery.key.messageId}, ${delivery.version}, ${delivery.status}, ${messageDeliveryDeadline(delivery)}, ${text} ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, ${messageDeliveryMetadata(delivery)}::jsonb` })})`);
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
                sql`UPDATE ${table("effect_agent_threads")} SET tail_sequence=${result.tailSequence}, tail_digest=${result.tailDigest} WHERE thread_id=${id}`,
              );
              const reader = yield* ThreadImportReader;
              const readAdmission = reader.admission;
              // Exact Run selections include continuations as well as the sparse record_runs membership.
              let afterRun: string | undefined;

              while (true) {
                const found =
                  yield* query(sql`SELECT run_id FROM ${table("effect_agent_canonical_records")} ${seekIndex("effect_agent_records_run_identity")}
        WHERE thread_id=${id} AND run_id IS NOT NULL
        ${afterRun === undefined ? sql`` : sql`AND run_id>${afterRun}`} GROUP BY run_id ORDER BY run_id LIMIT 1`).pipe(
                    Effect.flatMap((r) =>
                      decode(Schema.Array(Schema.Struct({ run_id: Schema.String })), r),
                    ),
                  );

                const next = found[0];

                if (next === undefined) break;
                const runId = yield* json(RunId, next.run_id);

                yield* verifyRunContinuations(yield* reader.runRecords(runId), {
                  verifyContext: false,
                }).pipe(Effect.mapError((e) => invalidThreadArchive(e.message, id)));
                afterRun = next.run_id;
              }
              let afterQueue = -1;

              while (true) {
                const selected = yield* admissionRows(id, sql`s.queue_sequence>${afterQueue}`, 1);
                const row = selected[0];

                if (row === undefined) break;
                const a = yield* admission(row);
                const rebuilt = yield* rebuildImportedSubmission(a);

                if (workerSeal !== undefined && rebuilt.state !== "settled")
                  return yield* reject(
                    "unsupported-obligations",
                    "Sealed worker retains outstanding admissions",
                  );
                if (
                  rebuilt.settlement !== undefined &&
                  workerTerminalFromRecord(
                    SubmissionSnapshot.make({ ...a, state: rebuilt.state }),
                    rebuilt.settlement,
                  ) === workerSeal?.terminal
                )
                  terminalEvidence = true;
                const settled = rebuilt.settlement?.payload;

                const suspension =
                  rebuilt.state === "suspended" && rebuilt.suspension !== undefined
                    ? yield* encode(SuspensionSnapshot, rebuilt.suspension)
                    : undefined;

                yield* checkValues([
                  suspension === undefined ? undefined : canonicalJson(suspension.reason),
                  canonicalJson(rebuilt.unknownToolCallIds),
                ]);
                yield* query(
                  sql`UPDATE ${table("effect_agent_submissions")} SET ${sql.update({
                    state: rebuilt.state,
                    settled_outcome: settled?._tag === "SubmissionSettled" ? settled.outcome : null,
                    settled_record_id: rebuilt.settlement?.recordId ?? null,
                    finalized_at:
                      rebuilt.settlement === undefined
                        ? null
                        : DateTime.formatIso(rebuilt.settlement.createdAt),
                    ready_at: rebuilt.state === "ready" ? row.created_at : null,
                    input_applied_record_id: rebuilt.inputApplied?.recordId ?? null,
                    input_applied_sequence: rebuilt.inputApplied?.sequence ?? null,
                    joined_host_submission_id: rebuilt.joinedHostSubmissionId ?? null,
                    suspended_reason_json:
                      suspension === undefined ? null : canonicalJson(suspension.reason),
                    suspended_at: suspension?.suspendedAt ?? null,
                    unknown_reason: rebuilt.state === "unknown" ? "ownership-lost" : null,
                    unknown_tool_call_ids_json:
                      rebuilt.state === "unknown"
                        ? canonicalJson(rebuilt.unknownToolCallIds)
                        : null,
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

              const conflict = yield* query(
                sql`SELECT admission_group FROM ${table("effect_agent_submissions")} WHERE thread_id=${id} AND state<>'settled' AND admission_group IS NOT NULL GROUP BY admission_group HAVING COUNT(*)>1 LIMIT 1`,
              );

              if (conflict.length > 0)
                return yield* reject(
                  "admission-policy-conflict",
                  "Imported active admissions conflict in a destination group",
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
                    return yield* invalidThreadArchive(
                      "Canonical inputs violate admission FIFO",
                      id,
                    );
                  lastInputQueue = admitted.queueSequence;
                  yield* query(sql`INSERT INTO ${table("effect_agent_import_fifo")}
                  (thread_id, submission_id, queue_sequence)
                  VALUES (${id}, ${admitted.submissionId}, ${admitted.queueSequence})`);
                }
                if (payload._tag === "SubmissionSettled") {
                  const admitted = yield* readAdmission(payload.submissionId);

                  if (admitted === undefined)
                    return yield* invalidThreadArchive("Settlement has no immutable admission", id);
                  // Only earlier executed inputs still open at this settlement can be predecessors.
                  yield* verifyImportedSettlementOrder(admitted);
                  yield* query(sql`DELETE FROM ${table("effect_agent_import_fifo")}
                  WHERE thread_id=${id} AND submission_id=${payload.submissionId}`);
                }
                afterSequence = row.sequence;
              }
              let afterDelivery: string | undefined;
              let pending = 0;
              let pendingUpdates = 0;

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

                if (
                  utf8ByteLength(canonicalJson(encoded.envelope)) > deliveryLimits.maxEnvelopeBytes
                )
                  return yield* reject(
                    "unsupported-capacity",
                    "Delivery envelope exceeds destination capacity",
                  );
                if (!["processed", "refused"].includes(d.status)) {
                  if (isWorkerUpdateDelivery(d)) pendingUpdates++;
                  else pending++;
                }
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
                    yield* query(sql`SELECT s.* FROM ${table("effect_agent_submissions")} s WHERE thread_id=${d.receipt.threadId}
          AND submission_id=${d.receipt.submissionId} LIMIT 1`).pipe(
                      Effect.flatMap((r) => decode(Schema.Array(AdmissionRow), r)),
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
                      sql`r.record_id=${submissionSettlementRecordId(destination.submissionId)}`,
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
              if (
                pending > deliveryLimits.maxPendingPerOwner ||
                pendingUpdates > (deliveryLimits.maxPendingUpdatesPerOwner ?? 32)
              )
                return yield* reject(
                  "unsupported-capacity",
                  "Imported pending deliveries exceed destination capacity",
                );

              const staged =
                yield* query(sql`SELECT admissions_count AS admissions, aborts_count AS aborts, approvals_count AS approvals,
          resolutions_count AS resolutions, deliveries_count AS deliveries FROM ${table("effect_agent_transfer_state")} WHERE thread_id=${id}`).pipe(
                  Effect.flatMap((r) =>
                    decode(
                      Schema.Array(
                        Schema.Struct({
                          admissions: SqlInteger,
                          aborts: SqlInteger,
                          approvals: SqlInteger,
                          resolutions: SqlInteger,
                          deliveries: SqlInteger,
                        }),
                      ),
                      r,
                    ),
                  ),
                );

              if (
                staged.length !== 1 ||
                canonicalJson(staged[0]!) !== canonicalJson(progress.counts)
              )
                return yield* invalidThreadArchive(
                  "Staged native fact counts differ from the captured manifest",
                  id,
                );
              const obligations = yield* checkForeignWork(id);

              if (obligations.length > 0)
                return yield* reject(
                  "unsupported-obligations",
                  "Staged canonical history retains unsupported live foreign obligations",
                );
              const epoch = Math.max(destinationEpoch, progress.producerEpoch);

              if (!Number.isSafeInteger(epoch) || epoch >= Number.MAX_SAFE_INTEGER)
                return yield* reject(
                  "unsupported-capacity",
                  "No fresh producer generation is available",
                );
              if (workerSeal !== undefined)
                yield* query(
                  sql`INSERT INTO ${table("effect_agent_worker_stops")} (thread_id, terminal) VALUES (${id}, ${workerSeal.terminal ?? null})`,
                );
              yield* query(
                sql`DELETE FROM ${table("effect_agent_import_fifo")} WHERE thread_id=${id}`,
              );
              yield* query(
                sql`DELETE FROM ${table("effect_agent_import_delivery_visits")} WHERE thread_id=${id}`,
              );
              yield* query(
                sql`UPDATE ${table("effect_agent_threads")} SET tail_sequence=${result.tailSequence}, tail_digest=${result.tailDigest}, producer_epoch=${epoch} WHERE thread_id=${id}`,
              );
              if (options.afterImport !== undefined)
                yield* options
                  .afterImport({ result })
                  .pipe(Effect.mapError((e) => failure("publish imported Thread", e)));

              return result;
            }).pipe(Effect.provide(readerLayer(result.threadId, result.tailSequence, "import")));
          }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
        );
      }),
    );

  const verify = Effect.fnUntraced(function* (request: {
    readonly threadId: ThreadId;
    readonly requireAllSettled?: boolean;
  }) {
    return yield* options.read(
      Effect.gen(function* () {
        const first = yield* exportPage({ threadId: request.threadId });
        const id = first.threadId;

        yield* ranges.verifyThread(id);

        const LedgerRow = Schema.Struct({
          ...AdmissionRow.fields,
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
                sql`SELECT s.* FROM ${table("effect_agent_submissions")} s WHERE s.thread_id=${id} AND ${predicate(after)} ORDER BY s.queue_sequence LIMIT 1`,
              ).pipe(Effect.flatMap((r) => decode(Schema.Array(LedgerRow), r)));

              const row = rows[0];

              return [
                row === undefined ? [] : [yield* snapshot(row)],
                row === undefined ? Option.none<number>() : Option.some(row.queue_sequence),
              ] as const;
            }),
          );

        const submissions = scan((after) => sql`s.queue_sequence>${after}`);

        const runs = Stream.paginate<string | undefined, RunId, ThreadStoreError>(
          undefined,
          (after) =>
            Effect.gen(function* () {
              const selected =
                yield* query(sql`SELECT run_id FROM ${table("effect_agent_canonical_records")} ${seekIndex("effect_agent_records_run_identity")}
          WHERE thread_id=${id} AND sequence<=${first.tailSequence} AND run_id IS NOT NULL
          ${after === undefined ? sql`` : sql`AND run_id>${after}`} GROUP BY run_id ORDER BY run_id LIMIT 1`).pipe(
                  Effect.flatMap((r) =>
                    decode(Schema.Array(Schema.Struct({ run_id: Schema.String })), r),
                  ),
                );

              const row = selected[0];

              return [
                row === undefined ? [] : [yield* json(RunId, row.run_id)],
                row === undefined ? Option.none<string | undefined>() : Option.some(row.run_id),
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
          Stream.provideService(ThreadExportSource, { export: exportPage }),
          Stream.tap((page) =>
            Effect.gen(function* () {
              if (page.snapshotId !== first.snapshotId)
                return yield* failure("verify snapshot", "Thread changed during verification");
              for (const entry of page.records)
                yield* intervals.verify(id, entry.sequence, entry.record);
            }),
          ),
        );

        const result = yield* verifyThreadInvariants({
          threadId: id,
          pages,
          submissions,
          runs,
          ...(checkpoint === undefined ? {} : { checkpoint }),
          checkpointsSupported: true,
          ...(request.requireAllSettled === undefined
            ? {}
            : { requireAllSettled: request.requireAllSettled }),
        }).pipe(Effect.provide(readerLayer(id, first.tailSequence, "verify")));

        const last = yield* exportPage({ threadId: id });

        if (last.snapshotId !== first.snapshotId)
          return yield* failure("verify snapshot", "Thread changed during verification");

        return result;
      }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
    );
  });

  return {
    export: exportThread,
    importer: ThreadImport.of({ import: importThread }),
    verification: { verify },
  };
});
