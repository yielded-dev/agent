import { EMPTY_TAIL_DIGEST, canonicalJson } from "@yielded/agent/digest";
import { decodeExportRecord } from "@yielded/agent/record-format";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  Digest,
  ObservationOffset,
  PersistedJson,
} from "@yielded/agent/records";
import {
  ApprovalDecisionIntent,
  AbortIntent,
  SuspensionSnapshot,
  UnknownResolutionIntent,
} from "@yielded/agent/submission-ledger";
import {
  ThreadImport,
  ThreadImportRejected,
  prepareThreadImport,
  type ThreadImportRequest,
  type PreparedThreadImport,
} from "@yielded/agent/thread-import";
import {
  MAX_THREAD_EXPORT_RECORDS,
  ThreadAdmission,
  ThreadExport,
  ThreadExportBatch,
  ThreadNotMaterialized,
  ThreadStoreError,
  ThreadExportRequest,
} from "@yielded/agent/thread-store";
import { Clock, Crypto, DateTime, Effect, Schema } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";
import { canonicalRecordMetadata } from "./SqlThreadNativeReads.ts";

export interface SqlThreadImportOptions<E extends { readonly message: string }> {
  readonly namespace?: string;
  readonly offsetPrefix: string;
  readonly format?: string;
  readonly maxValueBytes?: number;
  readonly afterThreadRead?: Effect.Effect<void, E>;
  readonly afterImport?: (
    prepared: PreparedThreadImport,
  ) => Effect.Effect<void, E | ThreadStoreError>;
  readonly read: <A>(
    body: Effect.Effect<A, ThreadStoreError | ThreadNotMaterialized>,
  ) => Effect.Effect<A, ThreadStoreError | ThreadNotMaterialized | E>;
  readonly write: <A>(
    body: Effect.Effect<A, ThreadStoreError | ThreadImportRejected>,
  ) => Effect.Effect<A, ThreadStoreError | ThreadImportRejected | E>;
}

const failure = (operation: string, cause: unknown) =>
  ThreadStoreError.make({
    operation,
    message: typeof cause === "string" ? cause : `Unable to ${operation}`,
    cause,
  });

const Header = Schema.Struct({
  thread_id: Schema.String,
  tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  tail_digest: Digest,
  producer_epoch: SqlInteger,
});

const Row = Schema.Struct({
  batch_id: Schema.String,
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  record_json: Schema.String,
});

const Batch = Schema.Struct({
  batch_id: Schema.String,
  batch_json: Schema.String,
  first_sequence: SqlInteger,
  last_sequence: SqlInteger,
});

const WireBatch = Schema.Struct({
  ...ThreadExportBatch.fields,
  records: Schema.NonEmptyArray(PersistedJson).check(Schema.isMaxLength(256)),
});

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
  submission_id: Schema.String,
  author: Schema.String,
  reason: Schema.String,
  requested_at: Schema.String,
});

const ApprovalRow = Schema.Struct({
  submission_id: Schema.String,
  tool_call_id: Schema.String,
  decision: Schema.String,
  resolver: Schema.String,
  reason: Schema.String,
  decided_at: Schema.String,
});

const ResolutionRow = Schema.Struct({
  submission_id: Schema.String,
  tool_call_id: Schema.String,
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

/** Shared log transfer; callers supply their actual owner transaction, never a payload migration. */
export const makeSqlThreadImport = Effect.fnUntraced(function* <
  E extends { readonly message: string },
>(options: SqlThreadImportOptions<E>) {
  const sql = yield* SqlClient;
  const crypto = yield* Crypto.Crypto;
  const { table, execute } = yield* makeSqlQuery(options.namespace);

  const query = <A extends object>(statement: ReturnType<typeof sql<A>>) =>
    execute(statement).pipe(
      Effect.mapError((cause) => failure("read or write archive storage", cause)),
    );

  const format = options.format ?? CURRENT_RECORD_FORMAT;

  const exportThread = Effect.fn("SqlThreadImport.export")(function* (input: ThreadExportRequest) {
    const request = yield* Schema.decodeEffect(Schema.toType(ThreadExportRequest))(input).pipe(
      Effect.mapError((cause) => failure("validate Thread export", cause)),
    );

    return yield* options
      .read(
        Effect.gen(function* () {
          const headers = yield* decode(
            Schema.Array(Header),
            yield* query(
              sql`SELECT thread_id, tail_sequence, tail_digest, producer_epoch FROM ${table("effect_agent_threads")} WHERE thread_id=${request.threadId}`,
            ),
          );

          if (headers.length === 0)
            return yield* ThreadNotMaterialized.make({ threadId: request.threadId });
          const header = headers[0];

          if (headers.length !== 1 || header.tail_sequence > MAX_THREAD_EXPORT_RECORDS)
            return yield* failure("export bounded Thread", "Invalid Thread header");
          if (options.afterThreadRead !== undefined)
            yield* options.afterThreadRead.pipe(
              Effect.mapError((cause) => failure("capture export tail", cause)),
            );

          const rows = yield* decode(
            Schema.Array(Row),
            yield* query(
              sql`SELECT batch_id, sequence, record_json FROM ${table("effect_agent_canonical_records")} WHERE thread_id=${request.threadId} ORDER BY sequence LIMIT ${MAX_THREAD_EXPORT_RECORDS + 1}`,
            ),
          );

          if (rows.some((row) => row.sequence > header.tail_sequence))
            return yield* failure(
              "export complete Thread",
              "Canonical records exist beyond the captured thread tail.",
            );
          if (
            rows.length !== header.tail_sequence ||
            rows.some((row, index) => row.sequence !== index + 1)
          )
            return yield* failure("export complete Thread", "Non-contiguous canonical prefix");

          const storedBatches = yield* decode(
            Schema.Array(Batch),
            yield* query(
              sql`SELECT batch_id, batch_json, first_sequence, last_sequence FROM ${table("effect_agent_canonical_batches")} WHERE thread_id=${request.threadId} ORDER BY first_sequence LIMIT ${MAX_THREAD_EXPORT_RECORDS + 1}`,
            ),
          );

          const batches: Array<typeof ThreadExportBatch.Type> = [];
          let cursor = 0;

          for (const stored of storedBatches) {
            const batch = yield* json(WireBatch, stored.batch_json);

            if (
              stored.batch_id !== batch.batchId ||
              stored.first_sequence !== cursor + 1 ||
              stored.last_sequence !== cursor + batch.records.length
            )
              return yield* failure("export canonical batches", "Batch identity or order mismatch");
            for (const record of batch.records) {
              const row = rows[cursor++];

              if (
                row === undefined ||
                row.batch_id !== batch.batchId ||
                canonicalJson(yield* json(PersistedJson, row.record_json)) !== canonicalJson(record)
              )
                return yield* failure("export canonical batches", "Batch and record rows disagree");
            }
            batches.push(
              ThreadExportBatch.make({ batchId: batch.batchId, producerId: batch.producerId }),
            );
          }
          if (cursor !== rows.length)
            return yield* failure("export canonical batches", "Missing batch metadata");

          const records = yield* Effect.forEach(
            rows,
            Effect.fnUntraced(function* (row) {
              const record = yield* decodeExportRecord(
                format,
                yield* json(PersistedJson, row.record_json),
              ).pipe(Effect.mapError((cause) => failure("decode export record", cause)));

              return CanonicalRecordEnvelope.make({
                threadId: request.threadId,
                batchId: yield* decode(CanonicalRecordEnvelope.fields.batchId, row.batch_id),
                sequence: row.sequence,
                offset: yield* decode(
                  ObservationOffset,
                  `${options.offsetPrefix}${encodeURIComponent(request.threadId)}:${row.sequence}`,
                ),
                record,
              });
            }),
          );

          const admissionRows = yield* decode(
            Schema.Array(AdmissionRow),
            yield* query(
              sql`SELECT submission_id, thread_id, receipt_id, queue_sequence, principal, idempotency_key, agent_id, agent_digests_json, deployment_id, input_json, input_digest, created_at, parent_submission_id, parent_tool_call_id, worker_admission_json, message_admission_json, admission_group, admission_fence_json FROM ${table("effect_agent_submissions")} WHERE thread_id=${request.threadId} ORDER BY queue_sequence LIMIT ${MAX_THREAD_EXPORT_RECORDS + 1}`,
            ),
          );

          if (admissionRows.length > MAX_THREAD_EXPORT_RECORDS)
            return yield* failure("export admissions", "Admission bound exceeded");

          const admissions = yield* Effect.forEach(
            admissionRows,
            Effect.fnUntraced(function* (row) {
              if ((row.parent_submission_id === null) !== (row.parent_tool_call_id === null))
                return yield* failure("export admission", "Incomplete parent identity");

              return yield* decode(ThreadAdmission, {
                submissionId: row.submission_id,
                threadId: row.thread_id,
                receiptId: row.receipt_id,
                queueSequence: row.queue_sequence,
                principal: row.principal,
                idempotencyKey: row.idempotency_key,
                agentId: row.agent_id,
                agentDigests: yield* json(PersistedJson, row.agent_digests_json),
                deploymentId: row.deployment_id,
                inputPayload: yield* json(PersistedJson, row.input_json),
                inputDigest: row.input_digest,
                createdAt: row.created_at,
                ...(row.parent_submission_id === null
                  ? {}
                  : {
                      parentLinkage: {
                        parentSubmissionId: row.parent_submission_id,
                        parentToolCallId: row.parent_tool_call_id,
                      },
                    }),
                ...(row.worker_admission_json === null
                  ? {}
                  : { workerAdmission: yield* json(PersistedJson, row.worker_admission_json) }),
                ...(row.message_admission_json === null
                  ? {}
                  : { messageAdmission: yield* json(PersistedJson, row.message_admission_json) }),
                ...(row.admission_group === null ? {} : { admissionGroup: row.admission_group }),
                ...(row.admission_fence_json === null
                  ? {}
                  : { admissionFence: yield* json(PersistedJson, row.admission_fence_json) }),
              });
            }),
          );

          const abortRows = yield* decode(
            Schema.Array(AbortRow),
            yield* query(
              sql`SELECT a.submission_id, a.author, a.reason, a.requested_at FROM ${table("effect_agent_abort_intents")} a JOIN ${table("effect_agent_submissions")} s ON s.submission_id=a.submission_id WHERE s.thread_id=${request.threadId} LIMIT ${MAX_THREAD_EXPORT_RECORDS + 1}`,
            ),
          );

          const approvalRows = yield* decode(
            Schema.Array(ApprovalRow),
            yield* query(
              sql`SELECT a.submission_id, a.tool_call_id, a.decision, a.resolver, a.reason, a.decided_at FROM ${table("effect_agent_approval_decisions")} a JOIN ${table("effect_agent_submissions")} s ON s.submission_id=a.submission_id WHERE s.thread_id=${request.threadId} LIMIT ${MAX_THREAD_EXPORT_RECORDS + 1}`,
            ),
          );

          const resolutionRows = yield* decode(
            Schema.Array(ResolutionRow),
            yield* query(
              sql`SELECT a.submission_id, a.tool_call_id, a.author, a.reason, a.resolution_json, a.resolved_at FROM ${table("effect_agent_unknown_resolutions")} a JOIN ${table("effect_agent_submissions")} s ON s.submission_id=a.submission_id WHERE s.thread_id=${request.threadId} LIMIT ${MAX_THREAD_EXPORT_RECORDS + 1}`,
            ),
          );

          if (
            [abortRows, approvalRows, resolutionRows].some(
              (rows) => rows.length > MAX_THREAD_EXPORT_RECORDS,
            )
          )
            return yield* failure("export accepted commands", "Accepted command bound exceeded");

          const commands = {
            aborts: yield* Effect.forEach(abortRows, (row) =>
              decode(AbortIntent, {
                submissionId: row.submission_id,
                author: row.author,
                reason: row.reason,
                requestedAt: row.requested_at,
              }),
            ),
            approvals: yield* Effect.forEach(approvalRows, (row) =>
              decode(ApprovalDecisionIntent, {
                submissionId: row.submission_id,
                toolCallId: row.tool_call_id,
                decision: row.decision,
                resolver: row.resolver,
                reason: row.reason,
                decidedAt: row.decided_at,
              }),
            ),
            resolutions: yield* Effect.forEach(
              resolutionRows,
              Effect.fnUntraced(function* (row) {
                return yield* decode(UnknownResolutionIntent, {
                  submissionId: row.submission_id,
                  toolCallId: row.tool_call_id,
                  author: row.author,
                  reason: row.reason,
                  resolution: yield* json(PersistedJson, row.resolution_json),
                  resolvedAt: row.resolved_at,
                });
              }),
            ),
          };

          const obligations = yield* query(
            sql`SELECT reservation_id FROM ${table("effect_agent_child_reservations")} WHERE parent_submission_id IN (SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE thread_id=${request.threadId}) LIMIT 1`,
          );

          const stop = yield* query(
            sql`SELECT thread_id FROM ${table("effect_agent_worker_stops")} WHERE thread_id=${request.threadId} LIMIT 1`,
          );

          const deliveries = yield* query(
            sql`SELECT message_id FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${request.threadId} LIMIT 1`,
          );

          const externalObligations = [
            ...(obligations.length > 0 ? ["child" as const] : []),
            ...(stop.length > 0 ? ["worker" as const] : []),
            ...(deliveries.length > 0 ? ["message-delivery" as const] : []),
          ];

          return ThreadExport.make({
            format,
            threadId: request.threadId,
            tailSequence: header.tail_sequence,
            tailDigest: header.tail_digest,
            records,
            batches,
            admissions,
            commands,
            ...(externalObligations.length === 0 ? {} : { externalObligations }),
          });
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          Schema.is(ThreadNotMaterialized)(error)
            ? error
            : Schema.is(ThreadStoreError)(error)
              ? error
              : failure("export Thread", error),
        ),
      );
  });

  const importThread = Effect.fn("SqlThreadImport.import")(function* (
    request: ThreadImportRequest,
  ) {
    const prepared = yield* prepareThreadImport(request).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
    );

    const { threadId } = prepared.result;
    const maxBytes = options.maxValueBytes ?? 16 * 1024 * 1024;

    for (const batch of prepared.batches) {
      if (
        [batch.batchJson, ...batch.recordJson].some(
          (value) => new TextEncoder().encode(value).byteLength > maxBytes,
        )
      )
        return yield* ThreadImportRejected.make({
          threadId,
          reason: "invalid-archive",
          message: "A canonical value exceeds this adapter's storage bound",
        });
    }
    const now = yield* Clock.currentTimeMillis;

    return yield* options
      .write(
        Effect.gen(function* () {
          const headers = yield* decode(
            Schema.Array(Header),
            yield* query(
              sql`SELECT thread_id, tail_sequence, tail_digest, producer_epoch FROM ${table("effect_agent_threads")} WHERE thread_id=${threadId}`,
            ),
          );

          for (const name of [
            "effect_agent_submissions",
            "effect_agent_canonical_records",
            "effect_agent_canonical_batches",
            "effect_agent_checkpoints",
            "effect_agent_recovery_checkpoints",
            "effect_agent_worker_stops",
            "effect_agent_attempts",
          ]) {
            if (
              (yield* query(
                sql`SELECT thread_id FROM ${table(name)} WHERE thread_id=${threadId} LIMIT 1`,
              )).length !== 0
            )
              return yield* ThreadImportRejected.make({
                threadId,
                reason: "target-not-empty",
                message:
                  "The destination Thread already contains records, admissions, or derivatives",
              });
          }
          if (
            (yield* query(
              sql`SELECT owner_thread_id FROM ${table("effect_agent_message_deliveries")} WHERE owner_thread_id=${threadId} LIMIT 1`,
            )).length > 0
          )
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "target-not-empty",
              message: "The destination Thread already owns message deliveries",
            });
          if (
            headers.length > 1 ||
            headers.some(
              (header) => header.tail_sequence !== 0 || header.tail_digest !== EMPTY_TAIL_DIGEST,
            )
          )
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "target-not-empty",
              message: "The destination Thread has a committed tail",
            });
          for (const { admission } of prepared.submissions) {
            const existing = yield* query(
              sql`SELECT submission_id FROM ${table("effect_agent_submissions")} WHERE submission_id=${admission.submissionId} OR receipt_id=${admission.receiptId} LIMIT 1`,
            );

            if (existing.length > 0)
              return yield* ThreadImportRejected.make({
                threadId,
                reason: "target-not-empty",
                message:
                  "The destination store already retains an imported Submission or Receipt identity",
              });
          }
          // The write itself arbitrates concurrent fresh imports on PostgreSQL. Conflicts roll back.
          const epoch = Math.max(prepared.producerEpoch, (headers[0]?.producer_epoch ?? -1) + 1);

          if (!Number.isSafeInteger(epoch) || epoch >= Number.MAX_SAFE_INTEGER)
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "invalid-archive",
              message: "No fresh producer generation is available",
            });
          if (headers.length === 0)
            yield* query(
              sql`INSERT INTO ${table("effect_agent_threads")} (thread_id, created_at, tail_sequence, tail_digest, producer_epoch) VALUES (${threadId}, ${DateTime.formatIso(DateTime.makeUnsafe(now))}, ${prepared.result.tailSequence}, ${prepared.result.tailDigest}, ${epoch})`,
            );
          else
            yield* query(
              sql`UPDATE ${table("effect_agent_threads")} SET tail_sequence=${prepared.result.tailSequence}, tail_digest=${prepared.result.tailDigest}, producer_epoch=${epoch} WHERE thread_id=${threadId}`,
            );
          for (const batch of prepared.batches) {
            yield* query(
              sql`INSERT INTO ${table("effect_agent_canonical_batches")} (thread_id, batch_id, first_sequence, last_sequence, batch_digest, tail_digest, batch_json) VALUES (${threadId}, ${batch.batch.batchId}, ${batch.firstSequence}, ${batch.lastSequence}, ${batch.tailDigest}, ${batch.tailDigest}, ${batch.batchJson})`,
            );
            for (const [index, record] of batch.batch.records.entries())
              yield* query(
                sql`INSERT INTO ${table("effect_agent_canonical_records")} (thread_id, sequence, record_id, batch_id, record_json${sql.onDialectOrElse({ pg: () => sql`, read_metadata`, orElse: () => sql`` })}) VALUES (${threadId}, ${batch.firstSequence + index}, ${record.recordId}, ${batch.batch.batchId}, ${batch.recordJson[index]}${sql.onDialectOrElse({ pg: () => sql`, ${canonicalRecordMetadata(record)}::jsonb`, orElse: () => sql`` })})`,
              );
          }
          for (const rebuilt of prepared.submissions) {
            const admission = yield* encode(ThreadAdmission, rebuilt.admission);
            const settled = rebuilt.settlement?.payload;
            const settlement = settled?._tag === "SubmissionSettled" ? settled : undefined;

            const finalizedAt =
              rebuilt.settlement === undefined
                ? null
                : DateTime.formatIso(rebuilt.settlement.createdAt);

            const suspension =
              rebuilt.suspension === undefined
                ? null
                : yield* encode(SuspensionSnapshot, rebuilt.suspension);

            const unknown =
              rebuilt.unknownToolCallIds.length === 0
                ? null
                : canonicalJson(rebuilt.unknownToolCallIds);

            const readyAt = rebuilt.state === "admitted" ? null : admission.createdAt;

            const fence =
              admission.admissionFence === undefined
                ? null
                : canonicalJson(admission.admissionFence);

            yield* query(
              sql`INSERT INTO ${table("effect_agent_submissions")} (submission_id, thread_id, queue_sequence, principal, idempotency_key, agent_id, agent_digests_json, deployment_id, input_json, input_digest, receipt_id, state, settled_outcome, settled_record_id, finalized_at, created_at, ready_at, input_applied_record_id, input_applied_sequence, joined_host_submission_id, suspended_reason_json, suspended_at, unknown_reason, unknown_tool_call_ids_json, parent_submission_id, parent_tool_call_id, admission_group, admission_fence_json, worker_admission_json, message_admission_json) VALUES (${admission.submissionId}, ${threadId}, ${admission.queueSequence}, ${admission.principal}, ${admission.idempotencyKey}, ${admission.agentId}, ${canonicalJson(admission.agentDigests)}, ${admission.deploymentId}, ${canonicalJson(admission.inputPayload)}, ${admission.inputDigest}, ${admission.receiptId}, ${rebuilt.state}, ${settlement?.outcome ?? null}, ${rebuilt.settlement?.recordId ?? null}, ${finalizedAt}, ${admission.createdAt}, ${readyAt}, ${rebuilt.inputApplied?.recordId ?? null}, ${rebuilt.inputApplied?.sequence ?? null}, ${rebuilt.joinedHostSubmissionId ?? null}, ${suspension === null ? null : canonicalJson(suspension.reason)}, ${suspension?.suspendedAt ?? null}, ${unknown === null ? null : "ownership-lost"}, ${unknown}, NULL, NULL, ${admission.admissionGroup ?? null}, ${fence}, NULL, NULL)`,
            );
            if (rebuilt.abort !== undefined) {
              const abort = yield* encode(AbortIntent, rebuilt.abort);

              yield* query(
                sql`INSERT INTO ${table("effect_agent_abort_intents")} (submission_id, author, reason, requested_at, canonical_record_id) VALUES (${admission.submissionId}, ${abort.author}, ${abort.reason}, ${abort.requestedAt}, ${abort.canonicalRecordId ?? null})`,
              );
            }
            for (const intent of rebuilt.approvals) {
              const command = yield* encode(ApprovalDecisionIntent, intent);

              yield* query(
                sql`INSERT INTO ${table("effect_agent_approval_decisions")} (submission_id, tool_call_id, decision, resolver, reason, decided_at) VALUES (${admission.submissionId}, ${command.toolCallId}, ${command.decision}, ${command.resolver}, ${command.reason}, ${command.decidedAt})`,
              );
            }
            for (const intent of rebuilt.resolutions) {
              const command = yield* encode(UnknownResolutionIntent, intent);

              yield* query(
                sql`INSERT INTO ${table("effect_agent_unknown_resolutions")} (submission_id, tool_call_id, author, reason, resolution_json, resolved_at) VALUES (${admission.submissionId}, ${command.toolCallId}, ${command.author}, ${command.reason}, ${canonicalJson(command.resolution)}, ${command.resolvedAt})`,
              );
            }
          }
          if (options.afterImport !== undefined)
            yield* options
              .afterImport(prepared)
              .pipe(Effect.mapError((cause) => failure("rebuild imported projections", cause)));

          return prepared.result;
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          Schema.is(ThreadImportRejected)(error)
            ? error
            : Schema.is(ThreadStoreError)(error)
              ? error
              : failure("import Thread", error),
        ),
      );
  });

  return { export: exportThread, importer: ThreadImport.of({ import: importThread }) };
});
