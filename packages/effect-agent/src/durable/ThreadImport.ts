import { Context, Effect, Schema } from "effect";

import { SubmissionId, ThreadId, ToolCallId } from "../core/Identifiers.ts";
import { canonicalJson, digestJson, EMPTY_TAIL_DIGEST } from "./Digest.ts";
import { toolOperationStates, unresolvedToolOperations } from "./internal/tool-operations.ts";
import {
  decodeExportRecord,
  encodeImportedRecord,
  PREVIOUS_RECORD_FORMAT,
} from "./RecordFormat.ts";
import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  Digest,
  PersistedJson,
  ProducerEpoch,
  RecordEnvelope,
} from "./Records.ts";
import { runIdForSubmission } from "./RunJournal.ts";
import { validateCanonicalSettlement, validateJoinedSettlement } from "./SettlementPublisher.ts";
import {
  AbortIntent,
  ApprovalDecisionIntent,
  ApprovalPendingSuspension,
  InputAppliedMarker,
  SubmissionState,
  SuspensionSnapshot,
  UnknownResolutionIntent,
  submissionInputRecordId,
} from "./SubmissionLedger.ts";
import type { ThreadStoreError } from "./ThreadStore.ts";
import {
  MAX_THREAD_EXPORT_RECORDS,
  ThreadAdmission,
  ThreadCommands,
  ThreadExport,
  ThreadExportBatch,
} from "./ThreadStore.ts";

/** The archive's wire boundary deliberately defers payload decoding to its format's union. */
export const ThreadArchive = Schema.Struct({
  format: Schema.NonEmptyString,
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(
    Schema.Struct({
      ...CanonicalRecordEnvelope.fields,
      record: PersistedJson,
    }),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  batches: Schema.optionalKey(
    Schema.Array(ThreadExportBatch).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  ),
  admissions: Schema.optionalKey(
    Schema.Array(ThreadAdmission).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  ),
  commands: Schema.optionalKey(ThreadCommands),
  externalObligations: ThreadExport.fields.externalObligations,
});

export class ThreadImportRequest extends Schema.Class<ThreadImportRequest>(
  "@effect-agent/thread/ThreadImportRequest",
)({
  archive: ThreadArchive,
}) {}

export class ThreadImportResult extends Schema.Class<ThreadImportResult>(
  "@effect-agent/thread/ThreadImportResult",
)({
  threadId: ThreadId,
  format: Schema.String,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  recordCount: Schema.Natural,
  submissionCount: Schema.Natural,
}) {}

export class ThreadImportRejected extends Schema.TaggedError<ThreadImportRejected>()(
  "ThreadImportRejected",
  {
    threadId: Schema.optionalKey(ThreadId),
    reason: Schema.Literals([
      "unsupported-format",
      "invalid-archive",
      "target-not-empty",
      "unsupported-obligations",
    ]),
    message: Schema.String,
  },
) {}

/** Privileged, local storage-owner port. Import never creates a live claim or runs application code. */
export class ThreadImport extends Context.Service<
  ThreadImport,
  {
    readonly import: (
      request: ThreadImportRequest,
    ) => Effect.Effect<ThreadImportResult, ThreadImportRejected | ThreadStoreError>;
  }
>()("@effect-agent/thread/ThreadImport") {}

/** Rebuilt operational state for adapter installation, never an archive input. */
export class RebuiltSubmission extends Schema.Class<RebuiltSubmission>(
  "@effect-agent/thread/RebuiltSubmission",
)({
  admission: ThreadAdmission,
  state: SubmissionState,
  inputApplied: Schema.optionalKey(InputAppliedMarker),
  joinedHostSubmissionId: Schema.optionalKey(SubmissionId),
  settlement: Schema.optionalKey(RecordEnvelope),
  suspension: Schema.optionalKey(SuspensionSnapshot),
  unknownToolCallIds: Schema.Array(ToolCallId),
  abort: Schema.optionalKey(AbortIntent),
  approvals: Schema.Array(ApprovalDecisionIntent),
  resolutions: Schema.Array(UnknownResolutionIntent),
}) {}

export interface PreparedImportBatch {
  readonly batch: CanonicalBatch;
  readonly batchJson: string;
  readonly recordJson: ReadonlyArray<string>;
  readonly firstSequence: CanonicalSequence;
  readonly lastSequence: CanonicalSequence;
  readonly tailDigest: Digest;
}

export interface PreparedThreadImport {
  readonly result: ThreadImportResult;
  readonly producerEpoch: ProducerEpoch;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly batches: ReadonlyArray<PreparedImportBatch>;
  readonly submissions: ReadonlyArray<RebuiltSubmission>;
}

const sequence = Schema.decodeSync(CanonicalSequence);

const invalid = (message: string, threadId?: ThreadId) =>
  ThreadImportRejected.make({
    reason: "invalid-archive",
    message,
    ...(threadId === undefined ? {} : { threadId }),
  });

/** Validate the complete input and compute its replacement before acquiring a destination writer. */
export const prepareThreadImport = Effect.fnUntraced(function* (input: ThreadImportRequest) {
  // Own the wire and admission JSON before waiting for a writer. Callers must not be able to
  // change a fact after validation but before the adapter installs it.
  const codec = Schema.fromJsonString(ThreadImportRequest);

  const { archive } = yield* Schema.encodeEffect(codec)(input).pipe(
    Effect.flatMap(Schema.decodeEffect(codec)),
    Effect.mapError(() => invalid("The import request is malformed")),
  );

  const { threadId, format } = archive;

  if (format !== CURRENT_RECORD_FORMAT && format !== PREVIOUS_RECORD_FORMAT)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-format",
      message: `Unsupported record format ${format}`,
    });
  if ((archive.externalObligations?.length ?? 0) > 0)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-obligations",
      message: "The source retains child, worker, or delivery obligations in other owning stores",
    });
  if (archive.tailSequence !== archive.records.length)
    return yield* invalid("The archive is not a complete canonical prefix", threadId);
  const batches = archive.batches ?? [];

  if (archive.records.length > 0 && batches.length === 0)
    return yield* invalid(
      "Batch producer identities are missing; export the original store again",
      threadId,
    );
  const records: Array<CanonicalRecordEnvelope> = [];
  const prepared: Array<PreparedImportBatch> = [];
  const recordIds = new Set<string>();
  const batchIds = new Set<string>();
  let sourceDigest = EMPTY_TAIL_DIGEST;
  let targetDigest = EMPTY_TAIL_DIGEST;
  let index = 0;

  for (const batch of batches) {
    if (batchIds.has(batch.batchId)) return yield* invalid("Duplicate canonical batch", threadId);
    batchIds.add(batch.batchId);
    const wireRecords: Array<PersistedJson> = [];
    const currentRecords: Array<RecordEnvelope> = [];
    const encodedRecords: Array<PersistedJson> = [];
    const firstSequence = sequence(index + 1);

    while (archive.records[index]?.batchId === batch.batchId) {
      const entry = archive.records[index];

      if (entry.threadId !== threadId || entry.sequence !== index + 1)
        return yield* invalid(
          "Canonical identity or sequence disagrees with the archive",
          threadId,
        );

      const record = yield* decodeExportRecord(format, entry.record).pipe(
        Effect.mapError((error) => invalid(error.message, threadId)),
      );

      if (recordIds.has(record.recordId))
        return yield* invalid("Duplicate canonical record identity", threadId);
      recordIds.add(record.recordId);
      records.push(CanonicalRecordEnvelope.make({ ...entry, record }));
      wireRecords.push(entry.record);
      currentRecords.push(record);
      encodedRecords.push(
        yield* encodeImportedRecord(format, record).pipe(
          Effect.mapError(() => invalid("The upgraded record cannot be encoded", threadId)),
        ),
      );
      index++;
    }

    const canonical = yield* Schema.decodeUnknownEffect(
      Schema.toType(Schema.Struct(CanonicalBatch.fields)),
    )({ ...batch, records: currentRecords }).pipe(
      Effect.mapError(() =>
        invalid("Empty, non-contiguous, or oversized canonical batch", threadId),
      ),
      Effect.map((fields) => CanonicalBatch.make(fields)),
    );

    sourceDigest = yield* digestJson({
      previousTailDigest: sourceDigest,
      batch: { ...batch, records: wireRecords },
    }).pipe(Effect.mapError(() => invalid("Source digest computation failed", threadId)));
    const encoded = { ...batch, records: encodedRecords };

    targetDigest = yield* digestJson({ previousTailDigest: targetDigest, batch: encoded }).pipe(
      Effect.mapError(() => invalid("Destination digest computation failed", threadId)),
    );
    prepared.push({
      batch: canonical,
      batchJson: canonicalJson(encoded),
      recordJson: encodedRecords.map(canonicalJson),
      firstSequence,
      lastSequence: sequence(index),
      tailDigest: targetDigest,
    });
  }
  if (
    index !== records.length ||
    index !== archive.records.length ||
    sourceDigest !== archive.tailDigest
  )
    return yield* invalid("The canonical batch chain does not match the exported tail", threadId);

  // A fresh fence is not restored execution authority. Leave room for the next claim and
  // keep its interruption audit distinct from every generation already in the log.
  let generation = 1;

  for (const {
    record: { payload },
  } of records) {
    if (payload._tag === "ModelResponseInterrupted")
      generation = Math.max(generation, payload.supersededEpoch + 1);
  }

  const producerEpoch = yield* Schema.decodeEffect(
    ProducerEpoch.check(Schema.isLessThan(Number.MAX_SAFE_INTEGER)),
  )(generation).pipe(
    Effect.mapError(() => invalid("No fresh producer generation is available", threadId)),
  );

  const admissions = archive.admissions ?? [];
  const byId = new Map(admissions.map((admission) => [admission.submissionId, admission]));

  const byRun = new Map(
    admissions.map((admission) => [runIdForSubmission(admission.submissionId), admission]),
  );

  const receipts = new Set<string>();
  const queues = new Set<number>();
  const keys = new Set<string>();
  const referenced = new Set<SubmissionId>();
  const inputs = new Map<SubmissionId, CanonicalRecordEnvelope>();
  const settlements = new Map<SubmissionId, CanonicalRecordEnvelope>();
  const commands = archive.commands ?? { aborts: [], approvals: [], resolutions: [] };

  if (byId.size !== admissions.length)
    return yield* invalid("Duplicate admission identity", threadId);
  for (const admission of admissions) {
    const key = JSON.stringify([admission.principal, admission.idempotencyKey]);

    if (
      admission.threadId !== threadId ||
      receipts.has(admission.receiptId) ||
      queues.has(admission.queueSequence) ||
      keys.has(key)
    )
      return yield* invalid("Conflicting admission identities, keys, or queue order", threadId);
    receipts.add(admission.receiptId);
    queues.add(admission.queueSequence);
    keys.add(key);
    if (
      admission.parentLinkage !== undefined ||
      admission.workerAdmission !== undefined ||
      admission.messageAdmission !== undefined
    )
      return yield* ThreadImportRejected.make({
        threadId,
        reason: "unsupported-obligations",
        message:
          "Linked Threads and message deliveries require their owning stores; a single-Thread import cannot restore them",
      });

    const inputDigest = yield* digestJson(admission.inputPayload).pipe(
      Effect.mapError(() => invalid("Admission digest computation failed", threadId)),
    );

    if (inputDigest !== admission.inputDigest)
      return yield* invalid("Admission input digest mismatch", threadId);
  }
  for (const entry of records) {
    const payload = entry.record.payload;

    if (
      [
        "SubagentRequested",
        "SubagentStarted",
        "SubagentJoined",
        "SubagentLineageRecorded",
        "WorkerOriginRecorded",
        "WorkerInputRequested",
        "WorkerInputCompleted",
        "WorkerStopRequested",
        "WorkerReportPrepared",
        "PeerMessagePrepared",
        "SubtreeBudgetReserved",
      ].includes(payload._tag) ||
      (payload._tag === "AgentUpdateEmitted" && payload.delivery !== undefined)
    )
      return yield* ThreadImportRejected.make({
        threadId,
        reason: "unsupported-obligations",
        message: "The log references external child, worker, or delivery obligations",
      });
    if ("submissionId" in payload && payload.submissionId !== undefined) {
      const admission = byId.get(payload.submissionId);

      if (admission === undefined)
        return yield* invalid("Canonical Submission reference has no admission fact", threadId);
      referenced.add(payload.submissionId);
      if (payload._tag === "UserInputRecorded") {
        if (
          payload.kind !== "user" ||
          inputs.has(payload.submissionId) ||
          entry.record.recordId !== submissionInputRecordId(payload.submissionId) ||
          canonicalJson(payload.input) !== canonicalJson(admission.inputPayload)
        )
          return yield* invalid("Canonical input disagrees with its immutable admission", threadId);
        inputs.set(payload.submissionId, entry);
      } else if (payload._tag === "SubmissionSettled") {
        if (settlements.has(payload.submissionId))
          return yield* invalid("Duplicate canonical settlement", threadId);
        yield* validateCanonicalSettlement(entry.record, admission).pipe(
          Effect.mapError(() =>
            invalid("Canonical settlement disagrees with its Receipt", threadId),
          ),
        );
        settlements.set(payload.submissionId, entry);
      }
    }
  }
  if (admissions.some((admission) => !referenced.has(admission.submissionId)))
    return yield* invalid(
      "An admission has no canonical reference; retain queued work in the source store",
      threadId,
    );
  for (const intents of [commands.aborts, commands.approvals, commands.resolutions]) {
    const seen = new Set<string>();

    for (const intent of intents) {
      const key = JSON.stringify([
        intent.submissionId,
        "toolCallId" in intent ? intent.toolCallId : "abort",
      ]);

      if (!byId.has(intent.submissionId) || seen.has(key))
        return yield* invalid("Unbound or duplicate accepted command", threadId);
      seen.add(key);
    }
  }

  const submissions: Array<RebuiltSubmission> = [];

  for (const admission of admissions) {
    const inputEntry = inputs.get(admission.submissionId);
    const payload = inputEntry?.record.payload;
    const runId = payload?._tag === "UserInputRecorded" ? payload.runId : undefined;
    const host = runId === undefined ? undefined : byRun.get(runId);

    if (inputEntry !== undefined && host === undefined)
      return yield* invalid("Canonical Run has no admitted owner", threadId);

    const joined =
      host !== undefined && host.submissionId !== admission.submissionId
        ? host.submissionId
        : undefined;

    if (joined !== undefined && host !== undefined && host.queueSequence >= admission.queueSequence)
      return yield* invalid("Joined input precedes its host admission", threadId);
    const settlement = settlements.get(admission.submissionId)?.record;

    if (settlement !== undefined && settlement.payload._tag === "SubmissionSettled") {
      if (
        settlement.payload.runId !== runId &&
        !(inputEntry === undefined && settlement.payload.runId === undefined)
      )
        return yield* invalid("Settlement Run does not own the canonical input", threadId);
      if (joined !== undefined) {
        const hostSettlement = settlements.get(joined)?.record.payload;

        if (hostSettlement?._tag !== "SubmissionSettled")
          return yield* invalid("Joined settlement has no canonical host settlement", threadId);
        yield* validateJoinedSettlement(settlement.payload, hostSettlement).pipe(
          Effect.mapError(() => invalid("Joined settlement conflicts with its host", threadId)),
        );
      }
    }
    const ownRun = runIdForSubmission(admission.submissionId);

    const runRecords = records.filter(
      ({ record: { payload } }) => "runId" in payload && payload.runId === ownRun,
    );

    const operations = toolOperationStates(runRecords, ownRun);

    const approvals = commands.approvals
      .filter((intent) => intent.submissionId === admission.submissionId)
      .map((intent) => ApprovalDecisionIntent.make(intent));

    const resolutions = commands.resolutions
      .filter((intent) => intent.submissionId === admission.submissionId)
      .map((intent) => UnknownResolutionIntent.make(intent));

    const acceptedAbort = commands.aborts.find(
      (intent) => intent.submissionId === admission.submissionId,
    );

    const canonicalAbort = records.find(
      ({ record: { payload } }) =>
        payload._tag === "AbortRequested" && payload.submissionId === admission.submissionId,
    );

    const abort =
      canonicalAbort?.record.payload._tag === "AbortRequested"
        ? AbortIntent.make({
            ...canonicalAbort.record.payload,
            requestedAt: canonicalAbort.record.createdAt,
            canonicalRecordId: canonicalAbort.record.recordId,
          })
        : acceptedAbort === undefined
          ? undefined
          : AbortIntent.make(acceptedAbort);

    if (
      acceptedAbort !== undefined &&
      abort !== undefined &&
      (acceptedAbort.author !== abort.author || acceptedAbort.reason !== abort.reason)
    )
      return yield* invalid("Accepted abort conflicts with the canonical command", threadId);
    for (const command of [...approvals, ...resolutions]) {
      if (
        operations.filter((state) => state.operation.toolCallId === command.toolCallId).length !== 1
      )
        return yield* invalid(
          "Accepted tool command has no unambiguous canonical declaration",
          threadId,
        );
    }
    for (const command of approvals) {
      const requested = runRecords.filter(
        ({ record: { payload } }) =>
          payload._tag === "ToolApprovalRequested" && payload.toolCallId === command.toolCallId,
      );

      const decided = runRecords.filter(
        ({ record: { payload } }) =>
          payload._tag === "ToolApprovalDecided" && payload.toolCallId === command.toolCallId,
      );

      if (
        requested.length !== 1 ||
        decided.length > 1 ||
        decided.some(
          ({ record: { payload } }) =>
            payload._tag === "ToolApprovalDecided" &&
            (payload.decision !== command.decision ||
              payload.resolver !== command.resolver ||
              payload.reason !== command.reason),
        )
      )
        return yield* invalid(
          "Accepted approval conflicts with its canonical request or decision",
          threadId,
        );
    }
    for (const command of resolutions) {
      if (command.resolution._tag === "AbortSubmission" && abort === undefined)
        return yield* invalid(
          "AbortSubmission resolution has no accepted or canonical abort",
          threadId,
        );

      const resolved = runRecords.filter(
        ({ record: { payload } }) =>
          payload._tag === "ToolCallResolved" && payload.toolCallId === command.toolCallId,
      );

      const expected =
        command.resolution._tag === "CompletedWithResult"
          ? command.resolution.isFailure
            ? "failed-with-error"
            : "completed-with-result"
          : command.resolution._tag === "NeverHappened"
            ? "never-started"
            : command.resolution._tag === "SafeToRetry"
              ? "safe-retry"
              : undefined;

      if (
        resolved.length > 1 ||
        resolved.some(
          ({ record: { payload } }) =>
            payload._tag === "ToolCallResolved" &&
            (payload.resolution !== expected ||
              payload.author !== command.author ||
              payload.reason !== command.reason),
        )
      )
        return yield* invalid("Accepted resolution conflicts with its canonical audit", threadId);
      if (command.resolution._tag === "CompletedWithResult") {
        const result = command.resolution;

        const outcomes = runRecords.filter(
          ({ record: { payload } }) =>
            payload._tag === "ToolCallSettled" && payload.toolCallId === command.toolCallId,
        );

        if (
          outcomes.length > 1 ||
          outcomes.some(
            ({ record: { payload } }) =>
              payload._tag === "ToolCallSettled" &&
              (payload.isFailure !== result.isFailure ||
                canonicalJson(payload.result) !== canonicalJson(result.result)),
          )
        )
          return yield* invalid(
            "Accepted resolution conflicts with the canonical tool result",
            threadId,
          );
      }
    }

    const unknown = unresolvedToolOperations(runRecords, ownRun)
      .filter((state) => state.unknown || state.operation.executionKind === "ordinary")
      .map((state) => state.operation.toolCallId);

    const pendingApprovals = runRecords
      .filter(({ record: { payload } }) => payload._tag === "ToolApprovalRequested")
      .flatMap(({ record }) => {
        if (record.payload._tag !== "ToolApprovalRequested") return [];
        const id = record.payload.toolCallId;

        return approvals.some((decision) => decision.toolCallId === id) ||
          runRecords.some(
            ({ record: { payload } }) =>
              payload._tag === "ToolApprovalDecided" && payload.toolCallId === id,
          )
          ? []
          : [id];
      });

    const suspension =
      pendingApprovals.length === 0
        ? undefined
        : SuspensionSnapshot.make({
            reason: ApprovalPendingSuspension.make({
              toolCallIds: [pendingApprovals[0], ...pendingApprovals.slice(1)],
            }),
            suspendedAt: records.at(-1)?.record.createdAt ?? admission.createdAt,
          });

    if (
      inputEntry === undefined &&
      settlement === undefined &&
      !records.some(
        ({ record: { payload } }) =>
          payload._tag === "ThreadCreated" && payload.agentId === admission.agentId,
      )
    )
      return yield* invalid("Pending input has no canonical Thread materialization", threadId);
    submissions.push(
      RebuiltSubmission.make({
        admission,
        state:
          settlement !== undefined
            ? "settled"
            : joined !== undefined
              ? "joined"
              : unknown.length > 0
                ? "unknown"
                : suspension !== undefined
                  ? "suspended"
                  : inputEntry !== undefined
                    ? "input-applied"
                    : "ready",
        ...(inputEntry === undefined
          ? {}
          : {
              inputApplied: InputAppliedMarker.make({
                recordId: inputEntry.record.recordId,
                sequence: inputEntry.sequence,
              }),
            }),
        ...(joined === undefined ? {} : { joinedHostSubmissionId: joined }),
        ...(settlement === undefined ? {} : { settlement }),
        ...(suspension === undefined ? {} : { suspension }),
        unknownToolCallIds: unknown,
        ...(abort === undefined ? {} : { abort }),
        approvals,
        resolutions,
      }),
    );
  }

  return {
    result: ThreadImportResult.make({
      threadId,
      format: CURRENT_RECORD_FORMAT,
      tailSequence: sequence(records.length),
      tailDigest: targetDigest,
      recordCount: records.length,
      submissionCount: submissions.length,
    }),
    producerEpoch,
    records,
    batches: prepared,
    submissions,
  } satisfies PreparedThreadImport;
});

/** Export from a quiesced source, then atomically install into an empty destination Thread. */
export const reencodeThread = Effect.fn("ThreadImport.reencodeThread")(function* <E, R>(
  source: Effect.Effect<ThreadExport, E, R>,
  target: ThreadImport["Service"],
) {
  const exported = yield* source;

  const encoded = yield* Schema.encodeEffect(ThreadExport)(exported).pipe(
    Effect.mapError(() => invalid("Unable to encode the source export", exported.threadId)),
  );

  const archive = yield* Schema.decodeEffect(ThreadArchive)(encoded).pipe(
    Effect.mapError(() => invalid("Unable to decode the source export", exported.threadId)),
  );

  return yield* target.import(ThreadImportRequest.make({ archive }));
});
