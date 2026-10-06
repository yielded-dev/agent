import { Context, Effect, Schema } from "effect";

import { SubmissionId, ThreadId, ToolCallId, type RunId } from "../core/Identifiers.ts";
import { canonicalJson, digestJson, EMPTY_TAIL_DIGEST } from "./Digest.ts";
import { toolOperationStates } from "./internal/tool-operations.ts";
import { decodeExportRecord, ExportRecord } from "./RecordFormat.ts";
import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  Digest,
  ProducerEpoch,
  RecordEnvelope,
  RecordJson,
} from "./Records.ts";
import { verifyRunContinuations } from "./RunContinuation.ts";
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
  unknownResolutionKind,
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

/** Keep wire data intact until import has checked the marker against the current record format. */
export const ThreadArchive = Schema.Struct({
  format: Schema.NonEmptyString,
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(
    Schema.Struct({
      ...CanonicalRecordEnvelope.fields,
      record: RecordJson,
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
      "destination-conflict",
      "admission-policy-conflict",
      "admission-policy-unavailable",
      "unsupported-capacity",
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
    message: `${message}; verify the source log and export it again with the matching release before retrying`,
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

  if (format !== CURRENT_RECORD_FORMAT)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-format",
      message: `Unsupported record format ${format}; this unreleased protocol accepts only ${CURRENT_RECORD_FORMAT} archives into fresh stores`,
    });
  if ((archive.externalObligations?.length ?? 0) > 0)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-obligations",
      message:
        "Complete or transfer the source child, worker, and delivery obligations with their owning stores before importing this Thread",
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
  let tailDigest = EMPTY_TAIL_DIGEST;
  let index = 0;

  for (const batch of batches) {
    if (batchIds.has(batch.batchId)) return yield* invalid("Duplicate canonical batch", threadId);
    batchIds.add(batch.batchId);
    const currentRecords: Array<RecordEnvelope> = [];
    const encodedRecords: Array<RecordJson> = [];
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
      currentRecords.push(record);
      encodedRecords.push(entry.record);
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

    const encoded = { ...batch, records: encodedRecords };

    tailDigest = yield* digestJson({ previousTailDigest: tailDigest, batch: encoded }).pipe(
      Effect.mapError(() => invalid("Canonical digest computation failed", threadId)),
    );
    prepared.push({
      batch: canonical,
      batchJson: canonicalJson(encoded),
      recordJson: encodedRecords.map(canonicalJson),
      firstSequence,
      lastSequence: sequence(index),
      tailDigest,
    });
  }
  if (index !== archive.records.length || tailDigest !== archive.tailDigest)
    return yield* invalid("The canonical batch chain does not match the exported tail", threadId);

  yield* verifyRunContinuations(records).pipe(
    Effect.mapError(() =>
      invalid("Run continuation differs from its exact canonical facts", threadId),
    ),
  );

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
    Effect.mapError(() =>
      ThreadImportRejected.make({
        threadId,
        reason: "unsupported-capacity",
        message:
          "The canonical producer generation is exhausted; retain the source and use a release with sufficient generation capacity",
      }),
    ),
  );

  const admissions = archive.admissions ?? [];
  const byId = new Map(admissions.map((admission) => [admission.submissionId, admission]));

  const byRun = new Map(
    admissions.map((admission) => [runIdForSubmission(admission.submissionId), admission]),
  );

  const receipts = new Set<string>();
  const queues = new Set<number>();
  const keys = new Set<string>();
  const recordsByRun = new Map<RunId, Array<CanonicalRecordEnvelope>>();
  const canonicalAborts = new Map<SubmissionId, CanonicalRecordEnvelope>();
  const materializedAgents = new Set<string>();
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
          "Complete or transfer linked Threads and message deliveries with their owning stores before importing this Thread",
      });

    const inputDigest = yield* digestJson(admission.inputPayload).pipe(
      Effect.mapError(() => invalid("Admission digest computation failed", threadId)),
    );

    if (inputDigest !== admission.inputDigest)
      return yield* invalid("Admission input digest mismatch", threadId);
  }
  for (const entry of records) {
    const payload = entry.record.payload;

    if ("runId" in payload && payload.runId !== undefined) {
      const runRecords = recordsByRun.get(payload.runId);

      if (runRecords === undefined) recordsByRun.set(payload.runId, [entry]);
      else runRecords.push(entry);
    }
    if (payload._tag === "ThreadCreated") materializedAgents.add(payload.agentId);
    if (payload._tag === "AbortRequested") {
      if (canonicalAborts.has(payload.submissionId))
        return yield* invalid("Duplicate canonical abort", threadId);
      canonicalAborts.set(payload.submissionId, entry);
    }

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
        message:
          "Transfer the child, worker, or delivery stores referenced by this log through their owning workflow before importing this Thread",
      });
    if ("submissionId" in payload && payload.submissionId !== undefined) {
      const admission = byId.get(payload.submissionId);

      if (admission === undefined)
        return yield* invalid("Canonical Submission reference has no admission fact", threadId);
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
  for (const intents of [commands.aborts, commands.approvals, commands.resolutions]) {
    const seen = new Set<string>();

    for (const intent of intents) {
      const key = JSON.stringify([
        intent.submissionId,
        "toolCallId" in intent ? intent.toolCallId : "abort",
        ...("resolution" in intent ? [unknownResolutionKind(intent.resolution)] : []),
      ]);

      if (!byId.has(intent.submissionId) || seen.has(key))
        return yield* invalid("Unbound or duplicate accepted command", threadId);
      seen.add(key);
    }
  }

  const approvalsBySubmission = new Map<SubmissionId, Array<ApprovalDecisionIntent>>();
  const resolutionsBySubmission = new Map<SubmissionId, Array<UnknownResolutionIntent>>();

  const abortsBySubmission = new Map(
    commands.aborts.map((intent) => [intent.submissionId, intent]),
  );

  for (const intent of commands.approvals) {
    const entries = approvalsBySubmission.get(intent.submissionId) ?? [];

    entries.push(ApprovalDecisionIntent.make(intent));
    approvalsBySubmission.set(intent.submissionId, entries);
  }
  for (const intent of commands.resolutions) {
    const entries = resolutionsBySubmission.get(intent.submissionId) ?? [];

    entries.push(UnknownResolutionIntent.make(intent));
    resolutionsBySubmission.set(intent.submissionId, entries);
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

    const runRecords = recordsByRun.get(ownRun) ?? [];
    const operations = toolOperationStates(runRecords, ownRun);
    const operationCounts = new Map<ToolCallId, number>();

    for (const state of operations)
      operationCounts.set(
        state.operation.toolCallId,
        (operationCounts.get(state.operation.toolCallId) ?? 0) + 1,
      );
    const toolRecords = new Map<ToolCallId, Array<CanonicalRecordEnvelope>>();

    for (const entry of runRecords) {
      const payload = entry.record.payload;

      if (!("toolCallId" in payload)) continue;
      const entries = toolRecords.get(payload.toolCallId) ?? [];

      entries.push(entry);
      toolRecords.set(payload.toolCallId, entries);
    }
    const approvals = approvalsBySubmission.get(admission.submissionId) ?? [];
    const resolutions = resolutionsBySubmission.get(admission.submissionId) ?? [];
    const acceptedAbort = abortsBySubmission.get(admission.submissionId);
    const canonicalAbort = canonicalAborts.get(admission.submissionId);

    const abort =
      canonicalAbort?.record.payload._tag === "AbortRequested"
        ? AbortIntent.make({
            ...canonicalAbort.record.payload,
            requestedAt: acceptedAbort?.requestedAt ?? canonicalAbort.record.createdAt,
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
      if (operationCounts.get(command.toolCallId) !== 1)
        return yield* invalid(
          "Accepted tool command has no unambiguous canonical declaration",
          threadId,
        );
    }
    for (const command of approvals) {
      const requested = (toolRecords.get(command.toolCallId) ?? []).filter(
        ({ record: { payload } }) =>
          payload._tag === "ToolApprovalRequested" && payload.toolCallId === command.toolCallId,
      );

      const decided = (toolRecords.get(command.toolCallId) ?? []).filter(
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

      const resolved = (toolRecords.get(command.toolCallId) ?? []).filter(
        ({ record: { payload } }) =>
          payload._tag === "ToolCallResolved" &&
          payload.toolCallId === command.toolCallId &&
          (payload.resolution === "safe-retry" ? "execution" : "factual") ===
            unknownResolutionKind(command.resolution),
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

        const outcomes = (toolRecords.get(command.toolCallId) ?? []).filter(
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

    const unknown = operations
      .filter(
        (state) =>
          !state.settled &&
          !state.resolved &&
          (state.unknown ||
            (!state.dispatchBlocked &&
              state.operation.executionKind === "ordinary" &&
              state.operation.executionClass !== "readonly")),
      )
      .map((state) => state.operation.toolCallId);

    const acceptedApprovals = new Set(approvals.map((intent) => intent.toolCallId));

    const pendingApprovals = runRecords
      .filter(({ record: { payload } }) => payload._tag === "ToolApprovalRequested")
      .flatMap(({ record }) => {
        if (record.payload._tag !== "ToolApprovalRequested") return [];
        const id = record.payload.toolCallId;

        return acceptedApprovals.has(id) ||
          (toolRecords.get(id) ?? []).some(
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
            suspendedAt: runRecords.at(-1)?.record.createdAt ?? admission.createdAt,
          });

    // An admitted input need not have reached the log. Its immutable fact is the work;
    // recovery materializes an admitted Thread before it becomes ready.
    const state =
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
                : materializedAgents.has(admission.agentId)
                  ? "ready"
                  : "admitted";

    submissions.push(
      RebuiltSubmission.make({
        admission,
        state,
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
        ...(state !== "suspended" || suspension === undefined ? {} : { suspension }),
        unknownToolCallIds: state === "unknown" ? unknown : [],
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
      tailDigest,
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
) {
  const target = yield* ThreadImport;
  const exported = yield* source;

  // Only the record view needs encoding here. Import captures and validates the complete
  // archive once; admission and command facts are already in their decoded representation.
  const records = yield* Effect.forEach(exported.records, (entry) =>
    Schema.encodeEffect(ExportRecord)(entry.record).pipe(
      Effect.map((record) => ({ ...entry, record })),
    ),
  ).pipe(Effect.mapError(() => invalid("Unable to encode the source export", exported.threadId)));

  const request = yield* ThreadImportRequest.makeEffect({ archive: { ...exported, records } }).pipe(
    Effect.mapError(() => invalid("The source export is malformed", exported.threadId)),
  );

  return yield* target.import(request);
});
