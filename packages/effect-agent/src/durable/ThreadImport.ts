import { Context, Effect, Pull, Schema, Stream } from "effect";

import { InputMessage } from "../capabilities/Messaging.ts";
import {
  SubmissionId,
  ThreadId,
  ToolCallId,
  type RunId,
  type AgentId,
} from "../core/Identifiers.ts";
import type { IdempotencyKey } from "../core/Receipt.ts";
import { WorkerRef } from "../core/Worker.ts";
import { canonicalJson, digestJson, EMPTY_TAIL_DIGEST, utf8ByteLength } from "./Digest.ts";
import { toolOperationStates } from "./internal/tool-operations.ts";
import { MessageDeliveryRecord } from "./MessageDelivery.ts";
import { decodeExportRecord, ExportedRecord, ExportRecord } from "./RecordFormat.ts";
import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  Digest,
  ProducerEpoch,
  RecordEnvelope,
  RecordJson,
  SubtreeBudgetReserved,
  type RecordId,
} from "./Records.ts";
import { isPreContinuationFact } from "./RunContinuation.ts";
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
  submissionSettlementRecordId,
  submissionAbortRecordId,
} from "./SubmissionLedger.ts";
import {
  type ThreadCommands,
  MAX_THREAD_EXPORT_PAGE_BYTES,
  MAX_THREAD_EXPORT_PAGE_RECORDS,
  ThreadAdmission,
  ThreadExport,
  type ThreadStoreError,
} from "./ThreadStore.ts";
import { transferDependencies, transferSnapshotId, transferSections } from "./ThreadTransfer.ts";
import { workIndexChanges } from "./ThreadWork.ts";

const sameInputMessage = Schema.toEquivalence(InputMessage);

/** One owned wire page. Canonical batches precede admissions; all admissions precede accepted commands.
 * Every fact section is required, including empty sections. This order permits indexed atomic staging. */
export const ThreadArchive = Schema.Struct({
  ...ThreadExport.fields,
  records: Schema.Array(
    Schema.Struct({ ...CanonicalRecordEnvelope.fields, record: RecordJson }),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS)),
});

export type ThreadArchive = typeof ThreadArchive.Type;

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

/** One owner transaction consumes the stream; E and R are preserved, including rollback on interruption. */
export class ThreadImport extends Context.Service<
  ThreadImport,
  {
    readonly import: <E, R>(
      pages: Stream.Stream<ThreadArchive, E, R>,
    ) => Effect.Effect<ThreadImportResult, E | ThreadImportRejected | ThreadStoreError, R>;
  }
>()("@effect-agent/thread/ThreadImport") {}

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
  readonly previousTailDigest: Digest;
  readonly tailDigest: Digest;
}

export interface PreparedImportPage {
  readonly archive: ThreadArchive;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly batches: ReadonlyArray<PreparedImportBatch>;
}

/** Adapter-private exact indexes. runRecords and commands must enforce per-Run count AND byte limits. */
export interface ThreadImportReader {
  readonly delivery: (
    messageId: IdempotencyKey,
  ) => Effect.Effect<MessageDeliveryRecord | undefined, ThreadStoreError>;
  readonly record: (
    id: RecordId,
  ) => Effect.Effect<CanonicalRecordEnvelope | undefined, ThreadStoreError>;
  readonly admission: (
    id: SubmissionId,
  ) => Effect.Effect<ThreadAdmission | undefined, ThreadStoreError>;
  readonly runOwner: (id: RunId) => Effect.Effect<ThreadAdmission | undefined, ThreadStoreError>;
  readonly runRecords: (
    id: RunId,
  ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ThreadStoreError>;
  readonly commands: (
    id: SubmissionId,
  ) => Effect.Effect<typeof ThreadCommands.Type, ThreadStoreError>;
  readonly hasAgent: (id: AgentId) => Effect.Effect<boolean, ThreadStoreError>;
}

/** Resolve every declared immutable reference, even in a preparation without a continuation. */
export const verifyImportedReferences = Effect.fnUntraced(function* (
  entry: CanonicalRecordEnvelope,
  resolve: ThreadImportReader["record"],
  delivery: ThreadImportReader["delivery"],
) {
  const payload = entry.record.payload;

  if (payload._tag === "WorkHandoffCompleted") {
    const preparation = yield* resolve(payload.preparationId);

    if (
      preparation === undefined ||
      preparation.threadId !== entry.threadId ||
      preparation.sequence >= entry.sequence ||
      entry.record.recordId !== `work-handoff-completed:${payload.preparationId}`
    )
      return yield* invalid("Missing or ambiguous handoff preparation", entry.threadId);
    const changes = yield* workIndexChanges(preparation, "none");

    if (
      !changes.some(
        (change) =>
          change._tag === "Put" &&
          change.entry.owner._tag === "Handoff" &&
          change.entry.id === payload.ownerId,
      )
    )
      return yield* invalid(
        "Handoff closure does not identify its preparation's exact owner",
        entry.threadId,
      );
  }

  const references =
    payload._tag === "WorkerInputRefused"
      ? [payload.reservation, payload.stop]
      : payload._tag === "RunContextRecorded"
        ? payload.history
        : payload._tag === "RunContinuation"
          ? [
              payload.originalInput,
              payload.savedContext,
              payload.latestResponse,
              payload.terminal,
              payload.lastFact,
            ].filter((ref) => ref !== undefined)
          : [];

  for (const ref of references) {
    const evidence = yield* resolve(ref.recordId);

    if (
      evidence === undefined ||
      evidence.threadId !== entry.threadId ||
      evidence.sequence >= entry.sequence ||
      ("sequence" in ref && evidence.sequence !== ref.sequence)
    )
      return yield* invalid("Missing, foreign or forward canonical dependency", entry.threadId);

    const wire = yield* (
      evidence.record instanceof ExportedRecord
        ? Schema.encodeEffect(ExportRecord)(evidence.record)
        : Schema.encodeEffect(RecordEnvelope)(evidence.record)
    ).pipe(Effect.mapError(() => invalid("Invalid dependency wire", entry.threadId)));

    if ((yield* digest(wire)) !== ref.digest)
      return yield* invalid("Canonical dependency digest mismatch", entry.threadId);
  }
  if (payload._tag === "WorkerInputRefused") {
    const request = (yield* resolve(payload.reservation.recordId))!.record.payload;
    const stop = (yield* resolve(payload.stop.recordId))!.record.payload;
    const retained = yield* delivery(payload.messageId);

    if (
      retained === undefined ||
      retained.key.ownerThreadId !== entry.threadId ||
      retained.key.messageId !== payload.messageId ||
      retained.status !== "refused" ||
      retained.receipt !== null ||
      retained.settlement !== null ||
      retained.version !== payload.deliveryVersion ||
      retained.envelopeDigest !== payload.envelopeDigest ||
      retained.envelope.threadId !== payload.workerThreadId ||
      retained.envelope.deliveryPrincipal !== payload.receiver.principal ||
      retained.envelope.admissionKey !== payload.receiver.idempotencyKey ||
      retained.envelope.inputDigest !== payload.receiver.inputDigest ||
      retained.envelope.workerAdmission === undefined ||
      retained.envelope.workerAdmission.messageId !== payload.messageId ||
      retained.envelope.workerAdmission.origin.source.threadId !== entry.threadId ||
      stop._tag !== "WorkerStopRequested" ||
      !Schema.toEquivalence(WorkerRef)(
        stop.command.worker,
        retained.envelope.workerAdmission.origin.worker,
      )
    )
      return yield* invalid(
        "Worker refusal has no exact refused delivery and source stop proof",
        entry.threadId,
      );
    if (request._tag === "WorkerInputRequested") {
      if (
        request.inputDigest !== retained.envelope.inputDigest ||
        !Schema.toEquivalence(MessageDeliveryRecord.fields.envelope.fields.workerAdmission)(
          request.admission,
          retained.envelope.workerAdmission,
        )
      )
        return yield* invalid(
          "Worker refusal reservation differs from its exact request",
          entry.threadId,
        );
    } else if (
      request._tag !== "SubtreeBudgetReserved" ||
      request.childThreadId !== payload.workerThreadId ||
      request.reservationId !== payload.messageId ||
      request.lifetime !== "background" ||
      request.executionRunId !== retained.envelope.workerAdmission.executionRunId ||
      request.sourceSubmissionId !== retained.envelope.workerAdmission.sourceSubmissionId ||
      request.depth !== retained.envelope.workerAdmission.origin.depth ||
      !Schema.toEquivalence(SubtreeBudgetReserved.fields.policy)(
        request.policy,
        retained.envelope.workerAdmission.origin.policy,
      ) ||
      !Schema.toEquivalence(SubtreeBudgetReserved.fields.grant)(
        request.grant,
        retained.envelope.workerAdmission.origin.grant,
      ) ||
      !Schema.toEquivalence(SubtreeBudgetReserved.fields.budget)(
        request.budget,
        retained.envelope.workerAdmission.origin.budget,
      )
    )
      return yield* invalid("Worker refusal has no exact source reservation", entry.threadId);
  }
});

const sequence = Schema.decodeSync(CanonicalSequence);

const digest = (value: Schema.Json) =>
  digestJson(value).pipe(Effect.mapError(() => invalid("Unable to digest imported facts")));

export const invalidThreadArchive = (message: string, threadId?: ThreadId) =>
  ThreadImportRejected.make({
    reason: "invalid-archive",
    message,
    ...(threadId === undefined ? {} : { threadId }),
  });

const invalid = invalidThreadArchive;

/** Own the first page before entering a destination gate; reuse the same scoped source pull. */
export const captureImportSource = Effect.fnUntraced(function* <E, R>(
  pages: Stream.Stream<ThreadArchive, E, R>,
) {
  const pull = yield* Stream.toPull(Stream.rechunk(pages, 1));
  const first = yield* Pull.catchDone(pull, () => Effect.fail(invalid("Missing transfer pages")));
  const codec = Schema.fromJsonString(ThreadArchive);

  const wire = yield* Schema.encodeEffect(codec)(first[0]).pipe(
    Effect.mapError((cause) => invalid(`Malformed first transfer page: ${cause.message}`)),
  );

  if (utf8ByteLength(wire) > MAX_THREAD_EXPORT_PAGE_BYTES)
    return yield* invalid("Transfer page exceeds 32 MiB");

  const owned = yield* Schema.decodeEffect(codec)(wire).pipe(
    Effect.mapError((cause) => invalid(`Malformed first transfer page: ${cause.message}`)),
  );

  return Stream.succeed(owned).pipe(Stream.concat(Stream.fromPull(Effect.succeed(pull))));
});

/** Constant-size fold state; identity uniqueness belongs to staged destination indexes. */
export interface ThreadImportProgress {
  manifest: ThreadArchive | undefined;
  sequence: number;
  tailDigest: Digest;
  producerEpoch: number;
  ended: boolean;
  cursor: string | undefined;
  counts: {
    admissions: number;
    aborts: number;
    approvals: number;
    resolutions: number;
    deliveries: number;
  };
}

export const makeThreadImportProgress = (): ThreadImportProgress => ({
  manifest: undefined,
  sequence: 0,
  tailDigest: EMPTY_TAIL_DIGEST,
  producerEpoch: 1,
  ended: false,
  cursor: undefined,
  counts: { admissions: 0, aborts: 0, approvals: 0, resolutions: 0, deliveries: 0 },
});

/** Capture before awaiting any writes; the retained manifest never retains page payload arrays. */
export const prepareImportPage = Effect.fnUntraced(function* (
  state: ThreadImportProgress,
  input: ThreadArchive,
  options?: { readonly allowExternalObligations?: boolean },
) {
  const codec = Schema.fromJsonString(ThreadArchive);

  const wire = yield* Schema.encodeEffect(codec)(input).pipe(
    Effect.mapError(() => invalid("Malformed transfer page")),
  );

  if (utf8ByteLength(wire) > MAX_THREAD_EXPORT_PAGE_BYTES)
    return yield* invalid("Transfer page exceeds 32 MiB");

  const archive = yield* Schema.decodeEffect(codec)(wire).pipe(
    Effect.mapError(() => invalid("Malformed transfer page")),
  );

  const { threadId } = archive;

  if (state.ended) return yield* invalid("Page after the final transfer page", threadId);
  if (archive.format !== CURRENT_RECORD_FORMAT)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-format",
      message: `Only ${CURRENT_RECORD_FORMAT} can be imported`,
    });
  if (!options?.allowExternalObligations && (archive.externalObligations?.length ?? 0) > 0)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-obligations",
      message: "Live foreign child, worker or delivery ownership must be closed before transfer",
    });

  const manifest = {
    format: archive.format,
    threadId,
    tailSequence: archive.tailSequence,
    tailDigest: archive.tailDigest,
    snapshot: archive.snapshot,
    ...(archive.workerSeal === undefined ? {} : { workerSeal: archive.workerSeal }),
    ...(archive.externalObligations === undefined
      ? {}
      : { externalObligations: archive.externalObligations }),
  };

  const snapshotId = yield* transferSnapshotId(manifest);

  if (
    snapshotId !== archive.snapshotId ||
    (state.manifest !== undefined &&
      (state.manifest.snapshotId !== archive.snapshotId || state.manifest.threadId !== threadId))
  )
    return yield* invalid("Transfer snapshot changed or its identity is invalid", threadId);
  if (
    archive.fromSequence !== state.sequence + 1 ||
    archive.previousTailDigest !== state.tailDigest ||
    (archive.cursor !== undefined && archive.cursor === state.cursor)
  )
    return yield* invalid("Transfer page position or digest boundary is invalid", threadId);
  if (archive.records.length > 0 && transferSections.some((section) => state.counts[section] > 0))
    return yield* invalid("Canonical records follow immutable fact sections", threadId);
  if (
    archive.admissions.length > 0 &&
    state.counts.aborts + state.counts.approvals + state.counts.resolutions > 0
  )
    return yield* invalid("Admissions follow accepted command sections", threadId);
  const records: Array<CanonicalRecordEnvelope> = [];
  const prepared: Array<PreparedImportBatch> = [];
  let index = 0;

  for (const batch of archive.batches) {
    const current: Array<RecordEnvelope> = [];
    const encoded: Array<RecordJson> = [];
    const firstSequence = sequence(state.sequence + 1);
    const previousTailDigest = state.tailDigest;

    while (archive.records[index]?.batchId === batch.batchId) {
      const entry = archive.records[index]!;

      if (entry.threadId !== threadId || entry.sequence !== state.sequence + 1)
        return yield* invalid("Canonical identity or sequence mismatch", threadId);

      const record = yield* decodeExportRecord(archive.format, entry.record).pipe(
        Effect.mapError((e) => invalid(e.message, threadId)),
      );

      records.push(CanonicalRecordEnvelope.make({ ...entry, record }));
      current.push(record);
      encoded.push(entry.record);
      index++;
      state.sequence++;
      if (record.payload._tag === "ModelResponseInterrupted")
        state.producerEpoch = Math.max(state.producerEpoch, record.payload.supersededEpoch + 1);
    }

    const canonical = yield* Schema.decodeUnknownEffect(
      Schema.toType(Schema.Struct(CanonicalBatch.fields)),
    )({ ...batch, records: current }).pipe(
      Effect.mapError(() => invalid("Empty or oversized canonical batch", threadId)),
      Effect.map(CanonicalBatch.make),
    );

    const batchJson = canonicalJson({ ...batch, records: encoded });

    if (utf8ByteLength(batchJson) > 16 * 1024 * 1024)
      return yield* invalid("Canonical batch exceeds 16 MiB", threadId);
    state.tailDigest = yield* digest({ previousTailDigest, batch: { ...batch, records: encoded } });
    prepared.push({
      batch: canonical,
      batchJson,
      recordJson: encoded.map(canonicalJson),
      firstSequence,
      lastSequence: sequence(state.sequence),
      previousTailDigest,
      tailDigest: state.tailDigest,
    });
  }
  if (index !== archive.records.length || state.sequence > archive.tailSequence)
    return yield* invalid(
      "Unbound canonical records or records beyond the captured tail",
      threadId,
    );
  if (
    archive.admissions.length +
      archive.commands.aborts.length +
      archive.commands.approvals.length +
      archive.commands.resolutions.length +
      archive.deliveries.length >
      0 &&
    (state.sequence !== archive.tailSequence || state.tailDigest !== archive.tailDigest)
  )
    return yield* invalid("Immutable sections precede the complete canonical prefix", threadId);
  if (
    archive.commands.aborts.length +
      archive.commands.approvals.length +
      archive.commands.resolutions.length >
      0 &&
    state.counts.admissions + archive.admissions.length !== archive.snapshot.admissions
  )
    return yield* invalid("Accepted commands precede the complete admission directory", threadId);
  const required = transferDependencies(records);

  if (canonicalJson(required) !== canonicalJson([...archive.dependencies].sort()))
    return yield* invalid("Cross-range dependency directory is incomplete or ambiguous", threadId);
  for (const section of transferSections) {
    state.counts[section] +=
      section === "admissions"
        ? archive.admissions.length
        : section === "deliveries"
          ? archive.deliveries.length
          : archive.commands[section].length;
    if (state.counts[section] > archive.snapshot[section])
      return yield* invalid(`Too many ${section} facts`, threadId);
  }
  for (const admission of archive.admissions) {
    if (
      admission.threadId !== threadId ||
      (yield* digest(admission.inputPayload)) !== admission.inputDigest
    )
      return yield* invalid("Admission identity or input digest mismatch", threadId);
  }
  for (const delivery of archive.deliveries) {
    const encoded = yield* Schema.encodeEffect(MessageDeliveryRecord)(delivery).pipe(
      Effect.mapError(() => invalid("Invalid delivery fact", threadId)),
    );

    if (
      delivery.key.ownerThreadId !== threadId ||
      delivery.leaseUntilMillis !== null ||
      (yield* digest(encoded.envelope)) !== delivery.envelopeDigest
    )
      return yield* invalid("Delivery ownership, lease or envelope digest mismatch", threadId);
  }
  state.manifest = {
    ...archive,
    records: [],
    batches: [],
    admissions: [],
    commands: { aborts: [], approvals: [], resolutions: [] },
    deliveries: [],
    dependencies: [],
  };
  state.cursor = archive.cursor;
  state.ended = archive.cursor === undefined;

  return { archive, records, batches: prepared } satisfies PreparedImportPage;
});

export const finishThreadImport = (state: ThreadImportProgress) =>
  Effect.gen(function* () {
    const page = state.manifest;

    if (
      page === undefined ||
      !state.ended ||
      state.sequence !== page.tailSequence ||
      state.tailDigest !== page.tailDigest ||
      transferSections.some((section) => state.counts[section] !== page.snapshot[section])
    )
      return yield* invalid(
        "Transfer stream ended before its complete canonical chain and fact sections",
      );
    yield* Schema.decodeEffect(ProducerEpoch.check(Schema.isLessThan(Number.MAX_SAFE_INTEGER)))(
      state.producerEpoch,
    ).pipe(
      Effect.mapError(() =>
        ThreadImportRejected.make({
          threadId: page.threadId,
          reason: "unsupported-capacity",
          message: "Producer generation exhausted",
        }),
      ),
    );

    return ThreadImportResult.make({
      threadId: page.threadId,
      format: page.format,
      tailSequence: page.tailSequence,
      tailDigest: page.tailDigest,
      recordCount: state.sequence,
      submissionCount: state.counts.admissions,
    });
  });

export interface ThreadSettlementPredecessorRequest {
  readonly submissionId: SubmissionId;
  readonly queueSequence: number;
  readonly inputSequence: number;
  readonly settlementSequence: number;
}

/** Check FIFO against indexed earlier executed inputs, including those still without a settlement. */
export const verifyImportedSettlementOrder = Effect.fnUntraced(function* <E, R>(
  admission: Pick<ThreadAdmission, "threadId" | "submissionId" | "queueSequence">,
  reader: ThreadImportReader,
  predecessors: (
    request: ThreadSettlementPredecessorRequest,
  ) => Stream.Stream<Pick<ThreadAdmission, "submissionId">, E, R>,
) {
  const settlement = yield* reader.record(submissionSettlementRecordId(admission.submissionId));

  if (settlement === undefined) return;
  const applied = yield* reader.record(submissionInputRecordId(admission.submissionId));
  const payload = settlement.record.payload;

  if (
    applied === undefined &&
    payload._tag === "SubmissionSettled" &&
    payload.outcome === "aborted"
  )
    return;
  if (applied === undefined || applied.sequence >= settlement.sequence)
    return yield* invalid(
      "Executed settlement has no preceding canonical input",
      admission.threadId,
    );

  yield* Stream.runForEach(
    predecessors({
      submissionId: admission.submissionId,
      queueSequence: admission.queueSequence,
      inputSequence: applied.sequence,
      settlementSequence: settlement.sequence,
    }),
    (earlier) =>
      Effect.gen(function* () {
        const evidence = yield* reader.runRecords(runIdForSubmission(earlier.submissionId));
        const unknown = new Set<ToolCallId>();
        let closed = false;

        for (const fact of evidence) {
          if (fact.sequence >= applied.sequence) break;
          const p = fact.record.payload;

          if (
            p._tag === "AbortRequested" ||
            p._tag === "SubmissionSettled" ||
            p._tag === "RunCompleted" ||
            p._tag === "RunFailed"
          ) {
            closed = true;
            unknown.clear();
          } else if (p._tag === "ToolCallUnknown" && !closed) unknown.add(p.toolCallId);
          else if (p._tag === "ToolCallResolved" || p._tag === "ToolCallSettled")
            unknown.delete(p.toolCallId);
        }
        if (unknown.size === 0)
          return yield* invalid(
            "Later settlement bypassed work that was not unknown when its input became active",
            admission.threadId,
          );
      }),
  );
});

/** Rebuild ONE admitted submission from exact staged evidence; terminal state does not erase uncertainty. */
export const rebuildImportedSubmission = Effect.fnUntraced(function* (
  admission: ThreadAdmission,
  reader: ThreadImportReader,
) {
  const threadId = admission.threadId;
  const inputEntry = yield* reader.record(submissionInputRecordId(admission.submissionId));
  const payload = inputEntry?.record.payload;

  if (
    inputEntry !== undefined &&
    (payload?._tag !== "UserInputRecorded" ||
      payload.submissionId !== admission.submissionId ||
      (payload.kind !== "user" && payload.kind !== "steering") ||
      (payload.messageAdmission === undefined
        ? admission.messageAdmission !== undefined
        : admission.messageAdmission === undefined ||
          !sameInputMessage(payload.messageAdmission, admission.messageAdmission)) ||
      canonicalJson(payload.input) !== canonicalJson(admission.inputPayload))
  )
    return yield* invalid("Canonical input disagrees with its immutable admission", threadId);
  const runId = payload?._tag === "UserInputRecorded" ? payload.runId : undefined;
  const host = runId === undefined ? undefined : yield* reader.runOwner(runId);

  if (inputEntry !== undefined && host === undefined)
    return yield* invalid("Canonical Run has no admitted owner", threadId);

  const joined =
    host !== undefined && host.submissionId !== admission.submissionId
      ? host.submissionId
      : undefined;

  if (
    payload?._tag === "UserInputRecorded" &&
    payload.kind !== (joined === undefined ? "user" : "steering")
  )
    return yield* invalid("Canonical input kind disagrees with its admitted Run owner", threadId);

  if (joined !== undefined && host !== undefined && host.queueSequence >= admission.queueSequence)
    return yield* invalid("Joined input precedes its host admission", threadId);

  const settlement = (yield* reader.record(submissionSettlementRecordId(admission.submissionId)))
    ?.record;

  if (settlement !== undefined) {
    yield* validateCanonicalSettlement(settlement, admission).pipe(
      Effect.mapError(() => invalid("Canonical settlement disagrees with its receipt", threadId)),
    );
    if (settlement.payload._tag !== "SubmissionSettled" || settlement.payload.runId !== runId)
      return yield* invalid("Settlement Run does not own its canonical input", threadId);
    if (joined !== undefined) {
      const hostSettlement = (yield* reader.record(submissionSettlementRecordId(joined)))?.record
        .payload;

      if (hostSettlement?._tag !== "SubmissionSettled")
        return yield* invalid("Joined settlement has no host settlement", threadId);
      yield* validateJoinedSettlement(settlement.payload, hostSettlement).pipe(
        Effect.mapError(() => invalid("Joined settlement conflicts with its host", threadId)),
      );
    }
  }
  const ownRun = runIdForSubmission(admission.submissionId);

  const runRecords = yield* reader.runRecords(ownRun);

  if (inputEntry === undefined && runRecords.some(({ record }) => !isPreContinuationFact(record)))
    return yield* invalid("Admitted Run executed without its original canonical input", threadId);
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
  const commands = yield* reader.commands(admission.submissionId);
  const approvals = commands.approvals.map((intent) => ApprovalDecisionIntent.make(intent));
  const resolutions = commands.resolutions.map((intent) => UnknownResolutionIntent.make(intent));
  const acceptedAbort = commands.aborts[0];

  if (commands.aborts.length > 1) return yield* invalid("Duplicate accepted abort", threadId);
  const canonicalAbort = yield* reader.record(submissionAbortRecordId(admission.submissionId));

  if (canonicalAbort !== undefined && acceptedAbort === undefined)
    return yield* invalid("Canonical abort has no immutable accepted command", threadId);

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
              : (yield* reader.hasAgent(admission.agentId))
                ? "ready"
                : "admitted";

  return RebuiltSubmission.make({
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
    unknownToolCallIds: unknown,
    ...(abort === undefined ? {} : { abort }),
    approvals,
    resolutions,
  });
});

/** Encode one page at a time and retain upstream stream errors and requirements. */
export const reencodeThread = Effect.fnUntraced(function* <E, R>(
  source: Stream.Stream<ThreadExport, E, R>,
) {
  const target = yield* ThreadImport;

  return yield* target.import(
    source.pipe(
      Stream.mapEffect((page) =>
        Schema.encodeEffect(ThreadExport)(page).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(ThreadArchive)),
          Effect.mapError(() => invalid("Unable to encode source page", page.threadId)),
        ),
      ),
    ),
  );
});
