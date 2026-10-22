/**
 * Streaming validation and atomic restoration of bounded Thread transfer pages. The destination
 * stages exact canonical wire and rejects incomplete, unsupported, or conflicting archives.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Pull from "effect/Pull";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { InputMessage } from "../capabilities/Messaging.ts";
import type { RunId } from "../core/Identifiers.ts";
import { SubmissionId, ThreadId, ToolCallId } from "../core/Identifiers.ts";
import { WorkerRef } from "../core/Worker.ts";
import { canonicalJson, digestJson, utf8ByteLength } from "./Digest.ts";
import { readRunContext, validateContextBoundary } from "./internal/run-context.ts";
import { ThreadImportReader } from "./internal/thread-import-reader.ts";
import { ImportSourcePhase } from "./internal/thread-transfer-source.ts";
import { toolOperationStates } from "./internal/tool-operations.ts";
import {
  MessageDeliveryRecord,
  prepareCanonicalMessageDeliveries,
  sameMessageDeliveryIdentity,
} from "./MessageDelivery.ts";
import { decodeExportRecord } from "./RecordFormat.ts";
import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CURRENT_RECORD_FORMAT,
  RecordEnvelope,
  RecordJson,
  SubtreeBudgetReserved,
} from "./Records.ts";
import {
  executionRunIds,
  isOperationalRecord,
  isPreContinuationFact,
  validateSuffix,
} from "./RunContinuation.ts";
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
  type ExecutionCheckpoint,
  MAX_THREAD_EXPORT_PAGE_BYTES,
  MAX_THREAD_EXPORT_PAGE_RECORDS,
  ThreadAdmission,
  ThreadExport,
  ThreadAbortFact,
  ThreadApprovalFact,
  ThreadResolutionFact,
  makeTransferFactCheck,
  transferPageFits,
  type ThreadStoreError,
} from "./ThreadStore.ts";
import { transferDependencies, sameTransferManifest, transferSections } from "./ThreadTransfer.ts";
import { workIndexChanges } from "./ThreadWork.ts";

export {
  /**
   * Exact indexes for one captured Thread transaction. Run records and commands enforce count and byte bounds.
   *
   * @category services
   * @since 0.1.0
   */
  ThreadImportReader,
  /**
   * Delivery evidence reads: the importing Thread uses staged facts; foreign Threads use retained facts.
   *
   * @category services
   * @since 0.1.0
   */
  ThreadDeliveryImportReader,
} from "./internal/thread-import-reader.ts";

const sameInputMessage = Schema.toEquivalence(InputMessage);

/**
 * One owned wire page. Canonical batches precede admissions; all admissions precede accepted commands.
 * Every fact section is required, including empty sections. This order permits indexed atomic staging.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadArchive = Schema.Struct({
  ...ThreadExport.fields,
  records: Schema.Array(
    Schema.Struct({ ...CanonicalRecordEnvelope.fields, record: RecordJson }),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS)),
});

/**
 * Decoded value of {@link ThreadArchive}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadArchive = typeof ThreadArchive.Type;

const admissionFitsTransfer = makeTransferFactCheck(ThreadAdmission);
const abortFitsTransfer = makeTransferFactCheck(ThreadAbortFact);
const approvalFitsTransfer = makeTransferFactCheck(ThreadApprovalFact);
const resolutionFitsTransfer = makeTransferFactCheck(ThreadResolutionFact);
const deliveryFitsTransfer = makeTransferFactCheck(MessageDeliveryRecord);

/**
 * Imported Thread identity, record format, final canonical sequence, and restored record and
 * submission counts.
 *
 * @category models
 * @since 0.1.0
 */
export class ThreadImportResult extends Schema.Class<ThreadImportResult>(
  "@effect-agent/thread/ThreadImportResult",
)({
  threadId: ThreadId,
  format: Schema.String,
  tailSequence: CanonicalSequence,
  recordCount: Schema.Natural,
  submissionCount: Schema.Natural,
}) {}

/**
 * A transfer cannot be imported because its format, evidence, destination state, obligations,
 * policy, or capacity is unsupported or conflicting.
 *
 * @category errors
 * @since 0.1.0
 */
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

/**
 * One owner transaction consumes the stream; E and R are preserved, including rollback on interruption.
 *
 * @category services
 * @since 0.1.0
 */
export class ThreadImport extends Context.Service<
  ThreadImport,
  {
    readonly import: <E, R>(
      pages: Stream.Stream<ThreadArchive, E, R>,
    ) => Effect.Effect<ThreadImportResult, E | ThreadImportRejected | ThreadStoreError, R>;
  }
>()("@effect-agent/thread/ThreadImport") {}

/**
 * One admitted Submission reconstructed from exact canonical input, terminal, suspension, approval,
 * and reconciliation evidence.
 *
 * @category models
 * @since 0.1.0
 */
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

/**
 * Validated batch and its exact encoded record strings, with the canonical sequence range to stage
 * atomically.
 *
 * @category models
 * @since 0.1.0
 */
export interface PreparedImportBatch {
  readonly batch: CanonicalBatch;
  readonly batchJson: string;
  readonly recordJson: ReadonlyArray<string>;
  readonly firstSequence: CanonicalSequence;
  readonly lastSequence: CanonicalSequence;
}

/**
 * Captured transfer page with decoded records and validated batches ready for destination staging.
 *
 * @category models
 * @since 0.1.0
 */
export interface PreparedImportPage {
  readonly archive: ThreadArchive;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly batches: ReadonlyArray<PreparedImportBatch>;
}

/**
 * Resolve every declared immutable reference, even in a preparation without a continuation.
 *
 * @category operations
 * @since 0.1.0
 */
export const verifyImportedReferences = Effect.fnUntraced(function* (
  entry: CanonicalRecordEnvelope,
) {
  const reader = yield* ThreadImportReader;
  const resolve = reader.record;
  const payload = entry.record.payload;

  const deliveries = yield* prepareCanonicalMessageDeliveries(entry.threadId, [entry.record]).pipe(
    Effect.mapError(() => invalid("Invalid canonical delivery", entry.threadId)),
  );

  for (const expected of deliveries) {
    const retained = yield* reader.delivery(expected.key.messageId);

    if (retained === undefined || !sameMessageDeliveryIdentity(retained, expected))
      return yield* invalid("Canonical delivery has no matching outbox row", entry.threadId);
  }

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

  if (payload._tag === "RunContextRecorded") {
    const owner = yield* reader.runOwner(payload.runId);

    const original =
      owner === undefined ? undefined : yield* resolve(submissionInputRecordId(owner.submissionId));

    if (
      original === undefined ||
      original.threadId !== entry.threadId ||
      original.sequence >= entry.sequence
    )
      return yield* invalid("Saved context has no original admitted input", entry.threadId);
    yield* validateContextBoundary(payload, original).pipe(
      Effect.mapError((error) => invalid(error.message, entry.threadId)),
    );
  }

  const references =
    payload._tag === "WorkerInputRefused"
      ? [payload.reservation, payload.stop]
      : payload._tag === "RunContextRecorded"
        ? payload.retained
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
  }
  if (payload._tag === "WorkerInputRefused") {
    const request = (yield* resolve(payload.reservation.recordId))!.record.payload;
    const stop = (yield* resolve(payload.stop.recordId))!.record.payload;
    const retained = yield* reader.delivery(payload.messageId);

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

/**
 * Constructs a typed invalid-archive rejection, optionally identifying the affected Thread.
 *
 * @category operations
 * @since 0.1.0
 */
export const invalidThreadArchive = (message: string, threadId?: ThreadId) =>
  ThreadImportRejected.make({
    reason: "invalid-archive",
    message,
    ...(threadId === undefined ? {} : { threadId }),
  });

const invalid = invalidThreadArchive;

/**
 * Validate authoritative active state against the same staged/captured history and admissions.
 *
 * @category operations
 * @since 0.1.0
 */
export const validateImportedExecution = Effect.fnUntraced(function* (
  threadId: ThreadId,
  checkpoint: ExecutionCheckpoint,
  tailSequence: CanonicalSequence,
) {
  const reader = yield* ThreadImportReader;
  const progress = checkpoint.progress;
  const admission = yield* reader.admission(progress.submissionId);

  const own = (yield* reader.runRecords(progress.runId)).filter((entry) =>
    executionRunIds(entry.record).includes(progress.runId),
  );

  const local = new Map(checkpoint.operational.map((record) => [record.recordId, record]));

  if (
    admission === undefined ||
    admission.threadId !== threadId ||
    runIdForSubmission(admission.submissionId) !== progress.runId ||
    checkpoint.progressRecord.recordId !==
      JSON.stringify(["continuation@1", progress.runId, checkpoint.batchId]) ||
    checkpoint.throughSequence > tailSequence ||
    checkpoint.originalSequence > checkpoint.throughSequence ||
    local.size !== checkpoint.operational.length ||
    checkpoint.operational.some(
      (record) => !isOperationalRecord(record) || !executionRunIds(record).includes(progress.runId),
    ) ||
    own.filter((entry) => entry.sequence <= checkpoint.throughSequence).length !==
      checkpoint.canonicalRecords ||
    !validateSuffix(own, checkpoint.throughSequence)
  )
    return yield* invalid(
      "Active execution identity, revision or history boundary is invalid",
      threadId,
    );
  for (const id of local.keys())
    if ((yield* reader.record(id)) !== undefined)
      return yield* invalid("Operational identity collides with retained history", threadId);

  const resolve = Effect.fnUntraced(function* (ref: typeof progress.originalInput) {
    const retained = yield* reader.record(ref.recordId);
    const record = local.get(ref.recordId) ?? retained?.record;

    if (
      record === undefined ||
      (retained !== undefined &&
        (retained.threadId !== threadId || retained.sequence > checkpoint.throughSequence)) ||
      !executionRunIds(record).includes(progress.runId)
    )
      return yield* invalid("Active execution reference is missing, foreign or corrupt", threadId);

    return record;
  });

  const original = yield* reader.record(progress.originalInput.recordId);
  const input = (yield* resolve(progress.originalInput)).payload;

  if (
    original === undefined ||
    original.sequence !== checkpoint.originalSequence ||
    input._tag !== "UserInputRecorded" ||
    input.kind !== "user" ||
    input.runId !== progress.runId ||
    input.submissionId !== progress.submissionId
  )
    return yield* invalid("Active execution has no exact original admitted input", threadId);
  yield* resolve(progress.lastFact);
  if (progress.savedContext !== undefined) {
    const context = yield* resolve(progress.savedContext);

    if (context.payload._tag !== "RunContextRecorded" || !local.has(context.recordId))
      return yield* invalid("Saved context is not owned active state", threadId);
    yield* validateContextBoundary(context.payload, original).pipe(
      Effect.mapError((cause) => invalid(cause.message, threadId)),
    );
    yield* readRunContext(context.payload, original, progress.savedContext.recordId).pipe(
      Effect.mapError((cause) => invalid(cause.message, threadId)),
    );
  }
  if (progress.latestResponse !== undefined) {
    const response = (yield* resolve(progress.latestResponse)).payload;

    if (
      response._tag !== "ModelResponseRecorded" ||
      response.turn !== progress.accounting.committedTurns ||
      progress.savedContext === undefined
    )
      return yield* invalid("Active execution has invalid declared operation evidence", threadId);
  } else if (progress.accounting.committedTurns !== 0)
    return yield* invalid("Active accounting has no corresponding response", threadId);
  if (progress.terminal !== undefined) {
    const terminal = (yield* resolve(progress.terminal)).payload;

    if (
      terminal._tag !== "RunCompleted" &&
      terminal._tag !== "RunFailed" &&
      terminal._tag !== "SubmissionSettled"
    )
      return yield* invalid("Active execution has invalid terminal intent", threadId);
    if (progress.position === "settled" && terminal._tag !== "SubmissionSettled")
      return yield* invalid("Settled active state has no canonical terminal outcome", threadId);
  }
});

/**
 * An executed unfinished Run cannot be imported without its authoritative operational owner.
 *
 * @category operations
 * @since 0.1.0
 */
export const validateImportedRunExecution = Effect.fnUntraced(function* (
  threadId: ThreadId,
  runId: RunId,
  tailSequence: CanonicalSequence,
) {
  const reader = yield* ThreadImportReader;
  const checkpoint = yield* reader.execution(runId);

  if (checkpoint !== undefined)
    return yield* validateImportedExecution(threadId, checkpoint, tailSequence);
  const records = yield* reader.runRecords(runId);

  if (
    records.some(
      ({ record }) =>
        record.payload._tag === "UserInputRecorded" &&
        record.payload.kind === "user" &&
        record.payload.submissionId !== undefined,
    ) &&
    !records.some(
      ({ record }) => record.payload._tag === "SubmissionSettled" && record.payload.runId === runId,
    ) &&
    records.some(({ record }) => !isPreContinuationFact(record))
  )
    return yield* invalid("Unfinished execution is missing its authoritative checkpoint", threadId);
});

/**
 * Own the first page before entering a destination gate; reuse the same scoped source pull.
 *
 * @category schemas
 * @since 0.1.0
 */
export const captureImportSource = <E, R, A, E2, R2>(
  pages: Stream.Stream<ThreadArchive, E, R>,
  use: (captured: Stream.Stream<ThreadArchive, E, R>) => Effect.Effect<A, E2, R2>,
): Effect.Effect<A, E | E2 | ThreadImportRejected, Exclude<R | R2, Scope.Scope>> =>
  Effect.scoped(
    Effect.suspend(() => {
      const phase = { writing: false };

      return Effect.gen(function* () {
        const pull = yield* Stream.toPull(Stream.rechunk(pages, 1));

        const first = yield* Pull.catchDone(pull, () =>
          Effect.fail(invalid("Missing transfer pages")),
        );

        const codec = Schema.fromJsonString(ThreadArchive);

        const wire = yield* Schema.encodeEffect(codec)(first[0]).pipe(
          Effect.mapError((cause) => invalid(`Malformed first transfer page: ${cause.message}`)),
        );

        if (utf8ByteLength(wire) > MAX_THREAD_EXPORT_PAGE_BYTES)
          return yield* invalid("Transfer page exceeds 32 MiB");

        const owned = yield* Schema.decodeEffect(codec)(wire).pipe(
          Effect.mapError((cause) => invalid(`Malformed first transfer page: ${cause.message}`)),
        );

        phase.writing = true;

        return yield* use(
          Stream.succeed(owned).pipe(Stream.concat(Stream.fromPull(Effect.succeed(pull)))),
        );
      }).pipe(Effect.provideService(ImportSourcePhase, phase));
    }),
  );

/**
 * Constant-size fold state; identity uniqueness belongs to staged destination indexes.
 *
 * @category models
 * @since 0.1.0
 */
export interface ThreadImportProgress {
  manifest: ThreadArchive | undefined;
  sequence: number;
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

/**
 * Creates empty bounded fold state for validating one ordered transfer stream.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeThreadImportProgress = (): ThreadImportProgress => ({
  manifest: undefined,
  sequence: 0,
  ended: false,
  cursor: undefined,
  counts: { admissions: 0, aborts: 0, approvals: 0, resolutions: 0, deliveries: 0 },
});

/**
 * Capture before awaiting any writes; the retained manifest never retains page payload arrays.
 *
 * @category schemas
 * @since 0.1.0
 */
export const prepareImportPage = Effect.fnUntraced(function* (
  state: ThreadImportProgress,
  input: ThreadArchive,
  options?: { readonly verification?: boolean },
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

  // A final source page may omit its cursor. Restored facts must remain exportable after
  // later admissions, commands or a worker seal change the manifest and continuation.
  if (
    !transferPageFits(threadId) ||
    archive.admissions.some((fact) => !admissionFitsTransfer(threadId, fact)) ||
    archive.commands.aborts.some((fact) => !abortFitsTransfer(threadId, fact)) ||
    archive.commands.approvals.some((fact) => !approvalFitsTransfer(threadId, fact)) ||
    archive.commands.resolutions.some((fact) => !resolutionFitsTransfer(threadId, fact)) ||
    archive.deliveries.some((fact) => !deliveryFitsTransfer(threadId, fact))
  )
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-capacity",
      message: "Imported fact exceeds its bounded transfer representation",
    });

  if (state.ended) return yield* invalid("Page after the final transfer page", threadId);
  if (archive.format !== CURRENT_RECORD_FORMAT)
    return yield* ThreadImportRejected.make({
      threadId,
      reason: "unsupported-format",
      message: `Only ${CURRENT_RECORD_FORMAT} can be imported`,
    });
  if (state.manifest !== undefined && !sameTransferManifest(state.manifest, archive))
    return yield* invalid("Transfer snapshot changed", threadId);
  if (
    archive.fromSequence !== state.sequence + 1 ||
    (archive.cursor !== undefined && archive.cursor === state.cursor)
  )
    return yield* invalid("Transfer page position is invalid", threadId);
  if (archive.records.length > 0 && transferSections.some((section) => state.counts[section] > 0))
    return yield* invalid("Canonical records follow immutable fact sections", threadId);
  if (
    archive.admissions.length > 0 &&
    state.counts.aborts +
      state.counts.approvals +
      state.counts.resolutions +
      state.counts.deliveries >
      0
  )
    return yield* invalid("Admissions follow accepted command sections", threadId);
  const records: Array<CanonicalRecordEnvelope> = [];
  const prepared: Array<PreparedImportBatch> = [];
  let index = 0;

  for (const batch of archive.batches) {
    const current: Array<RecordEnvelope> = [];
    const encoded: Array<RecordJson> = [];
    const firstSequence = sequence(state.sequence + 1);

    while (archive.records[index]?.batchId === batch.batchId) {
      const entry = archive.records[index]!;

      if (entry.threadId !== threadId || entry.sequence !== state.sequence + 1)
        return yield* invalid("Canonical identity or sequence mismatch", threadId);

      const record = yield* decodeExportRecord(archive.format, entry.record).pipe(
        Effect.mapError((e) => invalid(e.message, threadId)),
      );

      if (record.payload._tag === "RunContinuation")
        return yield* invalid("Mutable interpreter progress is not canonical history", threadId);

      records.push(CanonicalRecordEnvelope.make({ ...entry, record }));
      current.push(record);
      encoded.push(entry.record);
      index++;
      state.sequence++;
    }

    const canonical = yield* Schema.decodeUnknownEffect(
      Schema.toType(Schema.Struct(CanonicalBatch.fields)),
    )({ ...batch, records: current }).pipe(
      Effect.mapError(() => invalid("Empty or oversized canonical batch", threadId)),
      Effect.map((value) => CanonicalBatch.make(value)),
    );

    const batchJson = canonicalJson({ ...batch, records: encoded });

    if (utf8ByteLength(batchJson) > 16 * 1024 * 1024)
      return yield* invalid("Canonical batch exceeds 16 MiB", threadId);
    prepared.push({
      batch: canonical,
      batchJson,
      recordJson: encoded.map(canonicalJson),
      firstSequence,
      lastSequence: sequence(state.sequence),
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
    state.sequence !== archive.tailSequence
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

  const sectionLength = (section: (typeof transferSections)[number]) =>
    section === "admissions"
      ? archive.admissions.length
      : section === "deliveries"
        ? archive.deliveries.length
        : archive.commands[section].length;

  for (let index = 0; index < transferSections.length; index++) {
    const section = transferSections[index]!;

    if (sectionLength(section) === 0) continue;
    if (
      transferSections
        .slice(0, index)
        .some((prior) => state.counts[prior] + sectionLength(prior) !== archive.snapshot[prior]) ||
      transferSections.slice(index + 1).some((later) => state.counts[later] !== 0)
    )
      return yield* invalid("Transfer fact sections are out of order", threadId);
  }
  const required = transferDependencies(records);

  if (canonicalJson(required) !== canonicalJson([...archive.dependencies].sort()))
    return yield* invalid("Cross-range dependency directory is incomplete or ambiguous", threadId);
  for (const section of transferSections) {
    state.counts[section] += sectionLength(section);
    if (state.counts[section] > archive.snapshot[section])
      return yield* invalid(`Too many ${section} facts`, threadId);
  }
  for (const admission of archive.admissions) {
    if (!options?.verification && admission.settledAt === undefined)
      return yield* invalid(
        "Closed admission is missing its stable settlement timestamp",
        threadId,
      );
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
      (!options?.verification &&
        (delivery.leaseUntilMillis !== null ||
          !["processed", "refused"].includes(delivery.status))) ||
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

/**
 * Rejects incomplete canonical or fact sections and returns restored counts after the transfer's
 * final page.
 *
 * @category operations
 * @since 0.1.0
 */
export const finishThreadImport = Effect.fnUntraced(function* (state: ThreadImportProgress) {
  const page = state.manifest;

  if (
    page === undefined ||
    !state.ended ||
    state.sequence !== page.tailSequence ||
    transferSections.some((section) => state.counts[section] !== page.snapshot[section])
  )
    return yield* invalid(
      "Transfer stream ended before its complete canonical sequence and fact sections",
    );

  return ThreadImportResult.make({
    threadId: page.threadId,
    format: page.format,
    tailSequence: page.tailSequence,
    recordCount: state.sequence,
    submissionCount: state.counts.admissions,
  });
});

/**
 * An executed receipt must settle strictly after its own input. FIFO inputs are checked in one pass.
 *
 * @category operations
 * @since 0.1.0
 */
export const verifyImportedInputOrder = Effect.fnUntraced(function* (
  admission: Pick<ThreadAdmission, "threadId" | "submissionId" | "queueSequence">,
) {
  const reader = yield* ThreadImportReader;
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
});

/**
 * Rebuild ONE admitted submission from exact staged evidence; terminal state does not erase uncertainty.
 *
 * @category operations
 * @since 0.1.0
 */
export const rebuildImportedSubmission = Effect.fnUntraced(function* (admission: ThreadAdmission) {
  const reader = yield* ThreadImportReader;
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

/**
 * Immediate persistent history has no ledger/checkpoint owner, but still needs a terminal Run fact.
 *
 * @category operations
 * @since 0.1.0
 */
export const validateClosedRun = Effect.fnUntraced(function* (threadId: ThreadId, runId: RunId) {
  const reader = yield* ThreadImportReader;
  const records = yield* reader.runRecords(runId);

  const opened = records.some(
    ({ record: { payload } }) =>
      (payload._tag === "UserInputRecorded" ||
        payload._tag === "RunStarted" ||
        payload._tag === "ModelResponseRecorded") &&
      payload.runId === runId,
  );

  const closed = records.some(
    ({ record: { payload } }) =>
      (payload._tag === "RunCompleted" ||
        payload._tag === "RunFailed" ||
        payload._tag === "SubmissionSettled") &&
      payload.runId === runId,
  );

  if (opened && !closed) return yield* invalid("Transfer contains an unfinished Run", threadId);
});

/**
 * Idle transfer keeps closed receipts, never the authority to finish their former work.
 *
 * @category operations
 * @since 0.1.0
 */
export const rebuildClosedSubmission = Effect.fnUntraced(function* (admission: ThreadAdmission) {
  if (admission.settledAt === undefined)
    return yield* invalid(
      "Closed admission is missing its stable settlement timestamp",
      admission.threadId,
    );
  const rebuilt = yield* rebuildImportedSubmission(admission);
  const reader = yield* ThreadImportReader;

  if (
    rebuilt.state !== "settled" ||
    rebuilt.unknownToolCallIds.length > 0 ||
    rebuilt.suspension !== undefined
  )
    return yield* invalid("Transfer contains unfinished accepted work", admission.threadId);
  if (rebuilt.abort !== undefined && rebuilt.abort.canonicalRecordId === undefined)
    return yield* invalid("Transfer contains an unapplied abort", admission.threadId);
  const records = yield* reader.runRecords(runIdForSubmission(admission.submissionId));

  const decidedApprovals = new Set(
    records.flatMap(({ record: { payload } }) =>
      payload._tag === "ToolApprovalDecided" ? [payload.toolCallId] : [],
    ),
  );

  if (
    records.some(
      ({ record: { payload } }) =>
        payload._tag === "ToolApprovalRequested" && !decidedApprovals.has(payload.toolCallId),
    )
  )
    return yield* invalid("Transfer contains an unresolved approval", admission.threadId);
  for (const approval of rebuilt.approvals)
    if (
      !records.some(
        ({ record: { payload } }) =>
          payload._tag === "ToolApprovalDecided" && payload.toolCallId === approval.toolCallId,
      )
    )
      return yield* invalid("Transfer contains an unapplied approval", admission.threadId);
  for (const resolution of rebuilt.resolutions)
    if (
      resolution.resolution._tag !== "AbortSubmission" &&
      !records.some(
        ({ record: { payload } }) =>
          payload._tag === "ToolCallResolved" &&
          payload.toolCallId === resolution.toolCallId &&
          (payload.resolution === "safe-retry" ? "execution" : "factual") ===
            unknownResolutionKind(resolution.resolution),
      )
    )
      return yield* invalid("Transfer contains an unapplied resolution", admission.threadId);
  yield* verifyImportedInputOrder(admission);

  return rebuilt;
});

/**
 * Encode one page at a time and retain upstream stream errors and requirements.
 *
 * @category operations
 * @since 0.1.0
 */
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
