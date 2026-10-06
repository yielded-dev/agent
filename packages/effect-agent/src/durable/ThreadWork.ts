import type { Crypto } from "effect";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";

import { ReceiptId, RunId, SubmissionId, ThreadId, ToolCallId } from "../core/Identifiers.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import { IdempotencyKey, QueueSequence } from "../core/Receipt.ts";
import {
  CanonicalSequence,
  Digest,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_EVIDENCE_RECORDS,
  RecordId,
  type CanonicalRecordEnvelope,
  type ModelResponseRecorded,
} from "./Records.ts";
import { canonicalRecordBytes, reference, resolveEvidence } from "./RunContinuation.ts";
import { runIdForSubmission } from "./RunJournal.ts";
import type { SubmissionWorkItem } from "./SubmissionLedger.ts";
import { ThreadReader, ThreadStore, ThreadStoreError } from "./ThreadStore.ts";

export const WORK_INDEX_VERSION = 2;
export const MAX_WORK_PAGE_ENTRIES = 128;
export const MAX_WORK_ENTRY_BYTES = 8 * 1024;
export const MAX_WORK_PAGE_BYTES = MAX_WORK_PAGE_ENTRIES * MAX_WORK_ENTRY_BYTES;
export const MAX_WORK_REBUILD_RECORDS = 256;
export const MAX_WORK_REBUILD_BYTES = 32 * 1024 * 1024;
export const MAX_RECOVERY_WORK_ITEMS = 32;
export const MAX_RECOVERY_PAGES = 8;

const Cursor = Schema.NonEmptyString.check(Schema.isMaxLength(4096));
const WorkId = Schema.NonEmptyString.check(Schema.isMaxLength(4096));

/** Stable logical owners. Finding an owner does not acquire its lease, grant, or authority. */
export const WorkOwner = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Admission"), submissionId: SubmissionId }),
  Schema.Struct({ _tag: Schema.Literal("Operation"), runId: RunId, toolCallId: ToolCallId }),
  Schema.Struct({
    _tag: Schema.Literal("Child"),
    runId: RunId,
    toolCallId: ToolCallId,
    childThreadId: ThreadId,
  }),
  Schema.Struct({
    _tag: Schema.Literal("WorkerInput"),
    messageId: IdempotencyKey,
    workerThreadId: ThreadId,
    update: Schema.Boolean,
  }),
  Schema.Struct({ _tag: Schema.Literal("Report"), runId: RunId }),
  Schema.Struct({ _tag: Schema.Literal("WorkerEffects"), submissionId: SubmissionId }),
  Schema.Struct({ _tag: Schema.Literal("Delivery"), messageId: IdempotencyKey }),
  Schema.Struct({
    _tag: Schema.Literal("Handoff"),
    recordId: RecordId,
    kind: Schema.Literals([
      "peer",
      "report",
      "update",
      "worker-stop",
      "reservation",
      "child-accounting",
    ]),
    messageId: Schema.optionalKey(IdempotencyKey),
    childThreadId: Schema.optionalKey(ThreadId),
  }),
]);

export type WorkOwner = typeof WorkOwner.Type;

export const WorkStateReference = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Submission"), submissionId: SubmissionId }),
  Schema.Struct({ _tag: Schema.Literal("Canonical"), recordId: RecordId, digest: Digest }),
  Schema.Struct({
    _tag: Schema.Literal("Delivery"),
    messageId: IdempotencyKey,
    version: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
]);

/** Content-free scheduling metadata; resolve the selected owner through its storage port. */
export const ThreadWorkEntry = Schema.Struct({
  id: WorkId,
  owner: WorkOwner,
  stateReference: WorkStateReference,
  state: Schema.Literals(["ready", "waiting", "unknown", "handoff"]),
  wait: Schema.optionalKey(
    Schema.Literals([
      "admission",
      "ownership",
      "approval",
      "input",
      "child",
      "worker",
      "effect-resolution",
      "destination",
      "parked",
      "capacity",
    ]),
  ),
  notBeforeMillis: Schema.optionalKey(Schema.Natural),
  receiptId: Schema.optionalKey(ReceiptId),
  queueSequence: Schema.optionalKey(QueueSequence),
  originRecordId: Schema.optionalKey(RecordId),
  originDigest: Schema.optionalKey(Digest),
  /** Separate accepted-update capacity from ordinary message/worker transport. */
  partition: Schema.optionalKey(Schema.Literals(["ordinary", "update"])),
});

export type ThreadWorkEntry = typeof ThreadWorkEntry.Type;

/** The initial declaration remains addressable while later facts change an operation's state. */
export const CanonicalWorkEntry = Schema.Struct({
  ...ThreadWorkEntry.fields,
  createdSequence: CanonicalSequence,
  updatedSequence: CanonicalSequence,
  originRecordId: RecordId,
  originDigest: Digest,
});

export type CanonicalWorkEntry = typeof CanonicalWorkEntry.Type;

export const ThreadWorkRequest = Schema.Struct({
  threadId: ThreadId,
  cursor: Schema.optionalKey(Cursor),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_WORK_PAGE_ENTRIES })),
});

export type ThreadWorkRequest = typeof ThreadWorkRequest.Type;

export const ThreadWorkPage = Schema.Struct({
  entries: Schema.Array(ThreadWorkEntry).check(Schema.isMaxLength(MAX_WORK_PAGE_ENTRIES)),
  /** An empty page with a cursor is progress, not a complete empty inventory. */
  cursor: Schema.optionalKey(Cursor),
});

export type ThreadWorkPage = typeof ThreadWorkPage.Type;

export const ThreadWorkCursor = Schema.Struct({
  version: Schema.Literal(WORK_INDEX_VERSION),
  threadId: ThreadId,
  source: Schema.Literals(["admissions", "canonical", "deliveries"]),
  after: Schema.optionalKey(Cursor),
});

export type ThreadWorkCursor = typeof ThreadWorkCursor.Type;

export const WorkThreadsRequest = Schema.Struct({
  afterThreadId: Schema.optionalKey(ThreadId),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 32 })),
});

export type WorkThreadsRequest = typeof WorkThreadsRequest.Type;

export const WorkThreadsPage = Schema.Struct({
  threadIds: Schema.Array(ThreadId).check(Schema.isMaxLength(32)),
  afterThreadId: Schema.optionalKey(ThreadId),
});

export type WorkThreadsPage = typeof WorkThreadsPage.Type;

export const WorkIndexRebuildRequest = Schema.Struct({
  threadId: ThreadId,
  /** Explicitly discard a damaged derivative; canonical facts and owner state are untouched. */
  restart: Schema.optionalKey(Schema.Boolean),
  /** At most 256 records per pass by default; the 32 MiB byte cap may stop a pass sooner. */
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_WORK_REBUILD_RECORDS })),
  ),
});

export type WorkIndexRebuildRequest = typeof WorkIndexRebuildRequest.Type;

export const WorkIndexProgress = Schema.Struct({
  version: Schema.Literal(WORK_INDEX_VERSION),
  threadId: ThreadId,
  state: Schema.Literals(["ready", "rebuilding"]),
  throughSequence: CanonicalSequence,
  tailSequence: CanonicalSequence,
  processedRecords: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_WORK_REBUILD_RECORDS)),
  processedBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_WORK_REBUILD_BYTES)),
});

export type WorkIndexProgress = typeof WorkIndexProgress.Type;

export class WorkDiscoveryUnavailable extends Schema.TaggedError<WorkDiscoveryUnavailable>()(
  "WorkDiscoveryUnavailable",
  {
    threadId: ThreadId,
    reason: Schema.Literals(["missing-index", "incomplete-rebuild", "unsupported"]),
  },
) {}

/** Native owner pages and canonical-index maintenance, implemented at the storage owner. */
export interface ThreadWorkStorage {
  readonly threads: (
    request: WorkThreadsRequest,
  ) => Effect.Effect<WorkThreadsPage, ThreadStoreError>;
  readonly page: (
    request: ThreadWorkRequest,
  ) => Effect.Effect<ThreadWorkPage, WorkDiscoveryUnavailable | ThreadStoreError>;
  /** One bounded pass. Append transactions and the completion check serialize with this pass. */
  readonly rebuild: (
    request: WorkIndexRebuildRequest,
  ) => Effect.Effect<WorkIndexProgress, WorkDiscoveryUnavailable | ThreadStoreError>;
}

/**
 * Inventories combine current admission/delivery owners with canonical operations and handoffs.
 * Cursors are Thread-bound, forward-only live scans. Revalidate selected candidates; creations
 * before an already consumed cursor appear in the next scan. Rebuild never runs implicitly.
 */
export class ThreadWorkDiscovery extends Context.Service<ThreadWorkDiscovery, ThreadWorkStorage>()(
  "@effect-agent/thread/ThreadWorkDiscovery",
) {
  static readonly layer = Layer.effect(
    ThreadWorkDiscovery,
    Effect.map(
      ThreadStore,
      (store) =>
        store.work ?? {
          threads: () =>
            ThreadStoreError.make({
              operation: "discover Thread work",
              message: "The storage adapter has no work-discovery port",
            }),
          page: ({ threadId }) =>
            WorkDiscoveryUnavailable.make({ threadId, reason: "unsupported" }),
          rebuild: ({ threadId }) =>
            WorkDiscoveryUnavailable.make({ threadId, reason: "unsupported" }),
        },
    ),
  );
}

/** Opaque tuple identity; never split or reinterpret an application identifier. */
export const workId = (kind: string, ...ids: ReadonlyArray<string>): string =>
  JSON.stringify([kind, ...ids]);

export const admissionWork = (row: SubmissionWorkItem): ThreadWorkEntry => ({
  id: workId("admission", row.submissionId),
  owner: { _tag: "Admission", submissionId: row.submissionId },
  stateReference: { _tag: "Submission", submissionId: row.submissionId },
  receiptId: row.receiptId,
  queueSequence: row.queueSequence,
  state:
    row.state === "unknown"
      ? "unknown"
      : row.state === "suspended" || row.state === "joining" || row.state === "joined"
        ? "waiting"
        : "ready",
  ...(row.state === "unknown"
    ? { wait: "effect-resolution" as const }
    : row.state === "suspended"
      ? { wait: "input" as const }
      : row.state === "running"
        ? { wait: "ownership" as const }
        : row.state === "admitted"
          ? { wait: "admission" as const }
          : {}),
});

export const decodeWorkCursor = Effect.fnUntraced(function* (request: ThreadWorkRequest) {
  if (request.cursor === undefined)
    return ThreadWorkCursor.make({
      version: WORK_INDEX_VERSION,
      threadId: request.threadId,
      source: "admissions",
    });

  const cursor = yield* Schema.decodeEffect(Schema.fromJsonString(ThreadWorkCursor))(
    request.cursor,
  ).pipe(
    Effect.mapError((cause) =>
      ThreadStoreError.make({
        operation: "work cursor",
        message: "Invalid work-discovery cursor",
        cause,
      }),
    ),
  );

  if (cursor.threadId !== request.threadId)
    return yield* ThreadStoreError.make({
      operation: "work cursor",
      message: "Work cursor belongs to another Thread",
    });

  return cursor;
});

export const encodeWorkCursor = Schema.encodeSync(Schema.fromJsonString(ThreadWorkCursor));

/** Validate metadata before returning a page, including a resident encoded-byte ceiling. */
export const validateWorkPage = Effect.fnUntraced(function* (page: ThreadWorkPage, limit: number) {
  yield* Schema.decodeEffect(ThreadWorkPage)(page).pipe(
    Effect.mapError((cause) =>
      ThreadStoreError.make({
        operation: "work page",
        message: "Invalid work-discovery metadata",
        cause,
      }),
    ),
  );
  if (
    page.entries.length > limit ||
    new Set(page.entries.map((entry) => entry.id)).size !== page.entries.length ||
    page.entries.some((entry) => utf8ByteLength(JSON.stringify(entry)) > MAX_WORK_ENTRY_BYTES) ||
    utf8ByteLength(JSON.stringify(page)) > MAX_WORK_PAGE_BYTES
  )
    return yield* ThreadStoreError.make({
      operation: "work page",
      message: "Work-discovery metadata exceeds its page bound",
    });

  return page;
});

/** Exact current canonical owner state, checked against the inventory's immutable fingerprint. */
export const resolveWorkEvidence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  entry: ThreadWorkEntry,
) {
  if (entry.stateReference._tag !== "Canonical")
    return yield* ThreadStoreError.make({
      operation: "resolve work",
      message: "Work is owned by another storage port",
    });

  return yield* resolveEvidence(threadId, entry.stateReference);
});

/** Own-call facts plus the original batch's shared approval proof, without another Run's history. */
export const operationEvidence = (
  threadId: ThreadId,
  owner: Extract<WorkOwner, { _tag: "Operation" }>,
  originRecordId: RecordId,
  throughSequence: CanonicalSequence,
  declaration: ModelResponseRecorded,
): Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError, ThreadReader> =>
  Stream.unwrap(
    Effect.map(ThreadReader, (reader) =>
      Stream.suspend(() => {
        const approvalToolCallIds = declaration.toolOperations.map((call) => call.toolCallId);
        const approvalIds = new Set(approvalToolCallIds);
        let afterSequence = CanonicalSequence.make(0);
        let records = 0;
        let bytes = 0;

        return Stream.paginate(
          undefined,
          Effect.fnUntraced(function* () {
            const page = yield* Stream.runCollect(
              reader.read({
                threadId,
                selection: {
                  _tag: "OperationEvidence",
                  runId: owner.runId,
                  toolCallId: owner.toolCallId,
                  originRecordId,
                  approvalToolCallIds,
                  throughSequence,
                },
                page: { limit: 8, afterSequence },
              }),
            ).pipe(
              Effect.mapError((cause) =>
                cause._tag === "ThreadStoreError"
                  ? cause
                  : ThreadStoreError.make({
                      operation: "operation evidence",
                      message: "Selected operation is unavailable",
                      cause,
                    }),
              ),
            );

            if (page.length > 8)
              return yield* ThreadStoreError.make({
                operation: "operation evidence",
                message: "Selected operation page exceeds its bound",
              });
            for (const entry of page) {
              const payload = entry.record.payload;

              if (
                entry.threadId !== threadId ||
                entry.sequence <= afterSequence ||
                entry.sequence > throughSequence ||
                (entry.record.recordId === originRecordId
                  ? payload._tag !== "ModelResponseRecorded" ||
                    payload.runId !== owner.runId ||
                    payload.turn !== declaration.turn ||
                    payload.toolOperations.length !== approvalIds.size ||
                    payload.toolOperations.some((call) => !approvalIds.has(call.toolCallId)) ||
                    !payload.toolOperations.some((call) => call.toolCallId === owner.toolCallId)
                  : !("runId" in payload) ||
                    payload.runId !== owner.runId ||
                    !("toolCallId" in payload) ||
                    (payload._tag === "ToolApprovalRequested" ||
                    payload._tag === "ToolApprovalDecided"
                      ? payload.turn !== declaration.turn || !approvalIds.has(payload.toolCallId)
                      : payload.toolCallId !== owner.toolCallId))
              )
                return yield* ThreadStoreError.make({
                  operation: "operation evidence",
                  message: "Selected operation has invalid evidence identity",
                });
              afterSequence = entry.sequence;
              records++;
              bytes += canonicalRecordBytes(entry.record);
              if (records > MAX_RUN_EVIDENCE_RECORDS || bytes > MAX_RUN_EVIDENCE_BYTES)
                return yield* ThreadStoreError.make({
                  operation: "operation evidence",
                  message: "Selected operation exceeds its recovery bound",
                });
            }

            return [page, page.length < 8 ? Option.none() : Option.some(undefined)] as const;
          }),
        );
      }),
    ),
  );

/** The last frozen report/update, independent of completed deliveries and unrelated history. */
export const deliveryPredecessor = Effect.fnUntraced(function* (
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
) {
  const reader = yield* ThreadReader;

  const records = yield* Stream.runCollect(
    reader.read({
      threadId,
      selection: { _tag: "DeliveryPredecessor", throughSequence },
      page: { limit: 1 },
    }),
  );

  if (records.length > 1)
    return yield* ThreadStoreError.make({
      operation: "delivery predecessor",
      message: "Ambiguous delivery predecessor",
    });
  const found = records[0];

  if (found === undefined) return undefined;
  if (found.threadId !== threadId || found.sequence > throughSequence)
    return yield* ThreadStoreError.make({
      operation: "delivery predecessor",
      message: "Invalid delivery predecessor identity",
    });
  const payload = found.record.payload;

  if (payload._tag === "WorkerReportPrepared") return payload.messageId;
  if (payload._tag === "AgentUpdateEmitted" && payload.delivery !== undefined)
    return payload.delivery.messageId;

  return yield* ThreadStoreError.make({
    operation: "delivery predecessor",
    message: "Invalid delivery predecessor evidence",
  });
});

/** Origin reporting mode retained by the disposable fold, including private worker inputs. */
export type WorkerReportingMode = "none" | "private" | "standard";

export type WorkIndexChange =
  | { readonly _tag: "Put"; readonly entry: CanonicalWorkEntry }
  | { readonly _tag: "Remove"; readonly id: string; readonly stateRecordId?: RecordId }
  | { readonly _tag: "WorkerMode"; readonly mode: Exclude<WorkerReportingMode, "none"> };

/**
 * Fold only canonical creation, factual closure, and durable handoff evidence. Storage commits
 * these changes with the source batch. Rebuild folds the same rules in sequence order; it may
 * suppress a delivery handoff only after its authoritative delivery row exists.
 */
export const workIndexChanges = Effect.fnUntraced(function* (
  envelope: CanonicalRecordEnvelope,
  reporting: WorkerReportingMode,
): Effect.fn.Return<ReadonlyArray<WorkIndexChange>, ThreadStoreError, Crypto.Crypto> {
  const { record, sequence } = envelope;
  const payload = record.payload;

  if (
    !new Set([
      "ModelResponseRecorded",
      "ToolCallUnknown",
      "ToolApprovalRequested",
      "ToolApprovalDecided",
      "ToolCallSettled",
      "ToolCallResolved",
      "SubagentRequested",
      "SubagentJoined",
      "WorkerInputRequested",
      "WorkerInputCompleted",
      "WorkerInputRefused",
      "WorkerOriginRecorded",
      "SubmissionSettled",
      "WorkerReportPrepared",
      "WorkerReportRefused",
      "PeerMessagePrepared",
      "AgentUpdateEmitted",
      "WorkerStopRequested",
      "SubtreeBudgetReserved",
      "WorkHandoffCompleted",
    ]).has(payload._tag)
  )
    return [] as ReadonlyArray<WorkIndexChange>;
  const ref = yield* reference(record);

  const remove = (kind: string, ...ids: ReadonlyArray<string>): WorkIndexChange => ({
    _tag: "Remove",
    id: workId(kind, ...ids),
  });

  const put = (
    id: string,
    owner: WorkOwner,
    state: ThreadWorkEntry["state"],
    wait?: ThreadWorkEntry["wait"],
  ): WorkIndexChange => ({
    _tag: "Put",
    entry: {
      id,
      owner,
      stateReference: { _tag: "Canonical", ...ref },
      state,
      ...(wait === undefined ? {} : { wait }),
      createdSequence: sequence,
      updatedSequence: sequence,
      originRecordId: record.recordId,
      originDigest: ref.digest,
    },
  });

  const operation = (
    runId: RunId,
    toolCallId: ToolCallId,
    state: ThreadWorkEntry["state"],
    wait?: ThreadWorkEntry["wait"],
  ) =>
    put(
      workId("operation", runId, toolCallId),
      { _tag: "Operation", runId, toolCallId },
      state,
      wait,
    );

  const handoff = (
    kind: Extract<WorkOwner, { _tag: "Handoff" }>["kind"],
    messageId?: IdempotencyKey,
    childThreadId?: ThreadId,
  ) =>
    put(
      workId("handoff", record.recordId),
      {
        _tag: "Handoff",
        recordId: record.recordId,
        kind,
        ...(messageId === undefined ? {} : { messageId }),
        ...(childThreadId === undefined ? {} : { childThreadId }),
      },
      "handoff",
      "destination",
    );

  switch (payload._tag) {
    case "ModelResponseRecorded":
      return payload.toolOperations.map((call) =>
        operation(payload.runId, call.toolCallId, "ready"),
      );
    case "ToolCallUnknown":
      return [operation(payload.runId, payload.toolCallId, "unknown", "effect-resolution")];
    case "ToolApprovalRequested":
      return [operation(payload.runId, payload.toolCallId, "waiting", "approval")];
    case "ToolApprovalDecided":
      return [operation(payload.runId, payload.toolCallId, "ready")];
    case "ToolCallSettled":
      return [remove("operation", payload.runId, payload.toolCallId)];
    case "ToolCallResolved":
      return payload.resolution === "safe-retry"
        ? [operation(payload.runId, payload.toolCallId, "unknown", "effect-resolution")]
        : [remove("operation", payload.runId, payload.toolCallId)];
    case "SubagentRequested":
      return [
        remove("reservation", payload.reservationId),
        put(
          workId("child", payload.runId, payload.toolCallId),
          {
            _tag: "Child",
            runId: payload.runId,
            toolCallId: payload.toolCallId,
            childThreadId: payload.childThreadId,
          },
          "waiting",
          "child",
        ),
      ];
    case "SubagentJoined":
      return [remove("child", payload.runId, payload.toolCallId), handoff("child-accounting")];
    case "WorkerInputRequested":
      return [
        remove("reservation", payload.admission.messageId),
        put(
          workId("worker-input", payload.admission.messageId),
          {
            _tag: "WorkerInput",
            messageId: payload.admission.messageId,
            workerThreadId: payload.admission.origin.worker.threadId,
            update: payload.admission.reportKind === "update",
          },
          "waiting",
          "worker",
        ),
      ];
    case "WorkerInputCompleted":
      return payload.effectsResolved === true
        ? [
            remove("worker-input", payload.messageId),
            remove("worker-effects", payload.submissionId),
          ]
        : [];
    case "WorkerInputRefused":
      return [
        {
          _tag: "Remove",
          id: workId("worker-input", payload.messageId),
          stateRecordId: payload.reservation.recordId,
        },
        {
          _tag: "Remove",
          id: workId("reservation", payload.messageId),
          stateRecordId: payload.reservation.recordId,
        },
      ];
    case "WorkerOriginRecorded":
      return [
        {
          _tag: "WorkerMode",
          mode: payload.origin.reporting?.mode === "standard" ? "standard" : "private",
        },
      ];
    case "SubmissionSettled":
      return reporting === "none"
        ? []
        : [
            put(
              workId("worker-effects", payload.submissionId),
              { _tag: "WorkerEffects", submissionId: payload.submissionId },
              "waiting",
              "effect-resolution",
            ),
            ...(reporting === "standard" &&
            payload.runId === runIdForSubmission(payload.submissionId)
              ? [
                  put(
                    workId("report", payload.runId),
                    { _tag: "Report", runId: payload.runId },
                    "ready",
                  ),
                ]
              : []),
          ];
    case "WorkerReportPrepared":
      return [remove("report", payload.runId), handoff("report", payload.messageId)];
    case "WorkerReportRefused":
      return [remove("report", payload.runId)];
    case "PeerMessagePrepared":
      return [handoff("peer", payload.messageId)];
    case "AgentUpdateEmitted":
      return payload.delivery === undefined ? [] : [handoff("update", payload.delivery.messageId)];
    case "WorkerStopRequested":
      return [handoff("worker-stop", undefined, payload.command.worker.threadId)];
    case "SubtreeBudgetReserved":
      return [
        put(
          workId("reservation", payload.reservationId),
          {
            _tag: "Handoff",
            recordId: record.recordId,
            kind: "reservation",
            childThreadId: payload.childThreadId,
          },
          "waiting",
          "capacity",
        ),
      ];
    case "WorkHandoffCompleted":
      return [{ _tag: "Remove", id: payload.ownerId }];
    default:
      return [];
  }
});

/** Backfill is chronological. A state's creation identity never changes on later updates. */
export const advanceWorkEntry = (
  previous: CanonicalWorkEntry | undefined,
  next: CanonicalWorkEntry,
): CanonicalWorkEntry =>
  previous === undefined
    ? next
    : {
        ...next,
        createdSequence: previous.createdSequence,
        originRecordId: previous.originRecordId,
        originDigest: previous.originDigest,
      };
