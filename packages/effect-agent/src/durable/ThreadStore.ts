import { Context, Effect, Layer, Option, Schema, Stream } from "effect";

import { ReceiptId, RunId, SubmissionId, ThreadId, ToolCallId } from "../core/Identifiers.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import { QueueSequence } from "../core/Receipt.ts";
import { AssignmentTerminal } from "../core/Worker.ts";
import type { IntegrityReport } from "./Admin.ts";
import { digestCanonicalBatchJson } from "./Digest.ts";
import {
  captureRecord,
  recordEncoding,
  type ProgressAppendRecord,
} from "./internal/record-encoding.ts";
import { transferRecordDependencies } from "./internal/transfer-dependencies.ts";
import type { LifecyclePublicationStorage } from "./LifecyclePublication.ts";
import { MessageDeliveryRecord } from "./MessageDelivery.ts";
import { ExportRecord } from "./RecordFormat.ts";
import type { RecordJson } from "./Records.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  MAX_RUN_TOOL_CALL_IDENTITIES,
  ObservationOffset,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordId,
} from "./Records.ts";
import { subagentLineageRecordId, workerOriginRecordId } from "./RunJournal.ts";
import {
  AbortIntent,
  AdmissionRequest,
  ApprovalDecisionIntent,
  UnknownResolutionIntent,
} from "./SubmissionLedger.ts";
import { MAX_CANONICAL_BATCH_BYTES, type ThreadArchiveStorage } from "./ThreadArchiveRange.ts";
import type { ThreadWorkStorage } from "./ThreadWork.ts";

export const MAX_THREAD_EXPORT_PAGE_RECORDS = 256;
export const MAX_THREAD_EXPORT_PAGE_BYTES = 32 * 1024 * 1024;
export const MAX_THREAD_EXPORT_CURSOR_CHARS = 16_384;

/** Only model input and the facts needed to validate its ownership and compaction. */
export const PROMPT_EVIDENCE_TAGS = [
  "UserInputRecorded",
  "RunStarted",
  "ModelCompleted",
  "ModelResponseRecorded",
  "ToolCallSettled",
  "CompactionCreated",
  "RunCompleted",
  "RunFailed",
  "SubmissionSettled",
] as const;

/** Captures every fact owner, including facts that do not advance the canonical tail. */
export const ThreadExportSnapshot = Schema.Struct({
  revision: Schema.Natural,
  admissions: Schema.Natural,
  aborts: Schema.Natural,
  approvals: Schema.Natural,
  resolutions: Schema.Natural,
  deliveries: Schema.Natural,
});

export type ThreadExportSnapshot = typeof ThreadExportSnapshot.Type;

/** Irreversible Thread-local admission restriction, independent of claims and worker completion. */
export const ThreadWorkerSeal = Schema.Struct({ terminal: Schema.optionalKey(AssignmentTerminal) });
export type ThreadWorkerSeal = typeof ThreadWorkerSeal.Type;

/** Opaque, snapshot-bound transfer position. Only the exporting adapter constructs it. */
export const ThreadExportCursor = Schema.NonEmptyString.check(
  Schema.isMaxLength(MAX_THREAD_EXPORT_CURSOR_CHARS),
);

export type ThreadExportCursor = typeof ThreadExportCursor.Type;

/** Exact canonical identity; absence is not proof that an admission was never accepted. */
export const ThreadRecordRequest = Schema.Struct({ threadId: ThreadId, recordId: RecordId });
export type ThreadRecordRequest = typeof ThreadRecordRequest.Type;

/** The original user input for a Run, excluding later joined inputs. */
export const ThreadRunInputRequest = Schema.Struct({ threadId: ThreadId, runId: RunId });
export type ThreadRunInputRequest = typeof ThreadRunInputRequest.Type;

/** Native accounting for one funding Run; settled Thread history is never included. */
export const ThreadWorkerStateRequest = Schema.Struct({
  threadId: ThreadId,
  sourceSubmissionId: Schema.optionalKey(SubmissionId),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(16_384)),
});

export type ThreadWorkerStateRequest = typeof ThreadWorkerStateRequest.Type;

/** Saturating live peer-delivery count: retained terminal identities do not consume capacity. */
export const ThreadPeerCountRequest = Schema.Struct({
  threadId: ThreadId,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1000)),
});

export type ThreadPeerCountRequest = typeof ThreadPeerCountRequest.Type;

/** Capacity is derived from unresolved canonical obligations at this exact tail. */
export const ThreadWorkerCapacityRequest = Schema.Struct({
  threadId: ThreadId,
  workerThreadId: ThreadId,
  expectedTailSequence: CanonicalSequence,
  expectedTailDigest: Digest,
  update: Schema.Boolean,
  activeLimit: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(100)),
  pendingLimit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
});

export type ThreadWorkerCapacityRequest = typeof ThreadWorkerCapacityRequest.Type;

export const ThreadWorkerCapacity = Schema.Struct({
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  producerEpoch: ProducerEpoch,
  /** min(actual distinct workers, activeLimit + 1); equality remains distinguishable. */
  activeWorkers: Schema.Natural.check(Schema.isLessThanOrEqualTo(101)),
  workerActive: Schema.Boolean,
  /** min(actual inputs in the requested partition, pendingLimit + 1). */
  pendingInputs: Schema.Natural.check(Schema.isLessThanOrEqualTo(101)),
});

export type ThreadWorkerCapacity = typeof ThreadWorkerCapacity.Type;

/** Explicit maintenance may traverse all pages while retaining only one bounded page or Run. */
export const ThreadVerificationRequest = Schema.Struct({
  threadId: ThreadId,
  requireAllSettled: Schema.optionalKey(Schema.Boolean),
});

export type ThreadVerificationRequest = typeof ThreadVerificationRequest.Type;

export interface ThreadVerificationStorage {
  readonly verify: (
    request: ThreadVerificationRequest,
  ) => Effect.Effect<IntegrityReport, ThreadStoreError | ThreadNotMaterialized>;
}

const incomplete = (operation: string) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
  });

/** Collect a complete bounded selection, using canonical sequence as a sparse cursor. */
const selectedRecords = Effect.fnUntraced(function* (
  threadId: ThreadId,
  selection: ThreadSelection,
  limit: number,
) {
  const store = yield* ThreadReader;
  const records: Array<CanonicalRecordEnvelope> = [];
  let bytes = 0;
  let afterSequence: CanonicalSequence | undefined;

  while (true) {
    const pageLimit = Math.min(8, limit + 1 - records.length);

    const page = yield* Stream.runCollect(
      store.read({
        threadId,
        selection,
        page: { limit: pageLimit, ...(afterSequence === undefined ? {} : { afterSequence }) },
      }),
    );

    for (const entry of page) {
      if (
        entry.threadId !== threadId ||
        entry.sequence <= (afterSequence ?? 0) ||
        ("expectedTailSequence" in selection && entry.sequence > selection.expectedTailSequence)
      )
        return yield* incomplete("read selection identity");
      afterSequence = entry.sequence;
      bytes += yield* Effect.try({
        try: () => recordEncoding(entry.record).bytes,
        catch: () => incomplete("read selection bytes"),
      });
      if (bytes > MAX_THREAD_EXPORT_PAGE_BYTES)
        return yield* incomplete("read selection byte limit");
      records.push(entry);
    }
    if (page.length > pageLimit || records.length > limit)
      return yield* incomplete("read selection limit");
    if (page.length < pageLimit) return records;
  }
});

/** Host-owned read: authenticate the Thread and exact locator before calling. */
export const getRecord = Effect.fnUntraced(function* (request: ThreadRecordRequest) {
  yield* Schema.decodeEffect(ThreadRecordRequest)(request).pipe(
    Effect.mapError(() => incomplete("getRecord request")),
  );

  const records = yield* selectedRecords(
    request.threadId,
    { _tag: "RecordId", recordId: request.recordId },
    1,
  );

  if (records.some((entry) => entry.record.recordId !== request.recordId))
    return yield* incomplete("getRecord identity");

  return Option.fromUndefinedOr(records[0]);
});

export const getRunInput = Effect.fnUntraced(function* (request: ThreadRunInputRequest) {
  yield* Schema.decodeEffect(ThreadRunInputRequest)(request).pipe(
    Effect.mapError(() => incomplete("getRunInput request")),
  );

  const records = yield* selectedRecords(
    request.threadId,
    { _tag: "RunInput", runId: request.runId },
    1,
  );

  if (
    records.some(
      ({ record: { payload } }) =>
        payload._tag !== "UserInputRecorded" ||
        payload.kind !== "user" ||
        payload.runId !== request.runId,
    )
  )
    return yield* incomplete("getRunInput identity");

  return Option.fromUndefinedOr(records[0]);
});

/** A bounded snapshot of source identity and the selected funding Run's accounting. */
export const readWorkerState = Effect.fnUntraced(function* (request: ThreadWorkerStateRequest) {
  yield* Schema.decodeEffect(ThreadWorkerStateRequest)(request).pipe(
    Effect.mapError(() => incomplete("readWorkerState request")),
  );
  const store = yield* ThreadReader;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId: request.threadId }));

  const records = yield* selectedRecords(
    request.threadId,
    {
      _tag: "WorkerState",
      expectedTailSequence: tail.tailSequence,
      expectedTailDigest: tail.tailDigest,
      ...(request.sourceSubmissionId === undefined
        ? {}
        : { sourceSubmissionId: request.sourceSubmissionId }),
    },
    request.limit,
  );

  return {
    threadId: request.threadId,
    tailSequence: tail.tailSequence,
    tailDigest: tail.tailDigest,
    records,
  };
});

export class ThreadMaterialization extends Schema.Class<ThreadMaterialization>(
  "@effect-agent/thread/ThreadMaterialization",
)({
  threadId: ThreadId,
  producerEpoch: ProducerEpoch,
}) {}

export class FencedAppendRequest extends Schema.Class<FencedAppendRequest>(
  "@effect-agent/thread/FencedAppendRequest",
)({
  threadId: ThreadId,
  batch: CanonicalBatch,
  expectedTailSequence: CanonicalSequence,
  expectedTailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

const AppendHeader = Schema.Struct({
  threadId: ThreadId,
  expectedTailSequence: CanonicalSequence,
  expectedTailDigest: Digest,
  producerEpoch: ProducerEpoch,
  batchId: BatchId,
  producerId: ProducerId,
});

const validateHeader = Schema.decodeUnknownSync(AppendHeader);

const validateRecordCount = Schema.decodeUnknownSync(
  Schema.NonEmptyArray(Schema.Unknown).check(Schema.isMaxLength(256)),
);

const capturedAppends = new WeakMap<FencedAppendRequest, PreparedAppend>();

/**
 * Admission reserves one complete batch's envelope, references, snapshot and continuation cursor.
 * Adapters supply their native offset byte bound. Dependency literals already occur in record
 * wire, so ordinary batches need only arithmetic; near the page limit, charge the exact references.
 */
export const canonicalBatchFitsTransfer = (
  threadId: ThreadId,
  batch: Pick<CanonicalBatch, "batchId" | "records">,
  batchBytes: number,
  maxOffsetBytes: number,
): boolean => {
  const threadJson = JSON.stringify(threadId);
  const threadBytes = utf8ByteLength(threadJson);
  const batchIdBytes = utf8ByteLength(JSON.stringify(batch.batchId));

  // 128 bytes per envelope covers keys, sequence and punctuation. 128 KiB reserves the
  // maximum JSON-escaped cursor plus bounded snapshot, seal, digest and page metadata.
  const overhead =
    batch.records.length * (batchIdBytes + threadBytes + maxOffsetBytes + 128) +
    threadBytes +
    128 * 1024;

  if (2 * batchBytes + overhead + 2 <= MAX_THREAD_EXPORT_PAGE_BYTES) return true;

  const dependencyBytes = utf8ByteLength(JSON.stringify(transferRecordDependencies(batch.records)));

  return batchBytes + overhead + dependencyBytes <= MAX_THREAD_EXPORT_PAGE_BYTES;
};

/**
 * Adapter-owned append captured before Crypto or writer acquisition can suspend. Record JSON
 * is shared by the digest, batch and record rows. Privately owned facts and wire metadata
 * keep payloads, identities and validation stable across suspension; adapters persist the
 * captured strings rather than reconstructing bytes from caller-owned values.
 * Transport decoding intentionally returns an ordinary FencedAppendRequest for fresh capture.
 */
export interface PreparedAppend extends FencedAppendRequest {
  readonly batchJson: string;
  readonly batchBytes: number;
  readonly progress: ReadonlyArray<ProgressAppendRecord>;
  readonly records: ReadonlyArray<{
    readonly recordId: RecordId;
    readonly recordJson: string;
    readonly recordBytes: number;
    readonly wire: RecordJson;
    readonly canonical: CanonicalBatch["records"][number];
  }>;
  readonly digest: () => ReturnType<typeof digestCanonicalBatchJson>;
}

export const PreparedAppend = {
  capture: (input: FencedAppendRequest): Effect.Effect<PreparedAppend, ThreadStoreError> =>
    Effect.suspend(() => {
      const existing = capturedAppends.get(input);

      if (existing !== undefined) return Effect.succeed(existing);

      return Effect.try({
        try: () => {
          const header = validateHeader({
            ...input,
            batchId: input.batch.batchId,
            producerId: input.batch.producerId,
          });

          validateRecordCount(input.batch.records);
          const encodings = input.batch.records.map(captureRecord);
          const batchJson = `{"batchId":${JSON.stringify(header.batchId)},"producerId":${JSON.stringify(header.producerId)},"records":[${encodings.map(({ json }) => json).join(",")}]}`;

          const batchBytes = new TextEncoder().encode(batchJson).byteLength;

          if (batchBytes > MAX_CANONICAL_BATCH_BYTES)
            throw ThreadStoreError.make({
              operation: "prepare canonical append",
              message: `Canonical batch exceeds ${MAX_CANONICAL_BATCH_BYTES} bytes`,
            });
          const first = encodings[0]!;

          const batch = Object.freeze(
            new CanonicalBatch(
              {
                batchId: header.batchId,
                producerId: header.producerId,
                records: Object.freeze([
                  first.canonical,
                  ...encodings.slice(1).map(({ canonical }) => canonical),
                ]),
              },
              { disableChecks: true },
            ),
          );

          const request = new FencedAppendRequest(
            {
              threadId: header.threadId,
              expectedTailSequence: header.expectedTailSequence,
              expectedTailDigest: header.expectedTailDigest,
              producerEpoch: header.producerEpoch,
              batch,
            },
            { disableChecks: true },
          );

          const records = Object.freeze(
            encodings.map(({ canonical, json, wire, bytes }) =>
              Object.freeze({
                recordId: canonical.recordId,
                recordJson: json,
                recordBytes: bytes,
                wire,
                canonical,
              }),
            ),
          );

          const progress = Object.freeze(encodings.map(({ progress }) => progress));

          const captured = Object.freeze(
            Object.assign(request, {
              batchJson,
              batchBytes,
              records,
              progress,
              digest: () => digestCanonicalBatchJson(request.expectedTailDigest, batchJson),
            }),
          );

          capturedAppends.set(captured, captured);

          return captured;
        },
        catch: (cause) =>
          Schema.is(ThreadStoreError)(cause)
            ? cause
            : ThreadStoreError.make({
                operation: "prepare canonical append",
                message: Schema.isSchemaError(cause)
                  ? cause.message
                  : "Canonical append capture failed",
                cause,
              }),
      });
    }),
};

export class AppendResult extends Schema.Class<AppendResult>("@effect-agent/thread/AppendResult")({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  tailDigest: Digest,
  replayed: Schema.Boolean,
}) {}

export class ThreadRead extends Schema.Class<ThreadRead>("@effect-agent/thread/ThreadRead")({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Natural.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

/** Closed native selections; mutable metadata is read at this exact canonical tail. */
export const ThreadSelection = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("RecordId"), recordId: RecordId }),
  Schema.Struct({ _tag: Schema.Literal("RunInput"), runId: RunId }),
  /** Exact original declaration indexed by its deterministic settlement identity. */
  Schema.Struct({
    _tag: Schema.Literal("ToolDeclaration"),
    settlementRecordId: RecordId,
    throughSequence: CanonicalSequence,
  }),
  /** Canonical progress lookup. An absent locator never authorizes full-history fallback. */
  Schema.Struct({
    _tag: Schema.Literal("RunContinuation"),
    runId: RunId,
    throughSequence: CanonicalSequence,
  }),
  /** Only the selected Run's facts; unrelated Thread traffic cannot enlarge its suffix. */
  Schema.Struct({
    _tag: Schema.Literal("RunEvidence"),
    runId: RunId,
    submissionId: SubmissionId,
    throughSequence: CanonicalSequence,
  }),
  /** Exact declaration, this call's facts, and shared approval proof from its original batch. */
  Schema.Struct({
    _tag: Schema.Literal("OperationEvidence"),
    runId: RunId,
    toolCallId: ToolCallId,
    originRecordId: RecordId,
    approvalToolCallIds: Schema.Array(ToolCallId).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(MAX_RUN_TOOL_CALL_IDENTITIES),
      Schema.makeFilter((ids) => new Set(ids).size === ids.length),
    ),
    throughSequence: CanonicalSequence,
  }).check(
    Schema.makeFilter((selection) => selection.approvalToolCallIds.includes(selection.toolCallId)),
  ),
  Schema.Struct({
    _tag: Schema.Literal("LastAgentUpdate"),
    throughSequence: CanonicalSequence,
  }),
  /** Greatest original history boundary, including contexts committed late by parked Runs. */
  Schema.Struct({
    _tag: Schema.Literal("LatestRunContext"),
    throughSequence: CanonicalSequence,
  }),
  /** Sparse canonical pages; callers bound total evidence before building model metadata. */
  Schema.Struct({
    _tag: Schema.Literal("PromptEvidence"),
    throughSequence: CanonicalSequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("LatestModelCompleted"),
    throughSequence: CanonicalSequence,
  }),
  /** Latest application input; framework worker completion/update messages are excluded. */
  Schema.Struct({
    _tag: Schema.Literal("LatestApplicationInput"),
    throughSequence: CanonicalSequence,
  }),
  /** Presence of durable ownership forbids an immediate-history writer on this Thread. */
  Schema.Struct({
    _tag: Schema.Literal("DurableHistoryOwner"),
    throughSequence: CanonicalSequence,
  }),
  /** Latest rollover covering positions strictly before the addressed evidence. */
  Schema.Struct({
    _tag: Schema.Literal("ContextWindowBoundary"),
    atSequence: CanonicalSequence,
    throughSequence: CanonicalSequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("DeliveryPredecessor"),
    throughSequence: CanonicalSequence,
  }),
  /** Canonical creation/handoff intents retained independently of Run settlement. */
  Schema.Struct({
    _tag: Schema.Literal("WorkHandoffs"),
    throughSequence: CanonicalSequence,
  }),
  /** Unresolved inputs only, selected through disposable work membership. */
  Schema.Struct({
    _tag: Schema.Literal("LiveWorkerInputs"),
    throughSequence: CanonicalSequence,
    workerThreadId: Schema.optionalKey(ThreadId),
  }),
  /** A retained stop for the exact worker, independent of historical inputs. */
  Schema.Struct({
    _tag: Schema.Literal("WorkerStop"),
    workerThreadId: ThreadId,
    throughSequence: CanonicalSequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("WorkerExecution"),
    expectedTailSequence: CanonicalSequence,
    expectedTailDigest: Digest,
  }),
  Schema.Struct({
    _tag: Schema.Literal("WorkerState"),
    expectedTailSequence: CanonicalSequence,
    expectedTailDigest: Digest,
    sourceSubmissionId: Schema.optionalKey(SubmissionId),
  }),
]);

export type ThreadSelection = typeof ThreadSelection.Type;

/** Nested page makes an old ThreadRead decoder reject, never strip a selection into history. */
export const SelectedThreadRead = Schema.Struct({
  threadId: ThreadId,
  selection: ThreadSelection,
  page: Schema.Struct({
    afterSequence: Schema.optionalKey(CanonicalSequence),
    limit: ThreadRead.fields.limit,
  }),
});

export type SelectedThreadRead = typeof SelectedThreadRead.Type;
export const ThreadReadRequest = Schema.Union([SelectedThreadRead, ThreadRead]);
export type ThreadReadRequest = typeof ThreadReadRequest.Type;

export class ThreadObservation extends Schema.Class<ThreadObservation>(
  "@effect-agent/thread/ThreadObservation",
)({
  threadId: ThreadId,
  afterOffset: Schema.optionalKey(ObservationOffset),
}) {}

export class ThreadExportRequest extends Schema.Class<ThreadExportRequest>(
  "@effect-agent/thread/ThreadExportRequest",
)({
  threadId: ThreadId,
  cursor: Schema.optionalKey(ThreadExportCursor),
}) {}

export class ThreadTailRequest extends Schema.Class<ThreadTailRequest>(
  "@effect-agent/thread/ThreadTailRequest",
)({
  threadId: ThreadId,
}) {}

/**
 * The committed tail of one Thread Log. A resuming producer composes its next
 * FencedAppendRequest from this value instead of exporting the whole log.
 */
export class ThreadTail extends Schema.Class<ThreadTail>("@effect-agent/thread/ThreadTail")({
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

export class ThreadIdentityRequest extends Schema.Class<ThreadIdentityRequest>(
  "@effect-agent/thread/ThreadIdentityRequest",
)({ threadId: ThreadId }) {}

/**
 * One consistent canonical snapshot: tail/epoch, first record, exact worker origin,
 * then exact subagent lineage. A row selected twice appears only once. Missing
 * identity records remain absent; the snapshot itself grants no execution authority.
 */
export class ThreadIdentity extends Schema.Class<ThreadIdentity>(
  "@effect-agent/thread/ThreadIdentity",
)(
  Schema.Struct({
    ...ThreadTail.fields,
    /** Immutable admissions, including accepted inputs not yet materialized in the log. */
    admissions: Schema.Natural,
    records: Schema.Array(CanonicalRecordEnvelope).check(Schema.isMaxLength(3)),
  }).check(
    Schema.makeFilter((snapshot) => {
      if (snapshot.tailSequence === 0) return snapshot.records.length === 0;
      if (snapshot.records[0]?.sequence !== 1) return false;
      const origin = workerOriginRecordId(snapshot.threadId);
      const lineage = subagentLineageRecordId(snapshot.threadId);
      const ids = new Set<RecordId>();
      const sequences = new Set<CanonicalSequence>();
      let previous = -1;

      for (const entry of snapshot.records) {
        const rank =
          entry.sequence === 1
            ? 0
            : entry.record.recordId === origin
              ? 1
              : entry.record.recordId === lineage
                ? 2
                : -1;

        if (
          entry.threadId !== snapshot.threadId ||
          entry.sequence < 1 ||
          entry.sequence > snapshot.tailSequence ||
          rank <= previous ||
          ids.has(entry.record.recordId) ||
          sequences.has(entry.sequence)
        )
          return false;
        ids.add(entry.record.recordId);
        sequences.add(entry.sequence);
        previous = rank;
      }

      return true;
    }),
  ),
) {}

/** Immutable accepted input, distinct from all regenerable ledger execution state. */
export class ThreadAdmission extends Schema.Class<ThreadAdmission>(
  "@effect-agent/thread/ThreadAdmission",
)({
  ...AdmissionRequest.fields,
  submissionId: SubmissionId,
  receiptId: ReceiptId,
  queueSequence: QueueSequence,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

/** Reserve the complete immutable fact and manifest before accepting a fresh admission. */
export const admissionFitsTransfer = (request: AdmissionRequest) =>
  Schema.encodeEffect(AdmissionRequest)(request).pipe(
    Effect.map(
      (wire) =>
        utf8ByteLength(JSON.stringify(wire)) +
          utf8ByteLength(JSON.stringify(request.threadId)) +
          // Generated receipt/Submission identities, timestamp, snapshot and escaped cursor.
          128 * 1024 <=
        MAX_THREAD_EXPORT_PAGE_BYTES,
    ),
  );

/** Accepted operator commands are facts; their application markers are deliberately omitted. */
export const ThreadCommands = Schema.Struct({
  aborts: Schema.Array(
    AbortIntent.mapFields(({ canonicalRecordId: _, ...fields }) => fields),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS)),
  approvals: Schema.Array(
    ApprovalDecisionIntent.mapFields(({ canonicalRecordId: _, ...fields }) => fields),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS)),
  resolutions: Schema.Array(
    UnknownResolutionIntent.mapFields(({ canonicalRecordId: _, ...fields }) => fields),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS)),
});

export const ThreadExportBatch = Schema.Struct({ batchId: BatchId, producerId: ProducerId });

/** Other owning stores are required to restore these obligations; a single-Thread import refuses them. */
export const ThreadExternalObligation = Schema.Literals(["child", "worker", "message-delivery"]);

/** The wire is part of the export type: ordinary read envelopes are not lossless archives. */
export class ThreadExportRecord extends CanonicalRecordEnvelope.extend<ThreadExportRecord>(
  "@effect-agent/thread/ThreadExportRecord",
)({
  record: ExportRecord,
}) {}

/** One bounded, batch-aligned page. An absent cursor alone marks a complete transfer. */
export class ThreadExport extends Schema.Class<ThreadExport>("@effect-agent/thread/ThreadExport")({
  transferFormat: Schema.Literal("effect-agent/thread-transfer@1"),
  format: Schema.NonEmptyString,
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  snapshot: ThreadExportSnapshot,
  snapshotId: Digest,
  /** Presence seals the restored inbox; a terminal reason also requires exact settled worker evidence. */
  workerSeal: Schema.optionalKey(ThreadWorkerSeal),
  /** Inclusive first canonical position, even on a page containing only other owner facts. */
  fromSequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
  previousTailDigest: Digest,
  records: Schema.Array(ThreadExportRecord).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS),
  ),
  batches: Schema.Array(ThreadExportBatch).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS),
  ),
  admissions: Schema.Array(ThreadAdmission).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS),
  ),
  commands: ThreadCommands,
  /** Frozen delivery facts and outcomes; claims and leases are never transfer authority. */
  deliveries: Schema.Array(MessageDeliveryRecord).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_PAGE_RECORDS),
  ),
  cursor: Schema.optionalKey(ThreadExportCursor),
  /** Exact reference-bearing records identify cross-range recovery dependencies. */
  dependencies: Schema.Array(RecordId).check(Schema.isMaxLength(256 * 4_096)),
  externalObligations: Schema.optionalKey(
    Schema.Array(ThreadExternalObligation).check(Schema.isMaxLength(3)),
  ),
}) {}

/** Export-only authority. Native verification binds this port to its existing read snapshot. */
export class ThreadExportSource extends Context.Service<
  ThreadExportSource,
  Pick<ThreadStore["Service"], "export">
>()("@effect-agent/thread/ThreadExportSource") {
  static layer() {
    return Layer.effect(
      ThreadExportSource,
      Effect.map(ThreadStore, (store) => ThreadExportSource.of({ export: store.export })),
    );
  }
}

/** Pull bounded pages from one captured snapshot without collecting the Thread. Requires ThreadExportSource. */
export const streamExport = (request: ThreadExportRequest) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const source = yield* ThreadExportSource;
      let previous: ThreadExport | undefined;

      return Stream.paginate(request, (current) =>
        source.export(current).pipe(
          Effect.flatMap((page) => {
            if (
              page.threadId !== request.threadId ||
              (previous !== undefined &&
                (page.snapshotId !== previous.snapshotId ||
                  page.tailSequence !== previous.tailSequence ||
                  page.tailDigest !== previous.tailDigest ||
                  page.fromSequence !==
                    (previous.records.at(-1)?.sequence ?? previous.fromSequence - 1) + 1 ||
                  (page.cursor !== undefined && page.cursor === current.cursor)))
            )
              return Effect.fail(
                ThreadStoreError.make({
                  operation: "stream Thread export",
                  message: "Transfer snapshot or cursor is inconsistent",
                }),
              );
            previous = page;

            return Effect.succeed([
              [page],
              page.cursor === undefined
                ? Option.none()
                : Option.some(
                    ThreadExportRequest.make({ threadId: request.threadId, cursor: page.cursor }),
                  ),
            ] as const);
          }),
        ),
      );
    }),
  );

/**
 * A disposable projection snapshot. Adapters bind its sequence and digest to the canonical
 * log; consumers decode `state` and decide projection compatibility before suffix replay.
 * Run recovery uses canonical continuations independently of these application snapshots.
 */
export class ThreadCheckpoint extends Schema.Class<ThreadCheckpoint>(
  "@effect-agent/thread/ThreadCheckpoint",
)({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  state: PersistedJson,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

export class SaveCheckpointRequest extends Schema.Class<SaveCheckpointRequest>(
  "@effect-agent/thread/SaveCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
}) {}

export class LoadCheckpointRequest extends Schema.Class<LoadCheckpointRequest>(
  "@effect-agent/thread/LoadCheckpointRequest",
)({
  threadId: ThreadId,
  atOrBeforeSequence: Schema.optionalKey(CanonicalSequence),
}) {}

/**
 * Content-free storage provenance constructed by adapters from source-authored labels.
 * Raw foreign lookalikes are not trusted diagnostics. Never include rejected values, SQL,
 * or schema messages.
 */
export class ThreadStoreDiagnostic extends Schema.Class<ThreadStoreDiagnostic>(
  "@effect-agent/thread/ThreadStoreDiagnostic",
)({
  causeTag: Schema.String.check(Schema.isMaxLength(128)),
  operation: Schema.String.check(Schema.isMaxLength(256)),
  decoder: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(128))),
  issueTag: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(128))),
  sequence: Schema.optionalKey(CanonicalSequence),
}) {}

export class ThreadStoreError extends Schema.TaggedError<ThreadStoreError>()("ThreadStoreError", {
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
  diagnostic: Schema.optionalKey(ThreadStoreDiagnostic),
}) {}

export class ThreadNotMaterialized extends Schema.TaggedError<ThreadNotMaterialized>()(
  "ThreadNotMaterialized",
  { threadId: ThreadId },
) {}

/**
 * A canonical append that cannot commit: `batch-digest` replays a batch ID with different
 * content, `record-identity` reuses a canonical record ID, and `tail` declares a stale expected
 * tail. Tail conflicts carry the actual committed tail as a diagnostic resume hint.
 */
export class AppendConflict extends Schema.TaggedError<AppendConflict>()("AppendConflict", {
  threadId: ThreadId,
  batchId: BatchId,
  reason: Schema.Literals(["batch-digest", "record-identity", "tail"]),
  actualTailSequence: Schema.optionalKey(CanonicalSequence),
  actualTailDigest: Schema.optionalKey(Digest),
}) {}

export class FenceRejected extends Schema.TaggedError<FenceRejected>()("FenceRejected", {
  threadId: ThreadId,
  actualEpoch: ProducerEpoch,
  attemptedEpoch: ProducerEpoch,
}) {}

export class CheckpointRejected extends Schema.TaggedError<CheckpointRejected>()(
  "CheckpointRejected",
  {
    threadId: ThreadId,
    reason: Schema.Literals(["ahead-of-tail", "digest-mismatch", "unsupported-version", "corrupt"]),
  },
) {}

export type ThreadStoreFailure =
  | ThreadStoreError
  | ThreadNotMaterialized
  | AppendConflict
  | FenceRejected;

/**
 * Optional, disposable projection storage. Neither history execution nor durable recovery
 * requires it. Adapters that offer it must bind every checkpoint to a canonical batch tail.
 * Application consumers own projection compatibility; this port does not interpret metadata.
 */
export interface ThreadCheckpoints {
  readonly save: (
    request: SaveCheckpointRequest,
  ) => Effect.Effect<void, ThreadStoreError | ThreadNotMaterialized | CheckpointRejected>;
  readonly load: (
    request: LoadCheckpointRequest,
  ) => Effect.Effect<
    Option.Option<ThreadCheckpoint>,
    ThreadStoreError | ThreadNotMaterialized | CheckpointRejected
  >;
}

export class ThreadStore extends Context.Service<
  ThreadStore,
  {
    readonly lifecyclePublications?: LifecyclePublicationStorage;
    /** Native owner inventory and explicit canonical-index reconstruction. Absence fails closed. */
    readonly work?: ThreadWorkStorage;
    /** Bounded physical log ranges, independent of compaction and execution ownership. */
    readonly archives?: ThreadArchiveStorage;
    readonly verification?: ThreadVerificationStorage;
    readonly materialize: (
      request: ThreadMaterialization,
    ) => Effect.Effect<void, ThreadStoreError | FenceRejected>;
    readonly append: (
      request: FencedAppendRequest,
    ) => Effect.Effect<
      AppendResult,
      ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected
    >;
    readonly read: (
      request: ThreadReadRequest,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly observe: (
      request: ThreadObservation,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly export: (
      request: ThreadExportRequest,
    ) => Effect.Effect<ThreadExport, ThreadStoreError | ThreadNotMaterialized>;
    readonly inspectTail: (
      request: ThreadTailRequest,
    ) => Effect.Effect<ThreadTail, ThreadStoreError | ThreadNotMaterialized>;
    readonly readIdentity: (
      request: ThreadIdentityRequest,
    ) => Effect.Effect<ThreadIdentity, ThreadStoreError | ThreadNotMaterialized>;
    readonly readWorkerCapacity?: (
      request: ThreadWorkerCapacityRequest,
    ) => Effect.Effect<ThreadWorkerCapacity, ThreadStoreError | ThreadNotMaterialized>;
    /** Absent when this adapter does not support disposable checkpoints. */
    readonly checkpoints?: ThreadCheckpoints | undefined;
    /** Indexed scalar count; unsupported adapters fail closed at the caller. */
    readonly countPeerMessages?:
      | ((
          request: ThreadPeerCountRequest,
        ) => Effect.Effect<number, ThreadStoreError | ThreadNotMaterialized>)
      | undefined;
  }
>()("@effect-agent/thread/ThreadStore") {}

/** Canonical observation without append, materialization, or checkpoint mutation authority. */
export class ThreadReader extends Context.Service<
  ThreadReader,
  Pick<
    ThreadStore["Service"],
    | "read"
    | "observe"
    | "export"
    | "inspectTail"
    | "readIdentity"
    | "countPeerMessages"
    | "readWorkerCapacity"
  >
>()("@effect-agent/thread/ThreadReader") {
  static fromStore(store: ThreadStore["Service"]): ThreadReader["Service"] {
    return {
      read: store.read,
      observe: store.observe,
      export: store.export,
      inspectTail: store.inspectTail,
      readIdentity: store.readIdentity,
      ...(store.readWorkerCapacity === undefined
        ? {}
        : { readWorkerCapacity: store.readWorkerCapacity }),
      ...(store.countPeerMessages === undefined
        ? {}
        : { countPeerMessages: store.countPeerMessages }),
    };
  }

  /** Capture this assembly's store, independently of other local or routed stores. */
  static layer() {
    return Layer.effect(ThreadReader, Effect.map(ThreadStore, ThreadReader.fromStore));
  }
}
