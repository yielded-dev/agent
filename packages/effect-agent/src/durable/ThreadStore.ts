import { Context, Effect, Layer, Option, Schema, Stream } from "effect";

import { ReceiptId, RunId, SubmissionId, ThreadId, ToolCallId } from "../core/Identifiers.ts";
import { QueueSequence } from "../core/Receipt.ts";
import { digestCanonicalBatchJson } from "./Digest.ts";
import { captureRecord, type ProgressAppendRecord } from "./internal/record-encoding.ts";
import type { LifecyclePublicationStorage } from "./LifecyclePublication.ts";
import { ExportRecord } from "./RecordFormat.ts";
import type { RecordJson } from "./Records.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
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
import type { ThreadWorkStorage } from "./ThreadWork.ts";

export const MAX_THREAD_EXPORT_RECORDS = 131_072;

/** Exact canonical identity; absence is not proof that an admission was never accepted. */
export const ThreadRecordRequest = Schema.Struct({ threadId: ThreadId, recordId: RecordId });
export type ThreadRecordRequest = typeof ThreadRecordRequest.Type;

/** The original user input for a Run, excluding later joined inputs. */
export const ThreadRunInputRequest = Schema.Struct({ threadId: ThreadId, runId: RunId });
export type ThreadRunInputRequest = typeof ThreadRunInputRequest.Type;

/** Native worker reservation/accounting snapshot at a captured canonical tail. */
export const ThreadWorkerStateRequest = Schema.Struct({
  threadId: ThreadId,
  sourceSubmissionId: Schema.optionalKey(SubmissionId),
  limit: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(MAX_THREAD_EXPORT_RECORDS),
  ),
});

export type ThreadWorkerStateRequest = typeof ThreadWorkerStateRequest.Type;

/** Saturating lifetime peer-message count: the result is at most the requested cap. */
export const ThreadPeerCountRequest = Schema.Struct({
  threadId: ThreadId,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1000)),
});

export type ThreadPeerCountRequest = typeof ThreadPeerCountRequest.Type;

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
  let afterSequence: CanonicalSequence | undefined;

  while (true) {
    const pageLimit = Math.min(1024, limit + 1 - records.length);

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

/** A bounded snapshot of native lifetime and source-submission worker accounting. */
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
 * Adapter-owned append captured before Crypto or writer acquisition can suspend. Record JSON
 * is shared by the digest, batch and record rows. Privately owned facts and wire metadata
 * keep payloads, identities and validation stable across suspension; adapters persist the
 * captured strings rather than reconstructing bytes from caller-owned values.
 * Transport decoding intentionally returns an ordinary FencedAppendRequest for fresh capture.
 */
export interface PreparedAppend extends FencedAppendRequest {
  readonly batchJson: string;
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
              records,
              progress,
              digest: () => digestCanonicalBatchJson(request.expectedTailDigest, batchJson),
            }),
          );

          capturedAppends.set(captured, captured);

          return captured;
        },
        catch: (cause) =>
          ThreadStoreError.make({
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
  /** Exact declaration plus this operation's results, approvals, Steps, and resolution facts. */
  Schema.Struct({
    _tag: Schema.Literal("OperationEvidence"),
    runId: RunId,
    toolCallId: ToolCallId,
    originRecordId: RecordId,
    throughSequence: CanonicalSequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("LastAgentUpdate"),
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

/** Accepted operator commands are facts; their application markers are deliberately omitted. */
export const ThreadCommands = Schema.Struct({
  aborts: Schema.Array(
    AbortIntent.mapFields(({ canonicalRecordId: _, ...fields }) => fields),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  approvals: Schema.Array(
    ApprovalDecisionIntent.mapFields(({ canonicalRecordId: _, ...fields }) => fields),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  resolutions: Schema.Array(
    UnknownResolutionIntent.mapFields(({ canonicalRecordId: _, ...fields }) => fields),
  ).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
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

export class ThreadExport extends Schema.Class<ThreadExport>("@effect-agent/thread/ThreadExport")({
  format: Schema.NonEmptyString,
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(ThreadExportRecord).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  /** Required for import of a non-empty log; earlier exports must be taken again. */
  batches: Schema.optionalKey(
    Schema.Array(ThreadExportBatch).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  ),
  admissions: Schema.optionalKey(
    Schema.Array(ThreadAdmission).check(Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS)),
  ),
  commands: Schema.optionalKey(ThreadCommands),
  externalObligations: Schema.optionalKey(
    Schema.Array(ThreadExternalObligation).check(Schema.isMaxLength(3)),
  ),
}) {}

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
    "read" | "observe" | "export" | "inspectTail" | "readIdentity" | "countPeerMessages"
  >
>()("@effect-agent/thread/ThreadReader") {
  static fromStore(store: ThreadStore["Service"]): ThreadReader["Service"] {
    return {
      read: store.read,
      observe: store.observe,
      export: store.export,
      inspectTail: store.inspectTail,
      readIdentity: store.readIdentity,
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
