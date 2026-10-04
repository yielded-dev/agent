import { Context, DateTime, Effect, Layer, Option, Predicate, Schema, Stream } from "effect";

import { RunId, SubmissionId, ThreadId } from "../core/Identifiers.ts";
import { canonicalJson, digestCanonicalBatchJson } from "./Digest.ts";
import type { LifecyclePublicationStorage } from "./LifecyclePublication.ts";
import {
  BatchId,
  CanonicalBatch,
  type CanonicalRecordPayload,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
  PersistedJson,
  ProducerEpoch,
  RecordEnvelope,
  RecordId,
} from "./Records.ts";
import { subagentLineageRecordId, workerOriginRecordId } from "./RunJournal.ts";

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
export const getRecord = Effect.fn("ThreadStore.getRecord")(function* (
  request: ThreadRecordRequest,
) {
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

export const getRunInput = Effect.fn("ThreadStore.getRunInput")(function* (
  request: ThreadRunInputRequest,
) {
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
export const readWorkerState = Effect.fn("ThreadStore.readWorkerState")(function* (
  request: ThreadWorkerStateRequest,
) {
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

const encodeAppend = Schema.encodeSync(FencedAppendRequest);
const capturedAppends = new WeakMap<FencedAppendRequest, PreparedAppend>();

/** Own only lifecycle graphs consumed after SQL INSERT suspension; keep Schema/Duration prototypes. */
const capturePayload = (payload: CanonicalRecordPayload): CanonicalRecordPayload => {
  const captured = { ...payload };

  Object.setPrototypeOf(captured, Object.getPrototypeOf(payload));
  switch (payload._tag) {
    case "UserInputRecorded":
    case "RunStarted":
    case "AgentUpdateEmitted":
    case "ToolApprovalRequested":
    case "ToolApprovalDecided":
    case "AbortRequested":
    case "WorkerInputCompleted":
    case "WorkerInputRequested":
    case "WorkerStopRequested":
    case "SubagentRequested":
    case "SubagentStarted":
    case "SubagentJoined":
    case "SubmissionSettled":
      break;
    default:
      return captured;
  }

  const pending: Array<{ readonly source: object; readonly target: object }> = [
    { source: payload, target: captured },
  ];

  while (pending.length > 0) {
    const next = pending.pop()!;

    for (const [key, value] of Object.entries(next.source)) {
      if (!Predicate.isObject(value)) continue;
      const copy: object = Array.isArray(value) ? [] : Object.create(Object.getPrototypeOf(value));

      Object.assign(copy, value);
      Reflect.set(next.target, key, copy);
      pending.push({ source: value, target: copy });
    }
  }

  return captured;
};

/**
 * Adapter-owned append captured before Crypto or writer acquisition can suspend. Record JSON
 * is serialized once and shared by the digest, batch and record rows. Owned shallow metadata
 * keeps row identities and authority stable; lifecycle payloads are detached where storage
 * consumes them after suspension. Other nested typed values retain their readonly contract:
 * adapters persist the captured strings rather than reconstructing bytes from those values.
 * Transport decoding intentionally returns an ordinary FencedAppendRequest for fresh capture.
 */
export interface PreparedAppend extends FencedAppendRequest {
  readonly batchJson: string;
  readonly records: ReadonlyArray<{
    readonly recordId: RecordId;
    readonly recordJson: string;
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
          const encoded = encodeAppend(input);
          const recordJson = encoded.batch.records.map(canonicalJson);
          const batchJson = `{"batchId":${JSON.stringify(encoded.batch.batchId)},"producerId":${JSON.stringify(encoded.batch.producerId)},"records":[${recordJson.join(",")}]}`;

          const captureRecord = (record: RecordEnvelope) =>
            Object.freeze(
              new RecordEnvelope(
                {
                  ...record,
                  createdAt: DateTime.makeUnsafe(DateTime.toEpochMillis(record.createdAt)),
                  payload: Object.freeze(capturePayload(record.payload)),
                },
                { disableChecks: true },
              ),
            );

          // Encoding validated the typed graph. Retain its typed values without parsing and
          // decoding our own wire representation or traversing every nested value to freeze it.
          const batch = Object.freeze(
            new CanonicalBatch(
              {
                ...input.batch,
                records: Object.freeze([
                  captureRecord(input.batch.records[0]),
                  ...input.batch.records.slice(1).map(captureRecord),
                ]),
              },
              { disableChecks: true },
            ),
          );

          const request = new FencedAppendRequest(
            {
              threadId: input.threadId,
              expectedTailSequence: input.expectedTailSequence,
              expectedTailDigest: input.expectedTailDigest,
              producerEpoch: input.producerEpoch,
              batch,
            },
            { disableChecks: true },
          );

          const records = Object.freeze(
            batch.records.map((canonical, index) =>
              Object.freeze({
                recordId: canonical.recordId,
                recordJson: recordJson[index],
                canonical,
              }),
            ),
          );

          const captured = Object.freeze(
            Object.assign(request, {
              batchJson,
              records,
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

/** Maximum canonical records represented by one Thread export. */

export class ThreadExport extends Schema.Class<ThreadExport>("@effect-agent/thread/ThreadExport")({
  format: Schema.Literal("effect-agent/thread@1"),
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(CanonicalRecordEnvelope).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS),
  ),
}) {}

/**
 * A disposable projection snapshot. Adapters bind its sequence and digest to the canonical
 * log; consumers decode `state` and decide projection compatibility before suffix replay.
 * Legacy metadata is optional for application projections and preserved when supplied.
 * Runtime-owned recovery checkpoints populate and compare it before using cached state.
 */
export class ThreadCheckpoint extends Schema.Class<ThreadCheckpoint>(
  "@effect-agent/thread/ThreadCheckpoint",
)({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  engineVersion: Schema.optionalKey(Schema.NonEmptyString),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  agentDefinitionDigest: Schema.optionalKey(Digest),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  modelDigest: Schema.optionalKey(Digest),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  toolDigest: Schema.optionalKey(Digest),
  state: PersistedJson,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

export class SaveCheckpointRequest extends Schema.Class<SaveCheckpointRequest>(
  "@effect-agent/thread/SaveCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
}) {}

/** Replace the disposable recovery view only under the current canonical producer fence. */
export class SaveRecoveryCheckpointRequest extends Schema.Class<SaveRecoveryCheckpointRequest>(
  "@effect-agent/thread/SaveRecoveryCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
  producerEpoch: ProducerEpoch,
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

/**
 * Optional latest-only recovery cache, independent of application projection checkpoints.
 * Saves atomically validate the producer epoch and canonical batch tail. An older snapshot
 * cannot replace a newer one; equal-tail replacement repairs disposable state. Invalid cached
 * data fails with CheckpointRejected, while infrastructure failures remain ThreadStoreError.
 * Loading at an earlier tail or outside the adapter's cache locality may return none so callers
 * can replay canonical records. Canonical records and the ledger remain authority.
 */
export interface ThreadRecoveryCheckpoints {
  readonly save: (
    request: SaveRecoveryCheckpointRequest,
  ) => Effect.Effect<
    void,
    ThreadStoreError | ThreadNotMaterialized | CheckpointRejected | FenceRejected
  >;
  readonly load: ThreadCheckpoints["load"];
}

export class ThreadStore extends Context.Service<
  ThreadStore,
  {
    readonly lifecyclePublications?: LifecyclePublicationStorage;
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
    readonly recoveryCheckpoints?: ThreadRecoveryCheckpoints | undefined;
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
