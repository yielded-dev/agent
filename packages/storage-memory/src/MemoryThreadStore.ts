import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import type { IdempotencyKey } from "@yielded/agent/receipt";
import { ExportBatch } from "@yielded/agent/record-format";
import {
  ProducerEpoch,
  CURRENT_RECORD_FORMAT,
  CanonicalBatch,
  type RecordId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  type BatchId,
  type Digest,
} from "@yielded/agent/records";
import {
  canonicalRunIds,
  canonicalRecordBytes,
  isWorkHandoff,
  prepareProgressAppend,
  validateProgressAppend,
} from "@yielded/agent/run-continuation";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
} from "@yielded/agent/run-journal";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import {
  prepareThreadImport,
  ThreadImport,
  ThreadImportRejected,
} from "@yielded/agent/thread-import";
import {
  type ThreadCheckpoint,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadExportRequest,
  ThreadExport,
  ThreadExportRecord,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadStore,
  ThreadReader,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  MAX_THREAD_EXPORT_RECORDS,
} from "@yielded/agent/thread-store";
import {
  WORK_INDEX_VERSION,
  MAX_WORK_REBUILD_RECORDS,
  MAX_WORK_REBUILD_BYTES,
  WorkDiscoveryUnavailable,
  ThreadWorkRequest,
  WorkThreadsRequest,
  WorkIndexRebuildRequest,
  decodeWorkCursor,
  encodeWorkCursor,
  validateWorkPage,
  workIndexChanges,
  advanceWorkEntry,
  type CanonicalWorkEntry,
  type WorkerReportingMode,
  type ThreadWorkStorage,
} from "@yielded/agent/thread-work";
import {
  Context,
  Crypto,
  Effect,
  Layer,
  MutableRef,
  Option,
  PubSub,
  Ref,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { Base64 } from "effect/encoding";

import {
  MemoryThreadStoreKernel,
  type MemoryLedgerTransfer,
  type MemoryWorkOwner,
  type PreparedMemoryAppend,
} from "./internal/MemoryThreadStoreKernel.ts";
import { boundedWorkSelection } from "./internal/WorkSelection.ts";

const MAX_THREADS = 256;
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const MAX_CHECKPOINTS_PER_THREAD = 1_024;

const ThreadCapacity = Context.Reference<number>(
  "@effect-agent/storage-memory/MemoryThreadStore/ThreadCapacity",
  { defaultValue: () => MAX_THREADS },
);

interface StoredBatch {
  /** Preserve the exact destination wire, including additive fields unknown to this reader. */
  readonly batchJson: string;
  readonly digest: Digest;
  readonly result: AppendResult;
}

interface StoredThread {
  /** Append-only owner indexes; readers must constrain them to their captured canonical tail. */
  readonly runRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly continuations: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly operationRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly handoffs: Array<CanonicalRecordEnvelope>;
  readonly agentUpdates: Array<CanonicalRecordEnvelope>;
  readonly deliveryPredecessors: Array<CanonicalRecordEnvelope>;
  readonly peerCount: number;
  readonly workerRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly byId: Map<string, CanonicalRecordEnvelope>;
  readonly runInputs: Map<string, CanonicalRecordEnvelope | null>;
  readonly producerEpoch: ProducerEpoch;
  readonly tailSequence: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  /** Captured with canonical facts so rebuild checks bytes before reading a payload. */
  readonly recordBytes: ReadonlyArray<number>;
  readonly recordIds: ReadonlySet<RecordId>;
  readonly batches: ReadonlyMap<BatchId, StoredBatch>;
  readonly tailDigests: ReadonlyMap<CanonicalSequence, Digest>;
  readonly checkpoints: ReadonlyMap<CanonicalSequence, ThreadCheckpoint>;
}

interface MemoryState {
  readonly threads: ReadonlyMap<ThreadId, StoredThread>;
}

type NativeIndexes = Pick<
  StoredThread,
  | "peerCount"
  | "workerRecords"
  | "byId"
  | "runInputs"
  | "runRecords"
  | "continuations"
  | "handoffs"
  | "operationRecords"
  | "agentUpdates"
  | "deliveryPredecessors"
>;

const emptyIndexes = (): NativeIndexes => ({
  peerCount: 0,
  workerRecords: new Map(),
  byId: new Map(),
  runInputs: new Map(),
  runRecords: new Map(),
  continuations: new Map(),
  handoffs: [],
  agentUpdates: [],
  deliveryPredecessors: [],
  operationRecords: new Map(),
});

interface WorkIndex {
  readonly version: number;
  readonly state: "ready" | "rebuilding";
  readonly through: CanonicalSequence;
  readonly reporting: WorkerReportingMode;
  readonly entryCount: number;
  readonly entries: Map<string, CanonicalWorkEntry>;
  readonly messages: Map<IdempotencyKey, Set<string>>;
  readonly rebuildingIndexes?: NativeIndexes;
}

const emptyWorkIndex = (): WorkIndex => ({
  version: WORK_INDEX_VERSION,
  state: "ready",
  through: ZERO_CANONICAL_SEQUENCE,
  reporting: "none",
  entryCount: 0,
  entries: new Map(),
  messages: new Map(),
});

/** Prepare only this batch's changes; commit under the writer gate with canonical publication. */
const prepareWork = Effect.fnUntraced(function* (
  previous: WorkIndex,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  retained: (threadId: ThreadId, messageId: IdempotencyKey) => boolean,
) {
  const changes = new Map<string, CanonicalWorkEntry | undefined>();
  let reporting = previous.reporting;
  let through = previous.through;

  for (const record of records) {
    for (const change of yield* workIndexChanges(record, reporting)) {
      if (change._tag === "WorkerMode") reporting = change.mode;
      else if (change._tag === "Remove") changes.set(change.id, undefined);
      else {
        const entry = advanceWorkEntry(
          changes.has(change.entry.id)
            ? changes.get(change.entry.id)
            : previous.entries.get(change.entry.id),
          change.entry,
        );

        changes.set(
          entry.id,
          entry.owner._tag === "Handoff" &&
            entry.owner.messageId !== undefined &&
            retained(record.threadId, entry.owner.messageId)
            ? undefined
            : entry,
        );
      }
    }
    through = record.sequence;
  }

  return (): WorkIndex => {
    const { entries, messages } = previous;

    for (const [id, entry] of changes) {
      const old = entries.get(id);

      if (old?.owner._tag === "Handoff" && old.owner.messageId !== undefined) {
        const ids = messages.get(old.owner.messageId);

        ids?.delete(id);
        if (ids?.size === 0) messages.delete(old.owner.messageId);
      }
      if (entry === undefined) entries.delete(id);
      else {
        entries.set(id, entry);
        if (entry.owner._tag === "Handoff" && entry.owner.messageId !== undefined) {
          const ids = messages.get(entry.owner.messageId);

          if (ids === undefined) messages.set(entry.owner.messageId, new Set([id]));
          else ids.add(id);
        }
      }
    }

    return { ...previous, through, reporting, entryCount: entries.size };
  };
});

/** First sequence strictly after the cursor, without traversing a Run's earlier facts. */
const upperSequence = (records: ReadonlyArray<CanonicalRecordEnvelope>, sequence: number) => {
  let low = 0;
  let high = records.length;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);

    if (records[mid].sequence <= sequence) low = mid + 1;
    else high = mid;
  }

  return low;
};

const appendToIndex = (
  index: Map<string, Array<CanonicalRecordEnvelope>>,
  key: string,
  entry: CanonicalRecordEnvelope,
) => {
  const records = index.get(key);

  if (records === undefined) index.set(key, [entry]);
  else records.push(entry);
};

/** Only call at successful synchronous publication, or on indexes not yet reachable by readers. */
const commitIndex = (
  index: Map<string, Array<CanonicalRecordEnvelope>>,
  additions: ReadonlyMap<string, Array<CanonicalRecordEnvelope>>,
) => {
  for (const [key, entries] of additions) {
    const records = index.get(key);

    if (records === undefined) index.set(key, entries);
    else for (const entry of entries) records.push(entry);
  }
};

const prepareIndexes = (
  previous: NativeIndexes,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  reuse = false,
) => {
  let peerCount = previous.peerCount;
  // Latest execution entries are replaced in this private map; owner histories only append.
  // Rebuild staging is private: reuse it only in the synchronous cursor publication step.
  const workerRecords = reuse ? previous.workerRecords : new Map(previous.workerRecords);
  const workerAppends = new Map<string, Array<CanonicalRecordEnvelope>>();
  const byId = reuse ? previous.byId : new Map(previous.byId);
  const runInputs = reuse ? previous.runInputs : new Map(previous.runInputs);
  const runRecords = new Map<string, Array<CanonicalRecordEnvelope>>();
  const continuations = new Map<string, Array<CanonicalRecordEnvelope>>();
  const handoffs = reuse ? previous.handoffs : [...previous.handoffs];
  const agentUpdates = reuse ? previous.agentUpdates : [...previous.agentUpdates];

  const deliveryPredecessors = reuse
    ? previous.deliveryPredecessors
    : [...previous.deliveryPredecessors];

  const operationAppends = new Map<string, Array<CanonicalRecordEnvelope>>();

  for (const entry of records) {
    byId.set(entry.record.recordId, entry);
    const payload = entry.record.payload;

    for (const runId of canonicalRunIds(entry.record))
      if (payload._tag === "RunContinuation") appendToIndex(continuations, runId, entry);
      else appendToIndex(runRecords, runId, entry);
    if (isWorkHandoff(entry.record)) handoffs.push(entry);
    if (payload._tag === "AgentUpdateEmitted") agentUpdates.push(entry);
    if (
      payload._tag === "WorkerReportPrepared" ||
      (payload._tag === "AgentUpdateEmitted" && payload.delivery !== undefined)
    )
      deliveryPredecessors.push(entry);
    if ("runId" in payload && payload.runId !== undefined && "toolCallId" in payload) {
      appendToIndex(operationAppends, JSON.stringify([payload.runId, payload.toolCallId]), entry);
      if (payload._tag === "ToolApprovalRequested" || payload._tag === "ToolApprovalDecided")
        appendToIndex(
          operationAppends,
          JSON.stringify([payload.runId, payload.toolCallId, "approval"]),
          entry,
        );
    }

    if (payload._tag === "PeerMessagePrepared") peerCount++;
    if (
      (payload._tag === "UserInputRecorded" || payload._tag === "RunStarted") &&
      payload.runId !== undefined
    )
      workerRecords.set(`execution:${payload._tag}`, [entry]);

    const workerKey =
      payload._tag === "SubtreeBudgetReserved"
        ? `subtree:${payload.sourceSubmissionId ?? ""}`
        : payload._tag === "SubagentJoined"
          ? `joined:${payload.runId}`
          : [
                "ThreadCreated",
                "WorkerOriginRecorded",
                "SubagentLineageRecorded",
                "WorkerInputRequested",
                "WorkerInputCompleted",
                "WorkerStopRequested",
              ].includes(payload._tag)
            ? "worker"
            : undefined;

    if (workerKey !== undefined) appendToIndex(workerAppends, workerKey, entry);
    if (
      payload._tag === "UserInputRecorded" &&
      payload.kind === "user" &&
      payload.runId !== undefined
    )
      runInputs.set(payload.runId, runInputs.has(payload.runId) ? null : entry);
  }

  return {
    indexes: {
      peerCount,
      workerRecords,
      byId,
      runInputs,
      runRecords: previous.runRecords,
      continuations: previous.continuations,
      operationRecords: previous.operationRecords,
      handoffs,
      agentUpdates,
      deliveryPredecessors,
    },
    commit: () => {
      commitIndex(previous.runRecords, runRecords);
      commitIndex(previous.continuations, continuations);
      commitIndex(previous.operationRecords, operationAppends);
      commitIndex(workerRecords, workerAppends);
    },
  };
};

type AppendDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected;
    }
  | {
      readonly _tag: "success";
      readonly result: AppendResult;
      readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
    };

type MaterializeDecision =
  | { readonly _tag: "failure"; readonly error: ThreadStoreError | FenceRejected }
  | { readonly _tag: "success" };

type CheckpointDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadNotMaterialized | ThreadStoreError | CheckpointRejected;
    }
  | { readonly _tag: "success" };

const storeError = (operation: string, message: string, cause?: unknown): ThreadStoreError =>
  cause === undefined
    ? ThreadStoreError.make({ operation, message })
    : ThreadStoreError.make({ operation, message, cause });

const validate = <A, I>(
  schema: Schema.Codec<A, I>,
  operation: string,
  value: unknown,
): Effect.Effect<A, ThreadStoreError> =>
  Schema.encodeUnknownEffect(schema)(value).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    Effect.mapError((error) => storeError(operation, `Invalid ${operation} request`, error)),
  );

const decodeCanonicalSequence = Schema.decodeSync(CanonicalSequence);
const ZERO_CANONICAL_SEQUENCE = decodeCanonicalSequence(0);

const offsetSequence = Effect.fnUntraced(function* (
  threadId: ThreadId,
  offset: ObservationOffset | undefined,
): Effect.fn.Return<CanonicalSequence, ThreadStoreError> {
  if (offset === undefined) return yield* Effect.succeed(ZERO_CANONICAL_SEQUENCE);
  const prefix = `memory:v1:${Base64.encode(threadId)}:`;
  const encodedSequence = offset.startsWith(prefix) ? offset.slice(prefix.length) : "";

  if (!/^\d+$/.test(encodedSequence)) {
    return yield* Effect.fail(storeError("observe", "Malformed observation offset"));
  }
  const sequence = Number(encodedSequence);

  return yield* Number.isSafeInteger(sequence)
    ? Schema.decodeEffect(CanonicalSequence)(sequence).pipe(
        Effect.mapError(() => storeError("observe", "Malformed observation offset")),
      )
    : Effect.fail(storeError("observe", "Malformed observation offset"));
});

const observationOffset = (threadId: ThreadId, sequence: CanonicalSequence): ObservationOffset =>
  Schema.decodeSync(ObservationOffset)(`memory:v1:${Base64.encode(threadId)}:${sequence}`);

const batchEnvelopes = (
  threadId: ThreadId,
  batch: CanonicalBatch,
  firstSequence: CanonicalSequence,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  batch.records.map((record, index) => {
    const sequence = decodeCanonicalSequence(firstSequence + index);

    return CanonicalRecordEnvelope.make({
      threadId,
      batchId: batch.batchId,
      sequence,
      offset: observationOffset(threadId, sequence),
      record,
    });
  });

const decodeStoredBatch = (batchJson: string, operation: string) =>
  Schema.decodeEffect(Schema.fromJsonString(ExportBatch))(batchJson).pipe(
    Effect.mapError((cause) => storeError(operation, "Invalid canonical batch JSON", cause)),
  );

const findThread = Effect.fnUntraced(function* (
  state: MemoryState,
  threadId: ThreadId,
): Effect.fn.Return<StoredThread, ThreadNotMaterialized> {
  const thread = state.threads.get(threadId);

  return yield* thread === undefined
    ? Effect.fail(ThreadNotMaterialized.make({ threadId }))
    : Effect.succeed(thread);
});

const CheckpointVersionEnvelope = Schema.Struct({
  checkpoint: Schema.Struct({
    threadId: ThreadId,
    schemaVersion: Schema.Natural,
  }),
});

const validateCheckpointVersion = Effect.fnUntraced(function* (
  value: unknown,
): Effect.fn.Return<void, ThreadStoreError | CheckpointRejected> {
  const envelope = yield* Schema.decodeUnknownEffect(CheckpointVersionEnvelope)(value).pipe(
    Effect.mapError(() => storeError("saveCheckpoint", "Invalid saveCheckpoint request")),
  );

  if (envelope.checkpoint.schemaVersion !== 1) {
    return yield* CheckpointRejected.make({
      threadId: envelope.checkpoint.threadId,
      reason: "unsupported-version",
    });
  }
});

const makeThreadStore = Effect.gen(function* () {
  const maxThreads = yield* ThreadCapacity;
  const crypto = yield* Crypto.Crypto;
  const state = yield* Ref.make<MemoryState>({ threads: new Map() });
  const gate = yield* Semaphore.make(1);
  const withMutation = gate.withPermits(1);
  const updates = yield* PubSub.sliding<void>(1);
  let ledgerTransfer: MemoryLedgerTransfer | undefined;
  let hasMessageDeliveries: ((threadId: ThreadId) => Effect.Effect<boolean>) | undefined;

  const workState = yield* Ref.make(new Map<ThreadId, WorkIndex>());
  const workOwners = new Map<"admissions" | "deliveries", MemoryWorkOwner>();
  let deliveryLookup: ((threadId: ThreadId, messageId: IdempotencyKey) => boolean) | undefined;

  const retainedDelivery = (threadId: ThreadId, messageId: IdempotencyKey) =>
    deliveryLookup?.(threadId, messageId) ?? false;

  const publishWork = (threadId: ThreadId, index: WorkIndex) =>
    MutableRef.get(workState.ref).set(threadId, index);

  const retainDelivery = (threadId: ThreadId, messageId: IdempotencyKey) => {
    const index = MutableRef.get(workState.ref).get(threadId);
    const ids = index?.messages.get(messageId);

    if (
      index === undefined ||
      index.version !== WORK_INDEX_VERSION ||
      index.entryCount !== index.entries.size ||
      ids === undefined
    )
      return;
    const { entries, messages } = index;

    for (const id of ids) entries.delete(id);
    messages.delete(messageId);
    publishWork(threadId, { ...index, entryCount: entries.size });
  };

  const readyIndex = (threadId: ThreadId, tail: number) => {
    const index = MutableRef.get(workState.ref).get(threadId);

    if (
      index === undefined ||
      index.version !== WORK_INDEX_VERSION ||
      index.entries.size !== index.entryCount
    )
      return WorkDiscoveryUnavailable.make({ threadId, reason: "missing-index" });
    if (index.state !== "ready" || index.through !== tail)
      return WorkDiscoveryUnavailable.make({ threadId, reason: "incomplete-rebuild" });

    return Effect.succeed(index);
  };

  const work: ThreadWorkStorage = {
    threads: (unvalidated) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(WorkThreadsRequest, "work threads", unvalidated);
          const current = yield* Ref.get(state);
          const catalogue = yield* Ref.get(workState);

          const candidates = boundedWorkSelection<ThreadId>(
            request.limit,
            (id) => id,
            request.afterThreadId,
          );

          for (const [threadId, thread] of current.threads) {
            const index = catalogue.get(threadId);

            if (
              index === undefined ||
              index.version !== WORK_INDEX_VERSION ||
              index.state !== "ready" ||
              index.through !== thread.tailSequence ||
              index.entryCount !== index.entries.size ||
              index.entryCount > 0
            )
              candidates.add(threadId);
          }
          for (const owner of workOwners.values()) {
            const page = yield* owner.threads(request);

            for (const threadId of page.threadIds) candidates.add(threadId);
          }
          const threadIds = candidates.values;
          const afterThreadId = threadIds.length === request.limit ? threadIds.at(-1) : undefined;

          // A full page may have more owner rows; the next bounded call proves exhaustion.
          return {
            threadIds,
            ...(afterThreadId === undefined ? {} : { afterThreadId }),
          };
        }),
      ),
    page: (unvalidated) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(ThreadWorkRequest, "work page", unvalidated);
          const cursor = yield* decodeWorkCursor(request);
          const thread = (yield* Ref.get(state)).threads.get(request.threadId);
          // Accepted input can precede canonical Thread materialization entirely.
          const index = yield* readyIndex(request.threadId, thread?.tailSequence ?? 0);

          if (cursor.source === "canonical") {
            const candidates = boundedWorkSelection<CanonicalWorkEntry>(
              request.limit + 1,
              (entry) => entry.id,
              cursor.after,
            );

            for (const entry of index.entries.values()) candidates.add(entry);
            const selected = candidates.values;

            const entries = selected.slice(0, request.limit);
            const after = selected.length > request.limit ? entries.at(-1)?.id : undefined;

            return yield* validateWorkPage(
              {
                entries,
                cursor: encodeWorkCursor({
                  version: WORK_INDEX_VERSION,
                  threadId: request.threadId,
                  source: after === undefined ? "deliveries" : "canonical",
                  ...(after === undefined ? {} : { after }),
                }),
              },
              request.limit,
            );
          }

          const page = yield* (
            workOwners.get(cursor.source)?.page(request.threadId, cursor.after, request.limit) ??
              Effect.succeed<Effect.Success<ReturnType<MemoryWorkOwner["page"]>>>({ entries: [] })
          );

          const next =
            page.after === undefined
              ? cursor.source === "admissions"
                ? "canonical"
                : undefined
              : cursor.source;

          return yield* validateWorkPage(
            {
              entries: [...page.entries],
              ...(next === undefined
                ? {}
                : {
                    cursor: encodeWorkCursor({
                      version: WORK_INDEX_VERSION,
                      threadId: request.threadId,
                      source: next,
                      ...(page.after === undefined ? {} : { after: page.after }),
                    }),
                  }),
            },
            request.limit,
          );
        }),
      ),
    rebuild: (unvalidated) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(WorkIndexRebuildRequest, "rebuild work", unvalidated);
          const current = yield* Ref.get(state);
          const thread = current.threads.get(request.threadId);
          const tail = thread?.tailSequence ?? ZERO_CANONICAL_SEQUENCE;
          const prior = (yield* Ref.get(workState)).get(request.threadId);

          const index: WorkIndex =
            request.restart ||
            prior === undefined ||
            prior.version !== WORK_INDEX_VERSION ||
            prior.entries.size !== prior.entryCount ||
            (prior.state === "ready" && prior.through !== tail)
              ? { ...emptyWorkIndex(), state: "rebuilding", rebuildingIndexes: emptyIndexes() }
              : prior;

          if (index.state === "ready")
            return {
              version: WORK_INDEX_VERSION,
              threadId: request.threadId,
              state: "ready",
              throughSequence: index.through,
              tailSequence: tail,
              processedRecords: 0,
              processedBytes: 0,
            };
          if (index.rebuildingIndexes === undefined)
            return yield* WorkDiscoveryUnavailable.make({
              threadId: request.threadId,
              reason: "incomplete-rebuild",
            });
          const rebuildingIndexes = index.rebuildingIndexes;
          const records: Array<CanonicalRecordEnvelope> = [];
          let bytes = 0;
          const end = Math.min(tail, index.through + (request.limit ?? MAX_WORK_REBUILD_RECORDS));

          for (let position = index.through; position < end; position++) {
            const size = thread?.recordBytes[position];

            if (size === undefined)
              return yield* storeError("rebuild work", "Canonical record byte length is missing");
            if (bytes + size > MAX_WORK_REBUILD_BYTES) break;
            const record = thread?.records[position];

            if (record === undefined || record.sequence !== position + 1)
              return yield* storeError("rebuild work", "Canonical rebuild sequence is incomplete");
            bytes += size;
            records.push(record);
          }
          if (index.through < tail && records.length === 0)
            return yield* storeError(
              "rebuild work",
              "Canonical rebuild cannot advance within its byte bound",
            );

          const commitWork = yield* prepareWork(index, records, retainedDelivery).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
          );

          // Shared writer gate fences the current tail; publish native locators and catalogue together.
          const next = yield* Effect.sync(() => {
            const native = prepareIndexes(rebuildingIndexes, records, true);
            const rebuilt = commitWork();
            const complete = rebuilt.through === tail;

            const next: WorkIndex = {
              version: rebuilt.version,
              through: rebuilt.through,
              reporting: rebuilt.reporting,
              entryCount: rebuilt.entryCount,
              entries: rebuilt.entries,
              messages: rebuilt.messages,
              state: complete ? "ready" : "rebuilding",
              ...(complete ? {} : { rebuildingIndexes: native.indexes }),
            };

            native.commit();
            if (thread !== undefined && complete)
              MutableRef.set(state.ref, {
                threads: new Map(current.threads).set(request.threadId, {
                  ...thread,
                  ...native.indexes,
                }),
              });
            publishWork(request.threadId, next);

            return next;
          });

          return {
            version: WORK_INDEX_VERSION,
            threadId: request.threadId,
            state: next.state,
            throughSequence: next.through,
            tailSequence: tail,
            processedRecords: records.length,
            processedBytes: bytes,
          };
        }),
      ),
  };

  yield* Effect.addFinalizer(() => PubSub.shutdown(updates));

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fnUntraced(
    function* (unvalidated) {
      const request = yield* validate(ThreadMaterialization, "materialize", unvalidated);

      const decision = yield* Ref.modify(
        state,
        (current): readonly [MaterializeDecision, MemoryState] => {
          const existing = current.threads.get(request.threadId);

          if (existing !== undefined) {
            if (request.producerEpoch < existing.producerEpoch) {
              return [
                {
                  _tag: "failure",
                  error: FenceRejected.make({
                    threadId: request.threadId,
                    actualEpoch: existing.producerEpoch,
                    attemptedEpoch: request.producerEpoch,
                  }),
                },
                current,
              ];
            }
            if (request.producerEpoch === existing.producerEpoch) {
              return [{ _tag: "success" }, current];
            }
            const threads = new Map(current.threads);

            threads.set(request.threadId, {
              ...existing,
              producerEpoch: request.producerEpoch,
            });

            return [{ _tag: "success" }, { threads }];
          }
          if (current.threads.size >= maxThreads) {
            return [
              {
                _tag: "failure",
                error: storeError("materialize", `In-memory thread limit ${maxThreads} exceeded`),
              },
              current,
            ];
          }
          const threads = new Map(current.threads);

          threads.set(request.threadId, {
            producerEpoch: request.producerEpoch,
            tailSequence: ZERO_CANONICAL_SEQUENCE,
            tailDigest: EMPTY_TAIL_DIGEST,
            byId: new Map(),
            workerRecords: new Map(),
            peerCount: 0,
            runInputs: new Map(),
            runRecords: new Map(),
            continuations: new Map(),
            operationRecords: new Map(),
            handoffs: [],
            agentUpdates: [],
            deliveryPredecessors: [],
            records: [],
            recordBytes: [],
            recordIds: new Set(),
            batches: new Map(),
            tailDigests: new Map([[ZERO_CANONICAL_SEQUENCE, EMPTY_TAIL_DIGEST]]),
            checkpoints: new Map(),
          });

          publishWork(request.threadId, emptyWorkIndex());

          return [{ _tag: "success" }, { threads }];
        },
      );

      if (decision._tag === "failure") return yield* decision.error;
    },
  );

  const prepareAppend = Effect.fnUntraced(function* (unvalidated: FencedAppendRequest) {
    const codec = Schema.fromJsonString(FencedAppendRequest);

    const request = yield* Schema.encodeUnknownEffect(codec)(unvalidated).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(codec)),
      Effect.mapError((error) => storeError("append", "Invalid append request", error)),
    );

    const digest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError((error) => storeError("append", error.message, error)),
    );

    const batchJson = yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(
      request.batch,
    ).pipe(
      Effect.mapError((cause) => storeError("append", "Unable to encode canonical batch", cause)),
    );

    return { request, digest, batchJson };
  });

  const appendPrepared = Effect.fnUntraced(function* ({
    request,
    digest,
    batchJson,
  }: PreparedMemoryAppend) {
    // Every caller holds the shared kernel mutation gate across validation and Ref publication.
    const current = yield* Ref.get(state);
    const thread = current.threads.get(request.threadId);

    if (
      thread !== undefined &&
      thread.producerEpoch === request.producerEpoch &&
      thread.tailSequence === request.expectedTailSequence &&
      thread.tailDigest === request.expectedTailDigest &&
      !thread.batches.has(request.batch.batchId)
    )
      yield* validateProgressAppend(
        prepareProgressAppend(request.batch.records),
        (runId) =>
          Effect.gen(function* () {
            const latest = thread.continuations.get(runId)?.at(-1);

            if (latest === undefined) return undefined;
            if (latest.record.payload._tag !== "RunContinuation")
              return yield* storeError("append", "Invalid newest Run continuation");

            return latest.record.payload;
          }),
        (next) =>
          Effect.succeed((thread.runRecords.get(next.runId) ?? []).map((entry) => entry.record)),
      );

    const currentWork = (yield* Ref.get(workState)).get(request.threadId);

    const preparedWork =
      thread !== undefined &&
      thread.producerEpoch === request.producerEpoch &&
      thread.tailSequence === request.expectedTailSequence &&
      thread.tailDigest === request.expectedTailDigest &&
      !thread.batches.has(request.batch.batchId) &&
      currentWork?.version === WORK_INDEX_VERSION &&
      currentWork.state === "ready" &&
      currentWork.through === thread.tailSequence &&
      currentWork.entryCount === currentWork.entries.size
        ? yield* prepareWork(
            currentWork,
            batchEnvelopes(
              request.threadId,
              request.batch,
              decodeCanonicalSequence(thread.tailSequence + 1),
            ),
            retainedDelivery,
          ).pipe(Effect.provideService(Crypto.Crypto, crypto))
        : undefined;

    const decision = yield* Effect.uninterruptible(
      Ref.modify(state, (current): readonly [AppendDecision, MemoryState] => {
        const thread = current.threads.get(request.threadId);

        if (thread === undefined) {
          return [
            {
              _tag: "failure",
              error: ThreadNotMaterialized.make({
                threadId: request.threadId,
              }),
            },
            current,
          ];
        }
        if (request.producerEpoch !== thread.producerEpoch) {
          return [
            {
              _tag: "failure",
              error: FenceRejected.make({
                threadId: request.threadId,
                actualEpoch: thread.producerEpoch,
                attemptedEpoch: request.producerEpoch,
              }),
            },
            current,
          ];
        }
        const previous = thread.batches.get(request.batch.batchId);

        if (previous !== undefined) {
          if (previous.digest !== digest) {
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  threadId: request.threadId,
                  batchId: request.batch.batchId,
                  reason: "batch-digest",
                }),
              },
              current,
            ];
          }

          return [
            {
              _tag: "success",
              result: AppendResult.make({
                firstSequence: previous.result.firstSequence,
                lastSequence: previous.result.lastSequence,
                tailDigest: previous.result.tailDigest,
                replayed: true,
              }),
              records: [],
            },
            current,
          ];
        }
        if (
          request.expectedTailSequence !== thread.tailSequence ||
          request.expectedTailDigest !== thread.tailDigest
        ) {
          return [
            {
              _tag: "failure",
              error: AppendConflict.make({
                threadId: request.threadId,
                batchId: request.batch.batchId,
                reason: "tail",
                actualTailSequence: thread.tailSequence,
                actualTailDigest: thread.tailDigest,
              }),
            },
            current,
          ];
        }
        if (thread.records.length + request.batch.records.length > MAX_RECORDS_PER_THREAD) {
          return [
            {
              _tag: "failure",
              error: storeError(
                "append",
                `In-memory record limit ${MAX_RECORDS_PER_THREAD} exceeded`,
              ),
            },
            current,
          ];
        }

        const batchRecordIds = new Set<RecordId>();

        for (const record of request.batch.records) {
          if (thread.recordIds.has(record.recordId) || batchRecordIds.has(record.recordId)) {
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  threadId: request.threadId,
                  batchId: request.batch.batchId,
                  reason: "record-identity",
                }),
              },
              current,
            ];
          }
          batchRecordIds.add(record.recordId);
        }

        const records = batchEnvelopes(
          request.threadId,
          request.batch,
          decodeCanonicalSequence(thread.tailSequence + 1),
        );

        const lastSequence = decodeCanonicalSequence(thread.tailSequence + records.length);

        const result = AppendResult.make({
          firstSequence: decodeCanonicalSequence(thread.tailSequence + 1),
          lastSequence,
          tailDigest: digest,
          replayed: false,
        });

        const batches = new Map(thread.batches);

        batches.set(request.batch.batchId, {
          digest,
          result,
          batchJson,
        });
        const recordIds = new Set(thread.recordIds);

        for (const recordId of batchRecordIds) recordIds.add(recordId);
        const tailDigests = new Map(thread.tailDigests);

        tailDigests.set(lastSequence, digest);
        const threads = new Map(current.threads);
        const indexes = prepareIndexes(thread, records);

        threads.set(request.threadId, {
          ...thread,
          ...indexes.indexes,
          tailSequence: lastSequence,
          tailDigest: digest,
          records: [...thread.records, ...records],
          recordBytes: [
            ...thread.recordBytes,
            ...records.map((entry) => canonicalRecordBytes(entry.record)),
          ],
          recordIds,
          batches,
          tailDigests,
        });

        const publication: readonly [AppendDecision, MemoryState] = [
          { _tag: "success", result, records },
          { threads },
        ];

        // All rejection paths and preparation precede mutation. Ref.modify publishes without a yield.
        if (preparedWork !== undefined) publishWork(request.threadId, preparedWork());
        indexes.commit();

        return publication;
      }).pipe(
        Effect.tap((decision) =>
          decision._tag === "success" && decision.records.length > 0
            ? PubSub.publish(updates, undefined)
            : Effect.void,
        ),
      ),
    );

    if (decision._tag === "failure") return yield* decision.error;

    return decision.result;
  });

  const append: ThreadStore["Service"]["append"] = Effect.fn("MemoryThreadStore.append")(
    (request) =>
      prepareAppend(request).pipe(
        Effect.flatMap((prepared) => withMutation(appendPrepared(prepared))),
      ),
  );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(function* (request) {
      yield* validate(ThreadPeerCountRequest, "countPeerMessages", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      return Math.min(thread.peerCount, request.limit);
    });

  const readSnapshot = (
    threadId: ThreadId,
    afterSequence: CanonicalSequence | undefined,
    limit: number,
  ) =>
    Ref.get(state).pipe(
      Effect.flatMap((current) => findThread(current, threadId)),
      Effect.map((thread) => {
        // Append assigns gap-free sequences starting at 1, so the exclusive cursor is an index.
        const start = afterSequence ?? ZERO_CANONICAL_SEQUENCE;

        return thread.records.slice(start, start + limit);
      }),
    );

  const read: ThreadStore["Service"]["read"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadReadRequest, "read", unvalidated);

        if ("selection" in request) {
          const thread = yield* findThread(yield* Ref.get(state), request.threadId);
          const selection = request.selection;

          if (
            "expectedTailSequence" in selection &&
            (selection.expectedTailSequence !== thread.tailSequence ||
              selection.expectedTailDigest !== thread.tailDigest)
          )
            return yield* storeError("selected read", "Canonical tail changed");
          if ("throughSequence" in selection && selection.throughSequence > thread.tailSequence)
            return yield* storeError(
              "selected read",
              "Captured sequence is ahead of the canonical tail",
            );
          let records: ReadonlyArray<CanonicalRecordEnvelope>;

          switch (selection._tag) {
            case "LastAgentUpdate":
            case "DeliveryPredecessor": {
              const candidates =
                selection._tag === "LastAgentUpdate"
                  ? thread.agentUpdates
                  : thread.deliveryPredecessors;

              const latest = candidates[upperSequence(candidates, selection.throughSequence) - 1];

              records = latest === undefined ? [] : [latest];
              break;
            }
            case "RunContinuation": {
              const candidates = thread.continuations.get(selection.runId) ?? [];
              const index = upperSequence(candidates, selection.throughSequence) - 1;
              const latest = candidates[index];

              if (
                latest !== undefined &&
                (latest.record.payload._tag !== "RunContinuation" ||
                  latest.record.payload.runId !== selection.runId)
              )
                return yield* storeError("selected read", "Invalid newest Run continuation");
              records = latest === undefined ? [] : [latest];
              break;
            }
            case "RunEvidence": {
              const candidates = thread.runRecords.get(selection.runId) ?? [];
              const after = request.page.afterSequence ?? 0;

              const facts = candidates.slice(
                upperSequence(candidates, after),
                Math.min(
                  upperSequence(candidates, selection.throughSequence),
                  upperSequence(candidates, after) + request.page.limit,
                ),
              );

              const controls = [
                submissionInputRecordId(selection.submissionId),
                submissionAbortRecordId(selection.submissionId),
                submissionSettlementRecordId(selection.submissionId),
              ].flatMap((id) => {
                const entry = thread.byId.get(id);

                return entry === undefined ||
                  entry.sequence <= after ||
                  entry.sequence > selection.throughSequence
                  ? []
                  : [entry];
              });

              records = [
                ...new Map(
                  [...facts, ...controls].map((entry) => [entry.sequence, entry]),
                ).values(),
              ];
              if (records.some((entry) => entry.record.payload._tag === "RunContinuation"))
                return yield* storeError("selected read", "Invalid Run control identity");
              break;
            }
            case "OperationEvidence": {
              const candidates =
                thread.operationRecords.get(
                  JSON.stringify([selection.runId, selection.toolCallId]),
                ) ?? [];

              const origin = thread.byId.get(selection.originRecordId);
              const after = request.page.afterSequence ?? 0;

              const selectPage = (entries: ReadonlyArray<CanonicalRecordEnvelope>) => {
                const start = upperSequence(entries, after);

                return entries.slice(
                  start,
                  Math.min(
                    upperSequence(entries, selection.throughSequence),
                    start + request.page.limit,
                  ),
                );
              };

              let facts = selectPage(candidates);

              // Approval-only keys exclude siblings' Steps/results before applying page limits.
              for (const toolCallId of selection.approvalToolCallIds) {
                if (toolCallId === selection.toolCallId) continue;

                const approvals =
                  thread.operationRecords.get(
                    JSON.stringify([selection.runId, toolCallId, "approval"]),
                  ) ?? [];

                facts = [...facts, ...selectPage(approvals)]
                  .sort((left, right) => left.sequence - right.sequence)
                  .slice(0, request.page.limit);
              }

              records = [
                ...new Map(
                  [
                    ...facts,
                    ...(origin !== undefined &&
                    origin.sequence > after &&
                    origin.sequence <= selection.throughSequence
                      ? [origin]
                      : []),
                  ].map((entry) => [entry.sequence, entry]),
                ).values(),
              ];
              break;
            }
            case "WorkHandoffs":
              records = thread.handoffs.slice(
                upperSequence(thread.handoffs, request.page.afterSequence ?? 0),
                Math.min(
                  upperSequence(thread.handoffs, selection.throughSequence),
                  upperSequence(thread.handoffs, request.page.afterSequence ?? 0) +
                    request.page.limit,
                ),
              );
              break;
            case "RecordId": {
              const record = thread.byId.get(selection.recordId);

              records = record === undefined ? [] : [record];
              break;
            }
            case "RunInput": {
              const input = thread.runInputs.get(selection.runId);

              if (input === null)
                return yield* storeError("selected read", "Ambiguous original Run input");
              records = input === undefined ? [] : [input];
              break;
            }
            case "WorkerExecution":
              records = ["UserInputRecorded", "RunStarted"].flatMap(
                (tag) => thread.workerRecords.get(`execution:${tag}`) ?? [],
              );
              break;
            case "WorkerState":
              records = [
                ...(thread.workerRecords.get("worker") ?? []),
                ...(thread.workerRecords.get(`subtree:${selection.sourceSubmissionId ?? ""}`) ??
                  []),
                ...(selection.sourceSubmissionId === undefined
                  ? []
                  : (thread.workerRecords.get(
                      `joined:${runIdForSubmission(selection.sourceSubmissionId)}`,
                    ) ?? [])),
              ];
              break;
          }

          // Append-only owner histories may have grown since this Thread snapshot was captured.
          return Stream.fromIterable(
            records
              .filter(
                (entry) =>
                  entry.sequence > (request.page.afterSequence ?? 0) &&
                  entry.sequence <= thread.tailSequence,
              )
              .sort((a, b) => a.sequence - b.sequence)
              .slice(0, request.page.limit),
          );
        }
        const records = yield* readSnapshot(request.threadId, request.afterSequence, request.limit);

        return Stream.fromIterable(records);
      }),
    );

  const observe: ThreadStore["Service"]["observe"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadObservation, "observe", unvalidated);
        const afterSequence = yield* offsetSequence(request.threadId, request.afterOffset);

        return Stream.unwrap(
          Effect.gen(function* () {
            const subscription = yield* PubSub.subscribe(updates);

            const initial = yield* readSnapshot(
              request.threadId,
              afterSequence,
              MAX_RECORDS_PER_THREAD,
            );

            const highWater =
              initial.length === 0 ? afterSequence : (initial.at(-1)?.sequence ?? afterSequence);

            const live = Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
              Stream.mapAccumEffect(
                () => highWater,
                (lastSequence) =>
                  readSnapshot(request.threadId, lastSequence, MAX_RECORDS_PER_THREAD).pipe(
                    Effect.map(
                      (records) => [records.at(-1)?.sequence ?? lastSequence, records] as const,
                    ),
                  ),
              ),
            );

            return Stream.fromIterable(initial).pipe(Stream.concat(live));
          }),
        );
      }),
    );

  const exportThread: ThreadStore["Service"]["export"] = Effect.fnUntraced(function* (unvalidated) {
    const request = yield* validate(ThreadExportRequest, "export", unvalidated);

    const thread = (yield* Ref.get(state)).threads.get(request.threadId);

    const { externalObligations: ledgerObligations, ...facts } =
      ledgerTransfer === undefined
        ? { admissions: [], externalObligations: [] }
        : yield* ledgerTransfer.export(request.threadId);

    const externalObligations = [...(ledgerObligations ?? [])];

    if (hasMessageDeliveries !== undefined && (yield* hasMessageDeliveries(request.threadId)))
      externalObligations.push("message-delivery");

    if (thread === undefined && (facts.admissions?.length ?? 0) === 0)
      return yield* ThreadNotMaterialized.make({ threadId: request.threadId });
    const records: Array<typeof ThreadExportRecord.Type> = [];
    const batches: Array<Pick<CanonicalBatch, "batchId" | "producerId">> = [];

    for (const stored of thread?.batches.values() ?? []) {
      const batch = yield* decodeStoredBatch(stored.batchJson, "export");

      records.push(
        ...batch.records.map((record, index) => {
          const sequence = decodeCanonicalSequence(stored.result.firstSequence + index);

          return ThreadExportRecord.make({
            threadId: request.threadId,
            batchId: batch.batchId,
            sequence,
            offset: observationOffset(request.threadId, sequence),
            record,
          });
        }),
      );
      batches.push({ batchId: batch.batchId, producerId: batch.producerId });
    }

    return yield* validate(
      ThreadExport,
      "export",
      ThreadExport.make({
        format: CURRENT_RECORD_FORMAT,
        threadId: request.threadId,
        tailSequence: thread?.tailSequence ?? ZERO_CANONICAL_SEQUENCE,
        tailDigest: thread?.tailDigest ?? EMPTY_TAIL_DIGEST,
        records,
        batches,
        ...facts,
        ...(externalObligations.length === 0 ? {} : { externalObligations }),
      }),
    );
  });

  const importThread: ThreadImport["Service"]["import"] = Effect.fn("MemoryThreadStore.import")(
    function* (request) {
      const prepared = yield* prepareThreadImport(request).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );

      // Rebuild native observation offsets from owned destination batches.
      const records: Array<CanonicalRecordEnvelope> = [];

      for (const batch of prepared.batches) {
        const decoded = yield* decodeStoredBatch(batch.batchJson, "import");

        records.push(...batchEnvelopes(prepared.result.threadId, decoded, batch.firstSequence));
      }

      return yield* withMutation(
        Effect.gen(function* () {
          const current = yield* Ref.get(state);
          const threadId = prepared.result.threadId;
          const existing = current.threads.get(threadId);

          if (hasMessageDeliveries !== undefined && (yield* hasMessageDeliveries(threadId)))
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "target-not-empty",
              message:
                "The destination Thread already owns message deliveries; finish those deliveries or use a fresh destination store",
            });
          if (
            existing !== undefined &&
            (existing.records.length > 0 ||
              existing.batches.size > 0 ||
              existing.checkpoints.size > 0)
          )
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "target-not-empty",
              message:
                "The destination Thread already contains canonical or checkpoint data; import into a fresh destination store",
            });
          if (existing === undefined && current.threads.size >= maxThreads)
            return yield* storeError("import", `In-memory Thread limit ${maxThreads} exceeded`);
          if (ledgerTransfer === undefined && prepared.submissions.length > 0)
            return yield* ThreadImportRejected.make({
              threadId,
              reason: "unsupported-obligations",
              message:
                "Provide the paired MemorySubmissionLedger layer before importing admissions",
            });

          const producerEpoch = yield* Schema.decodeEffect(
            ProducerEpoch.check(Schema.isLessThan(Number.MAX_SAFE_INTEGER)),
          )(
            Math.max(
              prepared.producerEpoch,
              existing === undefined ? 1 : existing.producerEpoch + 1,
            ),
          ).pipe(
            Effect.mapError((cause) =>
              storeError("import", "Unable to advance the destination producer epoch", cause),
            ),
          );

          const batches = new Map<BatchId, StoredBatch>();

          const tailDigests = new Map<CanonicalSequence, Digest>([
            [ZERO_CANONICAL_SEQUENCE, EMPTY_TAIL_DIGEST],
          ]);

          for (const batch of prepared.batches) {
            batches.set(batch.batch.batchId, {
              batchJson: batch.batchJson,
              digest: batch.tailDigest,
              result: AppendResult.make({
                firstSequence: batch.firstSequence,
                lastSequence: batch.lastSequence,
                tailDigest: batch.tailDigest,
                replayed: false,
              }),
            });
            tailDigests.set(batch.lastSequence, batch.tailDigest);
          }
          const threads = new Map(current.threads);

          const indexes = prepareIndexes(
            {
              peerCount: 0,
              workerRecords: new Map(),
              byId: new Map(),
              runInputs: new Map(),
              runRecords: new Map(),
              continuations: new Map(),
              operationRecords: new Map(),
              handoffs: [],
              agentUpdates: [],
              deliveryPredecessors: [],
            },
            records,
          );

          threads.set(threadId, {
            ...indexes.indexes,
            producerEpoch,
            records,
            recordBytes: records.map((entry) => canonicalRecordBytes(entry.record)),
            batches,
            tailDigests,
            tailSequence: prepared.result.tailSequence,
            tailDigest: prepared.result.tailDigest,
            recordIds: new Set(records.map((entry) => entry.record.recordId)),
            checkpoints: new Map(),
          });

          const commitLedger =
            ledgerTransfer === undefined
              ? undefined
              : yield* ledgerTransfer.prepareImport(prepared, producerEpoch);

          const importedWork = yield* prepareWork(emptyWorkIndex(), records, retainedDelivery).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError(() =>
              ThreadImportRejected.make({
                reason: "invalid-archive",
                message: "Cannot reconstruct imported work",
              }),
            ),
          );

          // Readers do not take the gate. No Effect yield may separate these two publications.
          yield* Effect.uninterruptible(
            Effect.sync(() => {
              commitLedger?.();
              indexes.commit();
              publishWork(threadId, importedWork());
              MutableRef.set(state.ref, { threads });
            }).pipe(Effect.andThen(PubSub.publish(updates, undefined))),
          );

          return prepared.result;
        }),
      );
    },
  );

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fnUntraced(
    function* (unvalidated) {
      const request = yield* validate(ThreadTailRequest, "inspectTail", unvalidated);

      const thread = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findThread(current, request.threadId)),
      );

      return ThreadTail.make({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        producerEpoch: thread.producerEpoch,
      });
    },
  );

  const readIdentity: ThreadStore["Service"]["readIdentity"] = Effect.fnUntraced(
    function* (unvalidated) {
      const request = yield* validate(ThreadIdentityRequest, "readIdentity", unvalidated);

      const thread = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findThread(current, request.threadId)),
      );

      const selected = [
        thread.records[0],
        thread.byId.get(workerOriginRecordId(request.threadId)),
        thread.byId.get(subagentLineageRecordId(request.threadId)),
      ].filter((entry) => entry !== undefined);

      return yield* ThreadIdentity.makeEffect({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        producerEpoch: thread.producerEpoch,
        records: selected.filter(
          (entry, index) =>
            selected.findIndex((other) => other.record.recordId === entry.record.recordId) ===
            index,
        ),
      }).pipe(
        Effect.mapError((cause) => storeError("readIdentity", "Invalid canonical identity", cause)),
      );
    },
  );

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fnUntraced(function* (unvalidated) {
    yield* validateCheckpointVersion(unvalidated);
    const request = yield* validate(SaveCheckpointRequest, "saveCheckpoint", unvalidated);

    const decision = yield* Ref.modify(
      state,
      (current): readonly [CheckpointDecision, MemoryState] => {
        const checkpoint = request.checkpoint;
        const thread = current.threads.get(checkpoint.threadId);

        if (thread === undefined) {
          return [
            {
              _tag: "failure",
              error: ThreadNotMaterialized.make({
                threadId: checkpoint.threadId,
              }),
            },
            current,
          ];
        }
        if (checkpoint.throughSequence > thread.tailSequence) {
          return [
            {
              _tag: "failure",
              error: CheckpointRejected.make({
                threadId: checkpoint.threadId,
                reason: "ahead-of-tail",
              }),
            },
            current,
          ];
        }
        if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest) {
          return [
            {
              _tag: "failure",
              error: CheckpointRejected.make({
                threadId: checkpoint.threadId,
                reason: "digest-mismatch",
              }),
            },
            current,
          ];
        }
        if (
          !thread.checkpoints.has(checkpoint.throughSequence) &&
          thread.checkpoints.size >= MAX_CHECKPOINTS_PER_THREAD
        ) {
          return [
            {
              _tag: "failure",
              error: storeError(
                "saveCheckpoint",
                `In-memory checkpoint limit ${MAX_CHECKPOINTS_PER_THREAD} exceeded`,
              ),
            },
            current,
          ];
        }
        const checkpoints = new Map(thread.checkpoints);

        checkpoints.set(checkpoint.throughSequence, checkpoint);
        const threads = new Map(current.threads);

        threads.set(checkpoint.threadId, { ...thread, checkpoints });

        return [{ _tag: "success" }, { threads }];
      },
    );

    if (decision._tag === "failure") return yield* decision.error;
  });

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fnUntraced(function* (unvalidated) {
    const request = yield* validate(LoadCheckpointRequest, "loadCheckpoint", unvalidated);

    const thread = yield* Ref.get(state).pipe(
      Effect.flatMap((current) => findThread(current, request.threadId)),
    );

    const maximum = request.atOrBeforeSequence ?? thread.tailSequence;
    let selected: ThreadCheckpoint | undefined;

    for (const [sequence, checkpoint] of thread.checkpoints) {
      if (sequence <= maximum && (selected === undefined || sequence > selected.throughSequence)) {
        selected = checkpoint;
      }
    }
    if (
      selected !== undefined &&
      thread.tailDigests.get(selected.throughSequence) !== selected.tailDigest
    ) {
      return yield* CheckpointRejected.make({
        threadId: request.threadId,
        reason: "digest-mismatch",
      });
    }

    return Option.fromNullishOr(selected);
  });

  const threadStore = ThreadStore.of({
    work,
    readIdentity,
    countPeerMessages,
    materialize: (request) => withMutation(materialize(request)),
    append,
    read,
    observe,
    export: (request) => withMutation(exportThread(request)),
    inspectTail,
    checkpoints: { save: (request) => withMutation(saveCheckpoint(request)), load: loadCheckpoint },
  });

  return Context.make(ThreadStore, threadStore).pipe(
    Context.add(ThreadImport, { import: importThread }),
    Context.add(MemoryThreadStoreKernel, {
      withMutation,
      retainDelivery,
      initializeWork: (threadId) => {
        if (
          !MutableRef.get(state.ref).threads.has(threadId) &&
          !MutableRef.get(workState.ref).has(threadId)
        )
          publishWork(threadId, emptyWorkIndex());
      },
      registerWorkOwner: (kind, owner) =>
        withMutation(
          Effect.sync(() => {
            if (workOwners.has(kind))
              throw new Error(`MemoryThreadStore already has a ${kind} work owner`);
            workOwners.set(kind, owner);
          }),
        ),
      registerDeliveryLookup: (lookup) =>
        withMutation(
          Effect.sync(() => {
            if (deliveryLookup !== undefined)
              throw new Error("MemoryThreadStore already has a delivery lookup");
            deliveryLookup = lookup;
          }),
        ),
      registerLedgerTransfer: (transfer) =>
        withMutation(
          Effect.sync(() => {
            if (ledgerTransfer !== undefined)
              throw new Error("MemoryThreadStore already has a paired SubmissionLedger");
            ledgerTransfer = transfer;
          }),
        ),
      registerMessageDeliveryStore: (hasRetained) =>
        withMutation(
          Effect.sync(() => {
            if (hasMessageDeliveries !== undefined)
              throw new Error("MemoryThreadStore already has a paired MessageDeliveryStore");
            hasMessageDeliveries = hasRetained;
          }),
        ),
      prepareAppend,
      appendPrepared,
      record: (threadId, recordId) =>
        Ref.get(state).pipe(
          Effect.map((current) => current.threads.get(threadId)?.byId.get(recordId)?.record),
        ),
      tail: (threadId) => inspectTail(ThreadTailRequest.make({ threadId })),
    }),
  );
});

/**
 * In-memory canonical Thread persistence. Durable accepted work is served by the separate
 * SubmissionLedger port; this Layer provides ThreadStore, ThreadReader, and ThreadImport.
 * Importing admissions requires a MemorySubmissionLedger paired with this store.
 */
export const MemoryThreadStoreLive = ThreadReader.layer().pipe(
  Layer.provideMerge(Layer.effectContext(makeThreadStore)),
);

/** Configure a finite retained Thread capacity. Invalid construction options throw immediately. */
export const memoryThreadStoreLayer = (options: { readonly maxThreads?: number } = {}) =>
  MemoryThreadStoreLive.pipe(
    Layer.provide(
      Layer.succeed(ThreadCapacity)(
        Schema.decodeSync(
          Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(65_536)),
        )(options.maxThreads ?? MAX_THREADS),
      ),
    ),
  );
