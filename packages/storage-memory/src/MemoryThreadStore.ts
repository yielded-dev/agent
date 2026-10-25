import { canonicalJson, utf8ByteLength } from "@yielded/agent/digest";
import {
  type ThreadId,
  RunId,
  SubmissionId as importSubmissionId,
} from "@yielded/agent/identifiers";
import {
  MessageDeliveryError,
  prepareCanonicalMessageDeliveries,
} from "@yielded/agent/message-delivery";
import {
  ExportBatch,
  ExportedRecord,
  ExportRecord,
  ReadRecord,
} from "@yielded/agent/record-format";
import {
  CURRENT_RECORD_FORMAT,
  RecordEnvelope,
  type RecordId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  type BatchId,
  MAX_RUN_EVIDENCE_RECORDS,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_TERMINAL_BYTES,
  RUN_TERMINAL_RESERVE_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
  PromptRecord,
  type PromptRecordEnvelope,
} from "@yielded/agent/records";
import {
  canonicalRunIds,
  canonicalRecordBytes,
  isWorkHandoff,
  prepareProgressAppend,
  ProgressAppendReader,
  validateProgressAppend,
  verifyRunContinuations,
} from "@yielded/agent/run-continuation";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
  workerInputRecordId,
  toolCallSettledRecordId,
} from "@yielded/agent/run-journal";
import { StorageOwner, makeStorageOwner } from "@yielded/agent/run-storage";
import {
  submissionAbortRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "@yielded/agent/submission-ledger";
import {
  ThreadArchiveRange,
  ThreadArchiveRangeRead,
  ThreadArchiveRangeSeal,
  ThreadArchiveRangePublish,
  ThreadArchiveRangeRequest,
  MAX_ARCHIVE_RANGE_RECORDS,
  MAX_ARCHIVE_RANGE_BYTES,
  type ThreadArchiveStorage,
} from "@yielded/agent/thread-archive-range";
import {
  makeThreadImportProgress,
  captureImportSource,
  prepareImportPage,
  finishThreadImport,
  verifyImportedReferences,
  validateClosedRun,
  rebuildClosedSubmission,
  invalidThreadArchive,
  ThreadImportReader,
  ThreadDeliveryImportReader,
  ThreadImport,
  ThreadImportRejected,
} from "@yielded/agent/thread-import";
import { verifyThreadInvariants } from "@yielded/agent/thread-invariants";
import {
  type AppendRequest,
  type ExecutionCheckpoint,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  AppendConflict,
  AppendResult,
  ThreadExportRecord,
  ThreadExportRejected,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadPromptRead,
  ThreadStore,
  ThreadReader,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  PreparedAppend,
  type CommitBatch,
  PROMPT_EVIDENCE_TAGS,
  ThreadVerificationRequest,
  ThreadWorkerCapacityRequest,
  ThreadWorkerCapacity,
  streamExport,
  ThreadExportSource,
  canonicalBatchFitsTransfer,
  transferPageFits,
} from "@yielded/agent/thread-store";
import {
  checkExportSource,
  exportThreadPage,
  ThreadExporterReader,
  type TransferManifest,
} from "@yielded/agent/thread-transfer";
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
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as Layer from "effect/Layer";
import * as MutableRef from "effect/MutableRef";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import {
  MemoryThreadStoreKernel,
  type MemoryLedgerTransfer,
  type MemoryLedgerImport,
  type MemoryDeliveryTransfer,
  type MemoryDeliveryImport,
  type MemoryDeliveryWriter,
  type MemoryWorkOwner,
  type PreparedMemoryAppend,
} from "./internal/MemoryThreadStoreKernel.ts";
import { boundedWorkSelection } from "./internal/WorkSelection.ts";

const MAX_THREADS = 256;
const RECORD_CHUNK_SIZE = 1_024;
const READ_PAGE_SIZE = 8;

const ThreadCapacity = Context.Reference<number>(
  "@effect-agent/storage-memory/MemoryThreadStore/ThreadCapacity",
  { defaultValue: () => MAX_THREADS },
);

interface StoredBatch {
  /** Preserve the exact destination wire, including additive fields unknown to this reader. */
  readonly batchJson: string;
  readonly result: AppendResult;
}

interface StoredThread {
  /** Append-only owner indexes; readers must constrain them to their captured canonical tail. */
  readonly runRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly operationRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly handoffs: Array<CanonicalRecordEnvelope>;
  readonly agentUpdates: Array<CanonicalRecordEnvelope>;
  readonly deliveryPredecessors: Array<CanonicalRecordEnvelope>;
  readonly toolDeclarations: Map<string, CanonicalRecordEnvelope | null>;
  readonly promptEvidence: Array<CanonicalRecordEnvelope>;
  readonly modelCompletions: Array<CanonicalRecordEnvelope>;
  readonly applicationInputs: Array<CanonicalRecordEnvelope>;
  readonly durableOwners: Array<CanonicalRecordEnvelope>;
  readonly windowBoundaries: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly workerRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly byId: Map<string, CanonicalRecordEnvelope>;
  readonly runInputs: Map<string, CanonicalRecordEnvelope | null>;
  readonly tailSequence: CanonicalSequence;
  readonly ranges: Map<number, ThreadArchiveRange>;
  readonly rangeStarts: Array<number>;
  readonly archiveContents: Map<number, Map<BatchId, string>>;
  readonly chunks: Map<
    number,
    { readonly records: Array<CanonicalRecordEnvelope>; readonly bytes: Array<number> }
  >;
  readonly batches: Map<BatchId, StoredBatch>;
  readonly executions: Map<RunId, ExecutionCheckpoint>;
  readonly executionCommits: Map<
    RunId,
    { readonly batchId: BatchId; readonly intent: string; readonly result: AppendResult }
  >;
  readonly batchesByFirst: Map<number, BatchId>;
}

const storedRecord = (thread: StoredThread, sequence: number) =>
  thread.chunks.get(Math.floor((sequence - 1) / RECORD_CHUNK_SIZE))?.records[
    (sequence - 1) % RECORD_CHUNK_SIZE
  ];

const storedBytes = (thread: StoredThread, sequence: number) =>
  thread.chunks.get(Math.floor((sequence - 1) / RECORD_CHUNK_SIZE))?.bytes[
    (sequence - 1) % RECORD_CHUNK_SIZE
  ];

const storedPage = Effect.fnUntraced(function* (
  thread: StoredThread,
  after: number,
  limit: number,
) {
  const page: Array<CanonicalRecordEnvelope> = [];
  let bytes = 0;

  for (
    let sequence = after + 1;
    sequence <= Math.min(thread.tailSequence, after + limit);
    sequence++
  ) {
    const record = storedRecord(thread, sequence);

    if (record === undefined) return yield* storeError("read", "Incomplete canonical chunk table");
    const size = storedBytes(thread, sequence) ?? 0;

    if (bytes + size > 32 * 1024 * 1024) break;
    bytes += size;
    page.push(record);
  }

  return page;
});

const retainRecords = (thread: StoredThread, records: ReadonlyArray<CanonicalRecordEnvelope>) => {
  for (const entry of records) {
    const chunkId = Math.floor((entry.sequence - 1) / RECORD_CHUNK_SIZE);
    let chunk = thread.chunks.get(chunkId);

    if (chunk === undefined) {
      chunk = { records: [], bytes: [] };
      thread.chunks.set(chunkId, chunk);
    }
    chunk.records.push(entry);
    chunk.bytes.push(canonicalRecordBytes(entry.record));
  }
};

interface MemoryState {
  readonly threads: Map<ThreadId, StoredThread>;
}

type NativeIndexes = Pick<
  StoredThread,
  | "toolDeclarations"
  | "promptEvidence"
  | "modelCompletions"
  | "applicationInputs"
  | "durableOwners"
  | "windowBoundaries"
  | "workerRecords"
  | "byId"
  | "runInputs"
  | "runRecords"
  | "handoffs"
  | "operationRecords"
  | "agentUpdates"
  | "deliveryPredecessors"
>;

const emptyIndexes = (): NativeIndexes => ({
  toolDeclarations: new Map(),
  promptEvidence: [],
  modelCompletions: [],
  applicationInputs: [],
  durableOwners: [],
  windowBoundaries: new Map(),
  workerRecords: new Map(),
  byId: new Map(),
  runInputs: new Map(),
  runRecords: new Map(),
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
  readonly workers: Map<ThreadId, { ordinary: number; update: number; effects: number }>;
  readonly liveInputs: Array<LiveInputLocator>;
  readonly inputsByWorker: Map<ThreadId, Array<LiveInputLocator>>;
  readonly rebuildingIndexes?: NativeIndexes;
}

interface LiveInputLocator {
  readonly sequence: CanonicalSequence;
  readonly id: string;
}

const liveInputPosition = (entries: ReadonlyArray<LiveInputLocator>, sequence: number) => {
  let low = 0;
  let high = entries.length;

  while (low < high) {
    const middle = Math.floor((low + high) / 2);

    if (entries[middle].sequence < sequence) low = middle + 1;
    else high = middle;
  }

  return low;
};

const changeLiveInput = (
  entries: Array<LiveInputLocator>,
  entry: CanonicalWorkEntry,
  change: 1 | -1,
) => {
  const at = liveInputPosition(entries, entry.createdSequence);

  if (change === 1) entries.splice(at, 0, { sequence: entry.createdSequence, id: entry.id });
  else if (entries[at]?.id === entry.id) entries.splice(at, 1);
};

const emptyWorkIndex = (): WorkIndex => ({
  version: WORK_INDEX_VERSION,
  state: "ready",
  through: ZERO_CANONICAL_SEQUENCE,
  reporting: "none",
  entryCount: 0,
  entries: new Map(),
  workers: new Map(),
  liveInputs: [],
  inputsByWorker: new Map(),
});

const changeWorkCounts = (index: WorkIndex, entry: CanonicalWorkEntry, change: 1 | -1) => {
  const owner = entry.owner;

  if (owner._tag === "WorkerInput") {
    changeLiveInput(index.liveInputs, entry, change);
    let inputs = index.inputsByWorker.get(owner.workerThreadId);

    if (inputs === undefined) index.inputsByWorker.set(owner.workerThreadId, (inputs = []));
    changeLiveInput(inputs, entry, change);
    if (inputs.length === 0) index.inputsByWorker.delete(owner.workerThreadId);
    let worker = index.workers.get(owner.workerThreadId);

    if (worker === undefined)
      index.workers.set(owner.workerThreadId, (worker = { ordinary: 0, update: 0, effects: 0 }));
    if (owner.update) worker.update += change;
    else worker.ordinary += change;
    if (worker.ordinary + worker.update + worker.effects === 0)
      index.workers.delete(owner.workerThreadId);
  }
};

/** Prepare only this batch's changes; commit under the writer gate with canonical publication. */
const prepareWork = Effect.fnUntraced(function* (
  previous: WorkIndex,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
) {
  const changes = new Map<string, CanonicalWorkEntry | undefined>();
  let reporting = previous.reporting;
  let through = previous.through;

  for (const record of records) {
    for (const change of yield* workIndexChanges(record, reporting)) {
      if (change._tag === "WorkerMode") reporting = change.mode;
      else if (change._tag === "Remove") {
        const entry = changes.has(change.id)
          ? changes.get(change.id)
          : previous.entries.get(change.id);

        if (
          change.stateRecordId === undefined ||
          (entry?.stateReference._tag === "Canonical" &&
            entry.stateReference.recordId === change.stateRecordId)
        )
          changes.set(change.id, undefined);
      } else {
        const entry = advanceWorkEntry(
          changes.has(change.entry.id)
            ? changes.get(change.entry.id)
            : previous.entries.get(change.entry.id),
          change.entry,
        );

        changes.set(entry.id, entry);
      }
    }
    through = record.sequence;
  }

  return (): WorkIndex => {
    const { entries } = previous;

    for (const [id, entry] of changes) {
      const old = entries.get(id);

      if (old !== undefined) changeWorkCounts(previous, old, -1);

      if (entry === undefined) entries.delete(id);
      else {
        entries.set(id, entry);
        changeWorkCounts(previous, entry, 1);
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
) => {
  // Latest execution entries are replaced in this private map; owner histories only append.
  // Rebuild staging is private: reuse it only in the synchronous cursor publication step.
  const workerRecords = new Map<string, Array<CanonicalRecordEnvelope>>();

  const wasWorker = (previous.workerRecords.get("worker") ?? []).some(
    ({ record }) => record.payload._tag === "WorkerOriginRecorded",
  );

  const isWorker =
    wasWorker || records.some(({ record }) => record.payload._tag === "WorkerOriginRecorded");

  // A Thread can gain a Worker origin after its initial history. Initialize once on that boundary.
  if (isWorker && !wasWorker)
    for (const entry of previous.byId.values()) {
      const payload = entry.record.payload;

      if (
        (payload._tag === "UserInputRecorded" || payload._tag === "SubmissionSettled") &&
        payload.runId !== undefined
      ) {
        const key = `execution:${payload._tag}`;

        if ((workerRecords.get(key)?.[0]?.sequence ?? 0) < entry.sequence)
          workerRecords.set(key, [entry]);
      }
    }
  const workerAppends = new Map<string, Array<CanonicalRecordEnvelope>>();
  const byId = new Map<string, CanonicalRecordEnvelope>();
  const runInputs = new Map<string, CanonicalRecordEnvelope | null>();
  const runRecords = new Map<string, Array<CanonicalRecordEnvelope>>();
  const handoffs: Array<CanonicalRecordEnvelope> = [];
  const agentUpdates: Array<CanonicalRecordEnvelope> = [];

  const deliveryPredecessors: Array<CanonicalRecordEnvelope> = [];

  const nativeAdds = emptyIndexes();
  const operationAppends = new Map<string, Array<CanonicalRecordEnvelope>>();

  for (const entry of records) {
    byId.set(entry.record.recordId, entry);
    const payload = entry.record.payload;

    if (payload._tag === "ModelResponseRecorded")
      for (const operation of payload.toolOperations) {
        const id = toolCallSettledRecordId(payload.runId, payload.turn, operation.toolCallId);

        nativeAdds.toolDeclarations.set(
          id,
          previous.toolDeclarations.has(id) || nativeAdds.toolDeclarations.has(id) ? null : entry,
        );
      }
    if (PROMPT_EVIDENCE_TAGS.some((tag) => tag === payload._tag))
      nativeAdds.promptEvidence.push(entry);
    if (payload._tag === "ModelCompleted") nativeAdds.modelCompletions.push(entry);
    if (payload._tag === "UserInputRecorded") {
      const message = payload.messageAdmission;

      if (
        message === undefined ||
        !("_tag" in message) ||
        (message._tag !== "WorkerCompletion" && message._tag !== "WorkerUpdate")
      )
        nativeAdds.applicationInputs.push(entry);
    }
    if (
      (payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) ||
      [
        "RunStarted",
        "SubmissionSettled",
        "AbortRequested",
        "WorkerOriginRecorded",
        "SubagentLineageRecorded",
      ].includes(payload._tag)
    )
      nativeAdds.durableOwners.push(entry);
    if (payload._tag === "CompactionCreated" && payload.kind === "rollover")
      for (let height = 0; height < 53; height++)
        appendToIndex(
          nativeAdds.windowBoundaries,
          `${height}:${Math.floor(payload.coversThrough / 2 ** height)}`,
          entry,
        );

    for (const runId of canonicalRunIds(entry.record)) appendToIndex(runRecords, runId, entry);
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
      if (payload._tag === "ToolCallSettled")
        appendToIndex(
          operationAppends,
          JSON.stringify([payload.runId, payload.toolCallId, "settled"]),
          entry,
        );
    }

    if (
      isWorker &&
      (payload._tag === "UserInputRecorded" || payload._tag === "SubmissionSettled") &&
      payload.runId !== undefined
    )
      workerRecords.set(`execution:${payload._tag}`, [entry]);

    const workerKey =
      payload._tag === "SubtreeBudgetReserved"
        ? `subtree:${payload.sourceSubmissionId ?? ""}`
        : payload._tag === "SubagentJoined"
          ? `joined:${payload.runId}`
          : ["ThreadCreated", "WorkerOriginRecorded", "SubagentLineageRecorded"].includes(
                payload._tag,
              )
            ? "worker"
            : undefined;

    if (workerKey !== undefined) appendToIndex(workerAppends, workerKey, entry);
    if (payload._tag === "WorkerStopRequested")
      appendToIndex(workerAppends, `stop:${payload.command.worker.threadId}`, entry);
    if (
      payload._tag === "WorkerInputRequested" &&
      payload.admission.sourceSubmissionId !== undefined
    )
      appendToIndex(workerAppends, `funding:${payload.admission.sourceSubmissionId}`, entry);
    if (payload._tag === "WorkerInputCompleted") {
      const requested =
        previous.byId.get(workerInputRecordId(payload.messageId)) ??
        byId.get(workerInputRecordId(payload.messageId));

      const admission = requested?.record.payload;

      if (
        admission?._tag === "WorkerInputRequested" &&
        admission.admission.sourceSubmissionId !== undefined
      )
        appendToIndex(workerAppends, `funding:${admission.admission.sourceSubmissionId}`, entry);
    }
    if (payload._tag === "WorkerInputRefused") {
      const reserved = (
        previous.byId.get(payload.reservation.recordId) ?? byId.get(payload.reservation.recordId)
      )?.record.payload;

      const sourceSubmissionId =
        reserved?._tag === "WorkerInputRequested"
          ? reserved.admission.sourceSubmissionId
          : reserved?._tag === "SubtreeBudgetReserved"
            ? reserved.sourceSubmissionId
            : undefined;

      if (sourceSubmissionId !== undefined)
        appendToIndex(workerAppends, `funding:${sourceSubmissionId}`, entry);
    }
    if (
      payload._tag === "UserInputRecorded" &&
      payload.kind === "user" &&
      payload.runId !== undefined
    )
      runInputs.set(
        payload.runId,
        runInputs.has(payload.runId) || previous.runInputs.has(payload.runId) ? null : entry,
      );
  }

  return {
    indexes: previous,
    commit: () => {
      commitIndex(previous.runRecords, runRecords);
      commitIndex(previous.operationRecords, operationAppends);
      for (const [id, entry] of nativeAdds.toolDeclarations)
        previous.toolDeclarations.set(id, entry);
      for (const key of ["promptEvidence", "modelCompletions", "applicationInputs"] as const)
        for (const entry of nativeAdds[key]) previous[key].push(entry);
      commitIndex(previous.windowBoundaries, nativeAdds.windowBoundaries);
      if (previous.durableOwners.length === 0 && nativeAdds.durableOwners[0] !== undefined)
        previous.durableOwners.push(nativeAdds.durableOwners[0]);
      for (const [id, entry] of byId) previous.byId.set(id, entry);
      for (const [id, entry] of runInputs) previous.runInputs.set(id, entry);
      for (const [key, entries] of workerRecords) previous.workerRecords.set(key, entries);
      commitIndex(previous.workerRecords, workerAppends);
      for (const entry of handoffs) previous.handoffs.push(entry);
      for (const entry of agentUpdates) previous.agentUpdates.push(entry);
      for (const entry of deliveryPredecessors) previous.deliveryPredecessors.push(entry);
    },
  };
};

type AppendDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadStoreError | ThreadNotMaterialized | AppendConflict;
    }
  | {
      readonly _tag: "success";
      readonly result: AppendResult;
      readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
    };

type MaterializeDecision =
  | { readonly _tag: "failure"; readonly error: ThreadStoreError }
  | { readonly _tag: "success" };

const storeError = (operation: string, message: string, cause?: unknown): ThreadStoreError =>
  cause === undefined
    ? ThreadStoreError.make({ operation, message })
    : ThreadStoreError.make({ operation, message, cause });

const deliveryError = (message: string, cause: MessageDeliveryError): ThreadStoreError =>
  ThreadStoreError.make({ operation: "append", message, cause, deliveryReason: cause.reason });

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
    return yield* storeError("observe", "Malformed observation offset");
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
  batch: CommitBatch,
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

const makeThreadStore = Effect.gen(function* () {
  const maxThreads = yield* ThreadCapacity;
  const crypto = yield* Crypto.Crypto;
  const state = yield* Ref.make<MemoryState>({ threads: new Map() });
  const gate = yield* Semaphore.make(1);
  const owner = yield* makeStorageOwner(gate.withPermits(1));
  const withMutation = owner.withMutation;
  const updates = yield* PubSub.sliding<void>(1);
  let ledgerTransfer: MemoryLedgerTransfer | undefined;
  let deliveryTransfer: MemoryDeliveryTransfer | undefined;
  let deliveryWriter: MemoryDeliveryWriter | undefined;

  const workState = yield* Ref.make(new Map<ThreadId, WorkIndex>());
  const workOwners = new Map<"admissions" | "deliveries", MemoryWorkOwner>();

  const publishWork = (threadId: ThreadId, index: WorkIndex) =>
    MutableRef.get(workState.ref).set(threadId, index);

  const readyIndex = (
    threadId: ThreadId,
    tail: number,
  ): Effect.Effect<WorkIndex, WorkDiscoveryUnavailable> => {
    const index = MutableRef.get(workState.ref).get(threadId);

    if (
      index === undefined ||
      index.version !== WORK_INDEX_VERSION ||
      index.entries.size !== index.entryCount
    )
      return Effect.fail(WorkDiscoveryUnavailable.make({ threadId, reason: "missing-index" }));
    if (index.state !== "ready" || index.through !== tail)
      return Effect.fail(WorkDiscoveryUnavailable.make({ threadId, reason: "incomplete-rebuild" }));

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

          const cursor = yield* decodeWorkCursor(request).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
          );

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
                  threadDigest: cursor.threadDigest,
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
                      threadDigest: cursor.threadDigest,
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
            const size = thread === undefined ? undefined : storedBytes(thread, position + 1);

            if (size === undefined)
              return yield* storeError("rebuild work", "Canonical record byte length is missing");
            if (bytes + size > MAX_WORK_REBUILD_BYTES) break;
            const record = thread === undefined ? undefined : storedRecord(thread, position + 1);

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

          const commitWork = yield* prepareWork(index, records).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
          );

          // Shared writer gate fences the current tail; publish native locators and catalogue together.
          const next = yield* Effect.sync(() => {
            const native = prepareIndexes(rebuildingIndexes, records);
            const rebuilt = commitWork();
            const complete = rebuilt.through === tail;

            const next: WorkIndex = {
              version: rebuilt.version,
              through: rebuilt.through,
              reporting: rebuilt.reporting,
              entryCount: rebuilt.entryCount,
              entries: rebuilt.entries,
              workers: rebuilt.workers,
              liveInputs: rebuilt.liveInputs,
              inputsByWorker: rebuilt.inputsByWorker,
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

          if (existing !== undefined) return [{ _tag: "success" }, current];
          if (
            !MutableRef.get(workState.ref).has(request.threadId) &&
            MutableRef.get(workState.ref).size >= maxThreads
          ) {
            return [
              {
                _tag: "failure",
                error: storeError("materialize", `In-memory thread limit ${maxThreads} exceeded`),
              },
              current,
            ];
          }
          if (!transferPageFits(request.threadId))
            return [
              {
                _tag: "failure",
                error: storeError(
                  "materialize",
                  "Thread identity exceeds its bounded transfer representation",
                ),
              },
              current,
            ];
          const threads = new Map(current.threads);

          threads.set(request.threadId, {
            tailSequence: ZERO_CANONICAL_SEQUENCE,
            byId: new Map(),
            workerRecords: new Map(),
            runInputs: new Map(),
            runRecords: new Map(),
            operationRecords: new Map(),
            handoffs: [],
            agentUpdates: [],
            deliveryPredecessors: [],
            toolDeclarations: new Map(),
            promptEvidence: [],
            modelCompletions: [],
            applicationInputs: [],
            durableOwners: [],
            windowBoundaries: new Map(),
            ranges: new Map(),
            rangeStarts: [],
            archiveContents: new Map(),
            chunks: new Map(),
            batchesByFirst: new Map(),
            batches: new Map(),
            executions: new Map(),
            executionCommits: new Map(),
          });

          publishWork(request.threadId, emptyWorkIndex());

          return [{ _tag: "success" }, { threads }];
        },
      );

      if (decision._tag === "failure") return yield* decision.error;
    },
  );

  const prepareAppend = Effect.fnUntraced(function* (unvalidated: AppendRequest) {
    const captured = yield* PreparedAppend.capture(unvalidated);
    const request = captured;
    const batchJson = captured.batchJson;

    const batchBytes = new TextEncoder().encode(batchJson).byteLength;

    if (batchBytes > 16 * 1024 * 1024)
      return yield* storeError("append", "Canonical batch exceeds 16 MiB");

    const offsetBytes = 4 * Math.ceil(utf8ByteLength(request.threadId) / 3) + 36;

    if (!canonicalBatchFitsTransfer(request.threadId, request.batch, batchBytes, offsetBytes))
      return yield* storeError(
        "append",
        "Canonical batch exceeds its bounded transfer representation",
      );

    const deliveries = yield* prepareCanonicalMessageDeliveries(
      request.threadId,
      request.batch.records,
    ).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError((cause) => deliveryError("Invalid canonical delivery", cause)),
    );

    return { request, batchJson, deliveries };
  });

  const appendPrepared = Effect.fnUntraced(function* ({
    request,
    batchJson,
    deliveries,
  }: PreparedMemoryAppend) {
    // Every caller holds the shared kernel mutation gate across validation and Ref publication.
    const current = yield* Ref.get(state);
    const thread = current.threads.get(request.threadId);

    const executionIntent = JSON.stringify({
      batch: batchJson,
      operational: (request.executions ?? []).flatMap((update) =>
        update.facts.map((fact) => Schema.encodeSync(RecordEnvelope)(fact)),
      ),
    });

    const latestIntent =
      thread === undefined
        ? undefined
        : [...thread.executionCommits.values()].find(
            (entry) => entry.batchId === request.batch.batchId,
          );

    if (thread !== undefined && latestIntent === undefined && request.batch.records.length === 0) {
      for (const update of request.executions ?? []) {
        const prior = thread.executions.get(update.checkpoint.progress.runId);

        if (
          prior !== undefined &&
          update.previousRevision < prior.progress.revision &&
          update.facts.every((fact) =>
            prior.operational.some((known) => known.recordId === fact.recordId),
          )
        )
          return yield* AppendConflict.make({
            threadId: request.threadId,
            batchId: request.batch.batchId,
            reason: "stale-command",
          });
      }
    }
    if (
      thread !== undefined &&
      thread.tailSequence === request.expectedTailSequence &&
      !thread.batches.has(request.batch.batchId) &&
      latestIntent === undefined
    ) {
      const executions = request.executions ?? [];

      if (
        new Set(executions.map((update) => update.checkpoint.progress.runId)).size !==
        executions.length
      )
        return yield* storeError("append", "Duplicate active execution identity");
      for (const update of request.executions ?? []) {
        const prior = thread.executions.get(update.checkpoint.progress.runId);

        if ((prior?.progress.revision ?? 0) !== update.previousRevision)
          return yield* AppendConflict.make({
            threadId: request.threadId,
            batchId: request.batch.batchId,
            reason: "tail",
            actualTailSequence: thread.tailSequence,
          });
        if (
          update.checkpoint.throughSequence !==
            thread.tailSequence + request.batch.records.length ||
          update.checkpoint.batchId !== request.batch.batchId ||
          (prior !== undefined && update.checkpoint.originalSequence !== prior.originalSequence)
        )
          return yield* storeError(
            "append",
            "Active checkpoint differs from its committed history boundary",
          );
        if (
          update.retire &&
          ![
            ...request.batch.records,
            ...(thread.runRecords.get(update.checkpoint.progress.runId) ?? []).map(
              (entry) => entry.record,
            ),
          ].some(
            ({ payload }) =>
              payload._tag === "SubmissionSettled" &&
              payload.runId === update.checkpoint.progress.runId &&
              payload.submissionId === update.checkpoint.progress.submissionId,
          )
        )
          return yield* storeError(
            "append",
            "Active state cannot retire without a canonical terminal outcome",
          );
      }
      yield* validateProgressAppend(
        prepareProgressAppend(request.batch.records),
        request.executions,
      ).pipe(
        Effect.provideService(ProgressAppendReader, {
          previous: (runId) => Effect.succeed(thread.executions.get(runId)?.progress),
          initial: (next) =>
            Effect.succeed((thread.runRecords.get(next.runId) ?? []).map((entry) => entry.record)),
        }),
      );
    }

    const currentWork = (yield* Ref.get(workState)).get(request.threadId);

    const preparedWork =
      thread !== undefined &&
      thread.tailSequence === request.expectedTailSequence &&
      !thread.batches.has(request.batch.batchId) &&
      latestIntent === undefined &&
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
          ).pipe(Effect.provideService(Crypto.Crypto, crypto))
        : undefined;

    let commitDeliveries: (() => void) | undefined;

    if (deliveries.length > 0) {
      if (deliveryWriter === undefined)
        return yield* deliveryError(
          "Canonical messages require a paired MemoryMessageDeliveryStore",
          MessageDeliveryError.make({ reason: "storage", operation: "canonical publication" }),
        );
      commitDeliveries = yield* deliveryWriter(deliveries).pipe(
        Effect.mapError((cause) => deliveryError("Canonical delivery rejected", cause)),
      );
    }

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
        const priorExecution = latestIntent;

        if (priorExecution !== undefined) {
          if (priorExecution.intent !== executionIntent)
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  threadId: request.threadId,
                  batchId: request.batch.batchId,
                  reason: "batch-content",
                }),
              },
              current,
            ];

          commitDeliveries?.();

          return [
            {
              _tag: "success",
              result: AppendResult.make({ ...priorExecution.result, replayed: true }),
              records: [],
            },
            current,
          ];
        }
        const previous = thread.batches.get(request.batch.batchId);

        if (previous !== undefined) {
          if (previous.batchJson !== batchJson) {
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  threadId: request.threadId,
                  batchId: request.batch.batchId,
                  reason: "batch-content",
                }),
              },
              current,
            ];
          }

          commitDeliveries?.();

          return [
            {
              _tag: "success",
              result: AppendResult.make({
                firstSequence: previous.result.firstSequence,
                lastSequence: previous.result.lastSequence,
                replayed: true,
              }),
              records: [],
            },
            current,
          ];
        }
        if (request.expectedTailSequence !== thread.tailSequence) {
          return [
            {
              _tag: "failure",
              error: AppendConflict.make({
                threadId: request.threadId,
                batchId: request.batch.batchId,
                reason: "tail",
                actualTailSequence: thread.tailSequence,
              }),
            },
            current,
          ];
        }
        const batchRecordIds = new Set<RecordId>();

        for (const record of request.batch.records) {
          if (thread.byId.has(record.recordId) || batchRecordIds.has(record.recordId)) {
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
          firstSequence: decodeCanonicalSequence(
            thread.tailSequence + (records.length > 0 ? 1 : 0),
          ),
          lastSequence,
          replayed: false,
        });

        const indexes = prepareIndexes(thread, records);

        const next: StoredThread = {
          ...thread,
          ...indexes.indexes,
          tailSequence: lastSequence,
        };

        // Every rejection and asynchronous validation precedes this synchronous publication.
        if (records.length > 0) {
          thread.batches.set(request.batch.batchId, { result, batchJson });
          thread.batchesByFirst.set(result.firstSequence, request.batch.batchId);
          retainRecords(thread, records);
        }
        for (const update of request.executions ?? []) {
          const runId = update.checkpoint.progress.runId;

          if (update.retire) {
            thread.executions.delete(runId);
            thread.executionCommits.delete(runId);
          } else {
            thread.executions.set(runId, update.checkpoint);
            thread.executionCommits.set(runId, {
              batchId: request.batch.batchId,
              intent: executionIntent,
              result,
            });
          }
        }
        indexes.commit();
        if (preparedWork !== undefined) publishWork(request.threadId, preparedWork());
        current.threads.set(request.threadId, next);
        commitDeliveries?.();

        const publication: readonly [AppendDecision, MemoryState] = [
          { _tag: "success", result, records },
          current,
        ];

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
        Effect.flatMap((prepared) =>
          owner.withProgress(prepared.request.threadId, withMutation(appendPrepared(prepared))),
        ),
      ),
  );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(function* (request) {
      yield* validate(ThreadPeerCountRequest, "countPeerMessages", request);
      yield* findThread(yield* Ref.get(state), request.threadId);

      return Math.min(
        deliveryTransfer?.peerCount(request.threadId, request.nowMillis, request.limit) ?? 0,
        request.limit,
      );
    });

  const readSnapshot = (
    threadId: ThreadId,
    afterSequence: CanonicalSequence | undefined,
    limit: number,
  ) =>
    Ref.get(state).pipe(
      Effect.flatMap((current) => findThread(current, threadId)),
      Effect.flatMap((thread) => {
        // Append assigns gap-free sequences starting at 1, so the exclusive cursor is an index.
        const start = afterSequence ?? ZERO_CANONICAL_SEQUENCE;

        return storedPage(thread, start, limit);
      }),
    );

  const selectedStream = (
    sources: ReadonlyArray<ReadonlyArray<CanonicalRecordEnvelope>>,
    after: number,
    through: number,
    maximum: number,
  ) =>
    Stream.paginate({ after, count: 0 }, (cursor) =>
      Effect.gen(function* () {
        const page: Array<CanonicalRecordEnvelope> = [];
        let bytes = 0;
        let last = cursor.after;

        while (page.length < Math.min(READ_PAGE_SIZE, maximum - cursor.count)) {
          let selected: CanonicalRecordEnvelope | undefined;

          for (const source of sources) {
            const next = source[upperSequence(source, last)];

            if (
              next !== undefined &&
              next.sequence <= through &&
              (selected === undefined || next.sequence < selected.sequence)
            )
              selected = next;
          }
          if (selected === undefined) break;
          const size = canonicalRecordBytes(selected.record);

          if (size > 32 * 1024 * 1024)
            return yield* storeError("read", "Canonical value exceeds the page byte bound");
          if (bytes + size > 32 * 1024 * 1024) break;
          bytes += size;
          page.push(selected);
          last = selected.sequence;
        }
        const next = { after: last, count: cursor.count + page.length };

        return [
          page,
          page.length === 0 || next.count >= maximum
            ? Option.none<typeof next>()
            : Option.some(next),
        ] as const;
      }),
    );

  const readFromThread = (threadId: ThreadId, thread: StoredThread, request: ThreadReadRequest) =>
    Stream.unwrap(
      Effect.gen(function* () {
        if (request.threadId !== threadId)
          return yield* storeError("read snapshot", "Thread differs from the captured snapshot");

        if (!("selection" in request))
          return Stream.paginate(
            { after: request.afterSequence ?? ZERO_CANONICAL_SEQUENCE, count: 0 },
            (cursor) =>
              Effect.gen(function* () {
                const page = yield* storedPage(
                  thread,
                  cursor.after,
                  Math.min(READ_PAGE_SIZE, request.limit - cursor.count),
                );

                const next = {
                  after: page.at(-1)?.sequence ?? cursor.after,
                  count: cursor.count + page.length,
                };

                return [
                  page,
                  page.length === 0 || next.count >= request.limit
                    ? Option.none<typeof next>()
                    : Option.some(next),
                ] as const;
              }),
          );
        const selection = request.selection;

        if (
          "expectedTailSequence" in selection &&
          selection.expectedTailSequence !== thread.tailSequence
        )
          return yield* storeError("selected read", "Canonical tail changed");
        if ("throughSequence" in selection && selection.throughSequence > thread.tailSequence)
          return yield* storeError("selected read", "Captured sequence is ahead of canonical tail");

        const through =
          "throughSequence" in selection ? selection.throughSequence : thread.tailSequence;

        const after = request.page.afterSequence ?? 0;
        let sources: ReadonlyArray<ReadonlyArray<CanonicalRecordEnvelope>> = [];

        const one = (entry: CanonicalRecordEnvelope | null | undefined) =>
          entry === undefined || entry === null ? [] : [entry];

        const latest = (entries: ReadonlyArray<CanonicalRecordEnvelope>) =>
          one(entries[upperSequence(entries, through) - 1]);

        switch (selection._tag) {
          case "RecordId":
            sources = [one(thread.byId.get(selection.recordId))];
            break;
          case "RunInput": {
            const entry = thread.runInputs.get(selection.runId);

            if (entry === null)
              return yield* storeError("selected read", "Ambiguous original Run input");
            sources = [one(entry)];
            break;
          }
          case "ToolDeclaration": {
            const entry = thread.toolDeclarations.get(selection.settlementRecordId);

            if (entry === null)
              return yield* storeError("selected read", "Ambiguous original tool declaration");
            if (entry !== undefined) {
              const payload = entry.record.payload;

              if (
                payload._tag !== "ModelResponseRecorded" ||
                !payload.toolOperations.some(
                  (operation) =>
                    toolCallSettledRecordId(payload.runId, payload.turn, operation.toolCallId) ===
                    selection.settlementRecordId,
                )
              )
                return yield* storeError("selected read", "Invalid exact tool declaration pointer");
            }
            sources = [one(entry)];
            break;
          }
          case "RunEvidence":
            sources = [
              thread.runRecords.get(selection.runId) ?? [],
              [
                submissionInputRecordId(selection.submissionId),
                submissionAbortRecordId(selection.submissionId),
                submissionSettlementRecordId(selection.submissionId),
              ]
                .flatMap((id) => one(thread.byId.get(id)))
                .sort((a, b) => a.sequence - b.sequence),
            ];
            break;
          case "OperationEvidence":
            sources = [
              thread.operationRecords.get(
                JSON.stringify([selection.runId, selection.toolCallId]),
              ) ?? [],
              ...selection.approvalToolCallIds.flatMap((toolCallId) =>
                toolCallId === selection.toolCallId
                  ? []
                  : [
                      thread.operationRecords.get(
                        JSON.stringify([selection.runId, toolCallId, "approval"]),
                      ) ?? [],
                    ],
              ),
              one(thread.byId.get(selection.originRecordId)),
            ];
            break;
          case "LastAgentUpdate":
            sources = [latest(thread.agentUpdates)];
            break;
          case "DeliveryPredecessor":
            sources = [latest(thread.deliveryPredecessors)];
            break;
          case "LatestModelCompleted":
            sources = [latest(thread.modelCompletions)];
            break;
          case "LatestApplicationInput":
            sources = [latest(thread.applicationInputs)];
            break;
          case "DurableHistoryOwner":
            sources = [one(thread.durableOwners[0])];
            break;
          case "PromptEvidence":
            sources = [thread.promptEvidence];
            break;
          case "WorkHandoffs":
            sources = [thread.handoffs];
            break;
          case "ContextWindowBoundary": {
            // Match native SQL's disjoint coverage-prefix seeks. Each bucket contains
            // canonical sequence order; captured-tail reads never assume monotone coverage.
            let start = 0;
            let selected: CanonicalRecordEnvelope | undefined;

            while (start < selection.atSequence) {
              const remaining = selection.atSequence - start;
              let height = Math.floor(Math.log2(remaining));

              while (2 ** height > remaining) height--;
              const width = 2 ** height;
              const entries = thread.windowBoundaries.get(`${height}:${start / width}`) ?? [];
              const candidate = entries[upperSequence(entries, through) - 1];

              if (
                candidate !== undefined &&
                (selected === undefined || candidate.sequence > selected.sequence)
              )
                selected = candidate;
              start += width;
            }
            sources = [one(selected)];
            break;
          }
          case "WorkerStop":
            sources = [latest(thread.workerRecords.get(`stop:${selection.workerThreadId}`) ?? [])];
            break;
          case "WorkerExecution":
            sources = [
              thread.workerRecords.get("execution:UserInputRecorded") ?? [],
              thread.workerRecords.get("execution:SubmissionSettled") ?? [],
            ];
            break;
          case "WorkerState":
            sources = [
              thread.workerRecords.get("worker") ?? [],
              ...(selection.sourceSubmissionId === undefined
                ? []
                : [
                    thread.workerRecords.get(`subtree:${selection.sourceSubmissionId}`) ?? [],
                    thread.workerRecords.get(
                      `joined:${runIdForSubmission(selection.sourceSubmissionId)}`,
                    ) ?? [],
                    thread.workerRecords.get(`funding:${selection.sourceSubmissionId}`) ?? [],
                  ]),
            ];
            break;
          case "LiveWorkerInputs": {
            const index = yield* readyIndex(request.threadId, thread.tailSequence).pipe(
              Effect.mapError((e) => storeError("selected read", e.message)),
            );

            if (selection.throughSequence !== thread.tailSequence)
              return yield* storeError(
                "selected read",
                "Live work requires the current exact tail",
              );

            const locators =
              selection.workerThreadId === undefined
                ? index.liveInputs
                : (index.inputsByWorker.get(selection.workerThreadId) ?? []);

            const start = liveInputPosition(locators, after + 1);
            const selected: Array<CanonicalRecordEnvelope> = [];

            for (let at = start; at < Math.min(locators.length, start + request.page.limit); at++) {
              const entry = index.entries.get(locators[at].id);

              const origin =
                entry === undefined ? undefined : thread.byId.get(entry.originRecordId);

              if (
                entry?.owner._tag !== "WorkerInput" ||
                origin?.record.payload._tag !== "WorkerInputRequested" ||
                origin.sequence !== locators[at].sequence ||
                origin.record.payload.admission.origin.worker.threadId !==
                  entry.owner.workerThreadId ||
                (origin.record.payload.admission.reportKind === "update") !== entry.owner.update
              )
                return yield* storeError(
                  "selected read",
                  "Invalid live worker input locator or origin",
                );
              selected.push(origin);
            }
            sources = [selected];
            break;
          }
        }

        return selectedStream(sources, after, through, request.page.limit);
      }),
    );

  const read: ThreadStore["Service"]["read"] = (input) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadReadRequest, "read", input);
        const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);

        return readFromThread(request.threadId, thread, request);
      }),
    );

  const decodePromptRecord = Schema.decodeUnknownEffect(PromptRecord);

  const readPrompt: ThreadStore["Service"]["readPrompt"] = (input) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadPromptRead, "readPrompt", input);
        const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);

        if (request.throughSequence > thread.tailSequence)
          return yield* storeError("readPrompt", "Captured sequence is ahead of canonical tail");
        const start = upperSequence(thread.promptEvidence, request.afterSequence ?? 0);
        const records: Array<PromptRecordEnvelope> = [];

        for (
          let at = start;
          at < Math.min(thread.promptEvidence.length, start + request.limit);
          at++
        ) {
          const entry = thread.promptEvidence[at];

          if (entry.sequence > request.throughSequence) break;

          const record = yield* decodePromptRecord(
            Schema.is(ExportedRecord)(entry.record) &&
              Object.prototype.isPrototypeOf.call(ExportedRecord.prototype, entry.record)
              ? entry.record.wire
              : entry.record,
          ).pipe(
            Effect.mapError((cause) => storeError("readPrompt", "Invalid prompt record", cause)),
          );

          records.push({ threadId: request.threadId, sequence: entry.sequence, record });
        }

        return Stream.fromIterable(records);
      }),
    );

  const observe: ThreadStore["Service"]["observe"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadObservation, "observe", unvalidated);
        let after = yield* offsetSequence(request.threadId, request.afterOffset);
        const subscription = yield* PubSub.subscribe(updates);

        const drain = () =>
          Stream.paginate(after, (cursor) =>
            readSnapshot(request.threadId, cursor, READ_PAGE_SIZE).pipe(
              Effect.map((records) => {
                after = records.at(-1)?.sequence ?? cursor;

                return [
                  records,
                  records.length > 0 ? Option.some(after) : Option.none<CanonicalSequence>(),
                ] as const;
              }),
            ),
          );

        return drain().pipe(
          Stream.concat(
            Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(Stream.flatMap(drain)),
          ),
        );
      }),
    );

  const emptyThread = (): StoredThread => ({
    ...emptyIndexes(),
    tailSequence: ZERO_CANONICAL_SEQUENCE,
    ranges: new Map(),
    rangeStarts: [],
    archiveContents: new Map(),
    chunks: new Map(),
    batches: new Map(),
    executions: new Map(),
    executionCommits: new Map(),
    batchesByFirst: new Map(),
  });

  const makeExportReader = (idleOnly: boolean) =>
    ThreadExporterReader.of({
      snapshot: Effect.fnUntraced(function* (threadId: ThreadId, captured?: TransferManifest) {
        const thread = MutableRef.get(state.ref).threads.get(threadId);

        const facts =
          ledgerTransfer === undefined
            ? { idle: true, admissions: 0, aborts: 0, approvals: 0, resolutions: 0 }
            : yield* ledgerTransfer.snapshot(threadId);

        const delivered = deliveryTransfer?.snapshot(threadId) ?? { idle: true, deliveries: 0 };

        if (thread === undefined && facts.admissions === 0 && delivered.deliveries === 0)
          return yield* ThreadNotMaterialized.make({ threadId });
        if (idleOnly) {
          const index = yield* readyIndex(threadId, thread?.tailSequence ?? 0).pipe(
            Effect.mapError((e) => storeError("export", e.message)),
          );

          if (
            owner.hasOwner(threadId) ||
            (thread?.executions.size ?? 0) > 0 ||
            index.entries.size > 0 ||
            !facts.idle ||
            !delivered.idle
          )
            return yield* ThreadExportRejected.make({ threadId, reason: "non-idle" });
          // Only the first page performs the full closed-receipt proof. The caller keeps
          // the source quiescent; subsequent pages still check current work and the tail.
          if (captured === undefined && thread !== undefined) {
            yield* Effect.gen(function* () {
              for (const runId of thread.runRecords.keys())
                yield* validateClosedRun(
                  threadId,
                  yield* Schema.decodeEffect(RunId)(runId).pipe(Effect.orDie),
                );
              if (ledgerTransfer !== undefined) {
                const ledger = ledgerTransfer;

                yield* Stream.runForEach(ledger.submissions(threadId), (submission) =>
                  Effect.gen(function* () {
                    const admission = yield* ledger.admission(threadId, submission.submissionId);

                    if (admission === undefined)
                      return yield* storeError("export", "Missing immutable admission");
                    yield* rebuildClosedSubmission(admission);
                  }),
                );
              }
            }).pipe(
              Effect.provide(ThreadImportReader.layer(importReader(threadId, thread))),
              Effect.mapError((cause) =>
                Schema.is(ThreadImportRejected)(cause)
                  ? ThreadExportRejected.make({ threadId, reason: "non-idle" })
                  : cause,
              ),
            );
          }
        }

        return {
          format: CURRENT_RECORD_FORMAT,
          threadId,
          tailSequence: thread?.tailSequence ?? ZERO_CANONICAL_SEQUENCE,
          snapshot: {
            admissions: facts.admissions,
            aborts: facts.aborts,
            approvals: facts.approvals,
            resolutions: facts.resolutions,
            deliveries: delivered.deliveries,
          },
          ...(facts.workerSeal === undefined ? {} : { workerSeal: facts.workerSeal }),
        };
      }),
      batch: Effect.fnUntraced(function* (threadId: ThreadId, fromSequence: number) {
        const thread = yield* findThread(MutableRef.get(state.ref), threadId);
        const batchId = thread.batchesByFirst.get(fromSequence);
        const stored = batchId === undefined ? undefined : thread.batches.get(batchId);

        if (
          stored === undefined ||
          new TextEncoder().encode(stored.batchJson).byteLength > 16 * 1024 * 1024
        )
          return yield* storeError("export", "Missing or oversized canonical batch wire");
        const batch = yield* decodeStoredBatch(stored.batchJson, "export");
        const records: Array<ThreadExportRecord> = [];

        for (const [index, record] of batch.records.entries()) {
          const sequence = decodeCanonicalSequence(fromSequence + index);
          const exact = storedRecord(thread, sequence);

          if (
            exact === undefined ||
            exact.batchId !== batch.batchId ||
            exact.record.recordId !== record.recordId
          )
            return yield* storeError("export", "Canonical batch index differs from its wire");
          records.push(
            ThreadExportRecord.make({
              threadId,
              batchId: batch.batchId,
              sequence,
              offset: observationOffset(threadId, sequence),
              record,
            }),
          );
        }

        return { batches: [{ batchId: batch.batchId, producerId: batch.producerId }], records };
      }),
      facts: (threadId, section, after) => {
        const empty = Effect.succeed({
          facts: {
            admissions: [],
            commands: { aborts: [], approvals: [], resolutions: [] },
            deliveries: [],
          },
          after: after ?? "",
        });

        return section === "deliveries"
          ? (deliveryTransfer?.facts(threadId, after) ?? empty)
          : (ledgerTransfer?.facts(threadId, section, after) ?? empty);
      },
    });

  const exportReader = makeExportReader(true);
  const verificationReader = makeExportReader(false);

  const exportThread: ThreadStore["Service"]["export"] = (request) =>
    exportThreadPage(request).pipe(Effect.provideService(ThreadExporterReader, exportReader));

  const verificationPage: ThreadStore["Service"]["export"] = (request) =>
    exportThreadPage(request).pipe(Effect.provideService(ThreadExporterReader, verificationReader));

  const exactRunRecords = Effect.fnUntraced(function* (thread: StoredThread, runId: RunId) {
    const records: Array<CanonicalRecordEnvelope> = [];
    let bytes = 0;
    const facts = thread.runRecords.get(runId) ?? [];

    if (
      facts.length >
      MAX_RUN_EVIDENCE_RECORDS + RUN_TERMINAL_RESERVE_RECORDS + MAX_RUN_RECOVERY_SUFFIX_RECORDS
    )
      return yield* storeError("Run evidence", "Run record bounds exceeded");
    for (const entry of facts) {
      bytes += canonicalRecordBytes(entry.record);
      if (bytes > MAX_RUN_EVIDENCE_BYTES + MAX_RUN_TERMINAL_BYTES + MAX_RUN_RECOVERY_SUFFIX_BYTES)
        return yield* storeError("Run evidence", "Run byte bounds exceeded");
      records.push(entry);
    }

    return records.sort((a, b) => a.sequence - b.sequence);
  });

  const exactRecordReader = (
    threadId: ThreadId,
    thread: StoredThread,
  ): Pick<
    ThreadImportReader["Service"],
    "read" | "record" | "runRecords" | "execution" | "hasAgent"
  > => ({
    read: (input) =>
      Stream.unwrap(
        validate(ThreadReadRequest, "read snapshot", input).pipe(
          Effect.map((request) => readFromThread(threadId, thread, request)),
        ),
      ),
    record: (id) => Effect.succeed(thread.byId.get(id)),
    runRecords: (id) => exactRunRecords(thread, id),
    execution: (id) => Effect.succeed(thread.executions.get(id)),
    hasAgent: (id) =>
      Effect.succeed(
        (thread.workerRecords.get("worker") ?? []).some(
          ({ record }) => record.payload._tag === "ThreadCreated" && record.payload.agentId === id,
        ),
      ),
  });

  const importReader = (id: ThreadId, thread: StoredThread): ThreadImportReader["Service"] => ({
    ...exactRecordReader(id, thread),
    admission: (sid) => ledgerTransfer?.admission(id, sid) ?? Effect.as(Effect.void, undefined),
    runOwner: (runId) =>
      runId.startsWith("run:")
        ? (ledgerTransfer?.admission(id, Schema.decodeSync(importSubmissionId)(runId.slice(4))) ??
          Effect.as(Effect.void, undefined))
        : Effect.as(Effect.void, undefined),
    commands: (sid) =>
      ledgerTransfer?.commands(sid) ??
      Effect.succeed({ aborts: [], approvals: [], resolutions: [] }),
    delivery: (messageId) =>
      deliveryTransfer?.record(id, messageId) ?? Effect.as(Effect.void, undefined),
  });

  const importThread: ThreadImport["Service"]["import"] = (pages) =>
    captureImportSource(pages, (captured) =>
      withMutation(
        Effect.gen(function* () {
          const progress = makeThreadImportProgress();
          let staged: StoredThread | undefined;
          let stagedWork = emptyWorkIndex();
          let stagedLedger: MemoryLedgerImport | undefined;
          let stagedDelivery: MemoryDeliveryImport | undefined;
          let existing: StoredThread | undefined;

          yield* Stream.runForEach(captured, (input) =>
            Effect.gen(function* () {
              const page = yield* prepareImportPage(progress, input).pipe(
                Effect.provideService(Crypto.Crypto, crypto),
              );

              const id = page.archive.threadId;

              if (staged === undefined) {
                const current = MutableRef.get(state.ref);

                existing = current.threads.get(id);
                const existingWork = MutableRef.get(workState.ref).get(id);

                if (
                  (existing !== undefined &&
                    (existing.tailSequence > 0 ||
                      existing.batches.size > 0 ||
                      existing.executions.size > 0 ||
                      existing.executionCommits.size > 0 ||
                      existing.ranges.size > 0 ||
                      existing.archiveContents.size > 0)) ||
                  (existingWork !== undefined &&
                    (existingWork.entries.size > 0 ||
                      existingWork.state !== "ready" ||
                      existingWork.through !== 0))
                )
                  return yield* ThreadImportRejected.make({
                    threadId: id,
                    reason: "target-not-empty",
                    message: "Destination canonical Thread is not empty",
                  });
                if (
                  !MutableRef.get(workState.ref).has(id) &&
                  MutableRef.get(workState.ref).size >= maxThreads
                )
                  return yield* storeError("import", "Destination Thread capacity exceeded");
                staged = emptyThread();
                stagedLedger =
                  ledgerTransfer === undefined ? undefined : yield* ledgerTransfer.startImport(id);
                stagedDelivery =
                  deliveryTransfer === undefined
                    ? undefined
                    : yield* deliveryTransfer.startImport(id);
                if (page.archive.workerSeal !== undefined && stagedLedger === undefined)
                  return yield* ThreadImportRejected.make({
                    threadId: id,
                    reason: "unsupported-obligations",
                    message: "A paired ledger is required to preserve the worker seal",
                  });
              }
              if (page.archive.admissions.length > 0 && stagedLedger === undefined)
                return yield* ThreadImportRejected.make({
                  threadId: id,
                  reason: "unsupported-obligations",
                  message: "A paired ledger is required for admission restore",
                });
              if (page.archive.deliveries.length > 0 && stagedDelivery === undefined)
                return yield* ThreadImportRejected.make({
                  threadId: id,
                  reason: "unsupported-obligations",
                  message: "A paired delivery store is required for delivery restore",
                });
              for (const batch of page.batches) {
                if (
                  !canonicalBatchFitsTransfer(
                    id,
                    batch.batch,
                    utf8ByteLength(batch.batchJson),
                    4 * Math.ceil(utf8ByteLength(id) / 3) + 36,
                  )
                )
                  return yield* ThreadImportRejected.make({
                    threadId: id,
                    reason: "unsupported-capacity",
                    message: "Canonical batch exceeds its bounded transfer representation",
                  });

                if (staged.batches.has(batch.batch.batchId))
                  return yield* invalidThreadArchive("Duplicate canonical batch identity", id);

                const records = page.records
                  .filter((entry) => entry.batchId === batch.batch.batchId)
                  .map((entry) =>
                    CanonicalRecordEnvelope.make({
                      ...entry,
                      offset: observationOffset(id, entry.sequence),
                    }),
                  );

                for (const entry of records)
                  if (staged.byId.has(entry.record.recordId))
                    return yield* invalidThreadArchive("Duplicate canonical record identity", id);
                const native = prepareIndexes(staged, records);

                const commitWork = yield* prepareWork(stagedWork, records).pipe(
                  Effect.provideService(Crypto.Crypto, crypto),
                );

                retainRecords(staged, records);
                native.commit();
                staged = {
                  ...staged,
                  ...native.indexes,
                  tailSequence: batch.lastSequence,
                };
                staged.batches.set(batch.batch.batchId, {
                  batchJson: batch.batchJson,
                  result: AppendResult.make({
                    firstSequence: batch.firstSequence,
                    lastSequence: batch.lastSequence,
                    replayed: false,
                  }),
                });
                staged.batchesByFirst.set(batch.firstSequence, batch.batch.batchId);
                stagedWork = commitWork();
              }
              if (stagedLedger !== undefined) yield* stagedLedger.stage(page);
              if (stagedDelivery !== undefined) yield* stagedDelivery.stage(page);
            }),
          );
          const result = yield* finishThreadImport(progress);

          if (staged === undefined) return yield* invalidThreadArchive("Missing transfer pages");
          const restoredThread = staged;
          const id = result.threadId;

          const fallback = {
            admission: () => Effect.as(Effect.void, undefined),
            runOwner: () => Effect.as(Effect.void, undefined),
            commands: () => Effect.succeed({ aborts: [], approvals: [], resolutions: [] }),
          };

          const reader: ThreadImportReader["Service"] = {
            ...exactRecordReader(id, restoredThread),
            ...(stagedLedger?.reader ?? fallback),
            delivery: stagedDelivery?.record ?? (() => Effect.as(Effect.void, undefined)),
          };

          return yield* Effect.gen(function* () {
            for (const runId of restoredThread.runRecords.keys()) {
              yield* verifyRunContinuations(
                yield* reader.runRecords(
                  yield* Schema.decodeEffect(RunId)(runId).pipe(Effect.orDie),
                ),
                { verifyContext: false },
              ).pipe(Effect.mapError((e) => invalidThreadArchive(e.message, id)));
              yield* validateClosedRun(
                id,
                yield* Schema.decodeEffect(RunId)(runId).pipe(Effect.orDie),
              );
            }
            let lastInputQueue = -1;

            for (let sequence = 1; sequence <= restoredThread.tailSequence; sequence++) {
              const entry = storedRecord(restoredThread, sequence);

              if (entry === undefined)
                return yield* invalidThreadArchive("Incomplete canonical staging", id);
              yield* verifyImportedReferences(entry);
              const payload = entry.record.payload;

              if (payload._tag === "UserInputRecorded" && payload.submissionId !== undefined) {
                const admission = yield* reader.admission(payload.submissionId);

                if (admission === undefined || admission.queueSequence <= lastInputQueue)
                  return yield* invalidThreadArchive(
                    "Canonical inputs violate immutable admission order",
                    id,
                  );
                lastInputQueue = admission.queueSequence;
              }

              if (
                payload._tag === "UserInputRecorded" &&
                payload.submissionId !== undefined &&
                entry.record.recordId !== submissionInputRecordId(payload.submissionId)
              )
                return yield* invalidThreadArchive("Canonical input identity is ambiguous", id);
              if (
                payload._tag === "SubmissionSettled" &&
                entry.record.recordId !== submissionSettlementRecordId(payload.submissionId)
              )
                return yield* invalidThreadArchive(
                  "Canonical settlement identity is ambiguous",
                  id,
                );
              if (
                payload._tag !== "WorkerInputCompleted" &&
                "submissionId" in payload &&
                payload.submissionId !== undefined &&
                (yield* reader.admission(payload.submissionId)) === undefined
              )
                return yield* invalidThreadArchive("Canonical submission has no admission", id);
            }
            if (stagedWork.entries.size > 0)
              return yield* ThreadImportRejected.make({
                threadId: id,
                reason: "unsupported-obligations",
                message: "Transfer contains unresolved work",
              });

            const commitLedger =
              stagedLedger === undefined
                ? undefined
                : yield* stagedLedger.prepareCommit(progress.manifest?.workerSeal);

            const commitDelivery =
              stagedDelivery === undefined ? undefined : yield* stagedDelivery.prepareCommit;

            const installed = restoredThread;

            yield* Effect.uninterruptible(
              Effect.sync(() => {
                commitLedger?.();
                commitDelivery?.();
                publishWork(id, stagedWork);
                MutableRef.get(state.ref).threads.set(id, installed);
              }).pipe(Effect.andThen(PubSub.publish(updates, undefined))),
            );

            return result;
          }).pipe(
            Effect.provide(ThreadImportReader.layer(reader)),
            Effect.provideService(ThreadDeliveryImportReader, {
              admission: (threadId, sid) =>
                threadId === id
                  ? reader.admission(sid)
                  : (ledgerTransfer?.admission(threadId, sid) ?? Effect.as(Effect.void, undefined)),
              record: (threadId, recordId) =>
                Effect.succeed(
                  threadId === id
                    ? restoredThread.byId.get(recordId)?.record
                    : MutableRef.get(state.ref).threads.get(threadId)?.byId.get(recordId)?.record,
                ),
            }),
          );
        }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
      ),
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
        storedRecord(thread, 1),
        thread.byId.get(workerOriginRecordId(request.threadId)),
        thread.byId.get(subagentLineageRecordId(request.threadId)),
      ].filter((entry) => entry !== undefined);

      return yield* ThreadIdentity.makeEffect({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        hasAdmissions:
          ledgerTransfer !== undefined &&
          (yield* ledgerTransfer.snapshot(request.threadId)).admissions > 0,
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

  // Range contents remain volatile RAM. Publishing an archive locator adds no durability.
  const verifyRange = Effect.fnUntraced(function* (
    thread: StoredThread,
    range: ThreadArchiveRange,
    source?: Map<BatchId, string>,
  ) {
    let sequence = range.firstSequence;
    let bytes = 0;
    let batches = 0;
    const captured = new Map<BatchId, string>();

    while (sequence <= range.lastSequence) {
      const id = thread.batchesByFirst.get(sequence);
      const batch = id === undefined ? undefined : thread.batches.get(id);

      const text =
        id === undefined ? undefined : source === undefined ? batch?.batchJson : source.get(id);

      if (
        id === undefined ||
        batch === undefined ||
        text === undefined ||
        batch.result.lastSequence > range.lastSequence
      )
        return yield* storeError("verify range", "Incomplete canonical batch range");
      bytes += new TextEncoder().encode(text).byteLength;
      batches++;
      if (
        bytes > MAX_ARCHIVE_RANGE_BYTES ||
        batches > MAX_ARCHIVE_RANGE_RECORDS ||
        text !== batch.batchJson
      )
        return yield* storeError(
          "verify range",
          "Range wire or bounds differ from canonical storage",
        );

      const wire = yield* Schema.decodeEffect(Schema.fromJsonString(ExportBatch))(text).pipe(
        Effect.mapError((cause) => storeError("verify range", "Invalid range wire", cause)),
      );

      if (
        wire.batchId !== id ||
        batch.result.firstSequence !== sequence ||
        batch.result.lastSequence !== sequence + wire.records.length - 1
      )
        return yield* storeError("verify range", "Canonical batch span or identity differs");

      for (const [index, record] of wire.records.entries()) {
        const expected = storedRecord(thread, sequence + index);

        if (
          expected === undefined ||
          expected.threadId !== range.threadId ||
          expected.sequence !== sequence + index ||
          expected.batchId !== id ||
          expected.record.recordId !== record.recordId
        )
          return yield* storeError("verify range", "Canonical record identity or order differs");

        const canonical = yield* Schema.encodeEffect(ReadRecord)(expected.record).pipe(
          Effect.mapError((cause) => storeError("verify range", "Invalid canonical record", cause)),
        );

        const archived = yield* Schema.encodeEffect(ExportRecord)(record).pipe(
          Effect.mapError((cause) => storeError("verify range", "Invalid archive record", cause)),
        );

        if (canonicalJson(canonical) !== canonicalJson(archived))
          return yield* storeError("verify range", "Archive and canonical record bytes differ");
      }
      captured.set(id, text);
      sequence = decodeCanonicalSequence(batch.result.lastSequence + 1);
    }
    if (
      bytes !== range.byteCount ||
      batches !== range.batchCount ||
      range.recordCount !== range.lastSequence - range.firstSequence + 1
    )
      return yield* storeError("verify range", "Range spans or counts differ");

    return captured;
  });

  const archiveTarget = Effect.fnUntraced(function* (request: ThreadArchiveRangeSeal) {
    const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);

    return thread;
  });

  const archives: ThreadArchiveStorage = {
    page: Effect.fnUntraced(function* (unvalidated) {
      const request = yield* validate(ThreadArchiveRangeRead, "range page", unvalidated);
      const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);
      let low = 0;
      let high = thread.rangeStarts.length;

      while (low < high) {
        const middle = Math.floor((low + high) / 2);
        const range = thread.ranges.get(thread.rangeStarts[middle]);

        if (range === undefined)
          return yield* storeError("page ranges", "Range locator has no exact range");
        if (range.lastSequence <= (request.afterSequence ?? 0)) low = middle + 1;
        else high = middle;
      }
      const ranges: Array<ThreadArchiveRange> = [];
      const limit = request.limit ?? 32;

      for (let at = low; at < Math.min(thread.rangeStarts.length, low + limit); at++) {
        const range = thread.ranges.get(thread.rangeStarts[at]);

        if (range === undefined)
          return yield* storeError("page ranges", "Range locator has no exact range");
        ranges.push(range);
      }

      const afterSequence =
        low + ranges.length < thread.rangeStarts.length ? ranges.at(-1)?.lastSequence : undefined;

      return { ranges, ...(afterSequence === undefined ? {} : { afterSequence }) };
    }),
    seal: (unvalidated) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(ThreadArchiveRangeSeal, "seal range", unvalidated);
          const thread = yield* archiveTarget(request);
          const previousStart = thread.rangeStarts.at(-1);

          const previous =
            previousStart === undefined ? undefined : thread.ranges.get(previousStart);

          if (previousStart !== undefined && previous === undefined)
            return yield* storeError("seal range", "Range locator has no exact range");
          const firstSequence = (previous?.lastSequence ?? 0) + 1;

          if (firstSequence > thread.tailSequence) return Option.none<ThreadArchiveRange>();
          let nextSequence = firstSequence;
          let byteCount = 0;
          let batchCount = 0;

          while (nextSequence <= thread.tailSequence) {
            const batchId = thread.batchesByFirst.get(nextSequence);
            const batch = batchId === undefined ? undefined : thread.batches.get(batchId);

            if (batch === undefined)
              return yield* storeError("seal range", "Missing exact canonical batch");
            const bytes = utf8ByteLength(batch.batchJson);

            if (
              batchCount > 0 &&
              (batch.result.lastSequence - firstSequence + 1 > MAX_ARCHIVE_RANGE_RECORDS ||
                byteCount + bytes > MAX_ARCHIVE_RANGE_BYTES)
            )
              break;
            byteCount += bytes;
            batchCount++;
            nextSequence = batch.result.lastSequence + 1;
          }

          const range = ThreadArchiveRange.make({
            format: "effect-agent/thread-range@2",
            threadId: request.threadId,
            firstSequence: decodeCanonicalSequence(firstSequence),
            lastSequence: decodeCanonicalSequence(nextSequence - 1),
            recordCount: nextSequence - firstSequence,
            batchCount,
            byteCount,
            state: "sealed",
          });

          yield* verifyRange(thread, range);
          thread.ranges.set(firstSequence, range);
          thread.rangeStarts.push(firstSequence);

          return Option.some(range);
        }),
      ),
    verify: Effect.fnUntraced(function* (unvalidated) {
      const request = yield* validate(ThreadArchiveRangeRequest, "verify range", unvalidated);
      const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);
      const range = thread.ranges.get(request.firstSequence);

      if (range === undefined) return yield* storeError("verify range", "Unknown sealed range");
      yield* verifyRange(
        thread,
        range,
        range.state === "archived" ? thread.archiveContents.get(range.firstSequence) : undefined,
      );
      if (range.state === "archived" && !thread.archiveContents.has(range.firstSequence))
        return yield* storeError("verify range", "Published archive contents are missing");

      return range;
    }),
    archive: (unvalidated) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(ThreadArchiveRangePublish, "archive range", unvalidated);
          const thread = yield* archiveTarget(request);
          const range = thread.ranges.get(request.firstSequence);

          if (range === undefined)
            return yield* storeError("archive range", "Unknown sealed range");
          if (range.state === "archived") {
            const contents = thread.archiveContents.get(range.firstSequence);

            if (contents === undefined)
              return yield* storeError("archive range", "Published archive contents are missing");
            yield* verifyRange(thread, range, contents);

            return range;
          }
          const contents = yield* verifyRange(thread, range);

          yield* verifyRange(thread, range, contents);

          const published = ThreadArchiveRange.make({
            ...range,
            state: "archived",
            locator: `memory:${encodeURIComponent(request.threadId)}:${range.firstSequence}`,
          });

          thread.archiveContents.set(range.firstSequence, contents);
          thread.ranges.set(range.firstSequence, published);

          return published;
        }),
      ),
  };

  const readWorkerCapacity: NonNullable<ThreadStore["Service"]["readWorkerCapacity"]> =
    Effect.fnUntraced(function* (unvalidated) {
      const request = yield* validate(ThreadWorkerCapacityRequest, "worker capacity", unvalidated);
      const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);

      if (request.expectedTailSequence !== thread.tailSequence)
        return yield* storeError("worker capacity", "Canonical tail changed");

      const index = yield* readyIndex(request.threadId, thread.tailSequence).pipe(
        Effect.mapError((e) => storeError("worker capacity", e.message)),
      );

      const worker = index.workers.get(request.workerThreadId);

      return ThreadWorkerCapacity.make({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        activeWorkers: Math.min(index.workers.size, request.activeLimit + 1),
        workerActive: worker !== undefined,
        pendingInputs: Math.min(
          request.update ? (worker?.update ?? 0) : (worker?.ordinary ?? 0),
          request.pendingLimit + 1,
        ),
      });
    });

  const verification = {
    verify: (unvalidated: ThreadVerificationRequest) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(ThreadVerificationRequest, "verify Thread", unvalidated);
          const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);
          const id = request.threadId;

          const reader = importReader(id, thread);

          const submissions = () => ledgerTransfer?.submissions(id) ?? Stream.empty;

          const runs = Stream.fromIterable(thread.runRecords.keys()).pipe(
            Stream.map((runId) => Schema.decodeSync(RunId)(runId)),
          );

          return yield* verifyThreadInvariants({
            threadId: id,
            runs,
            executions: Stream.fromIterable(thread.executions.values()),
            submissions: submissions(),
            pages: streamExport({ threadId: id }).pipe(
              Stream.provideService(ThreadExportSource, { export: verificationPage }),
              Stream.mapError((cause) =>
                Schema.is(ThreadExportRejected)(cause)
                  ? storeError("verify Thread", "Internal verification snapshot changed")
                  : cause,
              ),
            ),
            ...(request.requireAllSettled === undefined
              ? {}
              : { requireAllSettled: request.requireAllSettled }),
          }).pipe(
            Effect.provide(ThreadImportReader.layer(reader)),
            Effect.provideService(Crypto.Crypto, crypto),
          );
        }),
      ),
  };

  const threadStore = ThreadStore.of({
    work,
    archives,
    verification,
    readWorkerCapacity,
    readIdentity,
    countPeerMessages,
    materialize: (request) => withMutation(materialize(request)),
    append,
    readExecution: (request) =>
      withMutation(
        Effect.gen(function* () {
          const thread = (yield* Ref.get(state)).threads.get(request.threadId);

          if (thread === undefined)
            return yield* ThreadNotMaterialized.make({ threadId: request.threadId });

          return Option.fromUndefinedOr(thread.executions.get(request.runId));
        }),
      ),
    read,
    readPrompt,
    observe,
    export: (request) =>
      checkExportSource(request.threadId).pipe(Effect.andThen(withMutation(exportThread(request)))),
    inspectTail,
  });

  return Context.make(ThreadStore, threadStore).pipe(
    Context.add(StorageOwner, owner),
    Context.add(ThreadImport, { import: importThread }),
    Context.add(MemoryThreadStoreKernel, {
      owner,
      withMutation,
      checkThreadCapacity: (threadId) =>
        Effect.suspend(() =>
          !MutableRef.get(workState.ref).has(threadId) &&
          MutableRef.get(workState.ref).size >= maxThreads
            ? Effect.fail(storeError("Thread capacity", "Retained Thread quota exceeded"))
            : Effect.void,
        ),
      initializeWork: (threadId) => {
        if (
          !MutableRef.get(state.ref).threads.has(threadId) &&
          !MutableRef.get(workState.ref).has(threadId)
        )
          publishWork(threadId, emptyWorkIndex());
      },
      registerWorkOwner: (kind, owner) =>
        withMutation(
          Effect.gen(function* () {
            if (workOwners.has(kind))
              return yield* Effect.die(
                storeError("initialize", `MemoryThreadStore already has a ${kind} work owner`),
              );
            workOwners.set(kind, owner);
          }),
        ),
      registerDeliveryWriter: (writer) =>
        withMutation(
          Effect.gen(function* () {
            if (deliveryWriter !== undefined)
              return yield* Effect.die(
                storeError("initialize", "MemoryThreadStore already has a delivery writer"),
              );
            deliveryWriter = writer;
          }),
        ),
      registerLedgerTransfer: (transfer) =>
        withMutation(
          Effect.gen(function* () {
            if (ledgerTransfer !== undefined)
              return yield* Effect.die(
                storeError("initialize", "MemoryThreadStore already has a paired SubmissionLedger"),
              );
            ledgerTransfer = transfer;
          }),
        ),
      registerDeliveryTransfer: (transfer) =>
        withMutation(
          Effect.gen(function* () {
            if (deliveryTransfer !== undefined)
              return yield* Effect.die(
                storeError("initialize", "MemoryThreadStore already has a paired delivery store"),
              );
            deliveryTransfer = transfer;
          }),
        ),
      prepareAppend,
      appendPrepared,
      record: (threadId, recordId) =>
        Ref.get(state).pipe(
          Effect.map((current) => current.threads.get(threadId)?.byId.get(recordId)?.record),
        ),
      toolCallResults: (threadId, runId, toolCallId) =>
        Ref.get(state).pipe(
          Effect.map((current) =>
            (
              current.threads
                .get(threadId)
                ?.operationRecords.get(JSON.stringify([runId, toolCallId, "settled"])) ?? []
            )
              .slice(0, 2)
              .map((entry) => entry.record),
          ),
        ),
      tail: (threadId) => inspectTail(ThreadTailRequest.make({ threadId })),
    }),
  );
});

/**
 * In-memory canonical Thread persistence. Durable accepted work is served by the separate
 * SubmissionLedger port; this Layer provides ThreadStore, ThreadReader, and ThreadImport.
 * Importing admissions requires a MemorySubmissionLedger paired with this store.
 * Publishing canonical messages requires a paired MemoryMessageDeliveryStore.
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
