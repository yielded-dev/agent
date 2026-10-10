import {
  digestCanonicalBatch,
  digestJson,
  EMPTY_TAIL_DIGEST,
  utf8ByteLength,
} from "@yielded/agent/digest";
import { ThreadId, RunId, SubmissionId as importSubmissionId } from "@yielded/agent/identifiers";
import type { IdempotencyKey } from "@yielded/agent/receipt";
import { ExportBatch, ExportedRecord } from "@yielded/agent/record-format";
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
  MAX_RUN_EVIDENCE_RECORDS,
  MAX_RUN_EVIDENCE_BYTES,
  MAX_RUN_TERMINAL_BYTES,
  RUN_TERMINAL_RESERVE_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_RECORDS,
  MAX_RUN_RECOVERY_SUFFIX_BYTES,
  MAX_RUN_CONTINUATION_BYTES,
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
  type ThreadSettlementPredecessorRequest,
  invalidThreadArchive,
  ThreadImportReader,
  ThreadDeliveryImportReader,
  ThreadImport,
  ThreadImportRejected,
} from "@yielded/agent/thread-import";
import { verifyThreadInvariants } from "@yielded/agent/thread-invariants";
import {
  type ThreadCheckpoint,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadExportRecord,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadPromptRead,
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
  PROMPT_EVIDENCE_TAGS,
  ThreadVerificationRequest,
  ThreadWorkerCapacityRequest,
  ThreadWorkerCapacity,
  streamExport,
  ThreadExportSource,
  canonicalBatchFitsTransfer,
  transferPageFits,
} from "@yielded/agent/thread-store";
import { exportThreadPage, ThreadExporterReader } from "@yielded/agent/thread-transfer";
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
  type MemoryLedgerImport,
  type MemoryDeliveryTransfer,
  type MemoryDeliveryImport,
  type MemoryWorkOwner,
  type PreparedMemoryAppend,
} from "./internal/MemoryThreadStoreKernel.ts";
import { boundedWorkSelection } from "./internal/WorkSelection.ts";

const MAX_THREADS = 256;
const RECORD_CHUNK_SIZE = 1_024;
const READ_PAGE_SIZE = 8;
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

interface SettlementInterval {
  maximum: number;
  left?: SettlementInterval;
  right?: SettlementInterval;
}

// Native metadata over the canonical address space. Closed historical prefixes prune in one
// lookup; an open input keeps Infinity until its own canonical settlement is published.
const updateSettlementInterval = (
  root: SettlementInterval,
  sequence: number,
  settlement: number,
) => {
  let node = root;
  let lower = 0;
  let width = 2 ** 53;
  const path = [node];

  while (width > 1) {
    width /= 2;
    if (sequence - 1 < lower + width) node = node.left ??= { maximum: 0 };
    else {
      lower += width;
      node = node.right ??= { maximum: 0 };
    }
    path.push(node);
  }
  node.maximum = settlement;
  for (let at = path.length - 2; at >= 0; at--) {
    const parent = path[at];

    parent.maximum = Math.max(parent.left?.maximum ?? 0, parent.right?.maximum ?? 0);
  }
};

function* earlierSettlementInputs(
  root: SettlementInterval,
  before: number,
  settledThrough: number,
) {
  const stack: Array<readonly [SettlementInterval, number, number]> = [[root, 0, 2 ** 53]];

  while (stack.length > 0) {
    const next = stack.pop();

    if (next === undefined) return;
    const [node, lower, width] = next;

    if (lower + 1 >= before || node.maximum <= settledThrough) continue;
    if (width === 1) yield lower + 1;
    else {
      if (node.right !== undefined) stack.push([node.right, lower + width / 2, width / 2]);
      if (node.left !== undefined) stack.push([node.left, lower, width / 2]);
    }
  }
}

interface StoredThread {
  /** Append-only owner indexes; readers must constrain them to their captured canonical tail. */
  readonly runRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly continuations: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly operationRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly handoffs: Array<CanonicalRecordEnvelope>;
  readonly agentUpdates: Array<CanonicalRecordEnvelope>;
  readonly deliveryPredecessors: Array<CanonicalRecordEnvelope>;
  readonly toolDeclarations: Map<string, CanonicalRecordEnvelope | null>;
  readonly contexts: Array<CanonicalRecordEnvelope>;
  readonly promptEvidence: Array<CanonicalRecordEnvelope>;
  readonly modelCompletions: Array<CanonicalRecordEnvelope>;
  readonly applicationInputs: Array<CanonicalRecordEnvelope>;
  readonly settlementIntervals: SettlementInterval;
  readonly durableOwners: Array<CanonicalRecordEnvelope>;
  readonly windowBoundaries: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly peerCount: number;
  readonly workerRecords: Map<string, Array<CanonicalRecordEnvelope>>;
  readonly byId: Map<string, CanonicalRecordEnvelope>;
  readonly runInputs: Map<string, CanonicalRecordEnvelope | null>;
  readonly producerEpoch: ProducerEpoch;
  readonly tailSequence: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly ranges: Map<number, ThreadArchiveRange>;
  readonly rangeStarts: Array<number>;
  readonly archiveContents: Map<number, Map<BatchId, string>>;
  readonly openRange: { current?: ThreadArchiveRange };
  readonly chunks: Map<
    number,
    { readonly records: Array<CanonicalRecordEnvelope>; readonly bytes: Array<number> }
  >;
  readonly batches: Map<BatchId, StoredBatch>;
  readonly batchesByFirst: Map<number, BatchId>;
  readonly tailDigests: Map<CanonicalSequence, Digest>;
  readonly checkpoints: Map<CanonicalSequence, ThreadCheckpoint>;
}

const storedRecord = (thread: StoredThread, sequence: number) =>
  thread.chunks.get(Math.floor((sequence - 1) / RECORD_CHUNK_SIZE))?.records[
    (sequence - 1) % RECORD_CHUNK_SIZE
  ];

const storedBytes = (thread: StoredThread, sequence: number) =>
  thread.chunks.get(Math.floor((sequence - 1) / RECORD_CHUNK_SIZE))?.bytes[
    (sequence - 1) % RECORD_CHUNK_SIZE
  ];

const storedPage = (thread: StoredThread, after: number, limit: number) => {
  const page: Array<CanonicalRecordEnvelope> = [];
  let bytes = 0;

  for (
    let sequence = after + 1;
    sequence <= Math.min(thread.tailSequence, after + limit);
    sequence++
  ) {
    const record = storedRecord(thread, sequence);

    if (record === undefined) throw new Error("Incomplete canonical chunk table");
    const size = storedBytes(thread, sequence) ?? 0;

    if (bytes + size > 32 * 1024 * 1024) break;
    bytes += size;
    page.push(record);
  }

  return page;
};

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
  | "contexts"
  | "promptEvidence"
  | "modelCompletions"
  | "applicationInputs"
  | "settlementIntervals"
  | "durableOwners"
  | "windowBoundaries"
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
  toolDeclarations: new Map(),
  contexts: [],
  promptEvidence: [],
  modelCompletions: [],
  applicationInputs: [],
  settlementIntervals: { maximum: 0 },
  durableOwners: [],
  windowBoundaries: new Map(),
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
  readonly foreign: { child: number; worker: number; delivery: number; peer: number };
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
  messages: new Map(),
  foreign: { child: 0, worker: 0, delivery: 0, peer: 0 },
  workers: new Map(),
  liveInputs: [],
  inputsByWorker: new Map(),
});

const changeWorkCounts = (index: WorkIndex, entry: CanonicalWorkEntry, change: 1 | -1) => {
  const owner = entry.owner;

  if (
    owner._tag === "Child" ||
    (owner._tag === "Handoff" &&
      (owner.kind === "reservation" || owner.kind === "child-accounting"))
  )
    index.foreign.child += change;
  if (
    ["WorkerInput", "WorkerEffects", "Report"].includes(owner._tag) ||
    (owner._tag === "Handoff" && owner.kind === "worker-stop")
  )
    index.foreign.worker += change;
  if (owner._tag === "Handoff" && ["peer", "update", "report"].includes(owner.kind))
    index.foreign.delivery += change;
  if (owner._tag === "Handoff" && owner.kind === "peer") index.foreign.peer += change;
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
  retained: (threadId: ThreadId, messageId: IdempotencyKey) => boolean,
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

      if (old !== undefined) changeWorkCounts(previous, old, -1);

      if (old?.owner._tag === "Handoff" && old.owner.messageId !== undefined) {
        const ids = messages.get(old.owner.messageId);

        ids?.delete(id);
        if (ids?.size === 0) messages.delete(old.owner.messageId);
      }
      if (entry === undefined) entries.delete(id);
      else {
        entries.set(id, entry);
        changeWorkCounts(previous, entry, 1);
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
) => {
  let peerCount = previous.peerCount;
  // Latest execution entries are replaced in this private map; owner histories only append.
  // Rebuild staging is private: reuse it only in the synchronous cursor publication step.
  const workerRecords = new Map<string, Array<CanonicalRecordEnvelope>>();
  const workerAppends = new Map<string, Array<CanonicalRecordEnvelope>>();
  const byId = new Map<string, CanonicalRecordEnvelope>();
  const runInputs = new Map<string, CanonicalRecordEnvelope | null>();
  const runRecords = new Map<string, Array<CanonicalRecordEnvelope>>();
  const continuations = new Map<string, Array<CanonicalRecordEnvelope>>();
  const handoffs: Array<CanonicalRecordEnvelope> = [];
  const agentUpdates: Array<CanonicalRecordEnvelope> = [];

  const deliveryPredecessors: Array<CanonicalRecordEnvelope> = [];

  const nativeAdds = emptyIndexes();
  const operationAppends = new Map<string, Array<CanonicalRecordEnvelope>>();

  for (const entry of records) {
    byId.set(entry.record.recordId, entry);
    const payload = entry.record.payload;

    if (payload._tag === "RunContextRecorded") nativeAdds.contexts.push(entry);
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
      if (payload._tag === "ToolCallSettled")
        appendToIndex(
          operationAppends,
          JSON.stringify([payload.runId, payload.toolCallId, "settled"]),
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
    indexes: { ...previous, peerCount },
    commit: () => {
      commitIndex(previous.runRecords, runRecords);
      commitIndex(previous.continuations, continuations);
      commitIndex(previous.operationRecords, operationAppends);
      for (const [id, entry] of nativeAdds.toolDeclarations)
        previous.toolDeclarations.set(id, entry);
      for (const entry of nativeAdds.contexts) {
        const payload = entry.record.payload;
        const old = previous.contexts.at(-1)?.record.payload;

        if (
          payload._tag === "RunContextRecorded" &&
          (old?._tag !== "RunContextRecorded" || payload.historyThrough >= old.historyThrough)
        )
          previous.contexts.push(entry);
      }
      for (const key of ["promptEvidence", "modelCompletions", "applicationInputs"] as const)
        for (const entry of nativeAdds[key]) previous[key].push(entry);
      commitIndex(previous.windowBoundaries, nativeAdds.windowBoundaries);
      if (previous.durableOwners.length === 0 && nativeAdds.durableOwners[0] !== undefined)
        previous.durableOwners.push(nativeAdds.durableOwners[0]);
      for (const [id, entry] of byId) previous.byId.set(id, entry);
      for (const entry of records) {
        const payload = entry.record.payload;

        if (
          (payload._tag !== "UserInputRecorded" && payload._tag !== "SubmissionSettled") ||
          payload.submissionId === undefined
        )
          continue;
        const input = previous.byId.get(submissionInputRecordId(payload.submissionId));

        if (input !== undefined)
          updateSettlementInterval(
            previous.settlementIntervals,
            input.sequence,
            previous.byId.get(submissionSettlementRecordId(payload.submissionId))?.sequence ??
              Infinity,
          );
      }
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

const decodeProducerEpoch = Schema.decodeSync(ProducerEpoch);
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
  let deliveryTransfer: MemoryDeliveryTransfer | undefined;

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

    for (const id of ids) {
      const entry = entries.get(id);

      if (entry !== undefined) changeWorkCounts(index, entry, -1);
      entries.delete(id);
    }
    messages.delete(messageId);
    publishWork(threadId, { ...index, entryCount: entries.size });
  };

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

          const commitWork = yield* prepareWork(index, records, retainedDelivery).pipe(
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
              messages: rebuilt.messages,
              foreign: rebuilt.foreign,
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
            toolDeclarations: new Map(),
            contexts: [],
            promptEvidence: [],
            modelCompletions: [],
            applicationInputs: [],
            settlementIntervals: { maximum: 0 },
            durableOwners: [],
            windowBoundaries: new Map(),
            ranges: new Map(),
            rangeStarts: [],
            archiveContents: new Map(),
            openRange: {},
            chunks: new Map(),
            batchesByFirst: new Map(),
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

  const retainRange = (
    thread: StoredThread,
    threadId: ThreadId,
    batch: StoredBatch,
    previousTailDigest: Digest,
  ) => {
    const bytes = new TextEncoder().encode(batch.batchJson).byteLength;
    const count = batch.result.lastSequence - batch.result.firstSequence + 1;
    let open = thread.openRange.current;

    if (
      open !== undefined &&
      (open.recordCount + count > MAX_ARCHIVE_RANGE_RECORDS ||
        open.byteCount + bytes > MAX_ARCHIVE_RANGE_BYTES)
    ) {
      thread.ranges.set(open.firstSequence, open);
      thread.rangeStarts.push(open.firstSequence);
      open = undefined;
    }
    thread.openRange.current = ThreadArchiveRange.make({
      format: "effect-agent/thread-range@1",
      threadId,
      firstSequence: open?.firstSequence ?? batch.result.firstSequence,
      lastSequence: batch.result.lastSequence,
      previousTailDigest: open?.previousTailDigest ?? previousTailDigest,
      tailDigest: batch.digest,
      recordCount: (open?.recordCount ?? 0) + count,
      batchCount: (open?.batchCount ?? 0) + 1,
      byteCount: (open?.byteCount ?? 0) + bytes,
      state: "sealed",
    });
  };

  const prepareAppend = Effect.fnUntraced(function* (unvalidated: FencedAppendRequest) {
    const codec = Schema.fromJsonString(FencedAppendRequest);

    const request = yield* Schema.encodeUnknownEffect(codec)(unvalidated).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(codec)),
      Effect.mapError((error) => storeError("append", "Invalid append request", error)),
    );

    const batchJson = yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(
      request.batch,
    ).pipe(
      Effect.mapError((cause) => storeError("append", "Unable to encode canonical batch", cause)),
    );

    const batchBytes = new TextEncoder().encode(batchJson).byteLength;

    if (batchBytes > 16 * 1024 * 1024)
      return yield* storeError("append", "Canonical batch exceeds 16 MiB");

    const offsetBytes = 4 * Math.ceil(utf8ByteLength(request.threadId) / 3) + 36;

    if (!canonicalBatchFitsTransfer(request.threadId, request.batch, batchBytes, offsetBytes))
      return yield* storeError(
        "append",
        "Canonical batch exceeds its bounded transfer representation",
      );

    const digest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError((error) => storeError("append", error.message, error)),
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
        request.expectedTailSequence,
      ).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(ProgressAppendReader, {
          previous: (runId) =>
            Effect.gen(function* () {
              const latest = thread.continuations.get(runId)?.at(-1);

              if (latest === undefined) return undefined;
              if (latest.record.payload._tag !== "RunContinuation")
                return yield* storeError("append", "Invalid newest Run continuation");

              return latest.record.payload;
            }),
          initial: (next) => Effect.succeed(thread.runRecords.get(next.runId) ?? []),
        }),
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
          firstSequence: decodeCanonicalSequence(thread.tailSequence + 1),
          lastSequence,
          tailDigest: digest,
          replayed: false,
        });

        const indexes = prepareIndexes(thread, records);

        const next: StoredThread = {
          ...thread,
          ...indexes.indexes,
          tailSequence: lastSequence,
          tailDigest: digest,
        };

        // Every rejection and asynchronous validation precedes this synchronous publication.
        thread.batches.set(request.batch.batchId, { digest, result, batchJson });
        retainRange(thread, request.threadId, { digest, result, batchJson }, thread.tailDigest);
        thread.batchesByFirst.set(result.firstSequence, request.batch.batchId);
        thread.tailDigests.set(lastSequence, digest);
        retainRecords(thread, records);
        indexes.commit();
        if (preparedWork !== undefined) publishWork(request.threadId, preparedWork());
        current.threads.set(request.threadId, next);

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
        Effect.flatMap((prepared) => withMutation(appendPrepared(prepared))),
      ),
  );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(function* (request) {
      yield* validate(ThreadPeerCountRequest, "countPeerMessages", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      const index = yield* readyIndex(request.threadId, thread.tailSequence).pipe(
        Effect.mapError((cause) => storeError("countPeerMessages", cause.message)),
      );

      return Math.min(
        index.foreign.peer +
          (deliveryTransfer?.pendingPeerCount(request.threadId, request.limit) ?? 0),
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
      Effect.map((thread) => {
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
                const page = storedPage(
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
          (selection.expectedTailSequence !== thread.tailSequence ||
            selection.expectedTailDigest !== thread.tailDigest)
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
          case "RunContinuation":
            sources = [latest(thread.continuations.get(selection.runId) ?? [])];
            break;
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
          case "LatestRunContext":
            sources = [latest(thread.contexts)];
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
              thread.workerRecords.get("execution:RunStarted") ?? [],
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
            entry.record instanceof ExportedRecord ? entry.record.wire : entry.record,
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

  const emptyThread = (producerEpoch: ProducerEpoch): StoredThread => ({
    ...emptyIndexes(),
    producerEpoch,
    tailSequence: ZERO_CANONICAL_SEQUENCE,
    tailDigest: EMPTY_TAIL_DIGEST,
    ranges: new Map(),
    rangeStarts: [],
    archiveContents: new Map(),
    openRange: {},
    chunks: new Map(),
    batches: new Map(),
    batchesByFirst: new Map(),
    tailDigests: new Map([[ZERO_CANONICAL_SEQUENCE, EMPTY_TAIL_DIGEST]]),
    checkpoints: new Map(),
  });

  const exportReader = ThreadExporterReader.of({
    snapshot: Effect.fnUntraced(function* (threadId: ThreadId) {
      const thread = MutableRef.get(state.ref).threads.get(threadId);

      const facts =
        ledgerTransfer === undefined
          ? { revision: 0, admissions: 0, aborts: 0, approvals: 0, resolutions: 0 }
          : yield* ledgerTransfer.snapshot(threadId);

      const delivered = deliveryTransfer?.snapshot(threadId) ?? { revision: 0, deliveries: 0 };

      if (thread === undefined && facts.admissions === 0 && delivered.deliveries === 0)
        return yield* ThreadNotMaterialized.make({ threadId });

      const index = yield* readyIndex(threadId, thread?.tailSequence ?? 0).pipe(
        Effect.mapError((e) => storeError("export", e.message)),
      );

      const foreign = new Set(facts.externalObligations ?? []);

      if (index.foreign.child > 0) foreign.add("child");
      if (index.foreign.worker > 0) foreign.add("worker");
      if (index.foreign.delivery > 0) foreign.add("message-delivery");
      const revision = facts.revision + delivered.revision;

      if (!Number.isSafeInteger(revision))
        return yield* storeError("export", "Transfer revision is exhausted");

      return {
        format: CURRENT_RECORD_FORMAT,
        threadId,
        tailSequence: thread?.tailSequence ?? ZERO_CANONICAL_SEQUENCE,
        tailDigest: thread?.tailDigest ?? EMPTY_TAIL_DIGEST,
        snapshot: {
          revision,
          admissions: facts.admissions,
          aborts: facts.aborts,
          approvals: facts.approvals,
          resolutions: facts.resolutions,
          deliveries: delivered.deliveries,
        },
        ...(facts.workerSeal === undefined ? {} : { workerSeal: facts.workerSeal }),
        ...(foreign.size === 0 ? {} : { externalObligations: [...foreign] }),
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
    facts: (threadId, section, after) =>
      section === "deliveries"
        ? (deliveryTransfer?.facts(threadId, after) ??
          Effect.succeed({
            facts: {
              admissions: [],
              commands: { aborts: [], approvals: [], resolutions: [] },
              deliveries: [],
            },
            after: after ?? "",
          }))
        : (ledgerTransfer?.facts(threadId, section, after) ??
          Effect.succeed({
            facts: {
              admissions: [],
              commands: { aborts: [], approvals: [], resolutions: [] },
              deliveries: [],
            },
            after: after ?? "",
          })),
  });

  const exportThread: ThreadStore["Service"]["export"] = (request) =>
    exportThreadPage(request).pipe(
      Effect.provideService(ThreadExporterReader, exportReader),
      Effect.provideService(Crypto.Crypto, crypto),
    );

  const exactRunRecords = (thread: StoredThread, runId: RunId) =>
    Effect.gen(function* () {
      const records: Array<CanonicalRecordEnvelope> = [];
      let bytes = 0;
      const facts = thread.runRecords.get(runId) ?? [];
      const progress = thread.continuations.get(runId) ?? [];

      if (
        facts.length >
          MAX_RUN_EVIDENCE_RECORDS +
            RUN_TERMINAL_RESERVE_RECORDS +
            MAX_RUN_RECOVERY_SUFFIX_RECORDS ||
        progress.length > MAX_RUN_EVIDENCE_RECORDS + RUN_TERMINAL_RESERVE_RECORDS
      )
        return yield* storeError("Run evidence", "Run record bounds exceeded");
      for (const entry of facts) {
        bytes += canonicalRecordBytes(entry.record);
        if (bytes > MAX_RUN_EVIDENCE_BYTES + MAX_RUN_TERMINAL_BYTES + MAX_RUN_RECOVERY_SUFFIX_BYTES)
          return yield* storeError("Run evidence", "Run byte bounds exceeded");
        records.push(entry);
      }
      for (const entry of progress) {
        if (canonicalRecordBytes(entry.record) > MAX_RUN_CONTINUATION_BYTES)
          return yield* storeError("Run evidence", "Continuation byte bound exceeded");
        records.push(entry);
      }

      return records.sort((a, b) => a.sequence - b.sequence);
    });

  const exactRecordReader = (
    threadId: ThreadId,
    thread: StoredThread,
  ): Pick<ThreadImportReader["Service"], "read" | "record" | "runRecords" | "hasAgent"> => ({
    read: (input) =>
      Stream.unwrap(
        validate(ThreadReadRequest, "read snapshot", input).pipe(
          Effect.map((request) => readFromThread(threadId, thread, request)),
        ),
      ),
    record: (id) => Effect.succeed(thread.byId.get(id)),
    runRecords: (id) => exactRunRecords(thread, id),
    hasAgent: (id) =>
      Effect.succeed(
        (thread.workerRecords.get("worker") ?? []).some(
          ({ record }) => record.payload._tag === "ThreadCreated" && record.payload.agentId === id,
        ),
      ),
  });

  const settlementInputs = (thread: StoredThread, request: ThreadSettlementPredecessorRequest) =>
    Stream.fromIterable(
      earlierSettlementInputs(
        thread.settlementIntervals,
        request.inputSequence,
        request.settlementSequence,
      ),
    ).pipe(
      Stream.mapEffect((sequence) =>
        Effect.gen(function* () {
          const input = storedRecord(thread, sequence)?.record.payload;

          if (input?._tag !== "UserInputRecorded" || input.submissionId === undefined)
            return yield* storeError(
              "settlement predecessors",
              "Indexed input evidence is missing",
            );

          return input.submissionId;
        }),
      ),
    );

  const importThread: ThreadImport["Service"]["import"] = (pages) =>
    Effect.scoped(
      Effect.gen(function* () {
        const captured = yield* captureImportSource(pages);

        return yield* withMutation(
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
                        existing.checkpoints.size > 0 ||
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
                  staged = emptyThread(decodeProducerEpoch(1));
                  stagedLedger =
                    ledgerTransfer === undefined
                      ? undefined
                      : yield* ledgerTransfer.startImport(id);
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

                  const commitWork = yield* prepareWork(
                    stagedWork,
                    records,
                    (_, messageId) => stagedDelivery?.has(messageId) ?? false,
                  ).pipe(Effect.provideService(Crypto.Crypto, crypto));

                  retainRecords(staged, records);
                  native.commit();
                  staged = {
                    ...staged,
                    ...native.indexes,
                    tailSequence: batch.lastSequence,
                    tailDigest: batch.tailDigest,
                  };
                  staged.batches.set(batch.batch.batchId, {
                    batchJson: batch.batchJson,
                    digest: batch.tailDigest,
                    result: AppendResult.make({
                      firstSequence: batch.firstSequence,
                      lastSequence: batch.lastSequence,
                      tailDigest: batch.tailDigest,
                      replayed: false,
                    }),
                  });
                  retainRange(
                    staged,
                    id,
                    staged.batches.get(batch.batch.batchId)!,
                    batch.previousTailDigest,
                  );
                  staged.batchesByFirst.set(batch.firstSequence, batch.batch.batchId);
                  staged.tailDigests.set(batch.lastSequence, batch.tailDigest);
                  stagedWork = commitWork();
                }
                if (stagedLedger !== undefined) yield* stagedLedger.stage(page);
                if (stagedDelivery !== undefined) yield* stagedDelivery.stage(page);
                for (const delivery of page.archive.deliveries) {
                  for (const workId of stagedWork.messages.get(delivery.key.messageId) ?? []) {
                    const entry = stagedWork.entries.get(workId);

                    if (entry !== undefined) changeWorkCounts(stagedWork, entry, -1);
                    stagedWork.entries.delete(workId);
                  }
                  stagedWork.messages.delete(delivery.key.messageId);
                  stagedWork = { ...stagedWork, entryCount: stagedWork.entries.size };
                }
              }),
            );
            const result = yield* finishThreadImport(progress);

            if (staged === undefined) return yield* invalidThreadArchive("Missing transfer pages");
            const restoredThread = staged;
            const id = result.threadId;

            const fallback = {
              admission: () => Effect.succeed(undefined),
              runOwner: () => Effect.succeed(undefined),
              commands: () => Effect.succeed({ aborts: [], approvals: [], resolutions: [] }),
            };

            const reader: ThreadImportReader["Service"] = {
              ...exactRecordReader(id, restoredThread),
              ...(stagedLedger?.reader ?? fallback),
              delivery: stagedDelivery?.record ?? (() => Effect.succeed(undefined)),
              settlementPredecessors: (request) =>
                settlementInputs(restoredThread, request).pipe(
                  Stream.mapEffect((sid) =>
                    reader.admission(sid).pipe(
                      Effect.filterOrFail(
                        (admission) => admission !== undefined,
                        () =>
                          storeError(
                            "settlement predecessors",
                            "Indexed input admission is missing",
                          ),
                      ),
                    ),
                  ),
                  Stream.filter((admission) => admission.queueSequence < request.queueSequence),
                  Stream.map(({ submissionId }) => ({ submissionId })),
                ),
            };

            return yield* Effect.gen(function* () {
              for (const runId of restoredThread.runRecords.keys())
                yield* verifyRunContinuations(
                  yield* reader.runRecords(Schema.decodeSync(RunId)(runId)),
                  { verifyContext: false },
                ).pipe(Effect.mapError((e) => invalidThreadArchive(e.message, id)));
              for (const runId of restoredThread.continuations.keys())
                if (!restoredThread.runRecords.has(runId))
                  yield* verifyRunContinuations(
                    yield* reader.runRecords(Schema.decodeSync(RunId)(runId)),
                    { verifyContext: false },
                  ).pipe(Effect.mapError((e) => invalidThreadArchive(e.message, id)));
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
              if (Object.values(stagedWork.foreign).some((count) => count > 0))
                return yield* ThreadImportRejected.make({
                  threadId: id,
                  reason: "unsupported-obligations",
                  message: "Live foreign ownership cannot be restored",
                });

              const nextEpoch = Math.max(
                progress.producerEpoch,
                (existing?.producerEpoch ?? 0) + 1,
              );

              if (!Number.isSafeInteger(nextEpoch))
                return yield* invalidThreadArchive("Producer epoch cannot be fenced safely", id);
              const producerEpoch = decodeProducerEpoch(nextEpoch);

              const commitLedger =
                stagedLedger === undefined
                  ? undefined
                  : yield* stagedLedger.prepareCommit(producerEpoch, progress.manifest?.workerSeal);

              const commitDelivery =
                stagedDelivery === undefined ? undefined : yield* stagedDelivery.prepareCommit();

              const installed = { ...restoredThread, producerEpoch };

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
                    : (ledgerTransfer?.admission(threadId, sid) ?? Effect.succeed(undefined)),
                record: (threadId, recordId) =>
                  Effect.succeed(
                    threadId === id
                      ? restoredThread.byId.get(recordId)?.record
                      : MutableRef.get(state.ref).threads.get(threadId)?.byId.get(recordId)?.record,
                  ),
              }),
            );
          }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
        );
      }),
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
        storedRecord(thread, 1),
        thread.byId.get(workerOriginRecordId(request.threadId)),
        thread.byId.get(subagentLineageRecordId(request.threadId)),
      ].filter((entry) => entry !== undefined);

      return yield* ThreadIdentity.makeEffect({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        producerEpoch: thread.producerEpoch,
        admissions:
          ledgerTransfer === undefined
            ? 0
            : (yield* ledgerTransfer.snapshot(request.threadId)).admissions,
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
        const checkpoints = thread.checkpoints;

        checkpoints.set(checkpoint.throughSequence, checkpoint);
        // Checkpoints are disposable cache entries; history never fails when this cache fills.
        if (checkpoints.size > MAX_CHECKPOINTS_PER_THREAD) {
          let oldest = checkpoint.throughSequence;

          for (const sequence of checkpoints.keys()) if (sequence < oldest) oldest = sequence;
          checkpoints.delete(oldest);
        }

        return [{ _tag: "success" }, current];
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

  // Range contents remain volatile RAM. Publishing an archive locator adds no durability.
  const verifyRange = Effect.fnUntraced(function* (
    thread: StoredThread,
    range: ThreadArchiveRange,
    source?: Map<BatchId, string>,
  ) {
    let sequence = range.firstSequence;
    let digest = range.previousTailDigest;
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

      const wire = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(text).pipe(
        Effect.mapError((cause) => storeError("verify range", "Invalid range wire", cause)),
      );

      digest = yield* digestJson({ previousTailDigest: digest, batch: wire }).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((cause) => storeError("verify range", cause.message, cause)),
      );
      if (digest !== batch.digest)
        return yield* storeError("verify range", "Canonical batch anchor mismatch");
      captured.set(id, text);
      sequence = decodeCanonicalSequence(batch.result.lastSequence + 1);
    }
    if (
      digest !== range.tailDigest ||
      bytes !== range.byteCount ||
      batches !== range.batchCount ||
      range.recordCount !== range.lastSequence - range.firstSequence + 1
    )
      return yield* storeError("verify range", "Range anchors or counts differ");

    return captured;
  });

  const archiveTarget = Effect.fnUntraced(function* (
    request: import("@yielded/agent/thread-archive-range").ThreadArchiveRangeSeal,
  ) {
    const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);

    if (request.producerEpoch !== thread.producerEpoch)
      return yield* FenceRejected.make({
        threadId: request.threadId,
        actualEpoch: thread.producerEpoch,
        attemptedEpoch: request.producerEpoch,
      });

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
          const open = thread.openRange.current;

          if (open === undefined) return Option.none<ThreadArchiveRange>();
          yield* verifyRange(thread, open);
          thread.ranges.set(open.firstSequence, open);
          thread.rangeStarts.push(open.firstSequence);
          delete thread.openRange.current;

          return Option.some(open);
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

      if (
        request.expectedTailSequence !== thread.tailSequence ||
        request.expectedTailDigest !== thread.tailDigest
      )
        return yield* storeError("worker capacity", "Canonical tail changed");

      const index = yield* readyIndex(request.threadId, thread.tailSequence).pipe(
        Effect.mapError((e) => storeError("worker capacity", e.message)),
      );

      const worker = index.workers.get(request.workerThreadId);

      return ThreadWorkerCapacity.make({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        producerEpoch: thread.producerEpoch,
        activeWorkers: Math.min(index.workers.size, request.activeLimit + 1),
        workerActive: worker !== undefined,
        pendingInputs: Math.min(
          request.update ? (worker?.update ?? 0) : (worker?.ordinary ?? 0),
          request.pendingLimit + 1,
        ),
      });
    });

  const verification = {
    verify: (unvalidated: import("@yielded/agent/thread-store").ThreadVerificationRequest) =>
      withMutation(
        Effect.gen(function* () {
          const request = yield* validate(ThreadVerificationRequest, "verify Thread", unvalidated);
          const thread = yield* findThread(MutableRef.get(state.ref), request.threadId);
          const id = request.threadId;

          const reader: ThreadImportReader["Service"] = {
            ...exactRecordReader(id, thread),
            admission: (sid) => ledgerTransfer?.admission(id, sid) ?? Effect.succeed(undefined),
            runOwner: (runId) =>
              runId.startsWith("run:")
                ? (ledgerTransfer?.admission(
                    id,
                    Schema.decodeSync(importSubmissionId)(runId.slice(4)),
                  ) ?? Effect.succeed(undefined))
                : Effect.succeed(undefined),
            commands: (sid) =>
              ledgerTransfer?.commands(sid) ??
              Effect.succeed({ aborts: [], approvals: [], resolutions: [] }),
            delivery: (messageId) =>
              deliveryTransfer?.record(id, messageId) ?? Effect.succeed(undefined),
            settlementPredecessors: (request) =>
              settlementInputs(thread, request).pipe(
                Stream.mapEffect((sid) =>
                  reader.admission(sid).pipe(
                    Effect.filterOrFail(
                      (admission) => admission !== undefined,
                      () =>
                        storeError("settlement predecessors", "Indexed input admission is missing"),
                    ),
                  ),
                ),
                Stream.filter((admission) => admission.queueSequence < request.queueSequence),
                Stream.map(({ submissionId }) => ({ submissionId })),
              ),
          };

          const submissions = () => ledgerTransfer?.submissions(id) ?? Stream.empty;

          const runs = Stream.fromIterable(thread.runRecords.keys()).pipe(
            Stream.concat(
              Stream.fromIterable(thread.continuations.keys()).pipe(
                Stream.filter((run) => !thread.runRecords.has(run)),
              ),
            ),
            Stream.map((runId) => Schema.decodeSync(RunId)(runId)),
          );

          const checkpoint = yield* loadCheckpoint(
            LoadCheckpointRequest.make({ threadId: id }),
          ).pipe(
            Effect.mapError((cause) =>
              Schema.is(CheckpointRejected)(cause)
                ? storeError("verify checkpoint", "Invalid checkpoint anchor", cause)
                : cause,
            ),
          );

          return yield* verifyThreadInvariants({
            threadId: id,
            runs,
            submissions: submissions(),
            pages: streamExport({ threadId: id }).pipe(
              Stream.provideService(ThreadExportSource, { export: exportThread }),
            ),
            ...(Option.isNone(checkpoint) ? {} : { checkpoint: checkpoint.value }),
            checkpointsSupported: true,
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
    read,
    readPrompt,
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
      registerDeliveryTransfer: (transfer) =>
        withMutation(
          Effect.sync(() => {
            if (deliveryTransfer !== undefined)
              throw new Error("MemoryThreadStore already has a paired delivery store");
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
