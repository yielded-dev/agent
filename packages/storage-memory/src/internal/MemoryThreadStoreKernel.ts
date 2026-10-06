import type { ThreadId, SubmissionId } from "@yielded/agent/identifiers";
import type { IdempotencyKey } from "@yielded/agent/receipt";
import type { Digest, ProducerEpoch, RecordEnvelope, RecordId } from "@yielded/agent/records";
import type { SubmissionSnapshot, WorkerLedgerState } from "@yielded/agent/submission-ledger";
import type {
  PreparedImportPage,
  ThreadImportReader,
  ThreadImportRejected,
} from "@yielded/agent/thread-import";
import type {
  AppendResult,
  FencedAppendRequest,
  ThreadStoreFailure,
  ThreadTail,
  ThreadAdmission,
  ThreadStoreError,
  ThreadExport,
  ThreadExportSnapshot,
} from "@yielded/agent/thread-store";
import type { TransferFacts, TransferSection } from "@yielded/agent/thread-transfer";
import type {
  ThreadWorkEntry,
  WorkThreadsRequest,
  WorkThreadsPage,
} from "@yielded/agent/thread-work";
import { Context, type Effect, type Stream } from "effect";

export interface PreparedMemoryAppend {
  readonly request: FencedAppendRequest;
  readonly digest: Digest;
  readonly batchJson: string;
}

/** Private storage, not a whole-Thread preparation value. Publication callbacks never yield. */
export interface MemoryLedgerImport {
  readonly reader: Pick<ThreadImportReader["Service"], "admission" | "runOwner" | "commands">;
  readonly stage: (
    page: PreparedImportPage,
  ) => Effect.Effect<void, ThreadImportRejected | ThreadStoreError>;
  readonly prepareCommit: (
    producerEpoch: ProducerEpoch,
    workerSeal?: Pick<WorkerLedgerState, "terminal">,
  ) => Effect.Effect<() => void, ThreadImportRejected | ThreadStoreError, ThreadImportReader>;
}

export interface MemoryLedgerTransfer {
  readonly snapshot: (threadId: ThreadId) => Effect.Effect<
    Omit<typeof ThreadExportSnapshot.Type, "deliveries"> & {
      readonly externalObligations?: ThreadExport["externalObligations"];
      readonly workerSeal?: Pick<WorkerLedgerState, "terminal">;
    },
    ThreadStoreError
  >;
  readonly facts: (
    threadId: ThreadId,
    section: Exclude<TransferSection, "deliveries">,
    after: string | undefined,
  ) => Effect.Effect<{ readonly facts: TransferFacts; readonly after: string }, ThreadStoreError>;
  readonly startImport: (
    threadId: ThreadId,
  ) => Effect.Effect<MemoryLedgerImport, ThreadImportRejected | ThreadStoreError>;
  readonly submissions: (threadId: ThreadId) => Stream.Stream<SubmissionSnapshot, ThreadStoreError>;
  readonly admission: (
    threadId: ThreadId,
    submissionId: SubmissionId,
  ) => Effect.Effect<ThreadAdmission | undefined, ThreadStoreError>;
  readonly commands: ThreadImportReader["Service"]["commands"];
}

export interface MemoryDeliveryImport {
  readonly record: ThreadImportReader["Service"]["delivery"];
  readonly stage: (
    page: PreparedImportPage,
  ) => Effect.Effect<void, ThreadImportRejected | ThreadStoreError>;
  readonly has: (messageId: IdempotencyKey) => boolean;
  readonly prepareCommit: (
    readAdmission: MemoryLedgerTransfer["admission"],
    readRecord: (
      threadId: ThreadId,
      recordId: RecordId,
    ) => Effect.Effect<RecordEnvelope | undefined, ThreadStoreError>,
  ) => Effect.Effect<() => void, ThreadImportRejected | ThreadStoreError>;
}

export interface MemoryDeliveryTransfer {
  readonly record: (
    threadId: ThreadId,
    messageId: IdempotencyKey,
  ) => ReturnType<ThreadImportReader["Service"]["delivery"]>;
  readonly snapshot: (threadId: ThreadId) => {
    readonly revision: number;
    readonly deliveries: number;
  };
  readonly facts: (
    threadId: ThreadId,
    after: string | undefined,
  ) => Effect.Effect<{ readonly facts: TransferFacts; readonly after: string }, ThreadStoreError>;
  readonly startImport: (
    threadId: ThreadId,
  ) => Effect.Effect<MemoryDeliveryImport, ThreadImportRejected | ThreadStoreError>;
  readonly pendingPeerCount: (threadId: ThreadId, limit: number) => number;
}

/** Native control metadata only; callers already hold the journal's shared gate. */
export interface MemoryWorkOwner {
  readonly page: (
    threadId: ThreadId,
    after: string | undefined,
    limit: number,
  ) => Effect.Effect<
    {
      readonly entries: ReadonlyArray<ThreadWorkEntry>;
      readonly after?: string;
    },
    ThreadStoreError
  >;
  readonly threads: (
    request: WorkThreadsRequest,
  ) => Effect.Effect<WorkThreadsPage, ThreadStoreError>;
}

/** One journal's private mutation boundary, shared with its paired ledger and delivery store. */
export class MemoryThreadStoreKernel extends Context.Service<
  MemoryThreadStoreKernel,
  {
    readonly withMutation: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    readonly registerWorkOwner: (
      kind: "admissions" | "deliveries",
      owner: MemoryWorkOwner,
    ) => Effect.Effect<void>;
    readonly registerDeliveryLookup: (
      exists: (threadId: ThreadId, messageId: IdempotencyKey) => boolean,
    ) => Effect.Effect<void>;
    /** Synchronous publication with the authoritative retained row, including replay membership. */
    readonly checkThreadCapacity: (threadId: ThreadId) => Effect.Effect<void, ThreadStoreError>;
    readonly initializeWork: (threadId: ThreadId) => void;
    readonly retainDelivery: (threadId: ThreadId, messageId: IdempotencyKey) => void;
    readonly registerLedgerTransfer: (transfer: MemoryLedgerTransfer) => Effect.Effect<void>;
    readonly registerDeliveryTransfer: (transfer: MemoryDeliveryTransfer) => Effect.Effect<void>;
    readonly prepareAppend: (
      request: FencedAppendRequest,
    ) => Effect.Effect<PreparedMemoryAppend, ThreadStoreFailure>;
    readonly appendPrepared: (
      prepared: PreparedMemoryAppend,
    ) => Effect.Effect<AppendResult, ThreadStoreFailure>;
    readonly record: (
      threadId: ThreadId,
      recordId: RecordId,
    ) => Effect.Effect<RecordEnvelope | undefined>;
    readonly tail: (threadId: ThreadId) => Effect.Effect<ThreadTail, ThreadStoreFailure>;
  }
>()("@effect-agent/storage-memory/internal/MemoryThreadStoreKernel") {}
