import type { RunId, SubmissionId, ThreadId, ToolCallId } from "@yielded/agent/identifiers";
import type { MessageDeliveryError, MessageDeliveryRecord } from "@yielded/agent/message-delivery";
import type { IdempotencyKey } from "@yielded/agent/receipt";
import type { RecordEnvelope, RecordId } from "@yielded/agent/records";
import type { StorageOwner } from "@yielded/agent/run-storage";
import type { SubmissionSnapshot, WorkerLedgerState } from "@yielded/agent/submission-ledger";
import type {
  PreparedImportPage,
  ThreadImportReader,
  ThreadDeliveryImportReader,
  ThreadImportRejected,
} from "@yielded/agent/thread-import";
import type {
  AppendResult,
  AppendRequest,
  ThreadStoreFailure,
  ThreadTail,
  ThreadAdmission,
  ThreadStoreError,
  ThreadExportSnapshot,
} from "@yielded/agent/thread-store";
import type { TransferFacts, TransferSection } from "@yielded/agent/thread-transfer";
import type {
  ThreadWorkEntry,
  WorkThreadsRequest,
  WorkThreadsPage,
} from "@yielded/agent/thread-work";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

export interface PreparedMemoryAppend {
  readonly request: AppendRequest;
  readonly batchJson: string;
  readonly deliveries: ReadonlyArray<MessageDeliveryRecord>;
}

/** Validate under the owner's gate, then publish without yielding or throwing. */
export type MemoryDeliveryWriter = (
  records: ReadonlyArray<MessageDeliveryRecord>,
) => Effect.Effect<() => void, MessageDeliveryError>;

/** Private storage, not a whole-Thread preparation value. Publication callbacks never yield. */
export interface MemoryLedgerImport {
  readonly reader: Pick<ThreadImportReader["Service"], "admission" | "runOwner" | "commands">;
  readonly stage: (
    page: PreparedImportPage,
  ) => Effect.Effect<void, ThreadImportRejected | ThreadStoreError>;
  readonly prepareCommit: (
    workerSeal?: Pick<WorkerLedgerState, "terminal">,
  ) => Effect.Effect<() => void, ThreadImportRejected | ThreadStoreError, ThreadImportReader>;
}

export interface MemoryLedgerTransfer {
  readonly snapshot: (threadId: ThreadId) => Effect.Effect<
    Omit<typeof ThreadExportSnapshot.Type, "deliveries"> & {
      readonly idle: boolean;
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
  readonly prepareCommit: () => Effect.Effect<
    () => void,
    ThreadImportRejected | ThreadStoreError,
    ThreadDeliveryImportReader
  >;
}

export interface MemoryDeliveryTransfer {
  readonly record: (
    threadId: ThreadId,
    messageId: IdempotencyKey,
  ) => ReturnType<ThreadImportReader["Service"]["delivery"]>;
  readonly snapshot: (threadId: ThreadId) => {
    readonly idle: boolean;
    readonly deliveries: number;
  };
  readonly facts: (
    threadId: ThreadId,
    after: string | undefined,
  ) => Effect.Effect<{ readonly facts: TransferFacts; readonly after: string }, ThreadStoreError>;
  readonly startImport: (
    threadId: ThreadId,
  ) => Effect.Effect<MemoryDeliveryImport, ThreadImportRejected | ThreadStoreError>;
  readonly peerCount: (threadId: ThreadId, nowMillis: number, limit: number) => number;
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
    readonly owner: StorageOwner["Service"];
    readonly withMutation: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    readonly registerWorkOwner: (
      kind: "admissions" | "deliveries",
      owner: MemoryWorkOwner,
    ) => Effect.Effect<void>;
    readonly registerDeliveryWriter: (writer: MemoryDeliveryWriter) => Effect.Effect<void>;
    readonly checkThreadCapacity: (threadId: ThreadId) => Effect.Effect<void, ThreadStoreError>;
    readonly initializeWork: (threadId: ThreadId) => void;
    readonly registerLedgerTransfer: (transfer: MemoryLedgerTransfer) => Effect.Effect<void>;
    readonly registerDeliveryTransfer: (transfer: MemoryDeliveryTransfer) => Effect.Effect<void>;
    readonly prepareAppend: (
      request: AppendRequest,
    ) => Effect.Effect<PreparedMemoryAppend, ThreadStoreFailure>;
    readonly appendPrepared: (
      prepared: PreparedMemoryAppend,
    ) => Effect.Effect<AppendResult, ThreadStoreFailure>;
    readonly record: (
      threadId: ThreadId,
      recordId: RecordId,
    ) => Effect.Effect<RecordEnvelope | undefined>;
    /** Bounded canonical outcomes for one call; caller holds the shared mutation gate. */
    readonly toolCallResults: (
      threadId: ThreadId,
      runId: RunId,
      toolCallId: ToolCallId,
    ) => Effect.Effect<ReadonlyArray<RecordEnvelope>>;
    readonly tail: (threadId: ThreadId) => Effect.Effect<ThreadTail, ThreadStoreFailure>;
  }
>()("@effect-agent/storage-memory/internal/MemoryThreadStoreKernel") {}
