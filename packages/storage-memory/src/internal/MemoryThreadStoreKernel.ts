import type { ThreadId } from "@yielded/agent/identifiers";
import type { Digest, ProducerEpoch, RecordEnvelope, RecordId } from "@yielded/agent/records";
import type { PreparedThreadImport, ThreadImportRejected } from "@yielded/agent/thread-import";
import type {
  AppendResult,
  FencedAppendRequest,
  ThreadStoreFailure,
  ThreadTail,
  ThreadAdmission,
  ThreadCommands,
  ThreadStoreError,
  ThreadExport,
} from "@yielded/agent/thread-store";
import { Context, type Effect } from "effect";

export interface PreparedMemoryAppend {
  readonly request: FencedAppendRequest;
  readonly digest: Digest;
  readonly batchJson: string;
}

/** Facts come from the paired ledger; installation is staged before either store changes. */
export interface MemoryLedgerTransfer {
  readonly export: (threadId: ThreadId) => Effect.Effect<
    {
      readonly admissions: ReadonlyArray<ThreadAdmission>;
      readonly commands: typeof ThreadCommands.Type;
      readonly externalObligations?: ThreadExport["externalObligations"];
    },
    ThreadStoreError
  >;
  readonly prepareImport: (
    prepared: PreparedThreadImport,
    producerEpoch: ProducerEpoch,
  ) => Effect.Effect<() => void, ThreadImportRejected>;
}

/** One journal's private mutation boundary, shared with its paired ledger and delivery store. */
export class MemoryThreadStoreKernel extends Context.Service<
  MemoryThreadStoreKernel,
  {
    readonly withMutation: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    readonly registerLedgerTransfer: (transfer: MemoryLedgerTransfer) => Effect.Effect<void>;
    readonly registerMessageDeliveryStore: (
      hasRetained: (threadId: ThreadId) => Effect.Effect<boolean>,
    ) => Effect.Effect<void>;
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
