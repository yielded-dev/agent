import type { ThreadId } from "@yielded/agent/identifiers";
import type { Digest, RecordEnvelope, RecordId } from "@yielded/agent/records";
import type {
  AppendResult,
  FencedAppendRequest,
  ThreadStoreFailure,
  ThreadTail,
} from "@yielded/agent/thread-store";
import { Context, type Effect } from "effect";

export interface PreparedMemoryAppend {
  readonly request: FencedAppendRequest;
  readonly digest: Digest;
}

/** One journal's private mutation boundary, shared only with its paired ledger. */
export class MemoryThreadStoreKernel extends Context.Service<
  MemoryThreadStoreKernel,
  {
    readonly withMutation: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
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
