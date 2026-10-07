import { Context, type Effect, type Stream } from "effect";

import type { AgentId, RunId, SubmissionId, ThreadId } from "../../core/Identifiers.ts";
import type { IdempotencyKey } from "../../core/Receipt.ts";
import type { MessageDeliveryRecord } from "../MessageDelivery.ts";
import type { CanonicalRecordEnvelope, RecordEnvelope, RecordId } from "../Records.ts";
import type {
  ThreadAdmission,
  ThreadCommands,
  ThreadReadRequest,
  ThreadStoreError,
} from "../ThreadStore.ts";

export interface ThreadSettlementPredecessorRequest {
  readonly submissionId: SubmissionId;
  readonly queueSequence: number;
  readonly inputSequence: number;
  readonly settlementSequence: number;
}

/** Exact indexes for one captured Thread transaction. Run records and commands enforce count and byte bounds. */
export class ThreadImportReader extends Context.Service<
  ThreadImportReader,
  {
    /** Full canonical history in this same snapshot, including archived facts. */
    readonly read: (
      request: ThreadReadRequest,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError>;
    readonly delivery: (
      messageId: IdempotencyKey,
    ) => Effect.Effect<MessageDeliveryRecord | undefined, ThreadStoreError>;
    readonly record: (
      id: RecordId,
    ) => Effect.Effect<CanonicalRecordEnvelope | undefined, ThreadStoreError>;
    readonly admission: (
      id: SubmissionId,
    ) => Effect.Effect<ThreadAdmission | undefined, ThreadStoreError>;
    readonly runOwner: (id: RunId) => Effect.Effect<ThreadAdmission | undefined, ThreadStoreError>;
    readonly runRecords: (
      id: RunId,
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ThreadStoreError>;
    readonly commands: (
      id: SubmissionId,
    ) => Effect.Effect<typeof ThreadCommands.Type, ThreadStoreError>;
    readonly hasAgent: (id: AgentId) => Effect.Effect<boolean, ThreadStoreError>;
    readonly settlementPredecessors: (
      request: ThreadSettlementPredecessorRequest,
    ) => Stream.Stream<Pick<ThreadAdmission, "submissionId">, ThreadStoreError>;
  }
>()("@effect-agent/thread/ThreadImportReader") {}

/** Delivery evidence reads: the importing Thread uses staged facts; foreign Threads use retained facts. */
export class ThreadDeliveryImportReader extends Context.Service<
  ThreadDeliveryImportReader,
  {
    readonly admission: (
      threadId: ThreadId,
      submissionId: SubmissionId,
    ) => Effect.Effect<ThreadAdmission | undefined, ThreadStoreError>;
    readonly record: (
      threadId: ThreadId,
      recordId: RecordId,
    ) => Effect.Effect<RecordEnvelope | undefined, ThreadStoreError>;
  }
>()("@effect-agent/thread/ThreadDeliveryImportReader") {}
