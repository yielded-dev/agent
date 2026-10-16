import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Stream from "effect/Stream";

import type { AgentId, RunId, SubmissionId, ThreadId } from "../../core/Identifiers.ts";
import type { IdempotencyKey } from "../../core/Receipt.ts";
import type { MessageDeliveryRecord } from "../MessageDelivery.ts";
import type { CanonicalRecordEnvelope, RecordEnvelope, RecordId } from "../Records.ts";
import type {
  ThreadAdmission,
  ThreadCommands,
  ThreadReadRequest,
  ThreadStoreError,
  ExecutionCheckpoint,
} from "../ThreadStore.ts";
import { RunContextReader } from "./run-context-reader.ts";

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
    readonly execution: (
      id: RunId,
    ) => Effect.Effect<ExecutionCheckpoint | undefined, ThreadStoreError>;
    readonly commands: (
      id: SubmissionId,
    ) => Effect.Effect<typeof ThreadCommands.Type, ThreadStoreError>;
    readonly hasAgent: (id: AgentId) => Effect.Effect<boolean, ThreadStoreError>;
  }
>()("@effect-agent/thread/ThreadImportReader") {
  /** Bind context reconstruction to this same captured transaction. */
  static layer(reader: ThreadImportReader["Service"]) {
    return Layer.succeedContext(
      Context.make(ThreadImportReader, reader).pipe(
        Context.add(RunContextReader, {
          record: reader.record,
          read: (request) =>
            reader.read({
              threadId: request.threadId,
              selection: { _tag: "PromptEvidence", throughSequence: request.throughSequence },
              page: {
                limit: request.limit,
                ...(request.afterSequence === undefined
                  ? {}
                  : { afterSequence: request.afterSequence }),
              },
            }),
        }),
      ),
    );
  }
}

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
