/**
 * Schemas for continuing child Threads, retained input, worker observations, assignment completion,
 * and parent-directed framework messages.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

import { AgentPolicy } from "./AgentPolicy.ts";
import { Update } from "./AgentUpdates.ts";
import * as FailureDiagnostic from "./FailureDiagnostic.ts";
import { AgentId, DelegationId, RunId, SettlementId, ThreadId, ToolCallId } from "./Identifiers.ts";
import { MessageStatus } from "./internal/message-status.ts";
import { IdempotencyKey, Receipt } from "./Receipt.ts";
import { SubagentExecutionFailure, SubagentGrant } from "./SubagentContract.ts";

/**
 * Assignment output selected through the Definition's runDisposition declaration.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AssignmentDisposition = Schema.Literals(["completed", "waiting"]);
/**
 * Decoded value of {@link AssignmentDisposition}.
 *
 * @category models
 * @since 0.1.0
 */
export type AssignmentDisposition = typeof AssignmentDisposition.Type;

/**
 * A permanent destination outcome; an ordinary Run completion does not imply this.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AssignmentTerminal = Schema.Literals(["completed", "failed", "cancelled"]);
/**
 * Decoded value of {@link AssignmentTerminal}.
 *
 * @category models
 * @since 0.1.0
 */
export type AssignmentTerminal = typeof AssignmentTerminal.Type;

/**
 * A reusable child Thread, correlated with its declaration. This value grants no authority.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerRef = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  delegationId: DelegationId,
  targetAgentId: AgentId,
  threadId: ThreadId,
});

/**
 * Decoded value of {@link WorkerRef}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerRef = typeof WorkerRef.Type;

/**
 * A continuing worker identity and retained first delivery, never an execution handle.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerStarted = Schema.Struct({
  worker: WorkerRef,
  delivery: MessageStatus,
}).check(
  Schema.makeFilter(
    ({ worker, delivery }) =>
      delivery.receipt === null || worker.threadId === delivery.receipt.threadId,
  ),
);

/**
 * Decoded value of {@link WorkerStarted}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerStarted = typeof WorkerStarted.Type;

/**
 * Stable owner command. Reusing its key for a different worker conflicts.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerStop = Schema.Struct({ worker: WorkerRef, idempotencyKey: IdempotencyKey });
/**
 * Decoded value of {@link WorkerStop}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerStop = typeof WorkerStop.Type;

/**
 * The inbox is sealed and active execution has released ownership. External actions are not undone.
 *
 * @category operations
 * @since 0.1.0
 */
export const WorkerStopped = WorkerStop;
/**
 * Decoded value of {@link WorkerStopped}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerStopped = typeof WorkerStopped.Type;

/**
 * A host-authorized Run allowance is independent of the immutable delegation lineage.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerBudgetScope = Schema.Literals(["source-subtree", "worker-run"]);
/**
 * Decoded value of {@link WorkerBudgetScope}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerBudgetScope = typeof WorkerBudgetScope.Type;

/**
 * Host-bound caller metadata. Programmatic calls never fabricate Run or Tool Call identities.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerSource = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("tool"),
    agentId: AgentId,
    threadId: ThreadId,
    runId: RunId,
    toolCallId: ToolCallId,
  }),
  Schema.Struct({
    _tag: Schema.Literal("programmatic"),
    agentId: AgentId,
    threadId: ThreadId,
  }),
]);

/**
 * Decoded value of {@link WorkerSource}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerSource = typeof WorkerSource.Type;

/**
 * The caller's effective policy and root-relative depth, resolved by its host.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerContext = Schema.Struct({
  source: WorkerSource,
  policy: AgentPolicy,
  depth: Schema.Natural,
  grant: Schema.optionalKey(SubagentGrant),
});

/**
 * Decoded value of {@link WorkerContext}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerContext = typeof WorkerContext.Type;

/**
 * Admission identity is distinct from canonical input application.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerInput = Schema.Struct({ receipt: Receipt, messageId: IdempotencyKey });
/**
 * Decoded value of {@link WorkerInput}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerInput = typeof WorkerInput.Type;

/**
 * Schema for a retained worker input associated with the Run and sequence that applied it.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerAppliedInput = Schema.Struct({
  ...WorkerInput.fields,
  runId: RunId,
  sequence: Schema.Natural,
});

/**
 * A Run outcome is not assignment completion. Interpret only a Definition's disposition.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerRun = Schema.Struct({
  runId: RunId,
  hostReceipt: Receipt,
  outcome: Schema.NullOr(Schema.Literals(["completed", "failed", "aborted"])),
  disposition: Schema.NullOr(Schema.Json),
});

/**
 * Bounded durable listing data; a latest Receipt never substitutes for a requested Receipt.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerSummary = Schema.Struct({
  worker: WorkerRef,
  latestReceipt: Schema.NullOr(Receipt),
  /**
   * Unknown work awaits reconciliation and is not progressing. stopping/stopped identify
   * an owner-issued worker stop, never an ordinary Receipt abort. */
  state: Schema.Literals([
    "starting",
    "active",
    "unknown",
    "idle",
    "stopping",
    "stopped",
    "completed",
    "failed",
    "cancelled",
  ]),
  acceptedInput: Schema.NullOr(WorkerInput),
  appliedInput: Schema.NullOr(WorkerAppliedInput),
  run: Schema.NullOr(WorkerRun),
  /** One retained unadmitted input, if any; its state is independent of destination execution. */
  pendingDelivery: Schema.NullOr(MessageStatus),
  /** Consistent destination facts; pending delivery has its own source-owned version. */
  watermark: Schema.Struct({
    canonicalSequence: Schema.Natural,
    acceptedQueueSequence: Schema.Natural,
    pendingDeliveryVersion: Schema.NullOr(Schema.Natural),
  }),
});

/**
 * Decoded value of {@link WorkerSummary}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerSummary = typeof WorkerSummary.Type;

/**
 * Schema for up to 100 worker summaries and an optional Thread continuation cursor.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerPage = Schema.Struct({
  items: Schema.Array(WorkerSummary).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(ThreadId),
});

/**
 * Decoded value of {@link WorkerPage}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerPage = typeof WorkerPage.Type;

/**
 * Authorized canonical history, encoded independently of storage-adapter representations.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerHistoryEntry = Schema.Struct({
  sequence: Schema.Natural.check(Schema.isGreaterThan(0)),
  recordId: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  record: Schema.Json,
});

/**
 * Decoded value of {@link WorkerHistoryEntry}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerHistoryEntry = typeof WorkerHistoryEntry.Type;

/**
 * Closed host-boundary failures. Original causes stay private and survive diagnostic transport.
 * Retained delivery states are successful MessageStatus values, including pending and refused.
 * After a storage failure, keep the same idempotency key and parameters when reconciling.
 *
 * @category errors
 * @since 0.1.0
 */
export class WorkerError extends Schema.TaggedError<WorkerError>()("WorkerError", {
  cause: Schema.optionalKey(FailureDiagnostic.Value),
  stack: Schema.optionalKey(Schema.String),
  /** Concurrency pressure may clear without changing the request. */
  retryable: Schema.optionalKey(Schema.Literal(true)),
  operation: Schema.Literals([
    "context",
    "start",
    "followUp",
    "inspect",
    "observe",
    "await",
    "list",
    "cancel",
    "stop",
  ]),
  reason: Schema.Literals([
    "denied",
    "declaration-unavailable",
    "worker-mismatch",
    "receipt-mismatch",
    "message-mismatch",
    "idempotency-conflict",
    "capacity",
    "not-found",
    // Decode earlier failures without emitting them for retained deliveries.
    "delivery-pending",
    "storage",
    "corrupt",
    "unavailable",
  ]),
}) {}

export {
  /**
   * Marks a host-managed idempotent worker operation, independently of its Tool name.
   *
   * @category services
   * @since 0.1.0
   */
  WorkerOperationTool,
} from "./SubagentContract.ts";

/**
 * One projected canonical Run outcome. Joined receipts share this identity.
 *
 * @category operations
 * @since 0.1.0
 */
export const WorkerReport = <Success extends Schema.Top>(success: Success) => {
  const identity = {
    _tag: Schema.Literal("Settled"),
    worker: WorkerRef,
    receipt: Receipt,
    runId: RunId,
    settlementId: SettlementId,
  };

  return Schema.Union([
    // Union preserves the Success codec while making the result field required.
    Schema.Struct({
      ...identity,
      outcome: Schema.Literal("completed"),
      result: Schema.Union([success]),
    }),
    Schema.Struct({
      ...identity,
      outcome: Schema.Literals(["failed", "aborted"]),
      failure: SubagentExecutionFailure,
    }),
  ]);
};

/**
 * Decoded value of {@link WorkerReport}.
 *
 * @category utility types
 * @since 0.1.0
 */
export type WorkerReport<Success extends Schema.Top> = ReturnType<
  typeof WorkerReport<Success>
>["Type"];

/**
 * Framework message stored separately from the parent's application input.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerCompletion = Schema.Struct({
  _tag: Schema.Literal("WorkerCompletion"),
  budgetExhausted: Schema.Boolean,
  schemaVersion: Schema.Literal(1),
  report: WorkerReport(Schema.Json),
}).check(Schema.makeFilter(({ report }) => report.worker.threadId === report.receipt.threadId));

/**
 * Decoded value of {@link WorkerCompletion}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerCompletion = typeof WorkerCompletion.Type;

/**
 * Source-worker intermediate output, kept separate from application input.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WorkerUpdate = Schema.Struct({
  _tag: Schema.Literal("WorkerUpdate"),
  schemaVersion: Schema.Literal(1),
  worker: WorkerRef,
  update: Update,
}).check(
  Schema.makeFilter(
    ({ worker, update }) =>
      worker.threadId === update.threadId && worker.targetAgentId === update.agentId,
  ),
);

/**
 * Decoded value of {@link WorkerUpdate}.
 *
 * @category models
 * @since 0.1.0
 */
export type WorkerUpdate = typeof WorkerUpdate.Type;
/**
 * Schema for framework-owned child completion and update messages delivered to a parent.
 *
 * @category schemas
 * @since 0.1.0
 */
export const FrameworkMessage = Schema.Union([WorkerCompletion, WorkerUpdate]);
/**
 * Decoded value of {@link FrameworkMessage}.
 *
 * @category models
 * @since 0.1.0
 */
export type FrameworkMessage = typeof FrameworkMessage.Type;
