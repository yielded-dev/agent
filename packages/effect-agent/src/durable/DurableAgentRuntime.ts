import type { Scope } from "effect";
import {
  Cause,
  Clock,
  Context,
  Crypto,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Equal,
  Exit,
  Layer,
  Option,
  Ref,
  Result,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { Prompt, type Tool } from "effect/ai";

import {
  type InputMessage,
  MessageAdmission,
  type MessagingError,
} from "../capabilities/Messaging.ts";
import type * as Agent from "../core/Agent.ts";
import { type RunDispositionDeclaration, type InputPromptSource } from "../core/Agent.ts";
import {
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentInputDecodeError,
  AgentPersistenceCapacityError,
  AgentInputError,
  AgentToolAuthorizationDenied,
  type AgentToolAuthorizationCheckError,
  PolicyLimit,
} from "../core/AgentError.ts";
import { AgentPolicy } from "../core/AgentPolicy.ts";
import { UpdateError } from "../core/AgentUpdates.ts";
import * as FailureDiagnostic from "../core/FailureDiagnostic.ts";
import {
  type ReceiptId,
  ThreadId,
  SubmissionId,
  ToolCallId,
  type AgentId,
  type AttemptId,
  type RunId,
  type TurnId,
} from "../core/Identifiers.ts";
import { IdGenerator } from "../core/IdGenerator.ts";
import { copyJson } from "../core/internal/json.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import { Receipt } from "../core/Receipt.ts";
import type { ExhaustedLimit } from "../core/RunEvent.ts";
import {
  SubagentBudgetReservation,
  SubagentReservationAmounts,
  SubagentGrant,
  DelegationDepth,
  getToolExecutionKind,
  SubagentParentLink,
} from "../core/SubagentContract.ts";
import { Selection, Snapshot } from "../core/ToolExposure.ts";
import type { ToolParameterRejection } from "../core/ToolResult.ts";
import {
  ModelCallUsage,
  ModelResponseIdentity,
  InputTokenUsage,
  ModelUsageGroup,
  RunUsageSummary,
  ChildRunUsage,
  RunUsageReport,
  emptyRunTotals,
  unknownRunTotals,
  runTotalsFromSummary,
  sumRunTotals,
  summarizeModelUsage,
  OutputTokenUsage,
} from "../core/Usage.ts";
import { FrameworkMessage } from "../core/Worker.ts";
import type { WorkerError } from "../core/Worker.ts";
import * as AgentRuntime from "../engine/AgentRuntime.ts";
import {
  AgentChildPending,
  renderInputPrompt,
  type AgentRuntimeRequirements,
  type AgentCompletionProjectionRequirements,
  type RuntimeBinding,
} from "../engine/AgentRuntime.ts";
import { ContextCompactor, CompactionError } from "../engine/ContextCompactor.ts";
import { ContextRolloverTool } from "../engine/ContextWindow.ts";
import { getToolExecutionClass } from "../engine/DurableStep.ts";
import { MessagingHost } from "../engine/MessagingHost.ts";
import {
  CurrentToolFailureObserver,
  AgentUpdateAcceptance,
  ModelUsageAccounting,
  RunContextPreparation,
  RunToolAuthorization,
  RunToolScheduling,
  RunContextPreparationPassthrough,
  type RunContextPreparationError,
  type ChildEstablishStatus,
  type RunApprovalHook,
  type RunContextHook,
  type RunCostEstimator,
  type RunDurabilityHook,
  type RunInputCommand,
  type RunInputHook,
  type RunOptions,
  type RunSubagentEstablishRequest,
  type RunSubagentHook,
  type RunSubagentJoinRequest,
  type RunToolAuthorizationHook,
  type RunToolAuthorizationRequest,
} from "../engine/RunOptions.ts";
import { SubagentHost } from "../engine/SubagentHost.ts";
import { ThreadHistory } from "../engine/ThreadHistory.ts";
import { RunToolVisibility } from "../engine/ToolExposure.ts";
import {
  type IntegrityReport,
  type RetryCommand,
  ExplainedEvidence,
  ExplainedSubmission,
  ExplainedUnknownCall,
  ObligationEntry,
  ObligationReport,
  RecoveryExplanation,
  RetryRefused,
  obligationSeverityOf,
  predictRecoveryDisposition,
  recoveryDecisionMeaning,
  type ObligationBlockedOn,
  type ObligationThresholds,
} from "./Admin.ts";
import { canonicalJson, digestJson, type DigestError } from "./Digest.ts";
import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "./DurableFailpoint.ts";
import {
  BindingUnavailable,
  compileRegistrations,
  definitionDigestsEqual,
  toolReplayContracts,
  type AgentRegistration,
  type DurableBindingFailure,
  makeLegacyWorkerBinding,
  type ResolvedBinding,
  resolveDefinitionBinding,
  resolveWorkerBinding,
  CurrentBindingSelection,
} from "./internal/agent-registration.ts";
import { makeAgentUpdateRuntime } from "./internal/agent-updates.ts";
import { inspectForeignDiagnostic, safeUnknownString } from "./internal/foreign-diagnostic.ts";
import { initialContext } from "./internal/initial-context.ts";
import {
  makeJournalMetadata,
  type JournalMetadata,
  type JournalRecordEnvelope,
} from "./internal/journal-metadata.ts";
import { makeMessagingRuntime } from "./internal/messaging-host.ts";
import { RunContextReader } from "./internal/run-context-reader.ts";
import { digestRunHistory, readRunContext } from "./internal/run-context.ts";
import * as ThreadInitialization from "./internal/thread-initialization.ts";
import {
  initialDispatchBlockedTurns,
  isUnresolvedToolOperation,
  toolOperationStates,
} from "./internal/tool-operations.ts";
import { makeWorkerRuntime, WorkerInputControl } from "./internal/worker-host.ts";
import { WorkerRuntime } from "./internal/worker-runtime.ts";
import { MessageDeliveryStore, prepareMessageDelivery } from "./MessageDelivery.ts";
import {
  OperationAuthorizationRequest,
  OperationAuthorizer,
  OperationDenied,
} from "./OperationAuthorizer.ts";
import {
  type RunContinuation,
  type CanonicalRecordEnvelope,
  type CanonicalRecordPayload,
  type DeploymentId,
  type Digest,
  type ProducerId,
  AbortRequested,
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  CompactionCreated,
  DefinitionDigests,
  ModelResponseInterrupted,
  ModelCallAborted,
  PersistedJson,
  ProducerEpoch,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  RunStartedRecord,
  RunContextRecorded,
  MAX_RUN_TOOL_CALL_IDENTITIES,
  MAX_PERSISTED_JSON_BYTES,
  MAX_RUN_CONTINUATION_BYTES,
  RunDurationExhausted,
  SettlementFailureDiagnostic,
  SubagentJoined,
  SubagentLineageRecorded,
  SubtreeBudgetReserved,
  SubagentRequested,
  SubagentStarted,
  SubmissionSettled,
  SubmissionSettledRecord,
  ToolApprovalDecided,
  ToolApprovalRequested,
  DeclaredToolCall,
  ToolOperation,
  ToolUnavailable,
  RunPolicyUsageReserved,
  ToolCallResolved,
  ToolCallSettled,
  ToolCallUnknown,
  ToolStepSettled,
  UserInputRecorded,
  WorkerAdmission,
  WorkHandoffCompleted,
  type ApprovalDecision,
  type SettlementOutcome,
  type ToolCallResolution,
} from "./Records.ts";
import {
  classifyRecovery,
  DeclaredPendingBatchEvidence,
  OpenDelegationCallEvidence,
  OpenToolCallEvidence,
  PendingApprovalEvidence,
  pendingApprovalForRecovery,
  RecoveryDecision,
  RecoveryEvidence,
  type DelegationAdmissionEvidence,
  type MarkUnknown as MarkUnknownDecision,
  type SettleAborted as SettleAbortedDecision,
} from "./Recovery.ts";
import {
  canonicalRecordBytes,
  CurrentRunWriter,
  CurrentRunSettlement,
  canonicalRunIds,
  executionRunIds,
  isPreContinuationFact,
  makeProgressWriter,
  readContinuation,
  readRunEvidenceSnapshot,
  reference,
  resolveEvidence,
  runEvidence,
  validateSuffix,
  terminalUsageCharge,
} from "./RunContinuation.ts";
import {
  RunJournalError,
  approvalDecisionBatchId,
  childThreadIdFor,
  childIdempotencyKeyFor,
  compactionBatchId,
  compactionRecordId,
  markUnknownBatchId,
  modelResponseInterruptedBatchId,
  modelResponseInterruptedRecordId,
  modelResponseRecordId,
  projectRunJournalStream,
  type JournalBoundary,
  type RunJournalProjection,
  type RunJournalContext,
  type TurnCommitInput,
  runCompletedRecordId,
  runCompletionDigest,
  runIdForSubmission,
  runStartedBatchId,
  runStartedRecordId,
  runDurationRecordId,
  runDurationBatchId,
  subagentJoinBatchId,
  subagentJoinedRecordId,
  subagentLineageBatchId,
  subagentLineageRecordId,
  subagentRequestedBatchId,
  subagentRequestedRecordId,
  subagentStartedBatchId,
  subagentStartedRecordId,
  toolApprovalDecisionRecordId,
  toolApprovalRequestRecordId,
  toolCallResolutionBatchId,
  toolCallResolvedRecordId,
  toolCallResultBatchId,
  toolCallSettledRecordId,
  toolCallUnknownRecordId,
  toolStepSettledBatchId,
  toolStepSettledRecordId,
  turnApprovalsBatchId,
  turnCanonicalBatch,
  turnIdForRun,
  turnResponseBatch,
  turnResponseBatchId,
  turnResultsBatch,
} from "./RunJournal.ts";
import {
  RunStorage,
  bindRunOwnership,
  makeRunWriter,
  type RunOwnership,
  type RunStorageSession,
  type RunWriter,
} from "./RunStorage.ts";
import {
  SettlementPublication,
  type SettlementPublicationAuthority,
  type SettlementPublicationFailure,
  type SettlementPublicationResult,
} from "./SettlementPublisher.ts";
import {
  type AdmissionFence,
  AdmissionPolicyError,
  type AbortIntent,
  AbortIntentRequest,
  type AdmissionConflict,
  type Claim,
  type JoinedToHost,
  type OwnershipLost,
  type OwnershipToken,
  type RecoverySnapshot,
  type SettlementConflict,
  type SubmissionSnapshot,
  AbortCommand,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimHandoff,
  SubmissionScheduling,
  type JoiningClaim,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  ParentLinkage,
  Principal,
  RecoverySnapshotRequest,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
  RevertJoiningRequest,
  SettlementFinalization,
  Settlement,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  SuspendRequest,
  UnknownResolutionCommand,
  unknownResolutionKind,
  WaitingChild,
  WaitingForChildSuspension,
  submissionAbortBatchId,
  submissionAbortRecordId,
  submissionInputBatchId,
  submissionInputRecordId,
  submissionSettlementBatchId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type AdmissionResult,
  type ApprovalConflict,
  type ApprovalDecisionIntent,
  type ChildBudgetReservationSnapshot,
  type JoinSnapshot,
  type UnknownResolutionConflict,
  type UnknownResolutionIntent,
} from "./SubmissionLedger.ts";
import { PendingSubmission, SettledSubmission, type SubmissionStatus } from "./SubmissionStatus.ts";
import { PreparedInput } from "./Subscription.ts";
import {
  type AppendConflict,
  type ThreadNotMaterialized,
  type FenceRejected,
  ThreadMaterialization,
  ThreadObservation,
  ThreadRead,
  ThreadIdentityRequest,
  ThreadStore,
  ThreadStoreDiagnostic,
  ThreadStoreError,
  ThreadTailRequest,
  FencedAppendRequest,
  getRecord,
  getRunInput,
  ThreadReader,
  MAX_THREAD_EXPORT_PAGE_BYTES,
} from "./ThreadStore.ts";
import {
  type WorkIndexRebuildRequest,
  type WorkIndexProgress,
  ThreadWorkDiscovery,
  ThreadWorkEntry,
  ThreadWorkRequest,
  WorkDiscoveryUnavailable,
  type WorkThreadsRequest,
  type WorkThreadsPage,
  resolveWorkEvidence,
  operationEvidence,
  validateWorkPage,
  workId,
  MAX_RECOVERY_WORK_ITEMS,
  MAX_RECOVERY_PAGES,
  MAX_WORK_CURSOR_CHARS,
  decodeWorkCursor,
  type ThreadWorkPage,
} from "./ThreadWork.ts";
import { DeclaredToolCallEvidence, ToolReconciler } from "./ToolReconciler.ts";
import { WakeScheduler } from "./WakeScheduler.ts";
import { WorkerAdmissionPort, WorkerAdmissionRequest } from "./WorkerAdmission.ts";

// Capture the tracing call site once; each application still creates a fresh Attempt span.
const withThreadHeadSpan = Effect.withSpan("DurableAgentRuntime.processThreadHead");

/**
 * Instruction failure/requirement derivation mirroring the engine's `RuntimeBinding` defaults:
 * declaring them as generic DEFAULTS (instead of independent inference sites) keeps a plain
 * string-returning instruction function from widening both parameters to `unknown`.
 */
type InstructionResultOf<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;

type InstructionErrorOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer Error,
    infer _Requirements
  >
    ? Error
    : never;

type InstructionRequirementsOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer _Error,
    infer Requirements
  >
    ? Requirements
    : never;

const decodeBatchId = Schema.decodeSync(BatchId);
const decodeCanonicalSequence = Schema.decodeSync(CanonicalSequence);
const decodeRecordId = Schema.decodeSync(RecordId);
const decodeToolCallIdUnknown = Schema.decodeUnknownEffect(ToolCallId);
const decodeUsageTotal = Schema.decodeEffect(Schema.Natural);
const ZERO_EPOCH = Schema.decodeSync(ProducerEpoch)(0);
const ZERO_SEQUENCE = decodeCanonicalSequence(0);
const MAX_FAILURE_MESSAGE_LENGTH = 16_384;
const RECONCILER_AUTHOR = "reconciler";
/** Canonical `ToolApprovalDecided.resolver` for policy-auto decisions made by the delegate. */
const APPROVAL_POLICY_RESOLVER = "approval-policy";
/** Ledger `ApprovalDecisionIntent.resolver` for the abort-driven suspension closure. */
const RECOVERY_RESOLVER = "recovery";
/** Upper bound of one `drain("all")` joining pass (`claimJoining.maxCount` must be positive). */
const MAX_JOIN_DRAIN = 32;

/** Bound a hook-supplied approval reason to the canonical `BoundedText` persistence limits. */
const boundedApprovalReason = (reason: string | undefined, fallback: string): string => {
  const value = reason === undefined || reason.length === 0 ? fallback : reason;

  return value.length > MAX_FAILURE_MESSAGE_LENGTH
    ? value.slice(0, MAX_FAILURE_MESSAGE_LENGTH)
    : value;
};

const boundedText = (value: string): string =>
  value.length > MAX_FAILURE_MESSAGE_LENGTH ? value.slice(0, MAX_FAILURE_MESSAGE_LENGTH) : value;

const decodePrincipalSync = Schema.decodeSync(Principal);
const decodeIdempotencyKeySync = Schema.decodeSync(IdempotencyKey);
const decodeChildReservationIdSync = Schema.decodeSync(ChildReservationId);
const decodeRecordIdSync = Schema.decodeSync(RecordId);
const decodeBatchIdSync = Schema.decodeSync(BatchId);
const decodeDefinitionDigests = Schema.decodeUnknownEffect(DefinitionDigests);

/**
 * Deterministic parent-owned child budget reservation identity (spec
 * §12 step 2, D4): one reservation per (parent Run, parent Tool Call) pair,
 * so a replayed establishment converges on the one existing row (SUB-016).
 */
export const childReservationIdFor = (runId: RunId, toolCallId: ToolCallId): ChildReservationId =>
  decodeChildReservationIdSync(`subagent-reservation:${runId}:${toolCallId}`);

/**
 * S2 fixes every attached durable child at delegation depth 1 (SUB-029
 * rejects all nested delegation at preflight), so recovery can rebuild the
 * child lineage from the canonical `SubagentRequested` record alone — the
 * record deliberately does not carry a depth field.
 */
const CHILD_DELEGATION_DEPTH: DelegationDepth = Schema.decodeSync(DelegationDepth)(1);

/** Canonical `AbortRequested.author` of every propagated parent-abort command (spec §13.1). */
const SUBAGENT_ABORT_AUTHOR = "subagent-parent-abort";

/** Keep this exact reason, including its historical citation: abort repair replays the same command. */
const SUBAGENT_ABORT_REASON =
  "The parent Submission was aborted; request-abort-and-join propagates the durable abort intent to every attached child (spec/subagents.md 13.1)";

/**
 * The deterministic zero-consumed accounting decision frozen for a
 * provably-childless orphaned reservation (spec §13/§14: "releases the
 * reservation exactly once"). Constant so every repair pass freezes the SAME
 * decision — an identical `beginChildBudgetRelease` replay is a no-op.
 */
const ORPHAN_ZERO_CONSUMED_ACCOUNTING = Schema.decodeSync(PersistedJson)({
  basis: "orphan-zero-consumed",
});

/** The four parent-log/child-log subagent records of one Run, indexed per Tool Call. */
interface SubagentCallRecords {
  readonly requested: Map<ToolCallId, SubagentRequested>;
  readonly started: Map<ToolCallId, SubagentStarted>;
  readonly joined: Map<ToolCallId, SubagentJoined>;
  /** Declared Tool name per application call (delegation joins reuse it for `ToolCallSettled`). */
  readonly declaredNames: Map<ToolCallId, string>;
}

/** Pure fold of one Run's canonical subagent lifecycle records (plan §1.2). */
const subagentRecordsOf = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId: RunId,
): SubagentCallRecords => {
  const requested = new Map<ToolCallId, SubagentRequested>();
  const started = new Map<ToolCallId, SubagentStarted>();
  const joined = new Map<ToolCallId, SubagentJoined>();
  const declaredNames = new Map<ToolCallId, string>();

  for (const envelope of records) {
    const payload = envelope.record.payload;

    switch (payload._tag) {
      case "SubagentRequested": {
        if (payload.runId === runId) requested.set(payload.toolCallId, payload);
        break;
      }
      case "SubagentStarted": {
        if (payload.runId === runId) started.set(payload.toolCallId, payload);
        break;
      }
      case "SubagentJoined": {
        if (payload.runId === runId) joined.set(payload.toolCallId, payload);
        break;
      }
      case "ModelResponseRecorded": {
        if (payload.runId === runId)
          for (const operation of payload.toolOperations)
            declaredNames.set(operation.toolCallId, operation.toolName);
        break;
      }
      default: {
        break;
      }
    }
  }

  return { requested, started, joined, declaredNames };
};

/** Joined reports are canonical snapshots, keyed by child Run so replay cannot double charge. */
const childUsageReportsOf = (state: SubagentCallRecords): ReadonlyArray<ChildRunUsage> =>
  [...state.started.entries()].map(([toolCallId, child]) => {
    const joined = state.joined.get(toolCallId);

    return ChildRunUsage.make({
      runId: child.childRunId,
      report: RunUsageReport.make({
        usage: joined?.usage ?? unknownRunTotals(),
        delegatedUsage: joined?.delegatedUsage ?? unknownRunTotals(),
      }),
    });
  });

export { threadCreatedBatchId, threadCreatedRecordId } from "./internal/thread-initialization.ts";

/** Deterministic batch identity of one executed recovery decision's audit append (DUR-013). */
export const recoveryRepairBatchId = (submissionId: SubmissionId, decisionTag: string): BatchId =>
  decodeBatchId(`repair:${submissionId}:${decisionTag}`);

/** Deterministic record identity of one executed recovery decision's `RepairAnnotated` record. */
export const recoveryRepairRecordId = (submissionId: SubmissionId, decisionTag: string): RecordId =>
  decodeRecordId(`repair:${submissionId}:${decisionTag}`);

/**
 * The durable identity returned once ledger admission, Thread materialization, and
 * readiness are committed (DUR-001). It is an identifier for observation and reattachment, not
 * an authorization capability.
 */
export { Receipt } from "../core/Receipt.ts";

// Foreign names and tags may contain payloads. Retain only these static classifications.
const RecoveryCauseTag = Schema.Literals([
  "AdmissionPolicyError",
  "DigestError",
  "LedgerError",
  "OwnershipLost",
  "SettlementConflict",
  "ThreadStoreError",
  "ThreadNotMaterialized",
  "AppendConflict",
  "FenceRejected",
  "RunJournalError",
  "WorkDiscoveryUnavailable",
  "DurableRuntimeFailpointError",
  "SchemaError",
  "Error",
  "TypeError",
  "RangeError",
  "ReferenceError",
  "SyntaxError",
  "URIError",
  "EvalError",
  "AggregateError",
  "ForeignError",
  "NonErrorCause",
]);

const isRecoveryCauseTag = Schema.is(RecoveryCauseTag);

/** Recovery could not establish execution authority. This is never a Settlement or replay grant. */
export class RecoveryFailure extends Schema.Class<RecoveryFailure>(
  "@effect-agent/thread/RecoveryFailure",
)({
  phase: Schema.Literals(["history", "recovery"]),
  reason: Schema.Literals(["failure", "defect", "timeout"]),
  errorTag: Schema.String.check(Schema.isMaxLength(128)),
  /** Static operation names only; error messages and payloads are deliberately excluded. */
  operation: Schema.String.check(Schema.isMaxLength(256)),
  /** Ordered, bounded classifications. Foreign labels, operations and prose stay private. */
  causes: Schema.Array(
    Schema.Struct({
      errorTag: RecoveryCauseTag,
    }),
  ).check(Schema.isMaxLength(16)),
  causesTruncated: Schema.optionalKey(Schema.Literal(true)),
  diagnostic: Schema.optionalKey(ThreadStoreDiagnostic),
}) {}

const decodeRecoveryCause = Schema.decodeUnknownOption(
  Schema.Struct({
    diagnostic: Schema.optionalKey(Schema.Unknown),
    cause: Schema.optionalKey(Schema.Unknown),
  }),
);

/** Reuse the bounded foreign-error capture, then retain only content-free causal metadata. */
const recoveryFailureDetails = (cause: Cause.Cause<DurableWorkerFailure>) => {
  const pending = [FailureDiagnostic.capture(cause)];
  const causes: Array<RecoveryFailure["causes"][number]> = [];
  let truncated = false;

  while (pending.length > 0 && causes.length < 16) {
    const node = pending.shift();

    if (node === undefined) break;
    switch (node._tag) {
      case "Cause":
        pending.unshift(
          ...node.reasons.flatMap((reason) =>
            reason._tag === "Fail" ? [reason.error] : reason._tag === "Die" ? [reason.defect] : [],
          ),
        );
        truncated ||= node.truncated === true;
        break;
      case "Error": {
        const tag = node.errorTag ?? node.name;

        causes.push({
          errorTag: isRecoveryCauseTag(tag) ? tag : "ForeignError",
        });
        pending.unshift(
          ...(node.cause === undefined ? [] : [node.cause]),
          ...(node.reason === undefined ? [] : [node.reason]),
          ...(node.errors ?? []),
        );
        truncated ||= node.truncated === true;
        break;
      }
      case "Omitted":
        truncated = true;
        break;
      case "Value":
        causes.push({ errorTag: "NonErrorCause" });
        break;
    }
  }

  // Retain only adapter-created diagnostics, never reify a foreign structural lookalike.
  // Foreign wrappers retain only approved classifications above, never their operation text.
  let nested: unknown = Option.getOrUndefined(Cause.findErrorOption(cause));
  let diagnostic: ThreadStoreDiagnostic | undefined;

  for (let depth = 0; depth < 16 && nested !== undefined; depth++) {
    const decoded = decodeRecoveryCause(nested);

    if (Option.isNone(decoded)) break;
    if (decoded.value.diagnostic instanceof ThreadStoreDiagnostic) {
      diagnostic = decoded.value.diagnostic;
      break;
    }
    nested = decoded.value.cause;
  }

  return {
    causes,
    ...(truncated || pending.length > 0 ? { causesTruncated: true as const } : {}),
    ...(diagnostic === undefined ? {} : { diagnostic }),
  };
};

/** @internal One bounded failure boundary for execution history and host recovery control reads. */
export const isolateRecovery = Effect.fnUntraced(function* <A, E extends DurableWorkerFailure, R>(
  body: Effect.Effect<A, E, R>,
  options: {
    readonly timeout: Duration.Duration;
    readonly phase: () => RecoveryFailure["phase"];
    readonly operation?: string;
  },
): Effect.fn.Return<Result.Result<A, RecoveryFailure>, E, Exclude<R, Scope.Scope>> {
  const outcome = yield* body.pipe(
    Effect.scoped,
    Effect.timeoutOption(options.timeout),
    Effect.exit,
  );

  if (Exit.isSuccess(outcome) && Option.isSome(outcome.value))
    return Result.succeed(outcome.value.value);
  if (
    Exit.isFailure(outcome) &&
    (Cause.hasInterrupts(outcome.cause) ||
      outcome.cause.reasons.some(
        (reason) => reason._tag === "Fail" && reason.error._tag === "DurableRuntimeFailpointError",
      ))
  )
    return yield* Effect.failCause(outcome.cause);

  const error = Exit.isFailure(outcome) ? Cause.findErrorOption(outcome.cause) : Option.none<E>();
  const phase = options.phase();

  return Result.fail(
    RecoveryFailure.make({
      phase,
      reason: Exit.isSuccess(outcome)
        ? "timeout"
        : Cause.hasDies(outcome.cause)
          ? "defect"
          : "failure",
      errorTag: Option.isSome(error)
        ? error.value._tag
        : Exit.isSuccess(outcome)
          ? "RecoveryTimeout"
          : "Defect",
      operation:
        Option.isSome(error) && "operation" in error.value
          ? error.value.operation.slice(0, 256)
          : (options.operation ??
            (phase === "history" ? "read recovery history" : "recover submission")),
      ...(Exit.isFailure(outcome) ? recoveryFailureDetails(outcome.cause) : { causes: [] }),
    }),
  );
});

/** One failed Thread remains pending; the host must not claim it before recovery succeeds. */
export class RecoveryBlocked extends Schema.TaggedError<RecoveryBlocked>()("RecoveryBlocked", {
  threadId: ThreadId,
  failure: RecoveryFailure,
}) {}

/** One executed (or deliberately deferred) recovery decision (durability §14, DUR-013). */
export class RecoveryReport extends Schema.Class<RecoveryReport>(
  "@effect-agent/thread/RecoveryReport",
)({
  submissionId: SubmissionId,
  threadId: ThreadId,
  decision: RecoveryDecision,
  /**
   * `repaired` = executed; `deferred` = a claiming worker must finish it; `none` = settled;
   * `unknown` = this Submission is parked on an Unknown Outcome awaiting the authorized
   * DUR-017 resolution path — the settlement obligation stays visible while no worker permit
   * is consumed (durability §16).
   */
  disposition: Schema.Literals(["repaired", "deferred", "none", "unknown"]),
}) {}

/** A selected owner was checked against its original evidence; this grants no execution lease. */
export class WorkRecoveryReport extends Schema.Class<WorkRecoveryReport>("WorkRecoveryReport")({
  threadId: ThreadId,
  workId: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
  disposition: Schema.Literals(["repaired", "deferred", "none", "unknown"]),
  submission: Schema.optionalKey(RecoveryReport),
  /**
   * An owning transport transition still needs a timed check; external Unknown waits omit it.
   * Derived fractional durations round up to the next millisecond.
   */
  retryAtMillis: Schema.optionalKey(Schema.Natural),
}) {}

export const WorkRecoveryRequest = Schema.Struct({ threadId: ThreadId, work: ThreadWorkEntry });
export type WorkRecoveryRequest = typeof WorkRecoveryRequest.Type;

// Global scans must retain one routing identity; scoped scans reuse their native work cursor.
const MAX_RECOVERY_CURSOR_CHARS = MAX_THREAD_EXPORT_PAGE_BYTES + 6 * MAX_WORK_CURSOR_CHARS + 1024;
const RecoveryCursor = Schema.NonEmptyString.check(Schema.isMaxLength(MAX_RECOVERY_CURSOR_CHARS));

const RecoverySweepCursor = Schema.Struct({
  version: Schema.Literal(2),
  threadId: Schema.optionalKey(ThreadId),
  workCursor: Schema.optionalKey(
    Schema.NonEmptyString.check(Schema.isMaxLength(MAX_WORK_CURSOR_CHARS)),
  ),
  afterThreadId: Schema.optionalKey(ThreadId),
});

const encodeRecoveryCursor = Schema.encodeSync(Schema.fromJsonString(RecoverySweepCursor));

/** Submission decisions and operational faults have different ownership and settlement semantics. */
export class RecoverySweepResult extends Schema.Class<RecoverySweepResult>("RecoverySweepResult")({
  reports: Schema.Array(RecoveryReport),
  /** Exactly one fault per failed Thread, regardless of its queued Submission count. */
  blocked: Schema.Array(RecoveryBlocked),
  workReports: Schema.optionalKey(
    Schema.Array(WorkRecoveryReport).check(Schema.isMaxLength(MAX_RECOVERY_WORK_ITEMS)),
  ),
  /**
   * Resume with the same Thread selection; new work behind the cursor appears on the next scan.
   * Scoped cursors are compact and Thread-bound. Global cursors retain one routing identity,
   * so their size can grow with a supported Thread identity.
   */
  cursor: Schema.optionalKey(RecoveryCursor),
}) {}

/** Recover one bounded page of native work, optionally confined to one Thread. */
export interface RecoverySweepOptions {
  readonly threadId?: ThreadId;
  /** Continue the previous pass with the same Thread selection until its result has no cursor. */
  readonly cursor?: string;
}

/** Per-submission options accepted by `DurableAgentRuntime.submit` (D2). */
export interface DurableSubmitOptions {
  readonly threadId: ThreadId;
  readonly principal: Principal;
  readonly idempotencyKey: IdempotencyKey;
  readonly admissionGroup?: string;
  readonly admissionFence?: AdmissionFence;
  /** Host-prepared worker input; immutable origin and per-input projection parameters. */
  readonly workerAdmission?: WorkerAdmission;
  /** Peer provenance or a worker completion, verified against frozen canonical delivery proof. */
  readonly messageAdmission?: InputMessage;
  /** Application-computed digests of the Agent/Model/Toolkit definitions (see `digestDefinitions`). */
  readonly definitions: DefinitionDigests;
}

export interface DurableObserveOptions {
  /** Resume the observation after an adapter-owned offset previously returned by `observe`. */
  readonly after?: CanonicalRecordEnvelope["offset"] | undefined;
}

/** The structural slice of an Agent Binding that `submit` needs. */
export interface DurableSubmitAgent<InputSchema extends Schema.Top> {
  readonly definition: {
    readonly id: AgentId;
    readonly input: InputSchema;
  };
}

export type DurableSubmitFailure =
  | AgentInputError
  | DigestError
  | AdmissionConflict
  | AdmissionPolicyError
  | LedgerError
  | ThreadStoreError
  | ThreadNotMaterialized
  | AppendConflict
  | FenceRejected
  | DurableRuntimeFailpointError;

/** Attempt resources could not be retained before deliberate approval suspension. */
export class ApprovalSuspensionError extends Schema.TaggedError<ApprovalSuspensionError>()(
  "ApprovalSuspensionError",
  { cause: Schema.Defect() },
) {}

export type DurableWorkerFailure =
  | AdmissionPolicyError
  | ApprovalSuspensionError
  | DigestError
  | LedgerError
  | OwnershipLost
  | SettlementConflict
  | ThreadStoreError
  | ThreadNotMaterialized
  | AppendConflict
  | FenceRejected
  | RunJournalError
  | WorkDiscoveryUnavailable
  | RecoveryBlocked
  | DurableRuntimeFailpointError;

export type DurableAwaitFailure = LedgerError | SettlementConflict | OperationDenied;

/** Typed failures from the public canonical progress boundary. */
export type DurableProgressFailure = ThreadStoreError | ThreadNotMaterialized | OperationDenied;

export type DurableAbortFailure =
  | OperationDenied
  | LedgerError
  | SettlementConflict
  | JoinedToHost
  | DurableRuntimeFailpointError;

/**
 * Failure family of `resolveUnknown` (DUR-017, abort-shaped): the durable intent may conflict
 * with a divergent prior resolution, the Submission may already be settled, an `AbortSubmission`
 * resolution routes through the abort path (whose `JoinedToHost` conflict stays visible), and the
 * failpoint after the intent write is armable like every other durable mutation.
 */
export type DurableResolveFailure =
  | LedgerError
  | SettlementConflict
  | UnknownResolutionConflict
  | JoinedToHost
  | DurableRuntimeFailpointError;

/**
 * Failure family of `resolveApproval` (plan §2.6, abort-shaped): the durable intent may conflict
 * with a divergent prior decision, and the Submission may already be settled. The ledger adapter
 * owns the crash boundaries of the intent write (`ledger:approval-decision:{before,after}`).
 */
export type DurableApprovalFailure = LedgerError | SettlementConflict | ApprovalConflict;

/**
 * Failure family of the read-only `explain`/`explainThread` operations (P7 WP1): pure
 * observation over the two ports plus the evidence assembler's typed failures, and the
 * fail-closed `OperationDenied` when a host-supplied authorizer refuses.
 */
export type DurableExplainFailure =
  | LedgerError
  | ThreadStoreError
  | RunJournalError
  | OperationDenied;

/**
 * Failure family of the read-only `verify` operation (P7 WP1). `ThreadNotMaterialized`
 * stays visible: verifying a Thread that does not exist is a caller error, not an empty
 * report.
 */
export type DurableVerifyFailure =
  | LedgerError
  | ThreadStoreError
  | ThreadNotMaterialized
  | OperationDenied;

/**
 * Failure family of `retry` (P7 WP1): the scoped recovery execution carries the worker failure
 * family, refusals are typed (`RetryRefused` — settled work and lanes owned by the
 * resolveUnknown/resolveApproval paths), and denial is fail-closed.
 */
export type DurableRetryFailure = DurableWorkerFailure | RetryRefused | OperationDenied;

/** Failure family of `scanObligations` (P7 WP1): ledger scan + canonical unknown-age reads. */
export type DurableObligationFailure = LedgerError | ThreadStoreError | OperationDenied;

/**
 * Optional policy-auto approval delegate consulted by the durable approval hook (plan §2.6 step
 * 2) AFTER the recorded-decision lookup misses. An immediate `approved`/`denied` answer becomes
 * canonical (`ToolApprovalRequested` + `ToolApprovalDecided`, one atomic batch) before it is
 * honored; `unresolved` falls through to the durable suspension path. The default (`undefined`)
 * suspends every undecided approval durably until `resolveApproval` decides it — the fail-closed
 * posture. Adapt the P2 `ApprovalResolver` capability stack through
 * `@effect-agent/capabilities`' `toDurableRunApprovalHook`.
 */
export const DurableApprovalResolver: Context.Reference<RunApprovalHook<never, never> | undefined> =
  Context.Reference<RunApprovalHook<never, never> | undefined>(
    "@effect-agent/thread/DurableApprovalResolver",
    { defaultValue: () => undefined },
  );

/**
 * Optional attempt-local retention before an unresolved approval releases ownership. Supply
 * this from the Attempt's Layer, capturing its live resources. The canonical approval request
 * already exists; claim renewal and abort observation remain active while this operation runs.
 * Return only after retention is confirmed. Failure leaves the accepted work owed and preserves
 * its cause; interruption keeps normal resource cleanup. No approval-gated Tool runs here.
 *
 * A decision racing with successful retention resumes the pending batch in a fresh Attempt,
 * after the old services and claim finalize. The default leaves existing suspension unchanged.
 */
export const DurableApprovalSuspension: Context.Reference<
  Effect.Effect<void, ApprovalSuspensionError> | undefined
> = Context.Reference<Effect.Effect<void, ApprovalSuspensionError> | undefined>(
  "@effect-agent/thread/DurableApprovalSuspension",
  { defaultValue: () => undefined },
);

/**
 * Services a durable worker needs beyond the runtime's own Layer: the Agent Binding's inferred
 * requirements minus its supplied identity and history services. The coordinator provides
 * deterministic Run/Turn identity and journal-owned history because its coordinator owns all
 * durable reads and commits across Attempts.
 */
export type DurableWorkerRequirements<
  AgentValue extends Agent.Any,
  InstructionRequirements = never,
> =
  | Exclude<
      AgentRuntimeRequirements<AgentValue, never, InstructionRequirements>,
      ThreadHistory | RunContextPreparation
    >
  | AgentCompletionProjectionRequirements<AgentValue>;

export interface DurableRuntimeConfigOptions {
  /**
   * Deployment identity and the conservative operation version for direct execution without
   * a registered binding. Reuse asserts unchanged handler, Step, and idempotency semantics.
   */
  readonly deploymentId: DeploymentId;
  readonly producerId: ProducerId;
  /** `awaitSettlement` ledger re-check cadence when no wake arrives (default 500ms). */
  readonly settlementPollInterval?: Duration.Duration | undefined;
  /** Worker ownership-lease renewal cadence (default 10s; D5 lease default is 30s). */
  readonly leaseRenewalInterval?: Duration.Duration | undefined;
  /** Active-Run abort-intent poll cadence (default 500ms). */
  readonly abortPollInterval?: Duration.Duration | undefined;
  /** Cooperative bound for one Thread's recovery, including child reads (default 30s). */
  readonly recoveryTimeout?: Duration.Duration | undefined;
  /** Deployment-owned model pricing authority, captured for every recoverable Run. */
  readonly estimateCostMicrousd?: RunCostEstimator | undefined;
}

/** Deployment-scoped identity and liveness cadences for the durable coordinator. */
export class DurableRuntimeConfig extends Context.Service<
  DurableRuntimeConfig,
  {
    readonly deploymentId: DeploymentId;
    readonly producerId: ProducerId;
    readonly settlementPollInterval: Duration.Duration;
    readonly leaseRenewalInterval: Duration.Duration;
    readonly abortPollInterval: Duration.Duration;
    readonly recoveryTimeout: Duration.Duration;
    readonly estimateCostMicrousd?: RunCostEstimator | undefined;
  }
>()("@effect-agent/thread/DurableRuntimeConfig") {
  static make(options: DurableRuntimeConfigOptions): (typeof DurableRuntimeConfig)["Service"] {
    return {
      deploymentId: options.deploymentId,
      producerId: options.producerId,
      settlementPollInterval: options.settlementPollInterval ?? Duration.millis(500),
      leaseRenewalInterval: options.leaseRenewalInterval ?? Duration.seconds(10),
      abortPollInterval: options.abortPollInterval ?? Duration.millis(500),
      recoveryTimeout: options.recoveryTimeout ?? Duration.seconds(30),
      ...(options.estimateCostMicrousd === undefined
        ? {}
        : { estimateCostMicrousd: options.estimateCostMicrousd }),
    };
  }

  static layer(options: DurableRuntimeConfigOptions): Layer.Layer<DurableRuntimeConfig> {
    return Layer.succeed(DurableRuntimeConfig)(DurableRuntimeConfig.make(options));
  }
}

/** Terminal outcome an Attempt decided before terminalization (DUR-011). */
type AttemptOutcome = { readonly uncommittedModelUsage?: ReadonlyArray<ModelCallUsage> } & (
  | {
      readonly _tag: "completed";
      readonly result: PersistedJson;
      /** Schema-encoded application disposition, only for an ordinary completed Run. */
      readonly runDisposition?: PersistedJson;
      /** Set when the Run settled through the final-answer exhaustion resolution (RUN-018). */
      readonly finishReason?: "budget-exhausted";
      /** The dimension that bound; set exactly alongside `finishReason` (RUN-011). */
      readonly exhausted?: ExhaustedLimit;
      readonly usageSummary?: RunUsageSummary;
    }
  | {
      readonly _tag: "failed";
      readonly result: SettlementFailureDiagnostic;
      /** The typed limit of an `AgentPolicyError` failure; absent otherwise (RUN-011). */
      readonly policyLimit?: PolicyLimit;
      readonly usageSummary?: RunUsageSummary;
    }
  | { readonly _tag: "aborted"; readonly usageSummary?: RunUsageSummary }
);

/**
 * What one `runModel` pass produced: a terminal `AttemptOutcome` for terminalization, a
 * durable approval suspension (plan §2.6) — the unresolved call's canonical request is already
 * appended, NO settlement is owed by this pass, and `runAttempt` transitions the ledger — or a
 * durable `waitingForChild` suspension: every non-waiting
 * sibling result is already committed as a per-call late-settle batch and `runAttempt` executes
 * `ledger.suspend(WaitingForChild)`.
 */
type RunPhaseOutcome =
  | AttemptOutcome
  | { readonly _tag: "yielded"; readonly nextSubmissionId?: SubmissionId }
  | { readonly _tag: "suspended"; readonly toolCallId: ToolCallId }
  | { readonly _tag: "suspendedChild"; readonly children: AgentChildPending["children"] };

const approvalSuspension = (toolCallId: ToolCallId): RunPhaseOutcome => ({
  _tag: "suspended",
  toolCallId,
});

const childSuspension = (children: AgentChildPending["children"]): RunPhaseOutcome => ({
  _tag: "suspendedChild",
  children,
});

const abortedRunPhase = (usageSummary: RunUsageSummary): RunPhaseOutcome => ({
  _tag: "aborted",
  usageSummary,
});

/**
 * Internal marker separating coordinator infrastructure failures (fencing, storage, failpoints —
 * the Attempt aborts cleanly and the accepted work stays owed) from Agent Run failures (typed
 * engine errors — the Submission settles `failed`). Never exported: it exists only so the two
 * failure families cannot be confused inside the run phase.
 */
class CoordinatorHalt {
  readonly _tag = "CoordinatorHalt";
  constructor(readonly failure: DurableWorkerFailure) {}
}

const isCoordinatorHaltCause = (
  cause: Cause.Cause<unknown>,
): cause is Cause.Cause<CoordinatorHalt> =>
  cause.reasons.every(
    (reason) => reason._tag !== "Fail" || reason.error instanceof CoordinatorHalt,
  );

const PolicyFailure = Schema.TaggedStruct("AgentPolicyError", {
  limit: PolicyLimit,
});

const decodePolicyFailure = Schema.decodeUnknownOption(PolicyFailure);

const decodePolicyFailureSafely = (error: unknown) => {
  try {
    return decodePolicyFailure(error);
  } catch {
    return Option.none();
  }
};

const agentApprovalPendingOption = (
  cause: Cause.Cause<unknown>,
): Option.Option<AgentApprovalPending> => {
  try {
    // Suspension authority is nominal. Schema.is/decode would accept a forged tagged object.
    const first = cause.reasons.find(Cause.isFailReason);

    return first !== undefined &&
      first.error instanceof AgentApprovalPending &&
      cause.reasons.every(
        (reason) => reason._tag !== "Fail" || reason.error instanceof AgentApprovalPending,
      )
      ? Option.some(first.error)
      : Option.none();
  } catch {
    return Option.none();
  }
};

const agentChildPendingOption = (error: unknown): Option.Option<AgentChildPending> => {
  try {
    // Suspension authority is nominal. Schema.is/decode would accept a forged tagged object.
    return error instanceof AgentChildPending ? Option.some(error) : Option.none();
  } catch {
    return Option.none();
  }
};

const errorMessageOf = (error: unknown): string => {
  const diagnostic = inspectForeignDiagnostic(error);

  return diagnostic.message ?? safeUnknownString(error, "Unknown failure");
};

const errorTagOf = (error: unknown): string =>
  inspectForeignDiagnostic(error).tag ?? "UnknownError";

const nowUtc: Effect.Effect<DateTime.Utc> = Effect.map(Clock.currentTimeMillis, (millis) =>
  DateTime.toUtc(DateTime.makeUnsafe(millis)),
);

const decodePrompt = Schema.decodeUnknownEffect(Prompt.Prompt);
const decodePersisted = Schema.decodeUnknownEffect(PersistedJson);
const encodeCompactionMessageJson = Schema.encodeEffect(Schema.fromJsonString(Prompt.Message));
const decodeCompactionMessageJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));

/** One application Tool Call declared inside a canonical `ModelResponseRecorded`'s messages. */
interface DeclaredApplicationCall {
  readonly id: string;
  readonly name: string;
  /** Encoded JSON parameters exactly as canonical history carries them. */
  readonly params: unknown;
  readonly providerExecuted?: boolean;
}

interface DeclaredToolCalls {
  readonly application: Array<DeclaredApplicationCall>;
  readonly all: Array<DeclaredApplicationCall>;
  readonly providerResults: Array<Prompt.ToolResultPart>;
}

/**
 * Pure inspection of every Tool Call declared in one canonical response. Provider-executed calls
 * retain their terminal results, while only application calls enter the durable declaration/settlement
 * protocol and completion singleton invariant.
 */
const declaredToolCalls = (
  messages: PersistedJson,
): Effect.Effect<DeclaredToolCalls, RunJournalError> =>
  Effect.suspend(() =>
    decodePrompt(messages).pipe(
      Effect.mapError((cause) =>
        RunJournalError.make({
          message: "ModelResponseRecorded messages are not Schema-encoded Prompt messages",
          cause,
        }),
      ),
      Effect.flatMap((prompt) => {
        const application: Array<DeclaredApplicationCall> = [];
        const all: Array<DeclaredApplicationCall> = [];
        const providerResults: Array<Prompt.ToolResultPart> = [];

        for (const message of prompt.content) {
          if (message.role !== "assistant") continue;
          for (const part of message.content) {
            if (part.type === "tool-result" && part.providerExecuted) providerResults.push(part);
            if (part.type !== "tool-call") continue;
            all.push({
              id: part.id,
              name: part.name,
              params: part.params,
              providerExecuted: part.providerExecuted,
            });
            if (!part.providerExecuted) {
              application.push({ id: part.id, name: part.name, params: part.params });
            }
          }
        }

        const providerCalls = new Map(
          all.filter((call) => call.providerExecuted).map((call) => [call.id, call]),
        );

        const resultIds = new Set<string>();

        for (const result of providerResults) {
          if (providerCalls.get(result.id)?.name !== result.name || resultIds.has(result.id)) {
            return Effect.fail(
              RunJournalError.make({
                message: `Invalid canonical provider result ${result.id}`,
              }),
            );
          }
          resultIds.add(result.id);
        }
        for (const id of providerCalls.keys()) {
          if (!resultIds.has(id)) {
            return Effect.fail(
              RunJournalError.make({
                message: `Turn lacks canonical provider result for ${id}`,
              }),
            );
          }
        }

        return Effect.succeed({ application, all, providerResults });
      }),
    ),
  );

/**
 * The declared-but-unsettled Tool batch of one Run's last committed Turn (§2.4 batch resume):
 * every declared call in canonical encoded form plus the recorded results of the calls that
 * already settled (results batch never committed, or per-call late settles from the resolution
 * path). `undefined` when the Run's journal ends at a complete Turn boundary.
 */
interface PendingToolBatch {
  readonly toolResultMaxBytes: number;
  readonly toolOperations: ReadonlyArray<ToolOperation>;
  readonly toolParameterRejections?: ReadonlyArray<ToolParameterRejection> | undefined;
  readonly toolExposure?: Snapshot | undefined;
  readonly turn: number;
  readonly turnId: TurnId;
  readonly calls: ReadonlyArray<DeclaredApplicationCall>;
  readonly settled: ReadonlyArray<{
    readonly id: string;
    readonly result: PersistedJson;
    readonly isFailure: boolean;
    readonly budgetRejected?: true;
    readonly toolSelection?: Selection | undefined;
  }>;
  readonly declaredIds: ReadonlySet<string>;
  readonly responseRecordId: RecordId;
  /** The pending `ModelResponseRecorded.messages`: its pre-assistant slice is the resume's leading messages. */
  readonly messages: PersistedJson;
}

type AttemptAppendContext = RunWriter;

interface RunTiming {
  readonly startedAt: DateTime.Utc;
  readonly deadline: DateTime.Utc;
}

/** Deferred start/context publication belongs to one owning Attempt. */
class CurrentRunStart extends Context.Service<
  CurrentRunStart,
  {
    readonly commit: (
      context?: RecordEnvelope,
    ) => Effect.Effect<
      void,
      Effect.Error<ReturnType<RunWriter["append"]>> | DurableRuntimeFailpointError
    >;
  }
>()("@effect-agent/thread/CurrentRunStart") {}

type PublishSettlement = (
  batch: CanonicalBatch,
) => Effect.Effect<SettlementPublicationResult, SettlementPublicationFailure>;

/** What the resuming worker knows about the ownership period it superseded (durability §9). */
interface AttemptLineage {
  readonly attemptId: AttemptId;
  /** The fencing generation preceding this claim (0 = no prior generation). */
  readonly supersededEpoch: ProducerEpoch;
  /** The canonical `input:{sid}` record existed before this Attempt started. */
  readonly inputWasRecorded: boolean;
}

/** Review of one Submission's open (declared-without-outcome) ordinary Tool Calls (DUR-009). */
interface OpenCallReview {
  /** No proof either way: these calls must become Unknown Outcomes (never auto-replayed). */
  readonly uncertain: Array<OpenToolCallEvidence>;
  /** Reconciliation or current execution support is unproven: defer without marking Unknown. */
  readonly unproven: Array<OpenToolCallEvidence>;
  /** Proven safe to re-execute (never-started / safe-retry / declared idempotent contract). */
  readonly retryable: Array<OpenToolCallEvidence>;
  /** Calls closed canonically with a recovered result (no handler execution). */
  readonly recovered: number;
}

const make = Effect.fnUntraced(function* (bindings: ReadonlyArray<ResolvedBinding>) {
  const registeredBindings = [...bindings];
  const bindingSelection = yield* CurrentBindingSelection;
  const workerAdmissionPort = yield* WorkerAdmissionPort;
  const ledger = yield* SubmissionLedger;
  const submissionScheduling = yield* SubmissionScheduling;
  const store = yield* ThreadStore;
  const reader = ThreadReader.fromStore(store);

  const workDiscovery = yield* ThreadWorkDiscovery.pipe(
    Effect.provide(ThreadWorkDiscovery.layer),
    Effect.provideService(ThreadStore, store),
  );

  const runStorage = yield* RunStorage;
  const deliveries = yield* Effect.serviceOption(MessageDeliveryStore);

  // Disposable: canonical tail and owner identify the exact projection, never ownership.
  // Retain only the most recent view; canonical continuations govern cold recovery.
  let projectedJournal:
    | {
        readonly threadId: ThreadId;
        readonly through: CanonicalSequence;
        readonly runId: RunId;
        readonly contextDigest: string | undefined;
        readonly contextEvidence: ReadonlyArray<JournalRecordEnvelope>;
        readonly journal: RunJournalProjection;
        readonly boundaries: ReadonlyArray<JournalBoundary>;
      }
    | undefined;

  let resolvedContext:
    | { readonly threadId: ThreadId; readonly digest: string; readonly value: RunJournalContext }
    | undefined;

  const wake = yield* WakeScheduler;

  const withProcessing: NonNullable<WakeScheduler["Service"]["withProcessing"]> =
    wake.withProcessing ?? ((_threadId, body) => body);

  const failpoint = yield* DurableRuntimeFailpoint;
  const config = yield* DurableRuntimeConfig;
  const crypto = yield* Crypto.Crypto;
  const reconciler = yield* ToolReconciler;
  const approvalResolver = yield* DurableApprovalResolver;

  // Capture independent host choices at acquisition so worker callers cannot replace them.
  const runContextPreparation = yield* Effect.serviceOption(RunContextPreparation).pipe(
    Effect.map(Option.getOrElse(() => RunContextPreparation.of({}))),
  );

  const runToolAuthorization = yield* RunToolAuthorization;

  const runToolVisibility = yield* RunToolVisibility;

  const runToolScheduling = yield* RunToolScheduling;

  const compactor = yield* Effect.serviceOption(ContextCompactor).pipe(
    Effect.flatMap(
      Option.match({
        onSome: Effect.succeed,
        onNone: () => Effect.provide(ContextCompactor, ContextCompactor.layer),
      }),
    ),
  );

  // Possession-default authorization reference (P7 WP1): the default allows everything —
  // exactly the pre-P7 service-possession boundary — and a host-supplied non-default Layer is
  // consulted fail-closed by observe, the admin operations, and the two resolution paths.
  const operationAuthorizer = yield* OperationAuthorizer;
  // RUN-036: capture once with the runtime, including explicit absence. A worker caller's ambient
  // observer must never replace this host choice on a fresh or replacement Attempt.
  const toolFailureObserver = yield* CurrentToolFailureObserver;

  const withCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>): Effect.Effect<A, E> =>
    Effect.provideService(effect, Crypto.Crypto, crypto);

  const resolutionIntentsFor = (snapshot: RecoverySnapshot) => {
    const intents = new Map<ToolCallId, UnknownResolutionIntent>();

    for (const intent of snapshot.unknownResolutions) {
      if (!intents.has(intent.toolCallId) || unknownResolutionKind(intent.resolution) === "factual")
        intents.set(intent.toolCallId, intent);
    }

    return intents;
  };

  const contractsFor = (definition: Agent.AnyDefinition) => {
    const registered = registeredBindings.filter(
      (binding) => binding.agentId === definition.id && Object.is(binding.definition, definition),
    );

    const current = registered.length === 1 ? registered[0] : undefined;
    const contracts = current?.digests.replay?.tools;

    return contracts === undefined
      ? withCrypto(
          toolReplayContracts(definition, undefined, current?.digests.tools ?? config.deploymentId),
        )
      : Effect.succeed(contracts);
  };

  const resolveCurrentBinding = (submission: SubmissionSnapshot) =>
    resolveWorkerBinding(registeredBindings, submission).pipe(
      Effect.provideService(CurrentBindingSelection, bindingSelection),
    );

  const currentOperationsFor = Effect.fnUntraced(function* (submission: SubmissionSnapshot) {
    const binding = yield* resolveCurrentBinding(submission).pipe(
      Effect.catchTag("BindingUnavailable", () => Effect.succeed(undefined)),
    );

    return binding === undefined
      ? undefined
      : {
          definition: binding.definition,
          contracts: yield* contractsFor(binding.definition),
        };
  });

  const operationsFor = (records: ReadonlyArray<CanonicalRecordEnvelope>, runId: RunId) => {
    const operations = new Map<string, ToolOperation>();

    for (const {
      record: { payload },
    } of records) {
      if (payload._tag !== "ModelResponseRecorded" || payload.runId !== runId) continue;
      for (const operation of payload.toolOperations)
        operations.set(operation.toolCallId, operation);
    }

    return operations;
  };

  const supportsOperation = (
    call: OpenToolCallEvidence,
    operation: ToolOperation | undefined,
    current:
      | {
          readonly definition: Agent.AnyDefinition;
          readonly contracts: Readonly<Record<string, Digest>>;
        }
      | undefined,
  ): boolean => {
    const tool = current?.definition.toolkit.tools[call.toolName];

    return (
      tool !== undefined &&
      operation !== undefined &&
      current?.contracts[call.toolName] === operation.replay &&
      operation.toolName === call.toolName &&
      operation.executionClass === getToolExecutionClass(tool) &&
      operation.executionKind === getToolExecutionKind(tool.annotations)
    );
  };

  const hit = (
    location: DurableRuntimeFailpointLocation,
  ): Effect.Effect<void, DurableRuntimeFailpointError> => failpoint.hit(location);

  const makeEnvelope = (
    recordId: RecordId,
    payload: CanonicalRecordPayload,
  ): Effect.Effect<RecordEnvelope> =>
    Effect.map(nowUtc, (createdAt) =>
      RecordEnvelope.make({
        recordId,
        family: "thread",
        schemaVersion: 1,
        createdAt,
        deploymentId: config.deploymentId,
        payload,
      }),
    );

  const materializeSettlement = (
    settlement: Settlement,
    record: RecordEnvelope | undefined,
  ): Settlement => {
    const common = {
      submissionId: settlement.submissionId,
      settlementId: settlement.settlementId,
      receiptId: settlement.receiptId,
      outcome: settlement.outcome,
      ...(settlement.failure === undefined ? {} : { failure: settlement.failure }),
      settledAt: settlement.settledAt,
    };

    const payload = record?.payload;

    if (
      payload === undefined ||
      payload._tag !== "SubmissionSettled" ||
      payload.submissionId !== settlement.submissionId ||
      payload.settlementId !== settlement.settlementId ||
      payload.receiptId !== settlement.receiptId ||
      payload.outcome !== settlement.outcome
    ) {
      return Settlement.make(common);
    }

    const canonical = {
      ...common,
      ...(payload.usageSummary === undefined ? {} : { usageSummary: payload.usageSummary }),
    };

    if (settlement.outcome !== "completed" || payload.runDisposition === undefined) {
      return Settlement.make(canonical);
    }

    return Settlement.make({
      ...canonical,
      runDisposition: payload.runDisposition,
    });
  };

  /** Canonical owner fields and exact admission identities define the control view. */
  const controlRecords = (submissionIds: ReadonlyArray<SubmissionId>) => {
    const runIds = new Set(submissionIds.map(runIdForSubmission));

    const recordIds = new Set(
      submissionIds.flatMap((id) => [
        submissionInputRecordId(id),
        submissionAbortRecordId(id),
        submissionSettlementRecordId(id),
        runStartedRecordId(runIdForSubmission(id)),
        runCompletedRecordId(runIdForSubmission(id)),
      ]),
    );

    return ({ record }: CanonicalRecordEnvelope): boolean =>
      recordIds.has(record.recordId) ||
      record.payload._tag === "SubagentLineageRecorded" ||
      record.payload._tag === "WorkerOriginRecorded" ||
      canonicalRunIds(record).some((runId) => runIds.has(runId));
  };

  const selectedRange = (
    threadId: ThreadId,
    submissionIds: ReadonlyArray<SubmissionId>,
    through: CanonicalSequence,
    after: CanonicalSequence = ZERO_SEQUENCE,
  ) =>
    Effect.forEach(submissionIds, (submissionId) =>
      Stream.runCollect(runEvidence(threadId, submissionId, through, after)).pipe(
        Effect.provideService(ThreadReader, reader),
      ),
    ).pipe(
      Effect.map((groups) =>
        [...new Map(groups.flat().map((entry) => [entry.sequence, entry])).values()].sort(
          (left, right) => left.sequence - right.sequence,
        ),
      ),
    );

  /**
   * A continuation locates immutable semantic evidence, independently of Thread age or a
   * disposable checkpoint. Missing locators and broken references fail before any mutation.
   */
  const recoveryView = Effect.fnUntraced(function* (
    threadId: ThreadId,
    throughSequence: CanonicalSequence,
    submissionIds: ReadonlyArray<SubmissionId>,
    journalOwner?: RunId,
    retained?: ReadonlyArray<CanonicalRecordEnvelope>,
  ) {
    const identity =
      retained === undefined
        ? yield* reader.readIdentity(ThreadIdentityRequest.make({ threadId }))
        : { records: [] };

    const selected =
      retained === undefined
        ? yield* selectedRange(threadId, submissionIds, throughSequence)
        : retained.filter((entry) => entry.sequence <= throughSequence);

    const byId = new Map(selected.map((entry) => [entry.record.recordId, entry]));
    let context: RunJournalContext | undefined;
    let progress: RunContinuation | undefined;
    let progressThrough: CanonicalSequence | undefined;

    const invalid = (message: string) =>
      ThreadStoreError.make({
        operation: "read Run continuation",
        message,
      });

    for (const submissionId of submissionIds) {
      const runId = runIdForSubmission(submissionId);
      const own = selected.filter((entry) => executionRunIds(entry.record).includes(runId));

      const loaded = yield* readContinuation(threadId, runId, throughSequence).pipe(
        Effect.provideService(ThreadReader, reader),
      );

      if (Option.isNone(loaded)) {
        if (own.some(({ record }) => !isPreContinuationFact(record)))
          return yield* invalid(
            "Run execution has no canonical continuation; repair its locator explicitly",
          );
        continue;
      }
      const envelope = loaded.value;
      const cursor = envelope.continuation;

      if (cursor.submissionId !== submissionId)
        return yield* invalid("Run continuation differs from its admitted owner");

      const resolve = Effect.fnUntraced(function* (ref: typeof cursor.originalInput) {
        const found = byId.get(ref.recordId);

        if (found === undefined) {
          // An exact lookup distinguishes missing evidence from an incomplete native index.
          yield* resolveEvidence(threadId, ref).pipe(
            Effect.provideService(ThreadReader, reader),
            Effect.provideService(Crypto.Crypto, crypto),
          );

          return yield* invalid(
            "Required Run evidence is absent from its locator; rebuild the index explicitly",
          );
        }
        if (
          (yield* reference(found.record).pipe(Effect.provideService(Crypto.Crypto, crypto)))
            .digest !== ref.digest
        )
          return yield* invalid("Run continuation evidence has invalid integrity");
        if (!canonicalRunIds(found.record).includes(runId))
          return yield* invalid("Run continuation references another owner");

        return found;
      });

      const input = yield* resolve(cursor.originalInput);

      if (
        input.record.payload._tag !== "UserInputRecorded" ||
        input.record.payload.kind !== "user" ||
        input.record.payload.submissionId !== submissionId ||
        input.record.payload.runId !== runId
      )
        return yield* invalid("Run continuation has no exact original admitted input");
      const frontier = yield* resolve(cursor.lastFact);

      // An intact frontier does not prove that every earlier locator entry is present.
      if (
        own.reduce((count, entry) => count + (entry.sequence <= frontier.sequence ? 1 : 0), 0) !==
        cursor.recordCount
      )
        return yield* invalid(
          "Run continuation has incomplete canonical evidence; rebuild its locator explicitly",
        );

      if (
        frontier.batchId !== envelope.batchId ||
        frontier.sequence >= envelope.sequence ||
        !validateSuffix(own, envelope.sequence)
      )
        return yield* invalid(
          "Run continuation has an invalid frontier or exceeds its selected suffix bound",
        );
      let saved: RunJournalContext | undefined;

      if (cursor.savedContext !== undefined) {
        const savedEnvelope = yield* resolve(cursor.savedContext);

        if (
          savedEnvelope.record.payload._tag !== "RunContextRecorded" ||
          savedEnvelope.sequence <= input.sequence
        )
          return yield* invalid("Run continuation has invalid saved original context");
        const initial = savedEnvelope.record.payload;
        const cachedContext = resolvedContext;

        if (
          cachedContext?.threadId === threadId &&
          cachedContext.digest === cursor.savedContext.digest
        ) {
          saved = cachedContext.value;
        } else {
          saved = yield* readRunContext(initial, input, cursor.savedContext.digest).pipe(
            Effect.provide(RunContextReader.layer(threadId)),
            Effect.provideService(ThreadReader, reader),
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError(() =>
              invalid("Saved original context cannot be resolved from its evidence"),
            ),
          );
          resolvedContext = { threadId, digest: cursor.savedContext.digest, value: saved };
        }
        if (own.filter(({ record }) => record.payload._tag === "RunContextRecorded").length !== 1)
          return yield* invalid("Run has conflicting saved original contexts");
      }
      if (cursor.latestResponse !== undefined) {
        const response = yield* resolve(cursor.latestResponse);

        if (
          response.record.payload._tag !== "ModelResponseRecorded" ||
          saved === undefined ||
          response.record.payload.turn !== cursor.accounting.committedTurns ||
          own.some(
            (entry) =>
              entry.record.payload._tag === "ModelResponseRecorded" &&
              entry.sequence <= frontier.sequence &&
              entry.sequence > response.sequence,
          )
        )
          return yield* invalid("Run continuation has invalid declared-operation evidence");
      } else if (cursor.accounting.committedTurns !== 0)
        return yield* invalid("Run continuation accounting has no corresponding response");
      if (cursor.terminal !== undefined) {
        const terminal = yield* resolve(cursor.terminal);

        if (
          terminal.record.payload._tag !== "RunCompleted" &&
          terminal.record.payload._tag !== "RunFailed" &&
          terminal.record.payload._tag !== "SubmissionSettled"
        )
          return yield* invalid("Run continuation has invalid terminal evidence");
      }
      if (journalOwner === runId) {
        context = saved;
        progress = cursor;
        progressThrough = frontier.sequence;
      }
    }

    const records = [
      ...new Map(
        [...identity.records.filter((entry) => entry.sequence <= throughSequence), ...selected].map(
          (entry) => [entry.sequence, entry],
        ),
      ).values(),
    ].sort((left, right) => left.sequence - right.sequence);

    return { canonical: Stream.fromIterable(records), context, progress, progressThrough };
  });

  const readControl = Effect.fnUntraced(function* (
    threadId: ThreadId,
    submissionIds: ReadonlyArray<SubmissionId>,
  ) {
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
    const view = yield* recoveryView(threadId, tail.tailSequence, submissionIds);

    return yield* Stream.runCollect(
      view.canonical.pipe(Stream.filter(controlRecords(submissionIds))),
    );
  });

  const readAllTolerant = (
    threadId: ThreadId,
    submissionIds: ReadonlyArray<SubmissionId>,
  ): Effect.Effect<
    { readonly records: ReadonlyArray<CanonicalRecordEnvelope>; readonly materialized: boolean },
    ThreadStoreError
  > =>
    Effect.suspend(() =>
      readControl(threadId, submissionIds).pipe(
        Effect.map((records) => ({ records, materialized: true })),
        Effect.catchTag("ThreadNotMaterialized", () =>
          Effect.succeed({ records: [], materialized: false }),
        ),
      ),
    );

  interface RecoveryHistorySnapshot {
    readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
    readonly materialized: boolean;
    readonly throughSequence: CanonicalSequence;
    readonly submissionIds: ReadonlyArray<SubmissionId>;
  }

  /**
   * Capture one tail and the addressed Runs' bounded control evidence. Recovery retains this
   * disposable snapshot only while repairing the selected owner; unrelated history is not read.
   */
  const readRecoveryHistory = Effect.fnUntraced(function* (
    threadId: ThreadId,
    submissionIds: ReadonlyArray<SubmissionId>,
  ): Effect.fn.Return<RecoveryHistorySnapshot, ThreadStoreError> {
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId })).pipe(
      Effect.map(Option.some),
      Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed(Option.none())),
    );

    if (Option.isNone(tail))
      return { records: [], materialized: false, throughSequence: ZERO_SEQUENCE, submissionIds };

    const records = yield* recoveryView(threadId, tail.value.tailSequence, submissionIds).pipe(
      Effect.flatMap((view) =>
        Stream.runCollect(view.canonical.pipe(Stream.filter(controlRecords(submissionIds)))),
      ),
      Effect.catchTag("ThreadNotMaterialized", (error) =>
        ThreadStoreError.make({
          operation: "read recovery history",
          message: `Thread ${threadId} disappeared after its recovery tail was captured`,
          cause: error,
        }),
      ),
    );

    return { records, materialized: true, throughSequence: tail.value.tailSequence, submissionIds };
  });

  /**
   * A recovery mutation that must distinguish a racing canonical append reads only the suffix
   * beyond its pass snapshot. The append-only prefix remains valid; no full-history retry is
   * needed, and malformed suffix pagination fails through the same typed boundary.
   */
  const refreshRecoveryHistory = Effect.fnUntraced(function* (
    threadId: ThreadId,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    after: CanonicalSequence,
    submissionIds: ReadonlyArray<SubmissionId>,
  ): Effect.fn.Return<ReadonlyArray<CanonicalRecordEnvelope>, DurableWorkerFailure> {
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

    if (tail.tailSequence <= after) return records;

    const suffix = yield* selectedRange(threadId, submissionIds, tail.tailSequence, after);

    return [...records, ...suffix];
  });

  const knownRecordIdsOf = (records: ReadonlyArray<CanonicalRecordEnvelope>): Set<string> =>
    new Set(records.map((envelope) => envelope.record.recordId));

  const settlementPayloadFromRecord = Effect.fnUntraced(function* (
    record: RecordEnvelope,
    submissionId: SubmissionId,
  ): Effect.fn.Return<SubmissionSettledRecord, LedgerError> {
    const payload = record.payload;

    if (
      payload._tag !== "SubmissionSettled" ||
      payload.submissionId !== submissionId ||
      payload.settlementId !== submissionSettlementId(submissionId) ||
      record.recordId !== submissionSettlementRecordId(submissionId)
    ) {
      return yield* LedgerError.make({
        operation: "settlementPayloadFromRecord",
        message: `The canonical record is not the exact Settlement for Submission ${submissionId}`,
      });
    }

    return payload;
  });

  const canonicalSettlementRecord = Effect.fnUntraced(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    submissionId: SubmissionId,
  ): Effect.fn.Return<RecordEnvelope, LedgerError> {
    const record = records.find(
      (envelope) => envelope.record.recordId === submissionSettlementRecordId(submissionId),
    )?.record;

    if (record === undefined) {
      return yield* LedgerError.make({
        operation: "canonicalSettlementRecord",
        message: `Canonical history has no Settlement for Submission ${submissionId}`,
      });
    }
    yield* settlementPayloadFromRecord(record, submissionId);

    return record;
  });

  /**
   * Fold canonical declaration and closure evidence. Unsettled ordinary effects are uncertain
   * unless canonical parameter rejection or a whole-batch approval blocker proves no dispatch.
   * Original operation contracts classify idempotent delegation and orchestration recovery.
   */
  const evidenceFor = Effect.fnUntraced(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    submissionId: SubmissionId,
    materialized: boolean,
    hostSubmissionId?: SubmissionId,
  ): Effect.fn.Return<RecoveryEvidence, RunJournalError | LedgerError> {
    const runId = runIdForSubmission(submissionId);

    const hostRunId =
      hostSubmissionId === undefined ? undefined : runIdForSubmission(hostSubmissionId);

    const inputId = submissionInputRecordId(submissionId);
    const abortId = submissionAbortRecordId(submissionId);
    const settlementId = submissionSettlementRecordId(submissionId);

    const hostSettlementId =
      hostSubmissionId === undefined ? undefined : submissionSettlementRecordId(hostSubmissionId);

    let inputRecorded = false;
    let inputSequence: CanonicalSequence | undefined;
    let abortRecorded = false;
    let subagentLineageRecorded = false;
    let recordedSettlementOutcome: SettlementOutcome | undefined;
    let hostSettlementOutcome: SettlementOutcome | undefined;
    let hostRespondedAfterInput = false;
    const unknownIds = new Set<string>();
    const declarationIds = new Set<string>();
    const responseTurns = new Set<number>();
    const requested: Array<PendingApprovalEvidence> = [];
    const canonicalApprovals = new Map<ToolCallId, ApprovalDecision>();

    let lastResponse:
      | {
          readonly turn: number;
          readonly messages: PersistedJson;
          readonly toolExposure?: Snapshot | undefined;
        }
      | undefined;

    for (const envelope of records) {
      const recordId = envelope.record.recordId;
      const payload = envelope.record.payload;

      if (recordId === inputId) {
        inputRecorded = true;
        inputSequence = envelope.sequence;
        continue;
      }
      if (recordId === abortId) {
        abortRecorded = true;
        continue;
      }
      if (recordId === settlementId && payload._tag === "SubmissionSettled") {
        recordedSettlementOutcome = payload.outcome;
        continue;
      }
      if (
        hostSettlementId !== undefined &&
        recordId === hostSettlementId &&
        payload._tag === "SubmissionSettled"
      ) {
        hostSettlementOutcome = payload.outcome;
        continue;
      }
      switch (payload._tag) {
        case "ToolCallUnknown": {
          if (payload.runId === runId) unknownIds.add(payload.toolCallId);
          break;
        }
        case "ToolApprovalRequested": {
          if (payload.runId !== runId) break;
          requested.push(
            PendingApprovalEvidence.make({ toolCallId: payload.toolCallId, turn: payload.turn }),
          );
          break;
        }
        case "ToolApprovalDecided": {
          if (payload.runId === runId) canonicalApprovals.set(payload.toolCallId, payload.decision);
          break;
        }
        case "SubagentLineageRecorded": {
          // Thread-level fact (one lineage record per child Thread, spec §11):
          // its presence gates the AwaitParentEstablishment row for parent-linked
          // Submissions; root Threads never carry it and never consult it.
          subagentLineageRecorded = true;
          break;
        }
        case "ModelResponseRecorded": {
          if (payload.runId === runId) {
            if (
              responseTurns.has(payload.turn) ||
              recordId !== modelResponseRecordId(runId, payload.turn) ||
              payload.turnId !== turnIdForRun(runId, payload.turn)
            )
              return yield* RunJournalError.make({
                message: "Tool declaration has no unique original response",
              });
            responseTurns.add(payload.turn);
            for (const operation of payload.toolOperations) {
              if (declarationIds.has(operation.toolCallId))
                return yield* RunJournalError.make({
                  message: "Tool Call identity is reused within a Run",
                });
              declarationIds.add(operation.toolCallId);
            }
          }
          if (
            payload.runId === runId &&
            (lastResponse === undefined || payload.turn > lastResponse.turn)
          ) {
            lastResponse = {
              turn: payload.turn,
              messages: payload.messages,
              ...(payload.toolExposure === undefined ? {} : { toolExposure: payload.toolExposure }),
            };
          }
          if (
            hostRunId !== undefined &&
            payload.runId === hostRunId &&
            inputSequence !== undefined &&
            envelope.sequence > inputSequence
          ) {
            hostRespondedAfterInput = true;
          }
          break;
        }
        default: {
          break;
        }
      }
    }

    const operationStates = toolOperationStates(records, runId);

    const operations = new Map<string, ToolOperation>(
      operationStates.map(({ operation }) => [operation.toolCallId, operation]),
    );

    for (const toolCallId of unknownIds)
      if (!operations.has(toolCallId))
        return yield* RunJournalError.make({
          message: "Unknown Tool call has no original declaration",
        });

    const allOpenCalls = operationStates.filter(isUnresolvedToolOperation).map((state) =>
      OpenToolCallEvidence.make({
        toolCallId: state.operation.toolCallId,
        toolName: state.operation.toolName,
        turn: state.turn,
      }),
    );

    // Only the original response's operation contract authorizes idempotent establishment.
    const subagent = subagentRecordsOf(records, runId);

    const isDeclaredDelegation = (call: OpenToolCallEvidence): boolean =>
      operations.get(call.toolCallId)?.executionKind === "delegation";

    const openByCallId = new Map(
      allOpenCalls.filter(isDeclaredDelegation).map((call) => [call.toolCallId, call]),
    );

    const delegationCallIds: Array<ToolCallId> = [];
    const seenDelegationIds = new Set<ToolCallId>();

    const noteDelegation = (toolCallId: ToolCallId): void => {
      if (seenDelegationIds.has(toolCallId)) return;
      seenDelegationIds.add(toolCallId);
      delegationCallIds.push(toolCallId);
    };

    for (const toolCallId of subagent.requested.keys()) noteDelegation(toolCallId);
    for (const toolCallId of subagent.started.keys()) noteDelegation(toolCallId);
    for (const toolCallId of subagent.joined.keys()) noteDelegation(toolCallId);
    for (const toolCallId of delegationCallIds) {
      const request = subagent.requested.get(toolCallId);

      if (request === undefined || operations.get(toolCallId)?.executionKind !== "delegation") {
        return yield* RunJournalError.make({
          message: "Subagent evidence conflicts with original Tool classification",
        });
      }
    }
    for (const call of allOpenCalls) {
      if (isDeclaredDelegation(call)) noteDelegation(call.toolCallId);
    }
    const openDelegationCalls: Array<OpenDelegationCallEvidence> = [];

    for (const toolCallId of delegationCallIds) {
      // Explicit uncertainty must reach ordinary reconciliation, even for a framework call.
      if (unknownIds.has(toolCallId)) continue;
      const open = openByCallId.get(toolCallId);
      const requestedRecord = subagent.requested.get(toolCallId);
      const startedRecord = subagent.started.get(toolCallId);
      // Authoritative admission tri-state for a requested-but-unstarted call (SUB-031): the
      // deterministic idempotency key queries the ledger directly — projection absence is
      // never proof of absence, and only a proven `not-admitted` permits an admission attempt.
      let admission: DelegationAdmissionEvidence | undefined;
      let childSubmissionId = startedRecord?.childSubmissionId;

      if (requestedRecord !== undefined && startedRecord === undefined) {
        const resolution = yield* ledger.resolveAdmission(
          SubmissionLookupByKey.make({
            threadId: requestedRecord.childThreadId,
            principal: decodePrincipalSync(requestedRecord.childPrincipal),
            idempotencyKey: decodeIdempotencyKeySync(requestedRecord.childIdempotencyKey),
          }),
        );

        switch (resolution._tag) {
          case "NotAdmitted": {
            admission = "not-admitted";
            break;
          }
          case "Admitted": {
            admission = "admitted";
            childSubmissionId = resolution.submission.submissionId;
            break;
          }
          case "Indeterminate": {
            admission = "indeterminate";
            break;
          }
        }
      }
      const childThreadId = startedRecord?.childThreadId ?? requestedRecord?.childThreadId;

      openDelegationCalls.push(
        OpenDelegationCallEvidence.make({
          toolCallId,
          toolName:
            open?.toolName ??
            subagent.declaredNames.get(toolCallId) ??
            requestedRecord?.delegationId ??
            "delegate_unknown",
          turn: open?.turn ?? requestedRecord?.turn ?? 1,
          requested: requestedRecord !== undefined,
          started: startedRecord !== undefined,
          joined: subagent.joined.has(toolCallId),
          ...(childThreadId === undefined ? {} : { childThreadId }),
          ...(childSubmissionId === undefined ? {} : { childSubmissionId }),
          ...(admission === undefined ? {} : { admission }),
        }),
      );
    }

    const isDeclaredWorker = (call: OpenToolCallEvidence): boolean =>
      operations.get(call.toolCallId)?.executionKind === "orchestration";

    const openWorkerCalls = allOpenCalls.filter(
      (call) => isDeclaredWorker(call) && !unknownIds.has(call.toolCallId),
    );

    const openToolCalls = allOpenCalls.filter(
      (call) =>
        unknownIds.has(call.toolCallId) || (!isDeclaredDelegation(call) && !isDeclaredWorker(call)),
    );

    let declaredPendingBatch: DeclaredPendingBatchEvidence | undefined;
    const requestedIds = new Set(requested.map((approval) => approval.toolCallId));

    if (lastResponse !== undefined) {
      const turn = lastResponse.turn;

      const pending = operationStates.filter((state) => state.turn === turn && !state.settled);

      if (pending.length > 0)
        declaredPendingBatch = DeclaredPendingBatchEvidence.make({
          turn,
          callCount: pending.length,
          approvals: operationStates
            .filter(
              (state) =>
                state.turn === turn &&
                !state.settled &&
                !state.resolved &&
                requestedIds.has(state.operation.toolCallId),
            )
            .map((state) => {
              const decision = canonicalApprovals.get(state.operation.toolCallId);

              return {
                toolCallId: state.operation.toolCallId,
                ...(decision === undefined ? {} : { decision }),
              };
            }),
        });
    }

    const approvalsPending = requested.filter(
      (pending) => !canonicalApprovals.has(pending.toolCallId),
    );

    return RecoveryEvidence.make({
      threadMaterialized: materialized,
      inputRecorded,
      abortRecorded,
      subagentLineageRecorded,
      openToolCalls,
      openDelegationCalls,
      openWorkerCalls,
      approvalsPending,
      joinedInputCovered: hostSubmissionId === undefined ? true : hostRespondedAfterInput,
      ...(recordedSettlementOutcome === undefined ? {} : { recordedSettlementOutcome }),
      ...(declaredPendingBatch === undefined ? {} : { declaredPendingBatch }),
      ...(hostSettlementOutcome === undefined ? {} : { hostSettlementOutcome }),
    });
  });

  /** Check all declaration identities; validate full owning responses and isolate selected calls. */
  const declaredCallsFor = Effect.fnUntraced(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    runId: RunId,
    selectedIds: ReadonlySet<string>,
  ) {
    const calls = new Map<string, DeclaredToolCall>();
    const turns = new Set<number>();
    const identities = new Set<string>();

    for (const { record } of records) {
      const response = record.payload;

      if (response._tag !== "ModelResponseRecorded" || response.runId !== runId) continue;
      if (
        turns.has(response.turn) ||
        record.recordId !== modelResponseRecordId(runId, response.turn) ||
        response.turnId !== turnIdForRun(runId, response.turn)
      )
        return yield* RunJournalError.make({
          message: "Tool declaration has no unique original response",
        });
      turns.add(response.turn);

      for (const operation of response.toolOperations) {
        if (identities.has(operation.toolCallId))
          return yield* RunJournalError.make({
            message: "Operation evidence differs from the original declaration",
          });
        identities.add(operation.toolCallId);
      }
      if (!response.toolOperations.some((operation) => selectedIds.has(operation.toolCallId)))
        continue;

      const messagesDigest = yield* withCrypto(digestJson(response.messages)).pipe(
        Effect.mapError((cause) =>
          RunJournalError.make({ message: "Cannot verify original Tool response", cause }),
        ),
      );

      if (messagesDigest !== response.messagesDigest)
        return yield* RunJournalError.make({
          message: "Original Tool response has an invalid digest",
        });
      const declared = yield* declaredToolCalls(response.messages);

      for (const operation of response.toolOperations) {
        const matches = declared.application.filter(
          (call) => call.id === operation.toolCallId && call.name === operation.toolName,
        );

        const call = matches[0];

        if (call === undefined || matches.length !== 1)
          return yield* RunJournalError.make({
            message: "Operation evidence differs from the original declaration",
          });
        if (!selectedIds.has(operation.toolCallId)) continue;

        const parameters = yield* decodePersisted(call.params).pipe(
          Effect.map(copyJson),
          Effect.mapError((cause) =>
            RunJournalError.make({ message: "Invalid canonical Tool parameters", cause }),
          ),
        );

        const parametersDigest = yield* withCrypto(digestJson(parameters)).pipe(
          Effect.mapError((cause) =>
            RunJournalError.make({ message: "Cannot verify declared Tool parameters", cause }),
          ),
        );

        calls.set(
          operation.toolCallId,
          DeclaredToolCall.make({
            ...operation,
            runId,
            turnId: response.turnId,
            turn: response.turn,
            parameters,
            parametersDigest,
          }),
        );
      }
      if (response.toolOperations.length !== declared.application.length)
        return yield* RunJournalError.make({
          message: "Declared Tool batch has incomplete operation evidence",
        });
    }

    return calls;
  });

  /**
   * The declared-but-unsettled Tool batch of the Run's LAST committed Turn (§2.4). Keyed on
   * SETTLED coverage deliberately: a call closed by a never-started/safe-retry `ToolCallResolved`
   * has authorized re-execution, so it stays in the resumed batch until its own `ToolCallSettled`
   * exists.
   */
  const pendingToolBatchFor = Effect.fnUntraced(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    runId: ReturnType<typeof runIdForSubmission>,
    completionTools: ReadonlyArray<string> = [],
  ): Effect.fn.Return<PendingToolBatch | undefined, RunJournalError> {
    let lastResponse:
      | {
          readonly turn: number;
          readonly messages: PersistedJson;
          readonly toolOperations: ReadonlyArray<ToolOperation>;
          readonly toolResultMaxBytes: number;
          readonly toolParameterRejections?: ReadonlyArray<ToolParameterRejection> | undefined;
          readonly toolExposure?: Snapshot | undefined;
        }
      | undefined;

    const settledByCallId = new Map<
      string,
      {
        readonly recordId?: RecordId;
        readonly toolName?: string;
        readonly result: PersistedJson;
        readonly isFailure: boolean;
        readonly budgetRejected?: true;
        readonly toolSelection?: Selection | undefined;
      }
    >();

    for (const envelope of records) {
      const payload = envelope.record.payload;

      if (payload._tag === "ModelResponseRecorded" && payload.runId === runId) {
        if (lastResponse === undefined || payload.turn > lastResponse.turn) {
          settledByCallId.clear();
          lastResponse = {
            turn: payload.turn,
            messages: payload.messages,
            toolParameterRejections: payload.toolParameterRejections,
            toolOperations: payload.toolOperations,
            toolResultMaxBytes: payload.toolResultMaxBytes,
            ...(payload.toolExposure === undefined ? {} : { toolExposure: payload.toolExposure }),
          };
        }
        continue;
      }
      if (payload._tag === "ToolCallSettled" && payload.runId === runId) {
        settledByCallId.set(payload.toolCallId, {
          recordId: envelope.record.recordId,
          toolName: payload.toolName,
          result: payload.result,
          isFailure: payload.isFailure,
          ...(payload.budgetRejected === true ? { budgetRejected: true } : {}),
          ...(payload.toolSelection === undefined ? {} : { toolSelection: payload.toolSelection }),
        });
      }
    }
    if (lastResponse === undefined) return undefined;
    const declared = yield* declaredToolCalls(lastResponse.messages);

    if (declared.application.length === 0) return undefined;
    const calls = declared.all;

    const identities = new Set<string>();

    for (const operation of lastResponse.toolOperations) {
      if (
        identities.has(operation.toolCallId) ||
        !declared.application.some(
          (call) => call.id === operation.toolCallId && call.name === operation.toolName,
        )
      )
        return yield* RunJournalError.make({
          message: "Operation evidence differs from the declared Tool batch",
        });
      identities.add(operation.toolCallId);
    }
    if (identities.size !== declared.application.length)
      return yield* RunJournalError.make({
        message: "Declared Tool batch has incomplete operation evidence",
      });

    for (const part of declared.providerResults) {
      const result = yield* decodePersisted(part.result).pipe(
        Effect.mapError((cause) =>
          RunJournalError.make({ message: `Invalid canonical provider result ${part.id}`, cause }),
        ),
      );

      settledByCallId.set(part.id, { result, isFailure: part.isFailure });
    }

    const settled: Array<{
      id: string;
      result: PersistedJson;
      isFailure: boolean;
      budgetRejected?: true;
      toolSelection?: Selection | undefined;
    }> = [];

    for (const call of calls) {
      const recorded = settledByCallId.get(call.id);

      if (call.providerExecuted === true && recorded === undefined) {
        return yield* RunJournalError.make({
          message: `Pending Turn lacks canonical provider result for ${call.id}`,
        });
      }
      if (recorded !== undefined) {
        const { recordId, toolName, ...result } = recorded;

        if (
          call.providerExecuted !== true &&
          (toolName !== call.name ||
            recordId !==
              toolCallSettledRecordId(
                runId,
                lastResponse.turn,
                Schema.decodeSync(ToolCallId)(call.id),
              ))
        )
          return yield* RunJournalError.make({
            message: `Settled Tool ${call.id} differs from its original declaration`,
          });
        settled.push({ id: call.id, ...result });
      }
    }

    const completionCall = declared.application[0];

    const projectSettledCompletion =
      declared.application.length === 1 &&
      completionCall !== undefined &&
      completionTools.includes(completionCall.name) &&
      settledByCallId.get(completionCall.id)?.isFailure === false &&
      !records.some(
        ({ record }) => record.payload._tag === "RunCompleted" && record.payload.runId === runId,
      );

    if (settled.length >= calls.length && !projectSettledCompletion) return undefined;

    return {
      turn: lastResponse.turn,
      turnId: turnIdForRun(runId, lastResponse.turn),
      calls,
      settled,
      declaredIds: new Set(calls.map((call) => call.id)),
      responseRecordId: modelResponseRecordId(runId, lastResponse.turn),
      messages: lastResponse.messages,
      toolParameterRejections: lastResponse.toolParameterRejections,
      toolOperations: lastResponse.toolOperations,
      toolResultMaxBytes: lastResponse.toolResultMaxBytes,
      ...(lastResponse.toolExposure === undefined
        ? {}
        : { toolExposure: lastResponse.toolExposure }),
    };
  });

  /**
   * Records without the pending Turn's response and its partial results: the resumed Attempt's
   * canonical prompt boundary sits BEFORE the pending Turn, whose messages re-enter official
   * history through the engine's batch-resume continuation.
   */
  const withoutPendingBatch = (
    records: Stream.Stream<JournalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>,
    pending: PendingToolBatch,
    runId: ReturnType<typeof runIdForSubmission>,
  ): Stream.Stream<JournalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized> =>
    records.pipe(
      Stream.filter((envelope) => {
        if (envelope.record.recordId === pending.responseRecordId) return false;
        const payload = envelope.record.payload;

        return !(
          payload._tag === "ToolCallSettled" &&
          payload.runId === runId &&
          pending.declaredIds.has(payload.toolCallId)
        );
      }),
    );

  /** Materialize without regressing a fence someone else already advanced. */
  const materializeAtLeast = (
    threadId: ThreadId,
    producerEpoch: ProducerEpoch,
  ): Effect.Effect<void, ThreadStoreError> =>
    store
      .materialize(ThreadMaterialization.make({ threadId, producerEpoch }))
      .pipe(Effect.catchTag("FenceRejected", () => Effect.void));

  const ensureThreadCreated = (
    threadId: ThreadId,
    agentId: AgentId,
    definitions: DefinitionDigests,
  ) =>
    ThreadInitialization.ensureThreadCreated(
      { producerId: config.producerId, deploymentId: config.deploymentId },
      threadId,
      agentId,
      definitions,
    ).pipe(Effect.provideService(ThreadStore, store), Effect.provideService(WakeScheduler, wake));

  type ProgressWriter = Effect.Success<ReturnType<typeof makeProgressWriter>>;
  const progressWriters = new WeakMap<RunWriter, ProgressWriter>();

  const activeProgress = new Map<
    ThreadId,
    { readonly epoch: ProducerEpoch; readonly writer: ProgressWriter }
  >();

  const withProgress = Effect.fnUntraced(function* (writer: RunWriter) {
    const active = activeProgress.get(writer.threadId);

    const progress =
      active?.epoch === writer.producerEpoch
        ? active.writer
        : yield* makeProgressWriter(writer.threadId, config.deploymentId).pipe(
            Effect.provideService(ThreadReader, reader),
            Effect.provideService(Crypto.Crypto, crypto),
          );

    const owned: RunWriter = {
      ...writer,
      append: (batch) =>
        progress.commit(batch).pipe(Effect.provideService(CurrentRunWriter, writer)),
    };

    progressWriters.set(owned, progress);

    return { owned, progress };
  });

  const attemptContextFor = (threadId: ThreadId, producerEpoch: ProducerEpoch) =>
    makeRunWriter(threadId, producerEpoch).pipe(
      Effect.provideService(ThreadStore, store),
      Effect.flatMap(withProgress),
      Effect.map(({ owned }) => owned),
    );

  const withProgressSession = Effect.fnUntraced(function* (session: RunStorageSession) {
    const { owned, progress } = yield* withProgress(session);

    activeProgress.set(session.threadId, { epoch: session.producerEpoch, writer: progress });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (activeProgress.get(session.threadId)?.writer === progress)
          activeProgress.delete(session.threadId);
      }),
    );

    const wrapped: RunStorageSession = {
      ...session,
      append: owned.append,
      publishSettlement: (batch) =>
        progress.publish(batch).pipe(Effect.provideService(CurrentRunSettlement, session)),
    };

    progressWriters.set(wrapped, progress);

    return wrapped;
  });

  // Preparations and emitted updates use the same progress gate as their active Run. This
  // also covers a destination handoff written before dispatch or after Run settlement.
  const progressStore = ThreadStore.of({
    ...store,
    append: (request) =>
      Effect.gen(function* () {
        const active = activeProgress.get(request.threadId);

        const progress =
          active?.epoch === request.producerEpoch
            ? active.writer
            : yield* makeProgressWriter(request.threadId, config.deploymentId).pipe(
                Effect.provideService(ThreadReader, reader),
                Effect.provideService(Crypto.Crypto, crypto),
              );

        let tail = {
          sequence: request.expectedTailSequence,
          digest: request.expectedTailDigest,
        };

        return yield* progress.commit(request.batch).pipe(
          Effect.provideService(CurrentRunWriter, {
            threadId: request.threadId,
            tail: Effect.sync(() => tail),
            append: (batch) =>
              store
                .append(
                  FencedAppendRequest.make({
                    ...request,
                    expectedTailSequence: tail.sequence,
                    expectedTailDigest: tail.digest,
                    batch,
                  }),
                )
                .pipe(
                  Effect.tapErrorTag("AppendConflict", (conflict) =>
                    Effect.sync(() => {
                      if (
                        conflict.reason === "tail" &&
                        conflict.actualTailSequence !== undefined &&
                        conflict.actualTailDigest !== undefined
                      )
                        tail = {
                          sequence: conflict.actualTailSequence,
                          digest: conflict.actualTailDigest,
                        };
                    }),
                  ),
                ),
          }),
        );
      }),
  });

  const recoveryOwnership = (
    threadId: ThreadId,
    submissionId: SubmissionId,
    ownershipToken: OwnershipToken,
  ) =>
    bindRunOwnership(threadId, submissionId, Effect.succeed(ownershipToken)).pipe(
      Effect.provideService(SubmissionLedger, ledger),
    );

  /** Administrative publication rechecks its explicit authority under the canonical writer. */
  const attemptContextAtTail = Effect.fnUntraced(function* (threadId: ThreadId) {
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

    return yield* attemptContextFor(threadId, tail.producerEpoch);
  });

  const appendBatch = (ctx: Pick<RunWriter, "threadId" | "append">, batch: CanonicalBatch) =>
    ctx.append(batch).pipe(Effect.tap(() => wake.notify(ctx.threadId, "progress")));

  const publicationFor =
    (
      ctx: AttemptAppendContext,
      submissionId: SubmissionId,
      authority: SettlementPublicationAuthority,
    ): PublishSettlement =>
    (batch) =>
      Effect.suspend(() => {
        const progress = progressWriters.get(ctx);

        if (progress === undefined)
          return Effect.fail(
            ThreadStoreError.make({
              operation: "publish Run continuation",
              message: "Settlement has no scoped progress writer",
            }),
          );

        return progress.publish(batch).pipe(
          Effect.provideService(CurrentRunSettlement, {
            threadId: ctx.threadId,
            tail: ctx.tail,
            publishSettlement: (prepared) =>
              Effect.gen(function* () {
                let tail = yield* ctx.tail;

                for (let retries = 0; ; retries++) {
                  const result = yield* runStorage
                    .publishSettlement(
                      SettlementPublication.make({
                        submissionId,
                        authority,
                        append: FencedAppendRequest.make({
                          threadId: ctx.threadId,
                          producerEpoch: ctx.producerEpoch,
                          expectedTailSequence: tail.sequence,
                          expectedTailDigest: tail.digest,
                          batch: prepared,
                        }),
                      }),
                    )
                    .pipe(
                      Effect.catchTag("AppendConflict", (conflict) => {
                        if (
                          conflict.reason !== "tail" ||
                          conflict.actualTailSequence === undefined ||
                          conflict.actualTailDigest === undefined ||
                          retries >= 8
                        )
                          return Effect.fail(conflict);
                        tail = {
                          sequence: conflict.actualTailSequence,
                          digest: conflict.actualTailDigest,
                        };

                        if (
                          prepared.records.some(({ payload }) => payload._tag === "RunContinuation")
                        )
                          return ctx.checkFence.pipe(Effect.andThen(Effect.fail(conflict)));

                        return Effect.succeed(undefined);
                      }),
                    );

                  if (result === undefined) continue;
                  yield* ctx.checkFence;

                  return result;
                }
              }),
          }),
        );
      });

  /** Append the canonical `AbortRequested` record; an identity conflict means it already exists. */
  const appendAbortRecord = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    intent: AbortIntent,
  ) {
    const envelope = yield* makeEnvelope(
      submissionAbortRecordId(intent.submissionId),
      AbortRequested.make({
        submissionId: intent.submissionId,
        author: intent.author,
        reason: intent.reason,
      }),
    );

    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: submissionAbortBatchId(intent.submissionId),
        producerId: config.producerId,
        records: [envelope],
      }),
    ).pipe(
      Effect.catchTag("AppendConflict", () => Effect.void),
      Effect.asVoid,
    );
  });

  /**
   * Append the canonical `ToolCallUnknown` audit records for the given open calls, grouped into
   * one batch per Turn (batch `mark-unknown:{sid}:{turn}`). Canonical-only: the operational
   * ledger marking is a separate step so abort settlement can record the uncertainty WITHOUT
   * blocking the lane (durability §13: abort never asserts external rollback).
   */
  const appendUnknownRecords = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submissionId: SubmissionId,
    knownIds: Set<string>,
    calls: ReadonlyArray<OpenToolCallEvidence>,
    reason: string,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const runId = runIdForSubmission(submissionId);
    const byTurn = new Map<number, Array<OpenToolCallEvidence>>();

    for (const call of calls) {
      const group = byTurn.get(call.turn);

      if (group === undefined) byTurn.set(call.turn, [call]);
      else group.push(call);
    }
    for (const [turn, group] of byTurn) {
      const missing = group.filter(
        (call) => !knownIds.has(toolCallUnknownRecordId(runId, turn, call.toolCallId)),
      );

      const first = missing[0];

      if (first === undefined) continue;
      const envelopes: Array<RecordEnvelope> = [];

      for (const call of missing) {
        envelopes.push(
          yield* makeEnvelope(
            toolCallUnknownRecordId(runId, turn, call.toolCallId),
            ToolCallUnknown.make({
              runId,
              turn: call.turn,
              toolCallId: call.toolCallId,
              toolName: call.toolName,
              reason,
            }),
          ),
        );
      }
      const head = envelopes[0];

      if (head === undefined) continue;
      // An identity conflict means another pass already recorded the same uncertainty.
      yield* appendBatch(
        ctx,
        CanonicalBatch.make({
          batchId: markUnknownBatchId(submissionId, turn),
          producerId: config.producerId,
          records: [head, ...envelopes.slice(1)],
        }),
      ).pipe(
        Effect.catchTag("AppendConflict", () => Effect.void),
        Effect.asVoid,
      );
      for (const call of missing) {
        knownIds.add(toolCallUnknownRecordId(runId, turn, call.toolCallId));
      }
    }
  });

  /**
   * Close the result and its resolution audit in one canonical commit. Removing operation
   * membership cannot strand the accepted resolution between two independent appends.
   */
  const appendClosedCall = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submissionId: SubmissionId,
    knownIds: Set<string>,
    call: OpenToolCallEvidence,
    closure: {
      readonly result?: { readonly value: PersistedJson; readonly isFailure: boolean } | undefined;
      readonly resolution: ToolCallResolution;
      readonly author: string;
      readonly reason: string;
    },
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const runId = runIdForSubmission(submissionId);

    const envelopes: Array<RecordEnvelope> = [];
    const settledId = toolCallSettledRecordId(runId, call.turn, call.toolCallId);

    if (closure.result !== undefined) {
      if (!knownIds.has(settledId)) {
        envelopes.push(
          yield* makeEnvelope(
            settledId,
            ToolCallSettled.make({
              runId,
              toolCallId: call.toolCallId,
              toolName: call.toolName,
              result: closure.result.value,
              isFailure: closure.result.isFailure,
            }),
          ),
        );
      }
    }
    const resolvedId = toolCallResolvedRecordId(runId, call.turn, call.toolCallId);

    if (!knownIds.has(resolvedId)) {
      envelopes.push(
        yield* makeEnvelope(
          resolvedId,
          ToolCallResolved.make({
            runId,
            toolCallId: call.toolCallId,
            resolution: closure.resolution,
            author: closure.author,
            reason: closure.reason,
          }),
        ),
      );
    }
    const [first, ...rest] = envelopes;

    if (first === undefined) return;
    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: envelopes.some(({ recordId }) => recordId === settledId)
          ? toolCallResultBatchId(runId, call.turn, call.toolCallId)
          : toolCallResolutionBatchId(submissionId, call.toolCallId),
        producerId: config.producerId,
        records: [first, ...rest],
      }),
    ).pipe(
      Effect.catchTag("AppendConflict", () =>
        Effect.forEach(
          envelopes,
          (expected) =>
            getRecord({ threadId: ctx.threadId, recordId: expected.recordId }).pipe(
              Effect.provideService(ThreadReader, reader),
              Effect.flatMap((actual) =>
                Option.isSome(actual) &&
                Schema.toEquivalence(RecordEnvelope.fields.payload)(
                  actual.value.record.payload,
                  expected.payload,
                )
                  ? Effect.void
                  : ThreadStoreError.make({
                      operation: "resolve operation",
                      message: "The factual closure did not commit under its original identity",
                    }),
              ),
            ),
          { discard: true },
        ),
      ),
      Effect.asVoid,
    );
    for (const envelope of envelopes) knownIds.add(envelope.recordId);
  });

  /**
   * Reconcile-then-mark (durability §10, DUR-009) over one Submission's open ordinary Tool Calls.
   *
   * Per open call, in order of authority: a durable DUR-017 resolution intent is applied
   * canonically (recovered results settle without execution; never-happened/safe-retry authorize
   * re-execution through the batch resume); a declared `idempotent` execution class is the
   * external idempotency contract and needs no proof; otherwise the registered `ToolReconciler`
   * is consulted — `NeverStarted`/`SafeToRetry` are point-in-time proofs (nothing is recorded;
   * every later pass re-proves them), `CompletedWithResult` becomes canonical supplier truth,
   * `Uncertain` collects for Unknown marking, and a reconciler FAILURE is no proof at all (the
   * call stays open and blocked without being marked). Nothing here ever executes a handler.
   */
  const reconcileOpenCalls = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    snapshot: RecoverySnapshot,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    openCalls: ReadonlyArray<OpenToolCallEvidence>,
    knownIds: Set<string>,
    current?: {
      readonly definition: Agent.AnyDefinition;
      readonly contracts: Readonly<Record<string, Digest>>;
    },
  ): Effect.fn.Return<OpenCallReview, DurableWorkerFailure> {
    const submissionId = submission.submissionId;
    const runId = runIdForSubmission(submissionId);

    yield* pendingToolBatchFor(records, runId);
    const operations = operationsFor(records, runId);

    const declaredByCallId = yield* declaredCallsFor(
      records,
      runId,
      new Set(openCalls.map((call) => call.toolCallId)),
    );

    for (const call of openCalls) {
      const declared = declaredByCallId.get(call.toolCallId);

      if (
        declared === undefined ||
        declared.turn !== call.turn ||
        declared.toolName !== call.toolName
      )
        return yield* RunJournalError.make({
          message: `Tool ${call.toolCallId} has no unique original declaration`,
        });
    }

    const intents = resolutionIntentsFor(snapshot);

    const review: OpenCallReview = { uncertain: [], unproven: [], retryable: [], recovered: 0 };
    let recovered = 0;

    for (const call of openCalls) {
      const declared = declaredByCallId.get(call.toolCallId);
      const operation = operations.get(call.toolCallId);

      const supported = supportsOperation(call, operation, current);

      const intent = intents.get(call.toolCallId);
      const author = intent?.author ?? RECONCILER_AUTHOR;

      const reason =
        intent?.reason ?? "A registered reconciliation policy recovered the external outcome";

      const neverStarted = Effect.gen(function* () {
        if (current === undefined) {
          // Low-level drivers supply their current Agent only when claiming. Missing metadata
          // cannot prove retirement or authorize execution; the claimant checks the contract.
          review.unproven.push(call);

          return;
        }
        if (supported) {
          review.retryable.push(call);

          return;
        }

        const unavailable = yield* Schema.encodeEffect(ToolUnavailable)(
          ToolUnavailable.make({
            toolName: call.toolName,
            execution: "not-executed",
            message:
              "This operation never started and its original implementation is unavailable. Continue with the current tools.",
          }),
        ).pipe(Effect.flatMap(decodePersisted), Effect.orDie);

        yield* hit("tools:before-unavailable-append");
        yield* appendClosedCall(ctx, submissionId, knownIds, call, {
          result: { value: unavailable, isFailure: true },
          resolution: "never-started",
          author,
          reason,
        });
        yield* hit("tools:after-unavailable-append");
        recovered += 1;
      });

      if (intent !== undefined) {
        switch (intent.resolution._tag) {
          case "AbortSubmission":
            continue;
          case "CompletedWithResult": {
            yield* appendClosedCall(ctx, submissionId, knownIds, call, {
              result: { value: intent.resolution.result, isFailure: intent.resolution.isFailure },
              resolution: intent.resolution.isFailure
                ? "failed-with-error"
                : "completed-with-result",
              author,
              reason,
            });
            recovered += 1;
            continue;
          }
          case "NeverHappened": {
            yield* neverStarted;
            continue;
          }
          case "SafeToRetry": {
            // Permission to repeat the original operation is not proof it never happened.
            if (current === undefined) review.unproven.push(call);
            else if (supported) review.retryable.push(call);
            else review.uncertain.push(call);
            continue;
          }
        }
      }
      const tool = current?.definition.toolkit.tools[call.toolName];

      if (current === undefined && operation?.executionClass === "idempotent") {
        review.unproven.push(call);
        continue;
      }
      if (supported && tool !== undefined && getToolExecutionClass(tool) === "idempotent") {
        review.retryable.push(call);
        continue;
      }
      if (declared === undefined) {
        review.unproven.push(call);
        continue;
      }

      const reconciled = yield* reconciler
        .reconcile(
          DeclaredToolCallEvidence.make({
            threadId: submission.threadId,
            submissionId,
            ...declared,
          }),
        )
        .pipe(
          Effect.map(Option.some),
          Effect.catchTag("ToolReconcilerError", () => Effect.succeed(Option.none())),
        );

      if (Option.isNone(reconciled)) {
        review.unproven.push(call);
        continue;
      }
      const decision = reconciled.value;

      switch (decision._tag) {
        case "CompletedWithResult": {
          yield* appendClosedCall(ctx, submissionId, knownIds, call, {
            result: { value: decision.result, isFailure: decision.isFailure },
            resolution: decision.isFailure ? "failed-with-error" : "completed-with-result",
            author,
            reason,
          });
          recovered += 1;
          continue;
        }
        case "NeverStarted": {
          yield* neverStarted;
          continue;
        }
        case "SafeToRetry": {
          if (current === undefined) review.unproven.push(call);
          else if (supported) review.retryable.push(call);
          else review.uncertain.push(call);
          continue;
        }
        case "Uncertain": {
          review.uncertain.push(call);
          continue;
        }
      }
    }

    return { ...review, recovered };
  });

  /**
   * Record the Unknown Outcomes durably: canonical `ToolCallUnknown` audit records FIRST
   * (history is the recovery truth, DUR-015), then the ownership-free ledger marking that blocks
   * the lane until the authorized DUR-017 resolution path covers every marked call.
   */
  const markCallsUnknown = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submissionId: SubmissionId,
    knownIds: Set<string>,
    calls: ReadonlyArray<OpenToolCallEvidence>,
    reason: string,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const first = calls[0];

    if (first === undefined) return;
    yield* appendUnknownRecords(ctx, submissionId, knownIds, calls, reason);
    yield* ledger.markUnknown(
      MarkUnknownRequest.make({
        submissionId,
        toolCallIds: [first.toolCallId, ...calls.slice(1).map((call) => call.toolCallId)],
        reason,
      }),
    );
  });

  /**
   * Plan step 2: append the deterministic `UserInputRecorded` record (batch idempotency makes it
   * exactly-once canonical, DUR-007) and mark it applied. When canonical history already carries
   * the record — an earlier Attempt crashed after its append — only the ledger marker is repaired
   * (DUR-015/DUR-016).
   */
  const applyCanonicalInput = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    ownership: RunOwnership,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    inputApplied: RecoverySnapshot["inputApplied"],
  ) {
    const submissionId = submission.submissionId;
    const recordId = submissionInputRecordId(submissionId);
    const existing = records.find((envelope) => envelope.record.recordId === recordId);

    if (existing !== undefined) {
      if (inputApplied === undefined) {
        yield* ownership.markInputApplied({ recordId, sequence: existing.sequence });
      }

      return;
    }

    const envelope = yield* makeEnvelope(
      recordId,
      UserInputRecorded.make({
        submissionId,
        kind: "user",
        runId: runIdForSubmission(submissionId),
        input: submission.inputPayload,
        ...(submission.messageAdmission === undefined
          ? {}
          : { messageAdmission: submission.messageAdmission }),
      }),
    );

    const result = yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: submissionInputBatchId(submissionId),
        producerId: config.producerId,
        records: [envelope],
      }),
    );

    yield* hit("input:after-canonical-append");
    yield* ownership.markInputApplied({ recordId, sequence: result.firstSequence });
  });

  const canonicalRunStartFromRecords = Effect.fnUntraced(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    runId: RunId,
  ) {
    const recordId = runStartedRecordId(runId);

    const starts = records.filter(
      ({ record }) => record.payload._tag === "RunStarted" && record.payload.runId === runId,
    );

    const existing = records.find(({ record }) => record.recordId === recordId);

    if (
      starts.length > 1 ||
      (starts.length === 1 && starts[0]?.record.recordId !== recordId) ||
      (existing !== undefined &&
        (existing.record.payload._tag !== "RunStarted" || existing.record.payload.runId !== runId))
    ) {
      return yield* RunJournalError.make({
        message: `Run ${runId} has conflicting start evidence`,
      });
    }

    return existing?.record;
  });

  const makeRunStart = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
    maxDurationMillis: number,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
  ) {
    const writer = yield* CurrentRunWriter;
    const runId = runIdForSubmission(submission.submissionId);
    const recordId = runStartedRecordId(runId);
    const existing = yield* canonicalRunStartFromRecords(records, runId);
    let start: RecordEnvelope;
    let allowance = maxDurationMillis;
    let committed = existing !== undefined;

    if (existing !== undefined && existing.payload._tag === "RunStarted") {
      allowance = existing.payload.maxDurationMillis;
      start = existing;
    } else {
      const executionStarted = records.some(
        ({ record: { payload } }) =>
          payload._tag !== "UserInputRecorded" &&
          payload._tag !== "RunContinuation" &&
          "runId" in payload &&
          payload.runId === runId,
      );

      if (executionStarted) {
        return yield* RunJournalError.make({
          message: `Run ${runId} has execution records but no canonical start; retain the request for evidence repair`,
        });
      }
      start = yield* makeEnvelope(
        recordId,
        RunStartedRecord.make({ runId, maxDurationMillis, policyAccountingVersion: 1 }),
      );
    }

    // The first model dispatch commits start and evaluated context in one existing boundary.
    // If preparation fails first, terminal handling still publishes the start exactly once.
    const commit = Effect.fnUntraced(function* (context?: RecordEnvelope) {
      if (committed) {
        if (context !== undefined)
          yield* appendBatch(
            writer,
            CanonicalBatch.make({
              batchId: decodeBatchIdSync(context.recordId),
              producerId: config.producerId,
              records: [context],
            }),
          );

        return;
      }
      yield* hit("run:before-start-append");
      yield* appendBatch(
        writer,
        CanonicalBatch.make({
          batchId: runStartedBatchId(runId),
          producerId: config.producerId,
          records: [start, ...(context === undefined ? [] : [context])],
        }),
      );
      committed = true;
      yield* hit("run:after-start-append");
    });

    return {
      publication: CurrentRunStart.of({ commit }),
      timing: {
        startedAt: start.createdAt,
        // Downtime is not execution. Reuse the original per-Attempt allowance without
        // moving the Run start or resetting journaled turn, Tool or cost accounting.
        deadline: records.some(
          ({ record }) =>
            record.payload._tag === "RunDurationExhausted" && record.payload.runId === runId,
        )
          ? start.createdAt
          : DateTime.addDuration(yield* DateTime.now, Duration.millis(allowance)),
      } satisfies RunTiming,
    };
  });

  /**
   * Cross-lane drive-forward after one child Submission settles (spec §12 step 10): the child's
   * canonical Settlement is already durable, so the idempotent `recordChildSettled` wake is the
   * durable notification — `suspended(WaitingForChild) → input-applied` once every listed child
   * settled — and the parent-lane wake hint is liveness only. Invoked after the exact canonical
   * append but BEFORE child ledger finalization: once a child may become terminal, its parent's
   * durable marker and maintenance generation already exist. Finalization replays the same
   * notification for single-store parent re-suspension races; `ResumeWaitingParent` remains the
   * repair for older data and any already-recorded canonical settlement.
   */
  const notifyParentOfChildSettlement = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
    record: RecordEnvelope,
  ): Effect.fn.Return<void, LedgerError> {
    const admission = submission.messageAdmission;
    const worker = submission.workerAdmission;

    const key =
      admission === undefined
        ? worker === undefined
          ? undefined
          : {
              ownerThreadId: worker.origin.source.threadId,
              messageId: worker.messageId,
            }
        : Schema.is(MessageAdmission)(admission)
          ? admission.message
          : {
              ownerThreadId:
                admission._tag === "WorkerCompletion"
                  ? admission.report.worker.threadId
                  : admission.worker.threadId,
              messageId: submission.idempotencyKey,
            };

    // Commit the destination's exact canonical acknowledgement at the source before
    // making the ledger terminal. A failed/lost response is replayed during recovery.
    if (key !== undefined && Option.isSome(deliveries)) {
      const settled = yield* settlementPayloadFromRecord(record, submission.submissionId);

      yield* deliveries.value
        .change(key, {
          _tag: "Complete",
          nowMillis: yield* Clock.currentTimeMillis,
          admissionKey: submission.idempotencyKey,
          inputDigest: submission.inputDigest,
          receipt: Receipt.make({
            threadId: submission.threadId,
            submissionId: submission.submissionId,
            receiptId: submission.receiptId,
            queueSequence: submission.queueSequence,
          }),
          settlement: Settlement.make({
            submissionId: settled.submissionId,
            receiptId: settled.receiptId,
            settlementId: settled.settlementId,
            outcome: settled.outcome,
            settledAt: record.createdAt,
            ...(settled.outcome === "failed" ? { failure: settled.result } : {}),
            ...(settled.usageSummary === undefined ? {} : { usageSummary: settled.usageSummary }),
            ...(settled.runDisposition === undefined
              ? {}
              : { runDisposition: settled.runDisposition }),
          }),
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (error) =>
                LedgerError.make({
                  operation: "delivery-completion",
                  message: "Destination acknowledgement remains pending",
                  cause: error,
                }),
              ),
            ),
          ),
        );
    }
    if (submission.workerAdmission?.origin.reporting?.mode === "standard")
      yield* updateRuntime.repairRun(submission.threadId, submission.submissionId).pipe(
        Effect.mapError(() =>
          LedgerError.make({
            operation: "update-delivery",
            message: "Update delivery remains pending",
          }),
        ),
      );
    yield* workerRuntime
      .completeInput(submission)
      .pipe(
        Effect.mapError((cause) =>
          LedgerError.make({ operation: "worker-completion", message: cause.reason, cause }),
        ),
      );
    const linkage = submission.parentLinkage;

    if (linkage === undefined) return;
    yield* ledger.recordChildSettled(
      ChildSettledNotification.make({
        parentSubmissionId: linkage.parentSubmissionId,
        childSubmissionId: submission.submissionId,
      }),
    );

    const parent = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: linkage.parentSubmissionId }),
    );

    if (Option.isSome(parent)) {
      yield* wake.notify(parent.value.threadId);
    }
  });

  const settleOneJoined = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    hostSettlement: SubmissionSettledRecord,
    joined: RecoverySnapshot,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    const hostSubmissionId = hostSettlement.submissionId;
    const outcome = hostSettlement.outcome;
    const submission = joined.submission;
    const submissionId = submission.submissionId;
    const settlementId = submissionSettlementId(submissionId);

    const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)(
      SubmissionSettled.make({
        submissionId,
        settlementId,
        receiptId: submission.receiptId,
        outcome,
        runId: runIdForSubmission(hostSubmissionId),
        ...(hostSettlement.outcome === "failed" ? { result: hostSettlement.result } : {}),
      }),
    ).pipe(Effect.orDie);

    const envelope = yield* makeEnvelope(submissionSettlementRecordId(submissionId), payload);

    yield* hit("terminalize:before-publication");

    const published = yield* publicationFor(ctx, submissionId, {
      _tag: "Joined",
      hostSubmissionId,
    })(
      CanonicalBatch.make({
        batchId: submissionSettlementBatchId(submissionId),
        producerId: config.producerId,
        records: [envelope],
      }),
    );

    const record = published.record;

    yield* wake.notify(ctx.threadId, "progress");
    yield* hit("terminalize:after-canonical-append");
    yield* notifyParentOfChildSettlement(submission, record);

    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({ submissionId, settlementId }),
    );

    yield* wake.notify(submission.threadId);
    yield* notifyParentOfChildSettlement(submission, record);

    return settlement;
  });

  /** Every joined receipt publishes the host outcome; canonical history repairs any lost finalization. */
  const settleJoinedSubmissions = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    hostSettlement: SubmissionSettledRecord,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const hostSubmissionId = hostSettlement.submissionId;

    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: hostSubmissionId }),
    );

    for (const join of snapshot.joins) {
      if (join.state !== "joined") continue;

      const joinedSnapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: join.submissionId }),
      );

      if (joinedSnapshot.submission.state === "settled") continue;
      yield* settleOneJoined(ctx, hostSettlement, joinedSnapshot);
    }
  });

  /**
   * Terminalization: publish the single canonical settlement, complete notifications, then
   * finalize its ledger projection, release the lane, and hint waiters.
   * Submissions joined to this host Run settle with the host outcome immediately after
   * (plan §2.5); recovery completes any prefix of that loop.
   */
  const terminalize = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    publishSettlement: PublishSettlement,
    outcome: AttemptOutcome,
    includeRunId: boolean,
    afterCanonical?: Effect.Effect<void, DurableWorkerFailure>,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    const submissionId = submission.submissionId;
    const settlementId = submissionSettlementId(submissionId);

    const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)(
      SubmissionSettled.make({
        submissionId,
        settlementId,
        receiptId: submission.receiptId,
        outcome: outcome._tag,
        ...(includeRunId ? { runId: runIdForSubmission(submissionId) } : {}),
        ...(outcome._tag === "aborted" ? {} : { result: outcome.result }),
        ...(includeRunId &&
        outcome._tag === "completed" &&
        outcome.finishReason === undefined &&
        outcome.runDisposition !== undefined
          ? { runDisposition: outcome.runDisposition }
          : {}),
        ...(outcome._tag === "completed" && outcome.finishReason !== undefined
          ? { finishReason: outcome.finishReason }
          : {}),
        ...(outcome._tag === "completed" && outcome.exhausted !== undefined
          ? { exhausted: outcome.exhausted }
          : {}),
        ...(outcome._tag === "failed" && outcome.policyLimit !== undefined
          ? { policyLimit: outcome.policyLimit }
          : {}),
        ...(outcome.usageSummary === undefined ? {} : { usageSummary: outcome.usageSummary }),
        ...(includeRunId &&
        outcome.uncommittedModelUsage !== undefined &&
        outcome.uncommittedModelUsage.length > 0
          ? { uncommittedModelUsage: outcome.uncommittedModelUsage }
          : {}),
      }),
    ).pipe(Effect.orDie);

    const record = yield* makeEnvelope(submissionSettlementRecordId(submissionId), payload);

    yield* hit("terminalize:before-publication");

    const published = yield* publishSettlement(
      CanonicalBatch.make({
        batchId: submissionSettlementBatchId(submissionId),
        producerId: config.producerId,
        records: [record],
      }),
    );

    yield* wake.notify(ctx.threadId, "progress");
    yield* hit("terminalize:after-canonical-append");
    const canonicalSettlement = yield* settlementPayloadFromRecord(published.record, submissionId);

    yield* notifyParentOfChildSettlement(submission, published.record);
    if (afterCanonical !== undefined && canonicalSettlement.outcome === "completed")
      yield* afterCanonical;

    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({ submissionId, settlementId }),
    );

    yield* wake.notify(submission.threadId);
    yield* notifyParentOfChildSettlement(submission, published.record);
    yield* settleJoinedSubmissions(ctx, canonicalSettlement);

    return materializeSettlement(settlement, published.record);
  });

  /** Canonical settlement exists: rebuild the ledger from history, never the reverse (DUR-015). */
  const finalizeFromHistory = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
    record: RecordEnvelope,
  ): Effect.fn.Return<Settlement, LedgerError | SettlementConflict> {
    const canonicalSettlement = yield* settlementPayloadFromRecord(record, submission.submissionId);

    yield* notifyParentOfChildSettlement(submission, record);

    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({
        submissionId: submission.submissionId,
        settlementId: canonicalSettlement.settlementId,
      }),
    );

    yield* wake.notify(submission.threadId);
    yield* notifyParentOfChildSettlement(submission, record);

    return materializeSettlement(settlement, record);
  });

  /**
   * Durable abort of owned work: canonical `AbortRequested` first, then `ToolCallUnknown` audit
   * records for every open ordinary Tool Call (abort settles the obligation but never asserts
   * external rollback, durability §13), then settle aborted.
   */
  const settleAborted = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    publishSettlement: PublishSettlement,
    intent: AbortIntent,
    evidence: RecoveryEvidence,
    knownIds: Set<string>,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    if (!evidence.abortRecorded) {
      yield* appendAbortRecord(ctx, intent);
    }
    if (evidence.openToolCalls.length > 0) {
      yield* appendUnknownRecords(
        ctx,
        submission.submissionId,
        knownIds,
        evidence.openToolCalls,
        "The Submission was aborted while this ordinary Tool call had no canonical outcome; abort never asserts external rollback",
      );
    }

    return yield* terminalize(
      ctx,
      submission,
      publishSettlement,
      { _tag: "aborted" },
      evidence.inputRecorded,
    );
  });

  /** Map a ledger conflict outside `DurableWorkerFailure` into the coordinator failure family. */
  const conflictToLedgerError =
    (operation: string) =>
    (conflict: { readonly _tag: string; readonly message?: string }): LedgerError =>
      LedgerError.make({
        operation,
        message: `${conflict._tag}${conflict.message === undefined ? "" : `: ${conflict.message}`}`,
        cause: conflict,
      });

  /**
   * Append the child Thread's immutable lineage record (spec §12 step 6, §11): its own
   * single-record batch under `subagent-lineage:{childThreadId}` so the generic
   * `thread-created:{cid}` batch identity is never contradicted. Idempotent by record
   * identity; a racing append is re-proved from the log. A claim of the admitted child may
   * advance only its fence, so bounded retries acquire the current tail before appending.
   */
  const ensureChildLineage = Effect.fnUntraced(
    function* (
      parent: SubmissionSnapshot,
      request: SubagentRequested,
      childRecords: ReadonlyArray<CanonicalRecordEnvelope>,
    ): Effect.fn.Return<void, DurableWorkerFailure> {
      const recordId = subagentLineageRecordId(request.childThreadId);

      if (childRecords.some((envelope) => envelope.record.recordId === recordId)) return;

      const envelope = yield* makeEnvelope(
        recordId,
        SubagentLineageRecorded.make({
          parentLink: SubagentParentLink.make({
            delegationId: request.delegationId,
            parentAgentId: parent.agentId,
            parentThreadId: parent.threadId,
            parentRunId: request.runId,
            parentToolCallId: request.toolCallId,
            depth: request.depth ?? CHILD_DELEGATION_DEPTH,
          }),
          parentSubmissionId: parent.submissionId,
          childDefinitionDigests: request.targetDigests,
          childInputDigest: request.childInputDigest,
          grantDigest: request.grantDigest,
          ...(request.toolCallAllowance === undefined
            ? {}
            : { toolCallAllowance: request.toolCallAllowance }),
          ...(request.policy === undefined ? {} : { policy: request.policy }),
          ...(request.budget === undefined ? {} : { budget: request.budget }),
          ...(request.grant === undefined ? {} : { grant: request.grant }),
        }),
      );

      const tail = yield* store.inspectTail(
        ThreadTailRequest.make({ threadId: request.childThreadId }),
      );

      yield* store
        .append(
          FencedAppendRequest.make({
            threadId: request.childThreadId,
            batch: CanonicalBatch.make({
              batchId: subagentLineageBatchId(request.childThreadId),
              producerId: config.producerId,
              records: [envelope],
            }),
            expectedTailSequence: tail.tailSequence,
            expectedTailDigest: tail.tailDigest,
            producerEpoch: tail.producerEpoch,
          }),
        )
        .pipe(
          Effect.catchTag(["AppendConflict", "FenceRejected"], (error) =>
            // A racing establishment pass (or the child's own claimed worker) advanced the
            // log; the deterministic identity means the record either exists or the next
            // pass re-proves it — verify instead of trusting the race blindly.
            readControl(request.childThreadId, []).pipe(
              Effect.flatMap((current) =>
                current.some((candidate) => candidate.record.recordId === recordId)
                  ? Effect.void
                  : Effect.fail(error),
              ),
            ),
          ),
          Effect.andThen(wake.notify(request.childThreadId)),
          Effect.asVoid,
        );
    },
    (effect) =>
      effect.pipe(
        Effect.retry({
          times: 7,
          while: (error) => error._tag === "AppendConflict" || error._tag === "FenceRejected",
        }),
      ),
  );

  /** Where one idempotent child admission pass ended (spec §12 steps 4-8). */
  type ChildAdmissionOutcome =
    | {
        readonly _tag: "established";
        readonly childSubmissionId: SubmissionId;
        readonly receiptId: ReceiptId;
      }
    | { readonly _tag: "indeterminate"; readonly reason: string };

  /**
   * Complete (or replay) the child-admission half of establishment from the canonical
   * `SubagentRequested` payload alone — no live delegation handler is required (D3): the
   * `resolveAdmission` tri-state gate (SUB-031), the idempotency-keyed `admit` with immutable
   * parent linkage, child Thread materialization plus the immutable lineage record, and
   * readiness. Every step is get-or-create; a replay converges on the one existing child
   * (SUB-016) and an `indeterminate` answer NEVER admits a second child.
   */
  const establishChildFromRequest = Effect.fnUntraced(function* (
    parent: SubmissionSnapshot,
    request: SubagentRequested,
    allowAdmission = true,
  ): Effect.fn.Return<ChildAdmissionOutcome, DurableWorkerFailure> {
    const principal = decodePrincipalSync(request.childPrincipal);
    const idempotencyKey = decodeIdempotencyKeySync(request.childIdempotencyKey);

    const resolution = yield* ledger.resolveAdmission(
      SubmissionLookupByKey.make({
        threadId: request.childThreadId,
        principal,
        idempotencyKey,
      }),
    );

    let childSubmissionId: SubmissionId;
    let receiptId: ReceiptId;

    switch (resolution._tag) {
      case "Indeterminate": {
        return { _tag: "indeterminate", reason: resolution.reason };
      }
      case "NotAdmitted": {
        if (!allowAdmission) {
          return {
            _tag: "indeterminate",
            reason: "The expired parent cannot admit a new child",
          };
        }

        const admitted: AdmissionResult = yield* ledger
          .admit(
            AdmissionRequest.make({
              threadId: request.childThreadId,
              principal,
              idempotencyKey,
              agentId: request.targetAgentId,
              agentDigests: request.targetDigests,
              deploymentId: config.deploymentId,
              inputPayload: request.childInput,
              inputDigest: request.childInputDigest,
              parentLinkage: ParentLinkage.make({
                parentSubmissionId: parent.submissionId,
                parentToolCallId: request.toolCallId,
              }),
            }),
          )
          .pipe(
            Effect.catchTag(
              "AdmissionConflict",
              conflictToLedgerError("establishChildFromRequest"),
            ),
          );

        childSubmissionId = admitted.submissionId;
        receiptId = admitted.receiptId;
        break;
      }
      case "Admitted": {
        // The one existing child: verify the immutable admission facts against the canonical
        // request before reattaching — a divergent row can never be "the same child"
        // (fail-closed; identifiers are never capabilities, D10).
        const child = resolution.submission;
        const linkage = child.parentLinkage;

        if (
          child.agentId !== request.targetAgentId ||
          !definitionDigestsEqual(child.agentDigests, request.targetDigests) ||
          child.inputDigest !== request.childInputDigest ||
          linkage === undefined ||
          linkage.parentSubmissionId !== parent.submissionId ||
          linkage.parentToolCallId !== request.toolCallId
        ) {
          return yield* LedgerError.make({
            operation: "establishChildFromRequest",
            message: `The admitted child ${child.submissionId} diverges from the canonical SubagentRequested record for Tool Call ${request.toolCallId}; establishment fails closed (SUB-016)`,
          });
        }
        childSubmissionId = child.submissionId;
        receiptId = child.receiptId;
        break;
      }
    }
    yield* hit("subagent:after-admit");
    yield* materializeAtLeast(request.childThreadId, ZERO_EPOCH);
    yield* ensureThreadCreated(request.childThreadId, request.targetAgentId, request.targetDigests);
    const childRead = yield* readAllTolerant(request.childThreadId, []);

    yield* ensureChildLineage(parent, request, childRead.records);
    yield* ledger.markReady(MarkReadyRequest.make({ submissionId: childSubmissionId }));
    yield* hit("subagent:after-child-ready");
    yield* wake.notify(request.childThreadId);

    return { _tag: "established", childSubmissionId, receiptId };
  });

  /**
   * Bounded usage summary rebuilt from canonical child evidence (D11 structural dimensions).
   * The two finite counters always satisfy the persistence bounds, so a decode failure is a
   * defect (`orDie`), matching the file's `annotateRepair` pattern for provably-valid inputs.
   */
  const childUsageSummaryOf = (
    childRecords: ReadonlyArray<CanonicalRecordEnvelope>,
    childRunId: RunId,
  ): Effect.Effect<PersistedJson> => {
    let turns = 0;
    let toolCalls = 0;

    for (const envelope of childRecords) {
      const payload = envelope.record.payload;

      if (payload._tag === "ModelResponseRecorded" && payload.runId === childRunId) turns += 1;
      if (payload._tag === "ToolCallSettled" && payload.runId === childRunId) toolCalls += 1;
    }

    return decodePersisted({ turns, toolCalls }).pipe(Effect.orDie);
  };

  /** The coordinator's bounded `{errorTag, message}` projection of a non-completed child. */
  const boundedChildFailureResult = (
    payload: SubmissionSettledRecord,
    childSubmissionId: SubmissionId,
  ): Effect.Effect<PersistedJson> => {
    if (payload.outcome === "failed") {
      return Effect.succeed(payload.result);
    }

    return decodePersisted({
      errorTag: "SubagentAborted",
      message: `Attached child ${childSubmissionId} settled aborted`,
    }).pipe(Effect.orDie);
  };

  /** One verified child Settlement, ready to join (spec §12 join steps 1-3). */
  interface VerifiedChildSettlement {
    readonly outcome: SettlementOutcome;
    /** Child terminal output for `completed`; the bounded `{errorTag, message}` projection otherwise. */
    readonly encodedResult: PersistedJson;
    readonly settlement: SubmissionSettledRecord;
    readonly childRecords: ReadonlyArray<CanonicalRecordEnvelope>;
  }

  const verifiedChildUsage = Effect.fnUntraced(function* (
    verified: VerifiedChildSettlement,
    childRunId: RunId,
  ) {
    const reports = childUsageReportsOf(subagentRecordsOf(verified.childRecords, childRunId));

    const delegatedUsage = yield* sumRunTotals(
      reports.flatMap(({ report }) => [report.usage, report.delegatedUsage]),
    ).pipe(Effect.catchTag("UsageAggregationError", () => Effect.succeed(unknownRunTotals())));

    return RunUsageReport.make({
      usage:
        verified.settlement.usageSummary === undefined
          ? unknownRunTotals()
          : runTotalsFromSummary(verified.settlement.usageSummary),
      delegatedUsage,
    });
  });

  type ChildVerification =
    | { readonly _tag: "verified"; readonly value: VerifiedChildSettlement }
    | { readonly _tag: "mismatch"; readonly message: string };

  /**
   * §1.6 join verification, fail-closed (SUB-019/SUB-023, D10): the child's CANONICAL
   * Settlement and lineage records are read from the child Thread Log — cached ledger
   * state never fabricates a Settlement — and every identity/digest is checked against the
   * canonical `SubagentRequested` payload: Parent Link identity, target agent, stored
   * definition digests, input/grant digests, settlement identity, and the settlement record
   * digest pinned by the child's reservation. Any mismatch is a typed verification failure.
   */
  const verifySettledChild = Effect.fnUntraced(function* (
    parent: SubmissionSnapshot,
    request: SubagentRequested,
    childSubmissionId: SubmissionId,
  ): Effect.fn.Return<ChildVerification, DurableWorkerFailure> {
    const mismatch = (message: string): ChildVerification => ({ _tag: "mismatch", message });

    // The child's lane state crosses the store boundary through `lookup` — a status check on
    // the child's owning ledger — because recovery snapshots are lane-local by construction:
    // on Cloudflare the child Thread lives in a different Durable Object whose
    // operational rows are not readable across the boundary (deployment §11). The child's
    // CANONICAL Settlement, read below from its Thread Log, remains the only
    // cross-lane verification authority (spec §12 join step 1, SUB-019, DUR-015).
    const childLookup = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: childSubmissionId }),
    );

    if (Option.isNone(childLookup)) {
      return yield* LedgerError.make({
        operation: "verifySettledChild",
        message: `Attached child ${childSubmissionId} is unknown to its owning Submission Ledger`,
      });
    }
    const child = childLookup.value;

    if (child.threadId !== request.childThreadId) {
      return mismatch("The child Thread does not match the intended child identity");
    }
    if (child.agentId !== request.targetAgentId) {
      return mismatch("The child Agent identity does not match the declared delegation target");
    }
    if (!definitionDigestsEqual(child.agentDigests, request.targetDigests)) {
      return mismatch("The child definition digests do not match the stored target digests");
    }
    if (child.inputDigest !== request.childInputDigest) {
      return mismatch("The child input digest does not match the canonical request");
    }
    const linkage = child.parentLinkage;

    if (
      linkage === undefined ||
      linkage.parentSubmissionId !== parent.submissionId ||
      linkage.parentToolCallId !== request.toolCallId
    ) {
      return mismatch("The child admission linkage does not name this parent Tool Call");
    }
    const childRecords = yield* readControl(request.childThreadId, [child.submissionId]);

    const lineageEnvelope = childRecords.find(
      (envelope) => envelope.record.recordId === subagentLineageRecordId(request.childThreadId),
    );

    const lineage = lineageEnvelope?.record.payload;

    if (lineage === undefined || lineage._tag !== "SubagentLineageRecorded") {
      return mismatch("The child Thread carries no immutable lineage record");
    }
    if (
      lineage.parentLink.delegationId !== request.delegationId ||
      lineage.parentLink.parentAgentId !== parent.agentId ||
      lineage.parentLink.parentThreadId !== parent.threadId ||
      lineage.parentLink.parentRunId !== request.runId ||
      lineage.parentLink.parentToolCallId !== request.toolCallId ||
      lineage.parentLink.depth !== (request.depth ?? CHILD_DELEGATION_DEPTH) ||
      lineage.parentSubmissionId !== parent.submissionId
    ) {
      return mismatch("The child Parent Link does not name exactly this parent Run and Tool Call");
    }
    if (
      !definitionDigestsEqual(lineage.childDefinitionDigests, request.targetDigests) ||
      lineage.childInputDigest !== request.childInputDigest ||
      lineage.grantDigest !== request.grantDigest ||
      lineage.toolCallAllowance !== request.toolCallAllowance ||
      !Equal.equals(lineage.policy, request.policy) ||
      !Equal.equals(lineage.budget, request.budget) ||
      !Equal.equals(lineage.grant, request.grant)
    ) {
      return mismatch("The child lineage digests or allowance do not match the canonical request");
    }

    const settlementEnvelope = childRecords.find(
      (envelope) => envelope.record.recordId === submissionSettlementRecordId(childSubmissionId),
    );

    const settlement = settlementEnvelope?.record.payload;

    if (settlement === undefined || settlement._tag !== "SubmissionSettled") {
      return mismatch("The child has no canonical Settlement record");
    }
    if (
      settlement.submissionId !== childSubmissionId ||
      settlement.settlementId !== submissionSettlementId(childSubmissionId) ||
      settlement.receiptId !== child.receiptId
    ) {
      return mismatch("The child Settlement identity does not match the child Receipt");
    }
    // No parent-side crosscheck against the child's settlement RESERVATION row exists here:
    // the reservation lives in the child's own store, invisible across the Object boundary on
    // Cloudflare, and its byte-identity with the canonical record is the child store's own
    // conformance-tested reserve→append→finalize invariant (DUR-011) — never a parent
    // obligation. The canonical Settlement verified above is the sole cross-lane authority.
    if (settlement.outcome === "completed" && settlement.result === undefined) {
      return mismatch("The completed child Settlement carries no terminal output");
    }

    const encodedResult =
      settlement.outcome === "completed" && settlement.result !== undefined
        ? settlement.result
        : yield* boundedChildFailureResult(settlement, childSubmissionId);

    return {
      _tag: "verified",
      value: { outcome: settlement.outcome, encodedResult, settlement, childRecords },
    };
  });

  /**
   * Apply the frozen accounting decision to one reservation (spec §12 join step 6, DUR-015):
   * `beginChildBudgetRelease` freezes it exactly once (an identical replay is a no-op) and
   * `releaseChildBudget` applies it exactly once — budget stays unavailable until repair, never
   * available twice.
   */
  const completeWorkHandoff = Effect.fnUntraced(function* (
    threadId: ThreadId,
    preparationId: RecordId,
    ownerId = workId("handoff", preparationId),
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const id = decodeRecordId(`work-handoff-completed:${preparationId}`);

    for (let attempt = 0; attempt < 16; attempt++) {
      const existing = yield* getRecord({ threadId, recordId: id }).pipe(
        Effect.provideService(ThreadReader, reader),
      );

      if (Option.isSome(existing)) {
        const payload = existing.value.record.payload;

        if (
          payload._tag !== "WorkHandoffCompleted" ||
          payload.preparationId !== preparationId ||
          payload.ownerId !== ownerId
        )
          return yield* ThreadStoreError.make({
            operation: "complete work handoff",
            message: "The closure identity conflicts with its owner",
          });

        return;
      }
      const ctx = yield* attemptContextAtTail(threadId);

      const envelope = yield* makeEnvelope(
        id,
        WorkHandoffCompleted.make({ preparationId, ownerId }),
      );

      const committed = yield* appendBatch(
        ctx,
        CanonicalBatch.make({
          batchId: decodeBatchId(`work-handoff-completed:${preparationId}`),
          producerId: config.producerId,
          records: [envelope],
        }),
      ).pipe(
        Effect.as(true),
        Effect.catchTag("AppendConflict", () => Effect.succeed(false)),
      );

      if (committed) return;
    }

    return yield* ThreadStoreError.make({
      operation: "complete work handoff",
      message: "The canonical tail remained contended",
    });
  });

  const applyReservationRelease = Effect.fnUntraced(function* (
    reservationId: ChildReservationId,
    accounting: PersistedJson,
    joinedOwner?: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly toolCallId: ToolCallId;
    },
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    yield* ledger
      .beginChildBudgetRelease(BeginChildBudgetReleaseRequest.make({ reservationId, accounting }))
      .pipe(
        Effect.catchTag(
          "ChildReservationConflict",
          conflictToLedgerError("beginChildBudgetRelease"),
        ),
      );
    yield* hit("subagent:after-release-pending");
    yield* ledger
      .releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId }))
      .pipe(
        Effect.catchTag("ChildReservationConflict", conflictToLedgerError("releaseChildBudget")),
      );
    yield* hit("subagent:after-release");
    if (joinedOwner !== undefined) {
      const preparationId = subagentJoinedRecordId(joinedOwner.runId, joinedOwner.toolCallId);

      const joined = yield* getRecord({
        threadId: joinedOwner.threadId,
        recordId: preparationId,
      }).pipe(Effect.provideService(ThreadReader, reader));

      if (Option.isSome(joined)) {
        if (joined.value.record.payload._tag !== "SubagentJoined")
          return yield* RunJournalError.make({ message: "Child accounting has no canonical join" });
        yield* completeWorkHandoff(joinedOwner.threadId, preparationId);
      }
    }
  });

  /**
   * Release a provably-childless reservation exactly once (spec §13 "reservation exists,
   * request absent"): freeze the deterministic zero-consumed decision — a conflict means a
   * different decision already froze first, and the release below applies THAT frozen decision.
   */
  const releaseOrphanReservation = Effect.fnUntraced(function* (
    reservationId: ChildReservationId,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    yield* ledger
      .beginChildBudgetRelease(
        BeginChildBudgetReleaseRequest.make({
          reservationId,
          accounting: ORPHAN_ZERO_CONSUMED_ACCOUNTING,
        }),
      )
      .pipe(
        Effect.catchTag("ChildReservationConflict", () => Effect.void),
        Effect.asVoid,
      );
    yield* hit("subagent:after-release-pending");
    yield* ledger
      .releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId }))
      .pipe(
        Effect.catchTag("ChildReservationConflict", conflictToLedgerError("releaseChildBudget")),
      );
    yield* hit("subagent:after-release");
  });

  /**
   * Join a settled child after parent abort or expiry without running application handlers.
   * The canonical child outcome stays intact; the parent result records why it cannot continue.
   */
  const joinSettledChildWithoutHandler = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    parent: SubmissionSnapshot,
    knownIds: Set<string>,
    subagent: SubagentCallRecords,
    reservation: ChildBudgetReservationSnapshot,
    toolCallId: ToolCallId,
    childSubmissionId: SubmissionId,
    reason: "aborted" | "duration" | "unavailable",
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const runId = runIdForSubmission(parent.submissionId);
    const request = subagent.requested.get(toolCallId);

    if (request === undefined) {
      return yield* LedgerError.make({
        operation: "joinSettledChildWithoutHandler",
        message: `Tool Call ${toolCallId} has an attached child but no canonical SubagentRequested record`,
      });
    }
    const joinedRecordId = subagentJoinedRecordId(runId, toolCallId);
    let finalAccounting: PersistedJson;
    const existing = subagent.joined.get(toolCallId);

    if (existing !== undefined) {
      finalAccounting = existing.finalAccounting;
    } else {
      const verification = yield* verifySettledChild(parent, request, childSubmissionId);

      if (verification._tag === "mismatch") {
        return yield* LedgerError.make({
          operation: "joinSettledChildWithoutHandler",
          message: `Child Settlement verification failed for Tool Call ${toolCallId}: ${verification.message}`,
        });
      }
      const verified = verification.value;

      // The child's actual outcome and usage remain authoritative even when the parent can
      // no longer project it with the original delegation implementation.
      const boundedResult = yield* decodePersisted(
        reason === "unavailable"
          ? {
              _tag: "ToolUnavailable",
              toolName: subagent.declaredNames.get(toolCallId) ?? request.delegationId,
              execution: "unavailable",
              message: `The original delegation implementation is unavailable. Its original child settled ${verified.outcome}; no replacement child was started.`,
            }
          : {
              errorTag:
                reason === "aborted" ? "SubagentParentAborted" : "SubagentParentDurationExceeded",
              message: boundedText(
                `The parent Submission ${reason === "aborted" ? "aborted" : "exceeded its duration"}; attached child ${childSubmissionId} settled ${verified.outcome}`,
              ),
            },
      ).pipe(Effect.orDie);

      finalAccounting = yield* decodePersisted({
        basis: `${reason}-conservative`,
        allocation: reservation.allocation,
      }).pipe(Effect.orDie);
      const childRunId = runIdForSubmission(childSubmissionId);

      yield* hit("subagent:before-join-append");

      const joinedPayload = SubagentJoined.make({
        runId,
        toolCallId,
        childSubmissionId,
        childSettlementId: verified.settlement.settlementId,
        childOutcome: verified.outcome,
        childResultDigest: yield* withCrypto(digestJson(verified.settlement.result ?? null)),
        projectedResultDigest: yield* withCrypto(digestJson(boundedResult)),
        usageSummary: yield* childUsageSummaryOf(verified.childRecords, childRunId),
        ...(yield* verifiedChildUsage(verified, childRunId)),
        reservationId: reservation.reservationId,
        finalAccounting,
      });

      const settledRecordId = toolCallSettledRecordId(runId, request.turn, toolCallId);
      const joinedEnvelope = yield* makeEnvelope(joinedRecordId, joinedPayload);

      const settledEnvelope = yield* makeEnvelope(
        settledRecordId,
        ToolCallSettled.make({
          runId,
          toolCallId,
          toolName: subagent.declaredNames.get(toolCallId) ?? request.delegationId,
          result: boundedResult,
          isFailure: true,
        }),
      );

      if (!knownIds.has(joinedRecordId)) {
        yield* appendBatch(
          ctx,
          CanonicalBatch.make({
            batchId: subagentJoinBatchId(runId, toolCallId),
            producerId: config.producerId,
            records: [joinedEnvelope, settledEnvelope],
          }),
        ).pipe(
          Effect.catchTag("AppendConflict", () => Effect.void),
          Effect.asVoid,
        );
        knownIds.add(joinedRecordId);
        knownIds.add(settledRecordId);
        subagent.joined.set(toolCallId, joinedPayload);
        yield* hit("subagent:after-join-append");
      }
    }
    yield* applyReservationRelease(reservation.reservationId, finalAccounting, {
      threadId: parent.threadId,
      runId,
      toolCallId,
    });
  });

  /**
   * Complete every joined-but-unreleased reservation BEFORE the parent settles (spec §12 join
   * step 6): the canonical `SubagentJoined` accounting authorizes the release (DUR-015), and a
   * parent that settled first would strand the repair — a settled lane classifies `NoAction`.
   * Returns whether any reservation remains unreleased (a reserved row without a canonical
   * join), which must block the settlement (spec §13: a parent never settles across an open
   * child obligation).
   */
  const completeJoinedReleases = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<boolean, DurableWorkerFailure> {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );

    if (snapshot.childReservations.length === 0) return false;
    const records = yield* readControl(submission.threadId, [submission.submissionId]);
    const subagent = subagentRecordsOf(records, runIdForSubmission(submission.submissionId));
    let open = false;

    for (const reservation of snapshot.childReservations) {
      if (reservation.status === "released") continue;
      const joined = subagent.joined.get(reservation.parentToolCallId);

      if (joined !== undefined) {
        yield* applyReservationRelease(reservation.reservationId, joined.finalAccounting, {
          threadId: submission.threadId,
          runId: runIdForSubmission(submission.submissionId),
          toolCallId: reservation.parentToolCallId,
        });
        continue;
      }
      if (reservation.status === "releasePending" && reservation.accounting !== undefined) {
        yield* applyReservationRelease(reservation.reservationId, reservation.accounting, {
          threadId: submission.threadId,
          runId: runIdForSubmission(submission.submissionId),
          toolCallId: reservation.parentToolCallId,
        });
        continue;
      }
      open = true;
    }

    return open;
  });

  /** Release unused reservations or repair and join existing children, never admit new work. */
  const reconcileRetainedChildren = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    parent: SubmissionSnapshot,
    ownership: RunOwnership,
    unavailableCalls?: ReadonlySet<ToolCallId>,
  ) {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
    );

    const records = yield* readControl(parent.threadId, [parent.submissionId]);
    const runId = runIdForSubmission(parent.submissionId);
    const pending = yield* pendingToolBatchFor(records, runId);
    const knownIds = knownRecordIdsOf(records);
    const subagent = subagentRecordsOf(records, runId);

    const notExecuted = new Set<ToolCallId>();
    const waiting: Array<WaitingChild> = [];
    const indeterminate = new Set<ToolCallId>();

    for (const toolCallId of unavailableCalls ?? []) {
      if (
        snapshot.childReservations.some(
          (reservation) => reservation.parentToolCallId === toolCallId,
        )
      )
        continue;
      if (subagent.requested.has(toolCallId) || subagent.started.has(toolCallId))
        return yield* LedgerError.make({
          operation: "reconcileRetainedChildren",
          message: "Child request has no original reservation",
        });
      notExecuted.add(toolCallId);
    }
    for (const reservation of snapshot.childReservations) {
      const toolCallId = reservation.parentToolCallId;

      if (unavailableCalls !== undefined && !unavailableCalls.has(toolCallId)) continue;
      const joined = subagent.joined.get(toolCallId);

      if (joined !== undefined) {
        yield* applyReservationRelease(reservation.reservationId, joined.finalAccounting, {
          threadId: parent.threadId,
          runId,
          toolCallId,
        });
        continue;
      }
      if (reservation.status !== "reserved") {
        if (reservation.accounting !== undefined)
          yield* applyReservationRelease(reservation.reservationId, reservation.accounting, {
            threadId: parent.threadId,
            runId,
            toolCallId,
          });
        // An orphan release proves non-admission; it never hides an attached child.
        if (reservation.childSubmissionId !== undefined || subagent.started.has(toolCallId))
          return yield* LedgerError.make({
            operation: "reconcileRetainedChildren",
            message: "Unjoined child reservation was already released",
          });
        notExecuted.add(toolCallId);
        continue;
      }
      let started = subagent.started.get(toolCallId);
      const request = subagent.requested.get(toolCallId);

      if (
        started === undefined &&
        request === undefined &&
        reservation.childSubmissionId === undefined
      ) {
        yield* releaseOrphanReservation(reservation.reservationId);
        notExecuted.add(toolCallId);
        continue;
      }
      const call = pending?.calls.find((candidate) => candidate.id === toolCallId);

      if (
        pending === undefined ||
        call === undefined ||
        request === undefined ||
        subagent.declaredNames.get(toolCallId) !== call.name ||
        request.delegationId !== call.name ||
        reservation.parentSubmissionId !== parent.submissionId ||
        request.reservationId !== reservation.reservationId ||
        request.reservationDigest !== reservation.allocationDigest ||
        request.turn !== pending.turn ||
        request.turnId !== pending.turnId
      ) {
        return yield* LedgerError.make({
          operation: "reconcileRetainedChildren",
          message: `Tool Call ${toolCallId} has inconsistent canonical child request evidence`,
        });
      }
      if (started === undefined) {
        const resolution = yield* ledger.resolveAdmission(
          SubmissionLookupByKey.make({
            threadId: request.childThreadId,
            principal: decodePrincipalSync(request.childPrincipal),
            idempotencyKey: decodeIdempotencyKeySync(request.childIdempotencyKey),
          }),
        );

        if (resolution._tag === "Indeterminate") {
          indeterminate.add(toolCallId);
          continue;
        }
        if (resolution._tag === "NotAdmitted") {
          if (reservation.childSubmissionId !== undefined) {
            return yield* LedgerError.make({
              operation: "reconcileRetainedChildren",
              message: `Tool Call ${toolCallId} has an attachment but no admitted child`,
            });
          }
          if (unavailableCalls === undefined) {
            yield* releaseOrphanReservation(reservation.reservationId);
            notExecuted.add(toolCallId);
            continue;
          }
        }

        // A canonical request already authorized this exact idempotent admission. Finish it
        // when its handler is unavailable: a stale admission can still arrive after the lookup,
        // so NotAdmitted cannot justify releasing its reservation. Expiry keeps admission closed.
        const admission = yield* establishChildFromRequest(
          parent,
          request,
          unavailableCalls !== undefined,
        );

        if (admission._tag === "indeterminate") {
          indeterminate.add(toolCallId);
          continue;
        }
        started = SubagentStarted.make({
          runId,
          toolCallId,
          childThreadId: request.childThreadId,
          childSubmissionId: admission.childSubmissionId,
          childReceiptId: admission.receiptId,
          childRunId: runIdForSubmission(admission.childSubmissionId),
        });
        const startRecordId = subagentStartedRecordId(runId, toolCallId);
        const envelope = yield* makeEnvelope(startRecordId, started);

        yield* appendBatch(
          ctx,
          CanonicalBatch.make({
            batchId: subagentStartedBatchId(runId, toolCallId),
            producerId: config.producerId,
            records: [envelope],
          }),
        );
        knownIds.add(startRecordId);
        subagent.started.set(toolCallId, started);
        yield* hit("subagent:after-start-append");
      }

      const child = yield* ledger.lookup(
        SubmissionLookupById.make({ submissionId: started.childSubmissionId }),
      );

      if (
        Option.isNone(child) ||
        (reservation.childSubmissionId !== undefined &&
          reservation.childSubmissionId !== started.childSubmissionId) ||
        request.childThreadId !== started.childThreadId ||
        started.childThreadId !== child.value.threadId ||
        started.childReceiptId !== child.value.receiptId ||
        started.childRunId !== runIdForSubmission(started.childSubmissionId)
      ) {
        return yield* LedgerError.make({
          operation: "reconcileRetainedChildren",
          message: `Tool Call ${call.id} has inconsistent canonical child attachment evidence`,
        });
      }
      yield* ownership
        .attachChildToReservation({
          reservationId: reservation.reservationId,
          childSubmissionId: started.childSubmissionId,
        })
        .pipe(
          Effect.catchTag(
            "ChildReservationConflict",
            conflictToLedgerError("attachChildToReservation"),
          ),
        );
      if (child.value.state !== "settled") {
        waiting.push(
          WaitingChild.make({ toolCallId, childSubmissionId: started.childSubmissionId }),
        );
        yield* wake.notify(child.value.threadId);
        continue;
      }
      yield* joinSettledChildWithoutHandler(
        ctx,
        parent,
        knownIds,
        subagent,
        reservation,
        started.toolCallId,
        started.childSubmissionId,
        unavailableCalls === undefined ? "duration" : "unavailable",
      );
    }

    return { notExecuted, waiting, indeterminate };
  });

  /** Where the request-abort-and-join pass over attached children ended (spec §13.1). */
  type ChildAbortDisposition = "clear" | "waiting" | "blocked";

  /**
   * Request-abort-and-join over every attached child of an aborting parent (spec §13.1,
   * SUB-022): joined-but-unreleased reservations finish their idempotent release; settled
   * children join coordinator-side; nonterminal children receive the ONE idempotent durable
   * abort command (the recorded `AbortIntent` row IS the propagation marker, DUR-012) and the
   * parent suspends `waitingForChild` for their joins; an admitted-but-unlinked child is
   * reattached first; a provably-childless reservation releases exactly once; an indeterminate
   * admission blocks honestly — never a second admission, never a fabricated settlement.
   */
  const abortAttachedChildren = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    parent: SubmissionSnapshot,
    ownership: RunOwnership,
    knownIds: Set<string>,
  ): Effect.fn.Return<ChildAbortDisposition, DurableWorkerFailure> {
    const submissionId = parent.submissionId;
    const runId = runIdForSubmission(submissionId);

    while (true) {
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId }),
      );

      if (snapshot.childReservations.length === 0) return "clear";
      const records = yield* readControl(parent.threadId, [parent.submissionId]);

      for (const envelope of records) knownIds.add(envelope.record.recordId);
      const subagent = subagentRecordsOf(records, runId);
      const waiting: Array<WaitingChild> = [];
      let blocked = false;

      for (const reservation of snapshot.childReservations) {
        if (reservation.status === "released") continue;
        const toolCallId = reservation.parentToolCallId;
        const joined = subagent.joined.get(toolCallId);

        if (joined !== undefined) {
          yield* applyReservationRelease(reservation.reservationId, joined.finalAccounting, {
            threadId: parent.threadId,
            runId,
            toolCallId,
          });
          continue;
        }
        if (reservation.status === "releasePending") {
          // The decision is already frozen (join or orphan): finish the idempotent release —
          // NEVER re-freeze, a divergent second decision would conflict.
          yield* applyReservationRelease(
            reservation.reservationId,
            reservation.accounting ?? ORPHAN_ZERO_CONSUMED_ACCOUNTING,
            { threadId: parent.threadId, runId, toolCallId },
          );
          continue;
        }
        const request = subagent.requested.get(toolCallId);

        let childSubmissionId =
          subagent.started.get(toolCallId)?.childSubmissionId ?? reservation.childSubmissionId;

        if (childSubmissionId === undefined) {
          if (request === undefined) {
            // Reservation without a canonical request under abort: provably childless —
            // release the unused allocation exactly once (spec §13/§14).
            yield* releaseOrphanReservation(reservation.reservationId);
            continue;
          }
          const admission = yield* establishChildFromRequest(parent, request);

          if (admission._tag === "indeterminate") {
            // A child may exist: never release, settle, or re-admit until the authoritative
            // owner answers (SUB-031).
            blocked = true;
            continue;
          }
          childSubmissionId = admission.childSubmissionId;
        }

        const child = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: childSubmissionId }),
        );

        if (Option.isNone(child)) {
          return yield* LedgerError.make({
            operation: "abortAttachedChildren",
            message: `Attached child ${childSubmissionId} is unknown to the ledger`,
          });
        }
        if (child.value.state === "settled") {
          yield* joinSettledChildWithoutHandler(
            ctx,
            parent,
            knownIds,
            subagent,
            reservation,
            toolCallId,
            childSubmissionId,
            "aborted",
          );
          continue;
        }
        yield* ledger
          .requestAbort(
            AbortCommand.make({
              submissionId: childSubmissionId,
              author: SUBAGENT_ABORT_AUTHOR,
              reason: SUBAGENT_ABORT_REASON,
            }),
          )
          .pipe(
            // The child settled concurrently: the one winning Settlement joins on the next
            // pass of this loop (spec §13 "child terminal races abort").
            Effect.catchTags({
              SettlementConflict: () => Effect.void,
              JoinedToHost: conflictToLedgerError("abortAttachedChildren"),
            }),
            Effect.asVoid,
          );
        yield* hit("subagent:after-child-abort-intent");
        yield* wake.notify(child.value.threadId);
        waiting.push(WaitingChild.make({ toolCallId, childSubmissionId }));
      }
      if (blocked) return "blocked";
      const first = waiting[0];

      if (first === undefined) return "clear";

      const suspension = yield* ownership.suspend(
        WaitingForChildSuspension.make({ children: [first, ...waiting.slice(1)] }),
      );

      yield* hit("subagent:after-suspend");
      if (suspension === "suspended") return "waiting";
      // Every listed child settled before the suspend committed: loop to join the winners.
    }
  });

  const failureOutcome = (
    error: unknown,
    cause: Cause.Cause<unknown>,
    context: FailureDiagnostic.Context,
  ): Effect.Effect<AttemptOutcome> =>
    Schema.decodeEffect(SettlementFailureDiagnostic)({
      errorTag: errorTagOf(error).slice(0, 256) || "UnknownError",
      message: errorMessageOf(error).slice(0, MAX_FAILURE_MESSAGE_LENGTH),
      diagnostic: FailureDiagnostic.capture(cause),
      context: FailureDiagnostic.captureContext(context),
    }).pipe(
      // The exact diagnostic Schema admits only causal fields, never arbitrary provider payloads.
      Effect.orDie,
      Effect.map((result) => ({
        _tag: "failed" as const,
        // Full private diagnostics are optional. A fixed bounded summary always fits the
        // space retained before dispatch; the original live Cause was already logged.
        result:
          utf8ByteLength(JSON.stringify(result)) <= 64 * 1024
            ? result
            : SettlementFailureDiagnostic.make({
                errorTag: result.errorTag,
                message: result.message.slice(0, 1_024),
                ...(result.context === undefined ? {} : { context: result.context }),
              }),
        // A hard-rail policy failure keeps its typed limit durable (RUN-011):
        // the bounded message stays diagnostic, never the dimension authority.
        ...Option.match(decodePolicyFailureSafely(error), {
          onNone: () => ({}),
          onSome: ({ limit }) => ({ policyLimit: limit }),
        }),
      })),
    );

  const halt = <A, R>(
    effect: Effect.Effect<A, DurableWorkerFailure, R>,
  ): Effect.Effect<A, CoordinatorHalt, R> =>
    Effect.catchCause(effect, (cause) =>
      Effect.failCause(Cause.map(cause, (failure) => new CoordinatorHalt(failure))),
    );

  /**
   * Superseding-Attempt interruption audit (durability §9): appended at most once per superseded
   * fence generation before this Attempt re-invokes the model. It deliberately over-approximates:
   * an owner may have stopped between commits, and host retirement or repair can advance the
   * fence without inference. History cannot distinguish these from a lost provider response.
   * This records incomplete accounting, not an exact number or identity of missing model calls.
   */
  const appendInterruptedAudit = Effect.fnUntraced(function* (
    ctx: AttemptAppendContext,
    runId: ReturnType<typeof runIdForSubmission>,
    lineage: AttemptLineage,
    knownIds: Set<string>,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const recordId = modelResponseInterruptedRecordId(runId, lineage.supersededEpoch);

    if (knownIds.has(recordId)) return;

    const envelope = yield* makeEnvelope(
      recordId,
      ModelResponseInterrupted.make({
        runId,
        supersededEpoch: lineage.supersededEpoch,
        attemptId: lineage.attemptId,
        reason:
          "A prior ownership period ended without a canonical settlement; any in-flight model response was lost and the model may be re-invoked (duplicate provider cost is observable)",
      }),
    );

    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: modelResponseInterruptedBatchId(runId, lineage.supersededEpoch),
        producerId: config.producerId,
        records: [envelope],
      }),
    ).pipe(
      Effect.catchTag("AppendConflict", () => Effect.void),
      Effect.asVoid,
    );
    knownIds.add(recordId);
  });

  /**
   * Drive the shared interpreter Effect with history rebuilt by the run journal, commit
   * each Turn canonically through the fenced append, watch for durable abort intent, and keep the
   * ownership lease renewed. Engine Run failures settle `failed`; coordinator failures abort the
   * Attempt cleanly with the obligation still owed.
   *
   * The interpreter supplies validated response, closed Tool-result, and completion facts.
   * Responses precede approval and effectful dispatch; readonly responses may join their closed
   * results. Tool results precede input drains and the next Turn. A no-tool response and Run
   * completion share one batch. A completion Tool's result
   * precedes its terminal-only completion batch, so recovery can repeat a pure output projection
   * without replaying the handler. Pending batches resume from their canonical declarations.
   */
  const runModel = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    RunDispositionValue extends
      | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
      | undefined = undefined,
    InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
      undefined,
    UpdatesSchema extends Schema.Top | undefined = undefined,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue,
      InputPromptValue,
      UpdatesSchema
    >,
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    session: RunStorageSession,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    canonical: Stream.Stream<JournalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>,
    canonicalThrough: CanonicalSequence,
    progressThrough: CanonicalSequence | undefined,
    continuation: RunContinuation | undefined,
    priorContext: RunJournalContext | undefined,
    journalMetadata: JournalMetadata | undefined,
    lineage: AttemptLineage,
    approvalDecisions: ReadonlyArray<ApprovalDecisionIntent>,
    currentContracts: Readonly<Record<string, Digest>>,
    runTiming: RunTiming,
    yieldAfter?: DateTime.Utc,
  ) =>
    Effect.gen(function* () {
      const submissionId = submission.submissionId;

      const runId = runIdForSubmission(submissionId);
      const boundaries: Array<JournalBoundary> = [];

      const cached = projectedJournal;
      // Unrelated Thread traffic does not invalidate an already validated owner projection.
      const projectionThrough = Math.max(progressThrough ?? 0, records.at(-1)?.sequence ?? 0);

      const originalInput = records.find(
        ({ record: { payload } }) =>
          payload._tag === "UserInputRecorded" &&
          payload.kind === "user" &&
          payload.submissionId === submissionId,
      );

      if (originalInput === undefined)
        return yield* RunJournalError.make({ message: "Run has no original context boundary" });
      const historyEvidence = new Map<RecordId, JournalRecordEnvelope>();

      const retainHistory = (entry: JournalRecordEnvelope) => {
        if (priorContext === undefined && entry.sequence < originalInput.sequence)
          historyEvidence.set(entry.record.recordId, entry);
      };

      const journal =
        cached !== undefined &&
        cached.threadId === ctx.threadId &&
        cached.through === projectionThrough &&
        cached.runId === runId &&
        cached.contextDigest === priorContext?.digest
          ? (boundaries.push(...cached.boundaries),
            cached.contextEvidence.forEach(retainHistory),
            cached.journal)
          : yield* projectRunJournalStream(
              canonical,
              runId,
              (boundary) => boundaries.push(boundary),
              priorContext,
              journalMetadata,
              priorContext === undefined ? retainHistory : undefined,
            );

      // Do not retain the metadata snapshot across model or Tool waits, including cache hits.
      // The projected prompt owns its needed context.
      journalMetadata = undefined;

      projectedJournal = {
        threadId: ctx.threadId,
        through: CanonicalSequence.make(projectionThrough),
        runId,
        contextDigest: priorContext?.digest,
        contextEvidence: [...historyEvidence.values()],
        journal,
        boundaries,
      };

      if (continuation !== undefined) {
        const accounting = continuation.accounting;

        const policyFields = [
          "committedTurns",
          "toolCalls",
          "programmaticToolCalls",
          "consecutiveToolFailures",
          "finalizationUsed",
          "modelRestarts",
        ] as const;

        const usageFields = [
          "modelCalls",
          "inputTokens",
          "outputTokens",
          "lastInputTokens",
          "lastOutputTokens",
          "costMicrousd",
        ] as const;

        if (
          policyFields.some((field) => (journal.policyUsage[field] ?? 0) !== accounting[field]) ||
          usageFields.some((field) => journal.usage[field] !== accounting[field]) ||
          (journal.usage.unobservedModelCalls ?? 0) !== accounting.unobservedModelCalls
        )
          return yield* RunJournalError.make({
            message: "Canonical continuation accounting differs from its exact execution evidence",
          });
      }

      const childLineage = records.find(
        ({ record }) => record.recordId === subagentLineageRecordId(submission.threadId),
      )?.record.payload;

      const inheritedGrant =
        submission.workerAdmission?.origin.grant ??
        (childLineage?._tag === "SubagentLineageRecorded" ? childLineage.grant : undefined);

      const delegationDepth =
        submission.workerAdmission?.origin.depth ??
        (childLineage?._tag === "SubagentLineageRecorded" ? childLineage.parentLink.depth : 0);

      // worker-run origins supply a frozen ceiling, not usage credit. Only this native
      // host Run's journal owns usage; joined inputs and replacement Attempts keep its RunId.
      const inheritedBudget =
        submission.workerAdmission?.origin.budget ??
        (childLineage?._tag === "SubagentLineageRecorded" ? childLineage.budget : undefined);

      if (
        submission.parentLinkage !== undefined &&
        (childLineage?._tag !== "SubagentLineageRecorded" ||
          childLineage.parentSubmissionId !== submission.parentLinkage.parentSubmissionId ||
          childLineage.parentLink.parentToolCallId !== submission.parentLinkage.parentToolCallId ||
          !definitionDigestsEqual(childLineage.childDefinitionDigests, submission.agentDigests) ||
          childLineage.childInputDigest !== submission.inputDigest)
      ) {
        return yield* RunJournalError.make({
          message: "The child Run has no matching canonical lineage and execution bounds",
        });
      }

      const completionTools = [
        ...(agent.definition.completion === undefined ? [] : [agent.definition.completion.tool]),
        ...(agent.definition.completionFromTools?.map((declaration) => declaration.tool) ?? []),
      ];

      const pending = yield* pendingToolBatchFor(records, runId, completionTools);

      const resumeProjection =
        pending === undefined
          ? journal
          : yield* projectRunJournalStream(
              withoutPendingBatch(canonical, pending, runId),
              runId,
              undefined,
              priorContext,
            );

      const rolloverOperation =
        journal.pendingContextToolCallId === undefined
          ? undefined
          : operationsFor(records, runId).get(journal.pendingContextToolCallId);

      const rolloverTool =
        rolloverOperation === undefined
          ? undefined
          : agent.definition.toolkit.tools[rolloverOperation.toolName];

      // A current annotation cannot reinterpret a formerly ordinary settled result as control.
      const pendingContextToolCallId =
        rolloverOperation !== undefined &&
        rolloverTool !== undefined &&
        Context.get(rolloverTool.annotations, ContextRolloverTool) &&
        currentContracts[rolloverOperation.toolName] === rolloverOperation.replay
          ? journal.pendingContextToolCallId
          : undefined;

      const applicationCalls = pending?.calls.filter((call) => !call.providerExecuted);

      // A single successful completion call can have its result committed before the terminal
      // marker. Reuse that result for either completion mode, after verifying its original
      // operation contract; no handler or provider call is needed to repeat output projection.
      const completionCandidate =
        applicationCalls?.length === 1 &&
        completionTools.some((tool) => tool === applicationCalls[0]?.name) &&
        pending?.settled.some(
          (result) => result.id === applicationCalls[0]?.id && !result.isFailure,
        )
          ? applicationCalls[0]
          : undefined;

      const settledCompletion =
        pending !== undefined &&
        completionCandidate !== undefined &&
        supportsOperation(
          OpenToolCallEvidence.make({
            toolCallId: Schema.decodeSync(ToolCallId)(completionCandidate.id),
            toolName: completionCandidate.name,
            turn: pending.turn,
          }),
          pending.toolOperations.find(
            (operation) => operation.toolCallId === completionCandidate.id,
          ),
          { definition: agent.definition, contracts: currentContracts },
        )
          ? Schema.decodeSync(ToolCallId)(completionCandidate.id)
          : undefined;

      const knownIds = knownRecordIdsOf(records);
      const stepOutputs = new Map<string, PersistedJson>();

      // Derive replay identity from the recorded tuple, never from its historical ID format.
      // This preserves committed Step outputs when reading legacy colon-separated record IDs.
      for (const envelope of records) {
        const payload = envelope.record.payload;

        if (payload._tag === "ToolStepSettled" && payload.runId === runId) {
          stepOutputs.set(
            toolStepSettledRecordId(runId, payload.toolCallId, payload.stepName),
            payload.output,
          );
        }
      }
      // Recorded approval authority (plan §2.6): canonical `ToolApprovalDecided` records are the
      // deterministic decision source across Attempts; durable `resolveApproval` intents become
      // canonical before they are honored; requested Turns pin the Turn-shared approval batch
      // identity so a later append never contradicts a committed batch.
      const canonicalApprovalDecisions = new Map<string, ApprovalDecision>();
      const approvalRequestedTurns = new Set<number>();

      for (const envelope of records) {
        const payload = envelope.record.payload;

        if (payload._tag === "ToolApprovalDecided" && payload.runId === runId) {
          canonicalApprovalDecisions.set(payload.toolCallId, payload.decision);
        } else if (payload._tag === "ToolApprovalRequested" && payload.runId === runId) {
          approvalRequestedTurns.add(payload.turn);
        }
      }
      const approvalIntents = new Map<string, ApprovalDecisionIntent>();

      for (const intent of approvalDecisions) {
        approvalIntents.set(intent.toolCallId, intent);
      }
      // Joined queued input (plan §2.5): canonical facts for the prompt-coverage rule, computed
      // once per Attempt from the same strongly consistent read as the journal. A joined
      // `input:{sid}` record is prompt-covered iff a host `ModelResponseRecorded` committed
      // after it — commit 1 of every later Turn carries all pending steering (D8 extension), so
      // covered inputs are already inside the journal prompt and must never re-deliver.
      const joinedInputEnvelopes = new Map<string, CanonicalRecordEnvelope>();
      let lastHostResponseSequence: CanonicalSequence | undefined;

      for (const envelope of records) {
        const payload = envelope.record.payload;

        if (
          payload._tag === "UserInputRecorded" &&
          payload.runId === runId &&
          payload.submissionId !== undefined &&
          payload.submissionId !== submissionId
        ) {
          joinedInputEnvelopes.set(payload.submissionId, envelope);
        } else if (payload._tag === "ModelResponseRecorded" && payload.runId === runId) {
          if (
            lastHostResponseSequence === undefined ||
            envelope.sequence > lastHostResponseSequence
          ) {
            lastHostResponseSequence = envelope.sequence;
          }
        }
      }

      // RUN-023: per-Turn usage staged by the engine's `noteTurnUsage` for the
      // Turn's canonical response record (keyed by CANONICAL turn number).
      const stagedUsage = new Map<
        number,
        {
          readonly inputTokens: number;
          readonly outputTokens: number;
          readonly costMicrousd: number;
          readonly modelUsage: ReadonlyArray<ModelCallUsage>;
        }
      >();

      // Staging also backs the cumulative summary; keep it intact and track canonical prefixes.
      const committedUsageLengths = new Map<number, number>();
      const committedUnobservedCalls = new Map<number, number>();

      const usageForCommit = (turn: number) => {
        const staged = stagedUsage.get(turn);

        if (staged === undefined) return undefined;
        const modelUsage = staged.modelUsage.slice(committedUsageLengths.get(turn) ?? 0);

        if (modelUsage.length === 0) return undefined;

        return {
          modelUsage,
          inputTokens: modelUsage.reduce((sum, call) => sum + call.inputTokens.total, 0),
          outputTokens: modelUsage.reduce((sum, call) => sum + call.outputTokens.total, 0),
          costMicrousd: modelUsage.reduce((sum, call) => sum + call.costMicrousd, 0),
        };
      };

      const unobservedForCommit = (turn: number) =>
        (stagedUnobservedCalls.get(turn) ?? 0) - (committedUnobservedCalls.get(turn) ?? 0);

      const recordCommittedUsage = (batch: CanonicalBatch) => {
        for (const record of batch.records) {
          if (
            record.payload._tag !== "ModelResponseRecorded" &&
            record.payload._tag !== "ModelCallAborted"
          )
            continue;
          const payload = record.payload;

          committedUsageLengths.set(
            payload.turn,
            (committedUsageLengths.get(payload.turn) ?? 0) + (payload.modelUsage?.length ?? 0),
          );
          committedUnobservedCalls.set(
            payload.turn,
            (committedUnobservedCalls.get(payload.turn) ?? 0) + (payload.unobservedModelCalls ?? 0),
          );
        }
        progressWriters.get(ctx)?.stageUsage(runId, terminalUsageCharge(uncommittedModelUsage()));
      };

      const uncommittedModelUsage = () =>
        [...stagedUsage.entries()].flatMap(([turn, usage]) =>
          usage.modelUsage.slice(committedUsageLengths.get(turn) ?? 0),
        );

      // Like stagedUsage, these counts remain relative to the initial journal snapshot.
      const stagedUnobservedCalls = new Map<number, number>();

      let interruptedUsage = records.some(
        ({ record }) =>
          record.payload._tag === "ModelResponseInterrupted" && record.payload.runId === runId,
      );

      const checkedUsageTotal = (
        field: string,
        value: number,
      ): Effect.Effect<number, RunJournalError> =>
        decodeUsageTotal(value).pipe(
          Effect.mapError((cause) =>
            RunJournalError.make({
              message: `Run usage summary exceeds safe-integer bounds at ${field}`,
              cause,
            }),
          ),
        );

      const addUsageTotal = (field: string, left: number, right: number) =>
        checkedUsageTotal(field, left + right);

      const subtractUsageTotal = (field: string, left: number, right: number) =>
        checkedUsageTotal(field, left - right);

      const currentUsageSummary = (): Effect.Effect<RunUsageSummary, RunJournalError> =>
        Effect.gen(function* () {
          const stagedCalls = [...stagedUsage.values()].flatMap((usage) => usage.modelUsage);
          let unobservedModelCalls = journal.usage.unobservedModelCalls ?? 0;

          for (const count of stagedUnobservedCalls.values()) {
            unobservedModelCalls = yield* addUsageTotal(
              "unobservedModelCalls",
              unobservedModelCalls,
              count,
            );
          }
          const detailedCalls = [...journal.usage.modelUsage, ...stagedCalls];

          const detailed = yield* summarizeModelUsage(
            detailedCalls,
            journal.usage.summarizedModelUsage,
          ).pipe(
            Effect.mapError((cause) =>
              RunJournalError.make({
                message: "Run detailed usage exceeds safe-integer accounting bounds",
                cause,
              }),
            ),
          );

          let inputTokens = journal.usage.inputTokens;
          let outputTokens = journal.usage.outputTokens;
          let costMicrousd = journal.usage.costMicrousd;

          for (const usage of stagedUsage.values()) {
            inputTokens = yield* addUsageTotal("inputTokens", inputTokens, usage.inputTokens);
            outputTokens = yield* addUsageTotal("outputTokens", outputTokens, usage.outputTokens);
            costMicrousd = yield* addUsageTotal("costMicrousd", costMicrousd, usage.costMicrousd);
          }

          const modelCalls = yield* addUsageTotal(
            "modelCalls",
            journal.usage.modelCalls,
            stagedCalls.length,
          );

          const legacyCalls = yield* subtractUsageTotal(
            "legacy.modelCalls",
            modelCalls,
            detailed.modelCalls,
          );

          const legacyInput = yield* subtractUsageTotal(
            "legacy.inputTokens",
            inputTokens,
            detailed.inputTokens.total,
          );

          const legacyOutput = yield* subtractUsageTotal(
            "legacy.outputTokens",
            outputTokens,
            detailed.outputTokens.total,
          );

          const legacyCost = yield* subtractUsageTotal(
            "legacy.costMicrousd",
            costMicrousd,
            detailed.costMicrousd,
          );

          if (legacyCalls === 0 && (legacyInput !== 0 || legacyOutput !== 0 || legacyCost !== 0)) {
            return yield* RunJournalError.make({
              message: "Run aggregate usage has legacy totals without a legacy model call",
            });
          }
          const byModel = [...detailed.byModel];

          if (legacyCalls > 0) {
            const existingIndex = byModel.findIndex(
              (group) =>
                group.provider === "unknown" &&
                group.model === "legacy-record" &&
                group.responseModel === undefined &&
                group.serviceTier === undefined &&
                group.pricingVersion === undefined,
            );

            const existing = existingIndex < 0 ? undefined : byModel[existingIndex];

            const legacyGroup = ModelUsageGroup.make({
              provider: "unknown",
              model: "legacy-record",
              modelCalls: yield* addUsageTotal(
                "byModel.modelCalls",
                existing?.modelCalls ?? 0,
                legacyCalls,
              ),
              inputTokens: InputTokenUsage.make({
                total: yield* addUsageTotal(
                  "byModel.inputTokens.total",
                  existing?.inputTokens.total ?? 0,
                  legacyInput,
                ),
                uncached: yield* addUsageTotal(
                  "byModel.inputTokens.uncached",
                  existing?.inputTokens.uncached ?? 0,
                  legacyInput,
                ),
                cacheRead: existing?.inputTokens.cacheRead ?? 0,
                cacheWrite: existing?.inputTokens.cacheWrite ?? 0,
              }),
              outputTokens: OutputTokenUsage.make({
                total: yield* addUsageTotal(
                  "byModel.outputTokens.total",
                  existing?.outputTokens.total ?? 0,
                  legacyOutput,
                ),
                text: yield* addUsageTotal(
                  "byModel.outputTokens.text",
                  existing?.outputTokens.text ?? 0,
                  legacyOutput,
                ),
                reasoning: existing?.outputTokens.reasoning ?? 0,
              }),
              costMicrousd: yield* addUsageTotal(
                "byModel.costMicrousd",
                existing?.costMicrousd ?? 0,
                legacyCost,
              ),
            });

            if (existingIndex < 0) byModel.push(legacyGroup);
            else byModel[existingIndex] = legacyGroup;
          }

          const uncached = yield* addUsageTotal(
            "inputTokens.uncached",
            detailed.inputTokens.uncached,
            legacyInput,
          );

          const text = yield* addUsageTotal(
            "outputTokens.text",
            detailed.outputTokens.text,
            legacyOutput,
          );

          return RunUsageSummary.make({
            ...(legacyCalls === 0 && detailed.webSearchCalls !== undefined
              ? { webSearchCalls: detailed.webSearchCalls }
              : {}),
            modelCalls,
            inputTokens: InputTokenUsage.make({
              total: inputTokens,
              // Legacy aggregate inputs are conservatively classified as uncached.
              uncached,
              cacheRead: detailed.inputTokens.cacheRead,
              cacheWrite: detailed.inputTokens.cacheWrite,
            }),
            outputTokens: OutputTokenUsage.make({
              total: outputTokens,
              text,
              reasoning: detailed.outputTokens.reasoning,
            }),
            costMicrousd,
            byModel,
            unobservedModelCalls,
            usageStatus:
              legacyCalls > 0 || unobservedModelCalls > 0 || interruptedUsage
                ? detailed.usageStatus === "unknown"
                  ? "unknown"
                  : "partial"
                : detailed.usageStatus,
            pricingStatus:
              legacyCalls > 0 || unobservedModelCalls > 0 || interruptedUsage
                ? detailed.pricingStatus === "unknown"
                  ? "unknown"
                  : "partial"
                : detailed.pricingStatus,
          });
        });

      const recordedCompletion = records.find(
        (envelope) => envelope.record.recordId === runCompletedRecordId(runId),
      )?.record.payload;

      if (recordedCompletion !== undefined) {
        if (recordedCompletion._tag !== "RunCompleted" || recordedCompletion.runId !== runId) {
          return yield* RunJournalError.make({
            message: `Run ${runId} has an invalid terminal completion marker`,
          });
        }

        const response = [...records]
          .reverse()
          .find(
            (envelope) =>
              envelope.record.payload._tag === "ModelResponseRecorded" &&
              envelope.record.payload.runId === runId,
          )?.record.payload;

        if (response?._tag !== "ModelResponseRecorded") {
          return yield* RunJournalError.make({
            message: `Run ${runId} has a terminal completion marker without a response`,
          });
        }
        const declared = yield* declaredToolCalls(response.messages);
        const calls = declared.application;

        if (calls.length > 0) {
          const call = calls[0];

          const settled =
            call === undefined
              ? undefined
              : records.find(
                  ({ record }) =>
                    record.payload._tag === "ToolCallSettled" &&
                    record.payload.runId === runId &&
                    record.payload.toolCallId === call.id,
                )?.record.payload;

          if (
            calls.length !== 1 ||
            call === undefined ||
            settled?._tag !== "ToolCallSettled" ||
            settled.toolName !== call.name ||
            settled.isFailure
          ) {
            return yield* RunJournalError.make({
              message: `Run ${runId} has a terminal completion marker without one successful declared Tool result`,
            });
          }
        }
        if (
          recordedCompletion.resultDigest !== undefined &&
          recordedCompletion.resultDigest !==
            (yield* withCrypto(runCompletionDigest(recordedCompletion)))
        )
          return yield* RunJournalError.make({
            message: "RunCompleted terminal values failed their integrity check",
          });
        // Output and disposition were validated before the atomic RunCompleted commit.
        // Future application codecs and completion functions have no authority to reinterpret it.

        return {
          _tag: "completed" as const,
          result: recordedCompletion.output,
          ...(recordedCompletion.runDisposition === undefined
            ? {}
            : { runDisposition: recordedCompletion.runDisposition }),
          ...(recordedCompletion.finishReason === undefined
            ? {}
            : { finishReason: recordedCompletion.finishReason }),
          ...(recordedCompletion.exhausted === undefined
            ? {}
            : { exhausted: recordedCompletion.exhausted }),
          usageSummary: yield* currentUsageSummary(),
        };
      }
      // Summaries cover only prior Runs. Pruning and rollover also map fully settled
      // current-Run records and preserves the canonical instruction/input block during replay.
      let ownerFirstSequence: CanonicalSequence | undefined;

      for (const envelope of records) {
        const payload = envelope.record.payload;

        if ("runId" in payload && payload.runId === runId) {
          ownerFirstSequence = envelope.sequence;
          break;
        }
      }

      /** Only canonical joined inputs can require restoration from an earlier Attempt. */
      let joinBacklog: ReadonlyArray<JoinSnapshot> | undefined =
        joinedInputEnvelopes.size === 0 ? [] : undefined;

      /** Joined inputs already handed to the engine during THIS Attempt (never re-deliver). */
      const deliveredJoinInputs = new Set<string>();
      // Canonical encoded parameters per declared call, for the approval request digest: seeded
      // by the direct response commit before approval preflight and by the resumed batch.
      const encodedParamsByCallId = new Map<string, unknown>();
      // Subagent joins append their atomic settlement outside the ordinary results commit.
      // Their Tool names come from canonical responses or the newly committed declaration.
      const declaredNamesByCallId = new Map<string, string>();
      // Canonical subagent lifecycle state of this Run (SUB-016): seeded from canonical records,
      // advanced by the establish/join hook closures below, and consulted on every replay so an
      // identical establishment converges on the one existing child.
      const subagentState = subagentRecordsOf(records, runId);

      const declaredToolIds = new Set(subagentState.declaredNames.keys());

      if (declaredToolIds.size > MAX_RUN_TOOL_CALL_IDENTITIES)
        return yield* RunJournalError.make({
          message: `Run exceeds the ${MAX_RUN_TOOL_CALL_IDENTITIES} Tool Call identity limit`,
        });

      for (const [callId, name] of subagentState.declaredNames) {
        declaredNamesByCallId.set(callId, name);
      }
      if (pending !== undefined) {
        for (const call of pending.calls) {
          encodedParamsByCallId.set(call.id, call.params);
          declaredNamesByCallId.set(call.id, call.name);
        }
      }
      // Task #12 (WP1 `resume.leadingMessages`): the pending canonical response's messages
      // BEFORE its first assistant message — Turn-1 evaluated instructions + input, or steering
      // committed inside the pending record — re-enter official history through the engine so
      // the resumed live model context does not silently drop them.
      let resumeLeadingMessages: Prompt.Prompt | undefined;
      let resumeResponseMessages: Prompt.Prompt | undefined;

      if (pending !== undefined) {
        const pendingMessages = yield* decodePrompt(pending.messages).pipe(
          Effect.mapError((cause) =>
            RunJournalError.make({
              message:
                "Pending ModelResponseRecorded messages are not Schema-encoded Prompt messages",
              cause,
            }),
          ),
        );

        const firstAssistant = pendingMessages.content.findIndex(
          (message) => message.role === "assistant",
        );

        if (firstAssistant > 0) {
          resumeLeadingMessages = Prompt.fromMessages(
            pendingMessages.content
              .slice(0, firstAssistant)
              .filter((message) => message.role !== "system"),
          );
        }
        if (firstAssistant >= 0) {
          resumeResponseMessages = Prompt.fromMessages(
            pendingMessages.content.slice(firstAssistant),
          );
        }
      }

      // A canonical initial blocker proves no dispatch preceded this Attempt. Carry that
      // proof across sequential approvals until the fence, as for a newly committed response.
      // Resumption, retry permission and parameter rejection alone provide no such proof.
      const initialDispatchProofTurns = new Set(initialDispatchBlockedTurns(records, runId));

      let currentToolTurn: { readonly turn: number; readonly turnId: TurnId } | undefined =
        pending === undefined ? undefined : { turn: pending.turn, turnId: pending.turnId };

      // Step-hook coordinator failures are re-wrapped by the engine as `DurableStepError` in the
      // handler channel; this side channel preserves the original failure so the Attempt aborts
      // (obligation still owed) instead of settling the Run `failed` on an infrastructure fault.
      const haltRef = yield* Ref.make<DurableWorkerFailure | undefined>(undefined);
      const yieldSignal = yield* Deferred.make<SubmissionId | undefined>();

      const recordHalt = <A, R>(
        effect: Effect.Effect<A, DurableWorkerFailure, R>,
      ): Effect.Effect<A, CoordinatorHalt | AgentPersistenceCapacityError, R> =>
        effect.pipe(
          Effect.tapError((failure) =>
            failure instanceof ThreadStoreError &&
            failure.cause instanceof AgentPersistenceCapacityError
              ? Effect.void
              : Ref.set(haltRef, failure),
          ),
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (failure) =>
                failure instanceof ThreadStoreError &&
                failure.cause instanceof AgentPersistenceCapacityError
                  ? failure.cause
                  : new CoordinatorHalt(failure),
              ),
            ),
          ),
        );

      // Infrastructure errors can be wrapped by broker/Tool APIs. The interpreter checks this
      // authority before semantic work; progress delivery is not a coordinator checkpoint.
      const checkpoint = recordHalt(
        Effect.gen(function* () {
          const failure = yield* Ref.get(haltRef);

          if (failure !== undefined) return yield* failure;
          const progress = progressWriters.get(ctx);

          if (progress !== undefined)
            yield* progress.check(runId).pipe(Effect.provideService(CurrentRunWriter, ctx));
        }),
      );

      // One readonly Turn may own its response without publishing it. Promotion and settlement
      // share this gate so concurrent Steps or updates cannot publish the response twice.
      const responseGate = yield* Semaphore.make(1);

      let deferredResponse:
        | { readonly turn: number; readonly turnId: TurnId; readonly batch: CanonicalBatch }
        | undefined;

      const acceptResponseDeclarations = (batch: CanonicalBatch) => {
        for (const record of batch.records) {
          if (record.payload._tag !== "ModelResponseRecorded") continue;
          for (const call of record.payload.toolOperations) {
            declaredToolIds.add(call.toolCallId);
            declaredNamesByCallId.set(call.toolCallId, call.toolName);
          }
        }
      };

      const promoteResponse = Effect.gen(function* () {
        const halted = yield* Ref.get(haltRef);

        if (halted !== undefined) return yield* halted;
        const deferred = deferredResponse;

        if (deferred === undefined) return;
        const record = deferred.batch.records[0];

        if (record?.payload._tag !== "ModelResponseRecorded")
          return yield* RunJournalError.make({ message: "Deferred Turn has no owned response" });
        const calls = yield* declaredToolCalls(record.payload.messages);

        const batch = CanonicalBatch.make({
          ...deferred.batch,
          batchId: turnResponseBatchId(runId, deferred.turn),
        });

        yield* appendBatch(ctx, batch);
        acceptResponseDeclarations(batch);
        for (const call of calls.application) encodedParamsByCallId.set(call.id, call.params);
        recordCommittedUsage(batch);
        for (const entry of batch.records) knownIds.add(entry.recordId);
        deferredResponse = undefined;
        // Readonly handlers may already have started; promotion does not prove initial dispatch.
        yield* hit("turn:after-response-append");
      });

      const flushDeferredResponse = promoteResponse.pipe(
        // Retain failure before releasing the permit; a waiting capability must not retry it.
        Effect.tapError((failure) =>
          failure instanceof ThreadStoreError &&
          failure.cause instanceof AgentPersistenceCapacityError
            ? Effect.void
            : Ref.set(haltRef, failure),
        ),
        responseGate.withPermits(1),
      );

      // Initial instruction metadata supports resumed context and compaction. The engine
      // owns all subsequent history boundaries and supplies complete commit facts directly.
      let initialHistoryLength = 0;
      let initialInstructions: ReadonlyArray<Prompt.Message> = [];

      const completionState: {
        committed?: NonNullable<TurnCommitInput["runCompletion"]>;
      } = {};

      const turnCounter = yield* Ref.make(
        journal.committedTurns + (journal.policyUsage.modelRestarts ?? 0),
      );

      const idGenerator: (typeof IdGenerator)["Service"] = {
        nextThreadId: Effect.succeed(submission.threadId),
        nextRunId: Effect.succeed(runId),
        nextTurnId: Ref.modify(turnCounter, (turn) => [turnIdForRun(runId, turn + 1), turn + 1]),
      };

      // Original evaluated instructions and input survive changes of ownership and Binding.
      // The pending Turn re-enters through its native batch without duplicating that input.
      // Retained immediate history can contain system messages. Its prefix stays untouched;
      // only this Run's instruction slots receive the freshly evaluated instructions.
      const instructionView = (
        messages: ReadonlyArray<Prompt.Message>,
        instructions: ReadonlyArray<Prompt.Message>,
        insertWhenAbsent: boolean,
        priorRunPrefixLength: number,
      ) => {
        const projected: Array<Prompt.Message> = [];
        const lengths = [0];
        let inserted = false;

        for (const [index, message] of messages.entries()) {
          if (index >= priorRunPrefixLength && message.role === "system") {
            if (!inserted) projected.push(...instructions);
            inserted = true;
          } else projected.push(message);
          lengths.push(projected.length);
        }
        if (!inserted && insertWhenAbsent) projected.push(...instructions);

        return {
          messages: projected,
          prefixLength: (length: number) => lengths[length] ?? projected.length,
        };
      };

      const resumeContext = {
        prepare: ({ source }: { readonly source: Prompt.Prompt }) =>
          Effect.sync(() => {
            // A compaction can commit before the first response owns this Run's input messages.
            const prefix =
              journal.committedTurns === 0 && priorContext !== undefined
                ? priorContext.prompt.content.slice(priorContext.priorHistoryLength)
                : [];

            const priorRunPrefixLength =
              journal.committedTurns === 0
                ? resumeProjection.prompt.content.length
                : resumeProjection.historyBefore.content.length;

            const view = instructionView(
              prefix.length === 0
                ? resumeProjection.prompt.content
                : [...resumeProjection.prompt.content, ...prefix],
              initialInstructions,
              true,
              priorRunPrefixLength,
            );

            return {
              prompt: Prompt.fromMessages([
                ...view.messages,
                ...source.content.slice(initialHistoryLength),
              ]),
              priorRunPrefixLength: view.prefixLength(priorRunPrefixLength),
            };
          }),
      } satisfies RunContextHook<never, never>;

      const externalContext = runContextPreparation.hook;
      const needsContextReplay = priorContext !== undefined || journal.committedTurns > 0;

      const preparedContext: RunContextHook<RunContextPreparationError, never> | undefined =
        externalContext === undefined
          ? needsContextReplay
            ? resumeContext
            : undefined
          : {
              prepare: (request) =>
                (needsContextReplay
                  ? resumeContext.prepare(request)
                  : Effect.succeed({
                      prompt: request.source,
                      priorRunPrefixLength: journal.historyBefore.content.length,
                    })
                ).pipe(
                  Effect.flatMap(({ prompt, priorRunPrefixLength }) =>
                    externalContext.prepare({
                      ...request,
                      source: prompt,
                      priorRunPrefixLength,
                    }),
                  ),
                  Effect.map((prepared) => {
                    if (
                      prepared.rollover === undefined ||
                      !knownIds.has(compactionRecordId(runId, request.turn, "rollover"))
                    )
                      return prepared;
                    // The committed rollover, not a reevaluated hook, owns this Turn's reset.
                    const { rollover: _, ...retained } = prepared;

                    return retained;
                  }),
                ),
            };

      const durability: RunDurabilityHook<
        CoordinatorHalt | AgentPersistenceCapacityError | CompactionError,
        CurrentRunStart
      > = {
        toolResultMaxBytes: MAX_PERSISTED_JSON_BYTES,
        checkpoint,
        initialize: ({ initialHistory, priorHistoryLength }) =>
          recordHalt(
            Effect.gen(function* () {
              initialHistoryLength = initialHistory.content.length;
              initialInstructions = initialHistory.content
                .slice(priorHistoryLength)
                .filter((message) => message.role === "system");
              if (priorContext !== undefined) return;
              const recordId = decodeRecordIdSync(JSON.stringify(["run-context@1", runId]));

              const prompt = yield* Schema.encodeEffect(Prompt.Prompt)(
                Prompt.fromMessages(initialHistory.content.slice(priorHistoryLength)),
              ).pipe(
                Effect.flatMap(decodePersisted),
                Effect.mapError((cause) =>
                  RunJournalError.make({
                    message: "Original Run context exceeds persistence bounds",
                    cause,
                  }),
                ),
              );

              const historyDigest = yield* withCrypto(
                digestRunHistory(
                  Prompt.fromMessages(initialHistory.content.slice(0, priorHistoryLength)),
                ),
              );

              const retained = yield* Effect.forEach(
                [...historyEvidence.values()]
                  .filter((entry) => entry.sequence < journal.historyFrom)
                  .sort((left, right) => left.sequence - right.sequence),
                (entry) =>
                  Effect.gen(function* () {
                    const full = yield* getRecord({
                      threadId: ctx.threadId,
                      recordId: entry.record.recordId,
                    });

                    if (Option.isNone(full) || full.value.sequence !== entry.sequence)
                      return yield* RunJournalError.make({
                        message: "Retained history has no exact canonical fact",
                      });

                    return {
                      ...(yield* withCrypto(reference(full.value.record))),
                      sequence: entry.sequence,
                    };
                  }).pipe(Effect.provideService(ThreadReader, reader)),
              );

              const payload = yield* RunContextRecorded.makeEffect({
                version: 1,
                runId,
                runScopedInput: prompt,
                historyFrom: journal.historyFrom,
                historyThrough: CanonicalSequence.make(originalInput.sequence - 1),
                retained,
                historyDigest,
                priorHistoryLength,
                ...(journal.contextWindowId === undefined
                  ? {}
                  : { contextWindowId: journal.contextWindowId }),
              }).pipe(
                Effect.mapError((cause) =>
                  RunJournalError.make({
                    message: "Original Run context mapping exceeds its bounds",
                    cause,
                  }),
                ),
              );

              const start = yield* CurrentRunStart;

              yield* start.commit(yield* makeEnvelope(recordId, payload));
              knownIds.add(runStartedRecordId(runId));
              knownIds.add(recordId);
              historyEvidence.clear();
              projectedJournal = undefined;
            }),
          ),
        commitModelRestart: (restart) =>
          recordHalt(
            Effect.gen(function* () {
              if (knownIds.has(modelResponseRecordId(runId, restart.turn)))
                return yield* RunJournalError.make({
                  message: "Cannot restart a canonical model response",
                });
              const recordId = decodeRecordIdSync(`model-aborted:${runId}:${restart.restart}`);

              const record = yield* makeEnvelope(
                recordId,
                ModelCallAborted.make({
                  runId,
                  ...restart,
                  reason: "joined-input",
                  modelUsage: usageForCommit(restart.turn)?.modelUsage ?? [],
                  unobservedModelCalls: unobservedForCommit(restart.turn),
                }),
              );

              const batch = CanonicalBatch.make({
                batchId: decodeBatchIdSync(recordId),
                producerId: config.producerId,
                records: [record],
              });

              yield* appendBatch(ctx, batch);
              recordCommittedUsage(batch);
              knownIds.add(recordId);
            }),
          ),
        reservePolicyUsage: (usage) =>
          recordHalt(
            Effect.gen(function* () {
              yield* flushDeferredResponse;

              const recordId = decodeRecordIdSync(
                `policy:${runId}:${usage.programmaticToolCalls}:${usage.finalizationUsed}`,
              );

              if (knownIds.has(recordId)) return;

              const record = yield* makeEnvelope(
                recordId,
                RunPolicyUsageReserved.make({ runId, ...usage }),
              );

              yield* hit("policy:before-reservation-append");
              yield* appendBatch(
                ctx,
                CanonicalBatch.make({
                  batchId: decodeBatchIdSync(recordId),
                  producerId: config.producerId,
                  records: [record],
                }),
              );
              knownIds.add(recordId);
              yield* hit("policy:after-reservation-append");
            }),
          ),
        commitTurn: (commit) =>
          recordHalt(
            Effect.gen(function* () {
              const halted = yield* Ref.get(haltRef);

              if (halted !== undefined) return yield* halted;
              if (
                deferredResponse !== undefined &&
                (deferredResponse.turn !== commit.turn || deferredResponse.turnId !== commit.turnId)
              )
                return yield* RunJournalError.make({
                  message: "A new Turn preceded settlement of its deferred response",
                });
              if (commit._tag === "Partial") yield* promoteResponse;
              const deferred = deferredResponse;

              if (commit._tag === "Response" && deferred !== undefined)
                return yield* RunJournalError.make({
                  message: "Turn response was already deferred",
                });
              const responseId = modelResponseRecordId(runId, commit.turn);
              const suppliedResponse = commit._tag === "Partial" ? undefined : commit.response;

              const response =
                knownIds.has(responseId) || deferred !== undefined ? undefined : suppliedResponse;

              if (suppliedResponse !== undefined)
                currentToolTurn = { turn: commit.turn, turnId: commit.turnId };
              if (commit._tag === "Response" && response === undefined) return "committed" as const;

              const toolOperations: Array<ToolOperation> = [];
              const responseToolIds = new Set<ToolCallId>();

              for (const call of response?.calls ?? []) {
                if (declaredToolIds.has(call.toolCallId) || responseToolIds.has(call.toolCallId))
                  return yield* RunJournalError.make({
                    message: "Tool Call identity is reused within a Run",
                  });
                // Preserve every prior identity across compaction and recovery. Refuse the
                // whole response before dispatch if adding this call exceeds the inclusive bound.
                if (declaredToolIds.size + responseToolIds.size + 1 > MAX_RUN_TOOL_CALL_IDENTITIES)
                  return yield* RunJournalError.make({
                    message: `Run exceeds the ${MAX_RUN_TOOL_CALL_IDENTITIES} Tool Call identity limit`,
                  });
                const replay = currentContracts[call.toolName];

                if (replay === undefined)
                  return yield* RunJournalError.make({
                    message: "Canonical Tool declaration has no original operation contract",
                  });
                toolOperations.push(
                  ToolOperation.make({
                    toolCallId: call.toolCallId,
                    toolName: call.toolName,
                    executionClass: call.executionClass,
                    executionKind: call.executionKind,
                    replay,
                  }),
                );
                responseToolIds.add(call.toolCallId);
              }

              const toolResults =
                commit._tag === "Response"
                  ? []
                  : commit.results.filter(
                      (result) =>
                        !knownIds.has(
                          toolCallSettledRecordId(runId, commit.turn, result.toolCallId),
                        ),
                    );

              let runCompletion: NonNullable<TurnCommitInput["runCompletion"]> | undefined;

              if (commit._tag === "Settled" && commit.completion !== undefined) {
                const completion = commit.completion;

                const output = yield* decodePersisted(completion.output).pipe(
                  Effect.mapError((cause) =>
                    LedgerError.make({
                      operation: "recordCompleted",
                      message: "Run output exceeds canonical persistence bounds",
                      cause,
                    }),
                  ),
                );

                if (
                  completion.finishReason === "budget-exhausted" &&
                  completion.runDisposition !== undefined
                )
                  return yield* LedgerError.make({
                    operation: "recordCompleted",
                    message: "A budget-exhausted Run cannot declare an application run disposition",
                  });

                const runDisposition =
                  completion.runDisposition === undefined
                    ? undefined
                    : yield* decodePersisted(completion.runDisposition).pipe(
                        Effect.mapError((cause) =>
                          LedgerError.make({
                            operation: "recordCompleted",
                            message: "Run disposition exceeds canonical persistence bounds",
                            cause,
                          }),
                        ),
                      );

                runCompletion = {
                  output,
                  ...(runDisposition === undefined ? {} : { runDisposition }),
                  ...(completion.finishReason === "budget-exhausted"
                    ? {
                        finishReason: completion.finishReason,
                        ...(completion.exhausted === undefined
                          ? {}
                          : { exhausted: completion.exhausted }),
                      }
                    : {}),
                };
              }
              if (
                response === undefined &&
                deferred === undefined &&
                toolResults.length === 0 &&
                runCompletion === undefined
              )
                return "committed" as const;
              if (response === undefined && deferred === undefined && !knownIds.has(responseId))
                return yield* RunJournalError.make({
                  message: "Tool results or completion preceded the canonical response",
                });

              const input: TurnCommitInput = {
                toolResultMaxBytes: Math.min(
                  agent.definition.policy.toolResultBounds.maxBytes,
                  MAX_PERSISTED_JSON_BYTES,
                ),
                toolSelectionMaxBytes: utf8ByteLength(
                  JSON.stringify({ toolSelection: { toolNames: Object.keys(currentContracts) } }),
                ),
                runId,
                turn: commit.turn,
                turnId: commit.turnId,
                responseMessages: response?.messages ?? [],
                toolResults,
                toolOperations,
                toolExposure: response?.toolExposure,
                toolParameterRejections: response?.toolParameterRejections,
                runScopedPrefixLength: response?.runScopedPrefixLength,
                producerId: config.producerId,
                deploymentId: config.deploymentId,
                createdAt: yield* nowUtc,
                runCompletion,
                usage: usageForCommit(commit.turn),
                unobservedModelCalls: unobservedForCommit(commit.turn),
              };

              if (commit._tag === "Partial") {
                // The interpreter supplies only closed siblings, in declaration order.
                // Per-call identities retain each result before the child suspension.
                for (const result of toolResults) {
                  const batch = yield* withCrypto(
                    turnResultsBatch({ ...input, toolResults: [result] }),
                  );

                  yield* appendBatch(
                    ctx,
                    CanonicalBatch.make({
                      ...batch,
                      batchId: toolCallResultBatchId(runId, commit.turn, result.toolCallId),
                    }),
                  );
                  for (const record of batch.records) knownIds.add(record.recordId);
                  yield* hit("subagent:after-sibling-settle");
                }

                return "committed" as const;
              }

              if (commit._tag === "Response" && commit.defer === true) {
                if (
                  toolOperations.length === 0 ||
                  toolOperations.some(
                    (call) =>
                      call.executionClass !== "readonly" || call.executionKind !== "ordinary",
                  )
                )
                  return yield* RunJournalError.make({
                    message: "Only ordinary readonly Tool responses may be deferred",
                  });

                const candidate = {
                  turn: commit.turn,
                  turnId: commit.turnId,
                  // The journal owns encoded messages. Detach the remaining small metadata
                  // trees too: Schema validation alone is not an ownership transfer.
                  batch: yield* withCrypto(
                    turnCanonicalBatch({
                      ...input,
                      toolParameterRejections: input.toolParameterRejections?.map((rejection) => ({
                        ...rejection,
                        parameters: copyJson(rejection.parameters),
                        error: { ...rejection.error, reason: { ...rejection.error.reason } },
                      })),
                      toolExposure:
                        input.toolExposure === undefined
                          ? undefined
                          : Snapshot.make({
                              exposedToolNames: [...input.toolExposure.exposedToolNames],
                              ...(input.toolExposure.selection === undefined
                                ? {}
                                : {
                                    selection: Selection.make({
                                      toolNames: [...input.toolExposure.selection.toolNames],
                                    }),
                                  }),
                            }),
                      usage:
                        input.usage === undefined
                          ? undefined
                          : {
                              ...input.usage,
                              modelUsage: input.usage.modelUsage?.map((usage) =>
                                ModelCallUsage.make({
                                  ...usage,
                                  ...(usage.response === undefined
                                    ? {}
                                    : {
                                        response: ModelResponseIdentity.make({ ...usage.response }),
                                      }),
                                  inputTokens: InputTokenUsage.make({ ...usage.inputTokens }),
                                  outputTokens: OutputTokenUsage.make({ ...usage.outputTokens }),
                                }),
                              ),
                            },
                    }),
                  ),
                };

                const progress = progressWriters.get(ctx);

                if (progress === undefined)
                  return yield* ThreadStoreError.make({
                    operation: "reserve readonly dispatch",
                    message: "Dispatch has no scoped progress writer",
                  });

                const batch = yield* progress
                  .defer(candidate.batch)
                  .pipe(Effect.provideService(CurrentRunWriter, ctx));

                deferredResponse = { ...candidate, batch };

                return "deferred" as const;
              }

              let batch: CanonicalBatch;

              if (deferred === undefined) {
                batch = yield* withCrypto(
                  commit._tag === "Response"
                    ? turnResponseBatch(input)
                    : response === undefined
                      ? turnResultsBatch(input)
                      : turnCanonicalBatch(input),
                );
              } else if (toolResults.length === 0 && runCompletion === undefined) {
                batch = deferred.batch;
              } else {
                const results = yield* withCrypto(turnResultsBatch(input));

                batch = CanonicalBatch.make({
                  ...deferred.batch,
                  records: [...deferred.batch.records, ...results.records],
                });
              }

              yield* appendBatch(ctx, batch);
              for (const call of response?.calls ?? []) {
                declaredToolIds.add(call.toolCallId);
                encodedParamsByCallId.set(call.toolCallId, call.parameters);
                declaredNamesByCallId.set(call.toolCallId, call.toolName);
              }
              if (deferred !== undefined) {
                acceptResponseDeclarations(batch);
                deferredResponse = undefined;
              }
              if (commit._tag === "Response") initialDispatchProofTurns.add(commit.turn);
              recordCommittedUsage(batch);
              for (const record of batch.records) knownIds.add(record.recordId);
              if (runCompletion !== undefined) completionState.committed = runCompletion;
              yield* hit(
                commit._tag === "Response"
                  ? "turn:after-response-append"
                  : response === undefined && deferred === undefined
                    ? "turn:after-results-append"
                    : "turn:after-canonical-append",
              );

              return "committed" as const;
            }).pipe(responseGate.withPermits(1)),
          ),
        checkToolDispatch: recordHalt(
          Effect.gen(function* () {
            if (currentToolTurn === undefined)
              return yield* RunJournalError.make({
                message: "Tool dispatch preceded its canonical response",
              });
            yield* hit("tools:before-dispatch-fence");
            yield* ctx.checkFence;
            initialDispatchProofTurns.delete(currentToolTurn.turn);
            yield* hit("tools:after-dispatch-fence");
          }),
        ),
        step: {
          lookup: (key) =>
            recordHalt(
              Effect.gen(function* () {
                yield* flushDeferredResponse;

                const output = stepOutputs.get(
                  toolStepSettledRecordId(runId, key.toolCallId, key.stepName),
                );

                return output === undefined
                  ? Option.none()
                  : Option.some({ encodedOutput: output });
              }),
            ),
          reserve: (key) =>
            recordHalt(
              Effect.gen(function* () {
                yield* flushDeferredResponse;
                const recordId = toolStepSettledRecordId(runId, key.toolCallId, key.stepName);

                const template = yield* makeEnvelope(
                  recordId,
                  yield* ToolStepSettled.makeEffect({
                    runId,
                    toolCallId: key.toolCallId,
                    stepName: key.stepName,
                    output: null,
                    outputDigest: yield* withCrypto(digestJson(null)),
                  }).pipe(
                    Effect.mapError(() =>
                      ThreadStoreError.make({
                        operation: "reserve Step dispatch",
                        message: "Durable Step identity exceeds canonical persistence bounds",
                        cause: AgentPersistenceCapacityError.make({
                          message: "Durable Step identity exceeds canonical persistence bounds",
                        }),
                      }),
                    ),
                  ),
                );

                const progress = progressWriters.get(ctx);

                if (progress === undefined)
                  return yield* ThreadStoreError.make({
                    operation: "reserve Step dispatch",
                    message: "Dispatch has no scoped progress writer",
                  });

                return yield* progress
                  .reserve(
                    runId,
                    CanonicalBatch.make({
                      batchId: decodeBatchIdSync(recordId),
                      producerId: config.producerId,
                      records: [template],
                    }),
                    canonicalRecordBytes(template) +
                      MAX_PERSISTED_JSON_BYTES +
                      MAX_RUN_CONTINUATION_BYTES,
                  )
                  .pipe(Effect.provideService(CurrentRunWriter, ctx));
              }),
            ),
          commit: (key, encodedOutput) =>
            recordHalt(
              Effect.gen(function* () {
                yield* flushDeferredResponse;
                const recordId = toolStepSettledRecordId(runId, key.toolCallId, key.stepName);

                if (knownIds.has(recordId)) return;

                const output = yield* decodePersisted(encodedOutput).pipe(
                  Effect.mapError(() =>
                    ThreadStoreError.make({
                      operation: "commit Step result",
                      message: "Durable Step output exceeds canonical persistence bounds",
                      cause: AgentPersistenceCapacityError.make({
                        message: "Durable Step output exceeds canonical persistence bounds",
                      }),
                    }),
                  ),
                );

                const outputDigest = yield* withCrypto(digestJson(output));

                const envelope = yield* makeEnvelope(
                  recordId,
                  ToolStepSettled.make({
                    runId,
                    toolCallId: key.toolCallId,
                    stepName: key.stepName,
                    output,
                    outputDigest,
                  }),
                );

                yield* appendBatch(
                  ctx,
                  CanonicalBatch.make({
                    batchId: toolStepSettledBatchId(runId, key.toolCallId, key.stepName),
                    producerId: config.producerId,
                    records: [envelope],
                  }),
                );
                knownIds.add(recordId);
                stepOutputs.set(recordId, output);
                yield* hit("step:after-step-append");
              }),
            ),
        },
        noteTurnUsage: (usage) =>
          Effect.sync(() => {
            // Accumulate, never replace: a compaction summarizer and the
            // Turn's own response stage into the same canonical Turn.
            const key = usage.turn;
            const prior = stagedUsage.get(key);

            stagedUsage.set(key, {
              inputTokens: (prior?.inputTokens ?? 0) + usage.usage.inputTokens.total,
              outputTokens: (prior?.outputTokens ?? 0) + usage.usage.outputTokens.total,
              costMicrousd: (prior?.costMicrousd ?? 0) + usage.usage.costMicrousd,
              modelUsage: [...(prior?.modelUsage ?? []), usage.usage],
            });
            progressWriters
              .get(ctx)
              ?.stageUsage(runId, terminalUsageCharge(uncommittedModelUsage()));
          }),
        commitCompaction: (commit) =>
          Effect.gen(function* () {
            const canonicalTurn = commit.turn;
            const recordId = compactionRecordId(runId, canonicalTurn, commit.kind);

            if (knownIds.has(recordId)) {
              return yield* CompactionError.make({
                message: "Compaction is already committed for this Turn and kind",
              });
            }

            let sourceJournal = journal;
            let sourceBoundaries = boundaries;

            if (commit.kind !== "summarize") {
              // Results must be canonical before pruning or rollover can cover them. Newly committed
              // compactions remain overlays on this Attempt's append-only source, so omit those
              // overlays while reconstructing the exact source-to-record mapping.
              const tail = yield* ctx.tail;

              sourceBoundaries = [];
              sourceJournal = yield* recordHalt(
                projectRunJournalStream(
                  Stream.concat(
                    canonical,
                    Stream.fromIterable(
                      yield* recordHalt(
                        selectedRange(
                          ctx.threadId,
                          [submissionId],
                          tail.sequence,
                          canonicalThrough,
                        ),
                      ),
                    ),
                  ).pipe(
                    Stream.filter(
                      (envelope) =>
                        envelope.record.payload._tag !== "CompactionCreated" ||
                        envelope.sequence <= canonicalThrough,
                    ),
                  ),
                  runId,
                  (boundary) => sourceBoundaries.push(boundary),
                  priorContext,
                ),
              );
            }

            // Summaries use the initial prior-Run boundaries; pruning and rollover also admit
            // complete current-Run boundaries from the fresh canonical source above.
            const coverable = sourceBoundaries.filter(
              (boundary) =>
                commit.kind !== "summarize" ||
                ownerFirstSequence === undefined ||
                boundary.sequence < ownerFirstSequence,
            );

            if (coverable.length === 0) {
              return yield* CompactionError.make({
                message:
                  commit.kind === "rollover"
                    ? "Durable rollover requires complete canonical records"
                    : "Durable compaction requires eligible canonical records",
              });
            }

            const comparisonView = instructionView(
              sourceJournal.prompt.content,
              initialInstructions,
              false,
              sourceJournal.historyBefore.content.length,
            );

            // Compare each visible message once. A transformed source can authorize only its
            // exact canonical prefix; no per-candidate rereads or full encoded Prompt copies.
            let matchingPrefix = 0;
            let requiredThrough = 0;
            const comparisonLimit = Math.min(comparisonView.messages.length, commit.through);

            for (let index = 0; index < commit.through; index += 1) {
              const message = commit.source.content[index];

              if (
                message !== undefined &&
                (commit.kind === "clear-tool-results"
                  ? message.role === "tool"
                  : message.role !== "system")
              )
                requiredThrough = index + 1;
              if (index >= comparisonLimit || index !== matchingPrefix || message === undefined)
                continue;
              const canonicalMessage = comparisonView.messages[index];

              if (canonicalMessage === undefined) continue;

              // Preserve Prompt's JSON projection while ignoring persisted object-key order.
              const encode = (entry: Prompt.Message) =>
                encodeCompactionMessageJson(entry).pipe(
                  Effect.flatMap(decodeCompactionMessageJson),
                  Effect.map(canonicalJson),
                  Effect.mapError((cause) =>
                    CompactionError.make({
                      message: "Could not encode the canonical compaction prefix",
                      cause,
                    }),
                  ),
                );

              const left = yield* encode(canonicalMessage);
              const right = yield* encode(message);

              if (left === right) matchingPrefix += 1;
            }

            let lastCovered: JournalBoundary | undefined;

            for (let index = 0; index < coverable.length; index += 1) {
              const candidate = coverable[index];

              if (candidate === undefined) continue;
              const following = coverable[index + 1];

              if (following?.tag === "ToolCallSettled") continue;

              const length = comparisonView.prefixLength(candidate.promptLength);

              if (length === 0 || length > commit.through) continue;
              if (length < requiredThrough || length > matchingPrefix) continue;
              lastCovered = candidate;
            }
            if (lastCovered === undefined) {
              return yield* CompactionError.make({
                message:
                  commit.kind === "rollover"
                    ? "Rollover coverage cannot be mapped to complete canonical records"
                    : "Compaction coverage cannot be mapped to complete canonical records",
              });
            }
            const coveredSequence = lastCovered.sequence;

            if (
              commit.kind !== "summarize" &&
              sourceBoundaries.some(
                (boundary) =>
                  boundary.incomplete === true &&
                  boundary.terminalPriorRun !== true &&
                  boundary.sequence <= coveredSequence,
              )
            ) {
              return yield* CompactionError.make({
                message: "Compaction cannot cover an incomplete Tool batch",
              });
            }
            if (commit.kind === "summarize" && (commit.summary ?? "").trim().length === 0) {
              return yield* CompactionError.make({
                message: "A summarize compaction commit carried no summary",
              });
            }

            const payload = yield* Schema.decodeEffect(CompactionCreated)({
              _tag: "CompactionCreated",
              runId,
              turn: canonicalTurn,
              kind: commit.kind,
              coversThrough: lastCovered.sequence,
              ...(commit.kind === "summarize" ? { summary: commit.summary } : {}),
              ...(commit.kind === "rollover" && commit.handoff !== undefined
                ? { handoff: commit.handoff }
                : {}),
            }).pipe(
              Effect.mapError((cause) =>
                CompactionError.make({
                  message: "Compaction decision exceeds canonical persistence bounds",
                  cause,
                }),
              ),
            );

            const envelope = yield* recordHalt(makeEnvelope(recordId, payload));

            yield* recordHalt(hit("compaction:before-canonical-append"));
            yield* recordHalt(
              appendBatch(
                ctx,
                CanonicalBatch.make({
                  batchId: compactionBatchId(runId, canonicalTurn, commit.kind),
                  producerId: config.producerId,
                  records: [envelope],
                }),
              ),
            );
            knownIds.add(recordId);
            yield* recordHalt(hit("compaction:after-canonical-append"));
          }),
      };

      /** Digest of one declared call's canonical encoded parameters (the original normalized wire). */
      const approvalParametersDigest = (
        toolCallId: string,
      ): Effect.Effect<Digest, DurableWorkerFailure> =>
        Effect.gen(function* () {
          if (!encodedParamsByCallId.has(toolCallId)) {
            return yield* RunJournalError.make({
              message: `Tool Call ${toolCallId} requested approval before its response commit`,
            });
          }

          const parameters = yield* decodePersisted(encodedParamsByCallId.get(toolCallId)).pipe(
            Effect.mapError((cause) =>
              RunJournalError.make({
                message: `Tool Call ${toolCallId} parameters exceed canonical persistence bounds`,
                cause,
              }),
            ),
          );

          return yield* withCrypto(digestJson(parameters));
        });

      /**
       * Append the canonical approval records for one declared call: the `ToolApprovalRequested`
       * record — plus the immediate `ToolApprovalDecided` for a policy-auto or intent-backed
       * decision, one atomic batch — or the decision alone when the request is already canonical.
       * Batch identity: the Turn's FIRST canonical approval append owns
       * `turn-approvals:{runId}:{turn}`; later appends of the same Turn (across suspension
       * cycles) use deterministic per-call batches so committed batch content is never
       * contradicted; decision-only appends use `approval-decision:{sid}:{toolCallId}`. Record
       * identity dedupes every replay; an identity conflict means another pass already committed
       * the same records.
       */
      const appendApprovalRecords = (
        turnInfo: { readonly turn: number; readonly turnId: TurnId },
        toolCallId: ToolCallId,
        toolName: string,
        decided:
          | {
              readonly decision: ApprovalDecision;
              readonly resolver: string;
              readonly reason: string;
            }
          | undefined,
      ): Effect.Effect<void, DurableWorkerFailure> =>
        Effect.gen(function* () {
          const requestRecordId = toolApprovalRequestRecordId(runId, turnInfo.turn, toolCallId);
          const decisionRecordId = toolApprovalDecisionRecordId(runId, turnInfo.turn, toolCallId);
          const envelopes: Array<RecordEnvelope> = [];
          const appendRequest = !knownIds.has(requestRecordId);

          if (appendRequest) {
            const parametersDigest = yield* approvalParametersDigest(toolCallId);

            envelopes.push(
              yield* makeEnvelope(
                requestRecordId,
                ToolApprovalRequested.make({
                  blocksInitialDispatch: initialDispatchProofTurns.has(turnInfo.turn),
                  runId,
                  turnId: turnInfo.turnId,
                  turn: turnInfo.turn,
                  toolCallId,
                  toolName,
                  parametersDigest,
                }),
              ),
            );
          }
          if (decided !== undefined && !knownIds.has(decisionRecordId)) {
            envelopes.push(
              yield* makeEnvelope(
                decisionRecordId,
                ToolApprovalDecided.make({
                  runId,
                  turn: turnInfo.turn,
                  toolCallId,
                  decision: decided.decision,
                  resolver: decided.resolver,
                  reason: decided.reason,
                }),
              ),
            );
          }
          const head = envelopes[0];

          if (head !== undefined) {
            const batchId = appendRequest
              ? approvalRequestedTurns.has(turnInfo.turn)
                ? decodeBatchId(`approval-request:${runId}:${turnInfo.turn}:${toolCallId}`)
                : turnApprovalsBatchId(runId, turnInfo.turn)
              : approvalDecisionBatchId(submissionId, toolCallId);

            yield* appendBatch(
              ctx,
              CanonicalBatch.make({
                batchId,
                producerId: config.producerId,
                records: [head, ...envelopes.slice(1)],
              }),
            ).pipe(
              Effect.catchTag("AppendConflict", () => Effect.void),
              Effect.asVoid,
            );
            for (const record of envelopes) knownIds.add(record.recordId);
          }
          if (appendRequest) {
            approvalRequestedTurns.add(turnInfo.turn);
            yield* hit("approval:after-request-append");
          }
          if (decided !== undefined) {
            canonicalApprovalDecisions.set(toolCallId, decided.decision);
          }
        });

      /**
       * Retain every required request before any decision can remove initial-dispatch proof.
       * Policy preparation shares the engine deadline; canonical acceptance is a separate,
       * fenced commit. Canonical decisions and accepted resolver intents keep their audit.
       */
      const approval: RunApprovalHook<CoordinatorHalt | AgentPersistenceCapacityError, never> = {
        prepareBatch: (requests) =>
          recordHalt(
            Effect.gen(function* () {
              if (requests.length === 0) return;
              yield* flushDeferredResponse;
              const turnInfo = currentToolTurn;

              if (turnInfo === undefined)
                return yield* RunJournalError.make({
                  message: "Approval preparation preceded its canonical response",
                });
              for (const request of requests) {
                if (
                  request.threadId !== submission.threadId ||
                  request.runId !== runId ||
                  request.turnId !== turnInfo.turnId ||
                  declaredNamesByCallId.get(request.toolCallId) !== request.toolName
                )
                  return yield* RunJournalError.make({
                    message: "Approval preparation does not match its original declaration",
                  });
                yield* appendApprovalRecords(
                  turnInfo,
                  request.toolCallId,
                  request.toolName,
                  undefined,
                );
              }
            }),
          ),
        request: (request) =>
          recordHalt(
            Effect.gen(function* () {
              const canonical = canonicalApprovalDecisions.get(request.toolCallId);

              if (canonical !== undefined)
                return canonical === "approved"
                  ? { _tag: "approved" as const }
                  : { _tag: "denied" as const };
              const intent = approvalIntents.get(request.toolCallId);

              if (intent !== undefined)
                return intent.decision === "approved"
                  ? { _tag: "approved" as const, reason: intent.reason }
                  : { _tag: "denied" as const, reason: intent.reason };

              return approvalResolver === undefined
                ? { _tag: "unresolved" as const }
                : yield* approvalResolver.request(request);
            }),
          ),
        commit: (request, decision) =>
          recordHalt(
            Effect.gen(function* () {
              const turnInfo = currentToolTurn;

              if (
                turnInfo === undefined ||
                request.threadId !== submission.threadId ||
                request.runId !== runId ||
                request.turnId !== turnInfo.turnId ||
                declaredNamesByCallId.get(request.toolCallId) !== request.toolName
              )
                return yield* RunJournalError.make({
                  message: "Approval acceptance does not match its original declaration",
                });
              const canonical = canonicalApprovalDecisions.get(request.toolCallId);

              if (canonical !== undefined) {
                if (decision._tag !== canonical)
                  return yield* RunJournalError.make({
                    message: "Approval preparation disagrees with its canonical decision",
                  });

                return;
              }
              const intent = approvalIntents.get(request.toolCallId);

              if (intent !== undefined && decision._tag !== intent.decision)
                return yield* RunJournalError.make({
                  message: "Approval preparation disagrees with its accepted intent",
                });
              yield* appendApprovalRecords(
                turnInfo,
                request.toolCallId,
                request.toolName,
                intent !== undefined
                  ? { decision: intent.decision, resolver: intent.resolver, reason: intent.reason }
                  : decision._tag === "unresolved"
                    ? undefined
                    : {
                        decision: decision._tag,
                        resolver: APPROVAL_POLICY_RESOLVER,
                        reason: boundedApprovalReason(
                          decision.reason,
                          "The configured approval policy decided immediately",
                        ),
                      },
              );
            }),
          ),
      };

      /**
       * Joining/Joined queued input hook (plan §2.5, DUR-016). At every engine drain seam:
       *
       * 1. Reattach first — pre-existing joins whose canonical `input:{sid}` record is not yet
       *    prompt-covered re-deliver WITHOUT re-appending (record identity is the dedupe);
       *    covered inputs are already inside the journal prompt. A `joining` row whose input is
       *    canonical only lost its marker: it is repaired from history under the host's live
       *    ownership (DUR-015). A `joining` row WITHOUT canonical input was never consumed and
       *    is recovery's to revert — the hook never guesses at it (fail-closed).
       * 2. Claim fresh — `claimJoining` atomically transitions the contiguous ready prefix of
       *    strictly-later queue sequences to `joining` under the host's ownership token (no
       *    epoch bump; an admitted-not-ready row breaks the prefix). Per fresh claim: honor a
       *    pre-consumption abort intent by reverting (revert-then-abort), else append the
       *    deterministic `UserInputRecorded` (batch `submission-input:{sid}`, record
       *    `input:{sid}`, `runId` = host Run, kind `steering`) → `join:after-canonical-append`
       *    → `markJoined` → hand the engine the `RunInputCommand`.
       *
       * Delivered input is `steering`: the engine appends it to official history at the seam,
       * so the next Turn's response commit makes it model-visible canonically — exactly the
       * prompt-coverage rule recovery relies on.
       */
      const renderJoinedInput = ({
        input: encodedInput,
        messageAdmission,
      }: Pick<UserInputRecorded, "input" | "messageAdmission">) =>
        Effect.gen(function* () {
          if (Schema.is(FrameworkMessage)(messageAdmission))
            return yield* Schema.encodeEffect(Schema.fromJsonString(FrameworkMessage))(
              messageAdmission,
            ).pipe(
              Effect.mapError(() =>
                AgentInputError.make({ message: "Invalid worker completion message" }),
              ),
            );
          const inputPrompt = agent.definition.inputPrompt;

          if (inputPrompt === undefined) {
            return yield* renderInputPrompt(undefined, encodedInput, encodedInput);
          }

          const decodedInput = yield* Schema.decodeEffect(agent.definition.input)(
            encodedInput,
          ).pipe(
            Effect.mapError((cause) =>
              AgentInputDecodeError.make({
                message: cause.message,
              }),
            ),
          );

          return yield* renderInputPrompt(inputPrompt, decodedInput, encodedInput);
        });

      // Claims are authority, wakes are hints. A cancelled waiter retains every claim
      // for the seam drain; it never appends input while an old response can still commit.
      type PreparedJoin = {
        readonly claim: JoiningClaim;
        readonly input: Pick<UserInputRecorded, "input" | "messageAdmission">;
        readonly rendered: Prompt.RawInput;
      };

      const pendingJoinClaims: Array<{ readonly claim: JoiningClaim; prepared?: PreparedJoin }> =
        [];

      const claimInputs = Effect.fnUntraced(function* (maxCount: number) {
        const claims = yield* session.claimJoining(maxCount);

        pendingJoinClaims.push(...claims.map((claim) => ({ claim })));

        return claims.length;
      }, Effect.uninterruptible);

      const revertClaims = Effect.fnUntraced(function* (
        claims: ReadonlyArray<(typeof pendingJoinClaims)[number]>,
      ) {
        for (const pending of claims) {
          yield* session.revertJoining(pending.claim.submissionId);
          pendingJoinClaims.splice(pendingJoinClaims.indexOf(pending), 1);
        }
      }, Effect.uninterruptible);

      // Preparation remains interruptible; a completed render stays with its claim so
      // cancellation of the waiter cannot lose it or invoke the callback again at the seam.
      const prepareInputs = Effect.fnUntraced(function* (limit: number, firstReady = false) {
        if (
          pendingJoinClaims.length < limit &&
          (yield* claimInputs(limit - pendingJoinClaims.length)) > 0
        )
          yield* hit("join:after-claim");
        const prepared: Array<PreparedJoin> = [];

        for (const pending of pendingJoinClaims.slice(0, limit)) {
          const claim = pending.claim;

          const snapshot = yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: claim.submissionId }),
          );

          if (snapshot.abortIntent !== undefined) {
            yield* revertClaims([pending]);
            continue;
          }
          if (pending.prepared === undefined) {
            const input = {
              input: claim.inputPayload,
              ...(snapshot.submission.messageAdmission === undefined
                ? {}
                : { messageAdmission: snapshot.submission.messageAdmission }),
            };

            const rendered = yield* Effect.result(renderJoinedInput(input));

            if (Result.isFailure(rendered)) {
              // A rejected prompt and its suffix retain their own Runs in queue order.
              yield* revertClaims(pendingJoinClaims.slice(pendingJoinClaims.indexOf(pending)));
              break;
            }
            pending.prepared = { claim, input, rendered: rendered.success };
          }
          prepared.push(pending.prepared);
          if (firstReady) break;
        }

        return prepared;
      });

      const awaitJoin = recordHalt(
        Effect.gen(function* () {
          while (true) {
            const ready = yield* Effect.scoped(
              Effect.gen(function* () {
                const notified = yield* wake.subscribe(submission.threadId);

                if ((yield* prepareInputs(MAX_JOIN_DRAIN, true)).length > 0) return true;
                yield* notified;

                return false;
              }),
            );

            if (ready) return;
          }
        }),
      );

      const input: RunInputHook<
        CoordinatorHalt | AgentPersistenceCapacityError | Agent.Failure<typeof agent>,
        Agent.DefinitionRequirements<(typeof agent)["definition"]>
      > = {
        awaitJoin,
        drain: (policy) =>
          Effect.gen(function* () {
            const joinedInputs = yield* recordHalt(
              Effect.gen(function* () {
                const limit = policy === "one" ? 1 : MAX_JOIN_DRAIN;

                const joinedInputs: Array<
                  Pick<UserInputRecorded, "input" | "messageAdmission"> & {
                    readonly rendered?: Prompt.RawInput;
                  }
                > = [];

                if (joinBacklog === undefined) {
                  const hostSnapshot = yield* ledger.loadRecoverySnapshot(
                    RecoverySnapshotRequest.make({ submissionId }),
                  );

                  joinBacklog = hostSnapshot.joins;
                }
                // These inputs were already consumed by an earlier attempt. Restore the
                // entire uncovered prompt; only fresh claims are subject to the drain bound.
                for (const join of joinBacklog) {
                  const joinId = join.submissionId;

                  if (deliveredJoinInputs.has(joinId)) continue;
                  if (join.state !== "joining" && join.state !== "joined") continue;
                  const existing = joinedInputEnvelopes.get(joinId);

                  if (existing === undefined) continue;
                  if (join.state === "joining") {
                    // Canonical input without its joined marker (crash between the append and
                    // `markJoined`): repair the marker from history before reattaching.
                    yield* session.markJoined(joinId, {
                      recordId: existing.record.recordId,
                      sequence: existing.sequence,
                    });
                  }
                  deliveredJoinInputs.add(joinId);
                  if (
                    lastHostResponseSequence !== undefined &&
                    lastHostResponseSequence > existing.sequence
                  ) {
                    continue;
                  }
                  const payload = existing.record.payload;

                  if (payload._tag !== "UserInputRecorded") continue;
                  joinedInputs.push(payload);
                }
                const claims = yield* prepareInputs(limit);

                pendingJoinClaims.splice(0, claims.length);
                for (const { claim, input: payload, rendered } of claims) {
                  const recordId = submissionInputRecordId(claim.submissionId);
                  let sequence: CanonicalSequence;
                  const existing = joinedInputEnvelopes.get(claim.submissionId);

                  if (existing !== undefined) {
                    // Defensive reattach: the exact record is already canonical, so only the
                    // marker and the delivery remain (DUR-016 — never a duplicate append).
                    sequence = existing.sequence;
                  } else {
                    const envelope = yield* makeEnvelope(
                      recordId,
                      UserInputRecorded.make({
                        submissionId: claim.submissionId,
                        kind: "steering",
                        runId,
                        input: claim.inputPayload,
                        ...(payload.messageAdmission === undefined
                          ? {}
                          : { messageAdmission: payload.messageAdmission }),
                      }),
                    );

                    const result = yield* appendBatch(
                      ctx,
                      CanonicalBatch.make({
                        batchId: submissionInputBatchId(claim.submissionId),
                        producerId: config.producerId,
                        records: [envelope],
                      }),
                    );

                    sequence = result.firstSequence;
                    knownIds.add(recordId);
                    yield* hit("join:after-canonical-append");
                  }
                  yield* session.markJoined(claim.submissionId, { recordId, sequence });
                  deliveredJoinInputs.add(claim.submissionId);
                  joinedInputs.push({
                    ...payload,
                    rendered,
                  });
                }

                return joinedInputs;
              }),
            );

            return yield* Effect.forEach(joinedInputs, (joinedInput) =>
              (joinedInput.rendered === undefined
                ? renderJoinedInput(joinedInput)
                : Effect.succeed(joinedInput.rendered)
              ).pipe(
                Effect.map((input): RunInputCommand => ({
                  kind: "steering",
                  input,
                })),
              ),
            );
          }),
      };

      /**
       * Durable child establishment: `establish` performs or replays
       * the idempotent get-or-create establishment protocol under the
       * parent's ownership fence and reports where the one child stands; `join` appends the
       * atomic `[SubagentJoined, ToolCallSettled]` settlement batch (SUB-019) and applies the
       * reservation release. Both use the halt side channel exactly like the durability/step
       * hooks: the engine wraps a `CoordinatorHalt` into `SubagentDurabilityError` in the
       * handler channel while the Attempt aborts with the original infrastructure failure.
       */
      const establishmentGate = yield* Semaphore.make(1);

      const establishSubagent = (
        request: RunSubagentEstablishRequest,
      ): Effect.Effect<ChildEstablishStatus, CoordinatorHalt | AgentPersistenceCapacityError> =>
        recordHalt(
          Effect.gen(function* () {
            const toolCallId = request.toolCallId;
            const turnInfo = currentToolTurn;

            if (turnInfo === undefined) {
              return yield* RunJournalError.make({
                message: `Delegation Tool Call ${toolCallId} established before any canonical response commit`,
              });
            }

            const denied = (errorTag: string, message: string): ChildEstablishStatus => ({
              _tag: "denied",
              errorTag,
              message: boundedText(message),
            });

            let requestedPayload = subagentState.requested.get(toolCallId);

            if (requestedPayload === undefined) {
              // FIRST establishment of this call: fix every digest fail-closed BEFORE any
              // durable mutation, then reserve → append the canonical request (spec §12
              // steps 2-3). Replays below proceed from the canonical record instead — the
              // recorded request, not the re-computed handler values, is the establishment
              // authority (SUB-016/SUB-018).
              if (request.depth !== delegationDepth + 1) {
                return denied(
                  "SubagentDepthUnsupported",
                  `The child depth must be exactly one greater than its source depth ${delegationDepth}`,
                );
              }

              // Resolve the exact declaration through ordinary host registration. Recovery
              // below uses the already-persisted request, never a newer registration.
              let targetDigests = request.targetDigests;

              if (request.target !== undefined) {
                if (request.target.id !== request.targetAgentId) {
                  return denied(
                    "SubagentBindingUnavailable",
                    "The child target identity differs from its Definition",
                  );
                }

                const binding = yield* resolveDefinitionBinding(
                  registeredBindings,
                  request.target,
                ).pipe(Effect.result);

                if (binding._tag === "Failure") {
                  return denied("SubagentBindingUnavailable", binding.failure.message);
                }
                if (
                  targetDigests !== undefined &&
                  (targetDigests.agent !== binding.success.digests.agent ||
                    targetDigests.model !== binding.success.digests.model ||
                    targetDigests.tools !== binding.success.digests.tools)
                ) {
                  return denied(
                    "SubagentDigestsInvalid",
                    "The explicit child digests differ from its exact registration",
                  );
                }
                targetDigests = binding.success.digests;
              }

              const decodedDigests = yield* decodeDefinitionDigests(targetDigests).pipe(
                Effect.option,
              );

              if (Option.isNone(decodedDigests)) {
                return denied(
                  "SubagentDigestsInvalid",
                  "The declared child Binding digests are not valid stored digests",
                );
              }

              const childInput = yield* decodePersisted(request.encodedChildInput).pipe(
                Effect.option,
              );

              if (Option.isNone(childInput)) {
                return denied(
                  "SubagentInputUnpersistable",
                  "The prepared child input does not satisfy the canonical persistence bounds",
                );
              }
              const grant = yield* decodePersisted(request.encodedGrant).pipe(Effect.option);

              const allocation = yield* decodePersisted(request.encodedAllocation).pipe(
                Effect.option,
              );

              if (Option.isNone(grant) || Option.isNone(allocation)) {
                return denied(
                  "SubagentDeclarationUnpersistable",
                  "The delegation grant or allocation does not satisfy the canonical persistence bounds",
                );
              }
              const decodedGrant = Schema.decodeUnknownOption(SubagentGrant)(grant.value);

              if (Option.isNone(decodedGrant))
                return denied("SubagentGrantInvalid", "The child authority grant is invalid");
              if (request.policy === undefined || request.budget === undefined)
                return denied(
                  "SubagentBudgetInvalid",
                  "New durable children require their full execution policy and subtree allocation",
                );
              if (
                request.toolCallAllowance !== undefined &&
                !Schema.is(SubagentRequested.fields.toolCallAllowance)(request.toolCallAllowance)
              ) {
                return denied(
                  "SubagentAllowanceInvalid",
                  "The child Tool Call allowance must be a positive integer",
                );
              }
              if (request.policy !== undefined && !Schema.is(AgentPolicy)(request.policy)) {
                return denied("SubagentPolicyInvalid", "The resolved child policy is invalid");
              }
              if (request.budget !== undefined) {
                if (!Schema.is(SubagentBudgetReservation)(request.budget)) {
                  return denied("SubagentBudgetInvalid", "The shared delegation budget is invalid");
                }

                const decodedAllocation = Schema.decodeUnknownOption(SubagentReservationAmounts)(
                  allocation.value,
                );

                if (
                  Option.isNone(decodedAllocation) ||
                  !Equal.equals(decodedAllocation.value, request.budget.allocation)
                ) {
                  return denied(
                    "SubagentBudgetInvalid",
                    "The reserved allocation differs from its budget declaration",
                  );
                }
                const { caps, allocation: requested } = request.budget;
                const prior = [...subagentState.requested.values()];

                if (
                  prior.some(
                    (entry) => entry.budget === undefined || !Equal.equals(entry.budget.caps, caps),
                  )
                ) {
                  return denied(
                    "SubagentParentBudgetConflict",
                    "Delegations in one parent Run must share the same caps",
                  );
                }
                if (
                  caps.maxTotalChildInvocations !== undefined &&
                  prior.reduce(
                    (sum, entry) => sum + 1 + (entry.budget?.descendantInvocations ?? 0),
                    1 + (request.budget.descendantInvocations ?? 0),
                  ) > caps.maxTotalChildInvocations
                ) {
                  return denied(
                    "SubagentBudgetExhausted",
                    "The parent Run exhausted its child invocation budget",
                  );
                }
                const active = prior.filter((entry) => !subagentState.joined.has(entry.toolCallId));

                if (
                  caps.maxConcurrentChildren !== undefined &&
                  active.length >= caps.maxConcurrentChildren
                ) {
                  return denied(
                    "SubagentBudgetExhausted",
                    "The parent Run exhausted its concurrent child budget",
                  );
                }

                const dimensions = [
                  ["turns", "maxTurns"],
                  ["toolCalls", "maxToolCalls"],
                  ["durationMillis", "maxDurationMillis"],
                  ["inputTokens", "maxInputTokens"],
                  ["outputTokens", "maxOutputTokens"],
                  ["costMicrousd", "maxCostMicrousd"],
                  ["resultBytes", "maxResultBytes"],
                ] as const;

                const accounting = Schema.Struct({ consumed: SubagentReservationAmounts });

                for (const [amount, cap] of dimensions) {
                  const limit = caps[cap];

                  if (limit === undefined) continue;
                  let consumed = requested[amount];

                  for (const entry of prior) {
                    const joined = subagentState.joined.get(entry.toolCallId);

                    const observed =
                      joined === undefined
                        ? Option.none()
                        : Schema.decodeUnknownOption(accounting)(joined.finalAccounting);

                    consumed += Option.isSome(observed)
                      ? observed.value.consumed[amount]
                      : (entry.budget?.allocation[amount] ?? limit);
                  }
                  if (consumed > limit) {
                    return denied(
                      "SubagentBudgetExhausted",
                      `The parent Run exhausted its shared ${amount} budget`,
                    );
                  }
                }
              }
              const childInputDigest = yield* withCrypto(digestJson(childInput.value));
              const grantDigest = yield* withCrypto(digestJson(grant.value));
              const allocationDigest = yield* withCrypto(digestJson(allocation.value));
              const reservationId = childReservationIdFor(runId, toolCallId);

              // Top-level attached declarations retain their independent explicit pool. A
              // child with an ancestor allocation shares its residual with both lifetimes.
              const subtree =
                delegationDepth === 0
                  ? undefined
                  : yield* workerRuntime
                      .reserveSubtree(
                        submission.threadId,
                        SubtreeBudgetReserved.make({
                          reservationId,
                          executionRunId: runId,
                          sourceSubmissionId: submissionId,
                          childThreadId: childThreadIdFor(submissionId, toolCallId),
                          lifetime: "attached",
                          depth: request.depth,
                          policy: request.policy,
                          grant: decodedGrant.value,
                          budget: request.budget,
                        }),
                      )
                      .pipe(Effect.result);

              if (subtree?._tag === "Failure") {
                if (subtree.failure.reason === "storage")
                  return yield* LedgerError.make({
                    operation: "reserve-subtree",
                    message: "Subtree reservation is unavailable",
                  });

                return denied(
                  "SubagentBudgetExhausted",
                  `The subtree reservation was refused: ${subtree.failure.reason}`,
                );
              }

              yield* session
                .reserveChildBudget({
                  reservationId,
                  parentToolCallId: toolCallId,
                  allocation: allocation.value,
                  allocationDigest,
                })
                .pipe(
                  Effect.catchTag(
                    "ChildReservationConflict",
                    conflictToLedgerError("reserveChildBudget"),
                  ),
                );
              yield* hit("subagent:after-reserve");
              requestedPayload = SubagentRequested.make({
                runId,
                turnId: turnInfo.turnId,
                turn: turnInfo.turn,
                toolCallId,
                delegationId: request.delegationId,
                targetAgentId: request.targetAgentId,
                targetDigests: decodedDigests.value,
                childInput: childInput.value,
                childInputDigest,
                grantDigest,
                grant: decodedGrant.value,
                depth: request.depth,
                reservationId,
                reservationDigest: allocationDigest,
                childThreadId: childThreadIdFor(submissionId, toolCallId),
                childPrincipal: submission.principal,
                childIdempotencyKey: childIdempotencyKeyFor(runId, toolCallId),
                ...(request.toolCallAllowance === undefined
                  ? {}
                  : { toolCallAllowance: request.toolCallAllowance }),
                ...(request.policy === undefined ? {} : { policy: request.policy }),
                ...(request.budget === undefined ? {} : { budget: request.budget }),
              });
              const requestRecordId = subagentRequestedRecordId(runId, toolCallId);

              if (!knownIds.has(requestRecordId)) {
                const envelope = yield* makeEnvelope(requestRecordId, requestedPayload);

                yield* appendBatch(
                  ctx,
                  CanonicalBatch.make({
                    batchId: subagentRequestedBatchId(runId, toolCallId),
                    producerId: config.producerId,
                    records: [envelope],
                  }),
                ).pipe(
                  Effect.catchTag("AppendConflict", () => Effect.void),
                  Effect.asVoid,
                );
                knownIds.add(requestRecordId);
              }
              subagentState.requested.set(toolCallId, requestedPayload);
              yield* hit("subagent:after-request-append");
            }
            // Steps 4-8: resolveAdmission-gated admission with immutable parent linkage,
            // child materialization + lineage, readiness, Receipt (SUB-016/SUB-017/SUB-031).
            const admission = yield* establishChildFromRequest(submission, requestedPayload);

            if (admission._tag === "indeterminate") {
              // Wait-and-retry, never a second admission: the Attempt aborts with the
              // obligation still owed and the next pass re-queries the authoritative owner.
              return yield* LedgerError.make({
                operation: "establishSubagent",
                message: `Child admission for Tool Call ${toolCallId} is indeterminate (${admission.reason}); retrying without a second admission (SUB-031)`,
              });
            }
            const childRunId = runIdForSubmission(admission.childSubmissionId);
            // Step 9: the start link is appended only after the child Receipt exists (SUB-017).
            let startedPayload = subagentState.started.get(toolCallId);

            if (startedPayload === undefined) {
              startedPayload = SubagentStarted.make({
                runId,
                toolCallId,
                childThreadId: requestedPayload.childThreadId,
                childSubmissionId: admission.childSubmissionId,
                childReceiptId: admission.receiptId,
                childRunId,
              });
              const startRecordId = subagentStartedRecordId(runId, toolCallId);

              if (!knownIds.has(startRecordId)) {
                const envelope = yield* makeEnvelope(startRecordId, startedPayload);

                yield* appendBatch(
                  ctx,
                  CanonicalBatch.make({
                    batchId: subagentStartedBatchId(runId, toolCallId),
                    producerId: config.producerId,
                    records: [envelope],
                  }),
                ).pipe(
                  Effect.catchTag("AppendConflict", () => Effect.void),
                  Effect.asVoid,
                );
                knownIds.add(startRecordId);
              }
              subagentState.started.set(toolCallId, startedPayload);
              yield* hit("subagent:after-start-append");
            } else if (startedPayload.childSubmissionId !== admission.childSubmissionId) {
              return yield* LedgerError.make({
                operation: "establishSubagent",
                message: `The canonical SubagentStarted record names child ${startedPayload.childSubmissionId} but admission resolved ${admission.childSubmissionId}; establishment fails closed (SUB-016)`,
              });
            }
            yield* session
              .attachChildToReservation({
                reservationId: decodeChildReservationIdSync(requestedPayload.reservationId),
                childSubmissionId: startedPayload.childSubmissionId,
              })
              .pipe(
                Effect.catchTag(
                  "ChildReservationConflict",
                  conflictToLedgerError("attachChildToReservation"),
                ),
              );

            const identity = {
              childThreadId: startedPayload.childThreadId,
              childSubmissionId: startedPayload.childSubmissionId,
              childRunId: startedPayload.childRunId,
              receiptId: startedPayload.childReceiptId,
            };

            const child = yield* ledger.lookup(
              SubmissionLookupById.make({ submissionId: startedPayload.childSubmissionId }),
            );

            if (Option.isNone(child)) {
              return yield* LedgerError.make({
                operation: "establishSubagent",
                message: `Established child ${startedPayload.childSubmissionId} is unknown to the ledger`,
              });
            }
            if (child.value.state !== "settled") {
              return { _tag: "waiting" as const, ...identity };
            }

            // §1.6: the child already settled — verify Parent Link, target, digests, and the
            // settlement record fail-closed before handing the outcome to the handler.
            const verification = yield* verifySettledChild(
              submission,
              requestedPayload,
              startedPayload.childSubmissionId,
            );

            if (verification._tag === "mismatch") {
              return denied("SubagentVerificationFailed", verification.message);
            }

            return {
              _tag: "settled" as const,
              ...identity,
              outcome: verification.value.outcome,
              encodedResult: verification.value.encodedResult,
              ...(yield* verifiedChildUsage(verification.value, startedPayload.childRunId)),
              // The child Settlement's honest exhaustion marker (RUN-018)
              // rides to the parent handler so the delegation can surface a
              // budget-truncated partial to the orchestrator (SUB-034).
              ...(verification.value.settlement.finishReason === undefined
                ? {}
                : { finishReason: verification.value.settlement.finishReason }),
            };
          }).pipe(establishmentGate.withPermits(1)),
        );

      const joinSubagent = (
        request: RunSubagentJoinRequest,
      ): Effect.Effect<void, CoordinatorHalt | AgentPersistenceCapacityError> =>
        recordHalt(
          Effect.gen(function* () {
            const toolCallId = request.toolCallId;
            const turnInfo = currentToolTurn;

            if (turnInfo === undefined) {
              return yield* RunJournalError.make({
                message: `Delegation Tool Call ${toolCallId} joined before any canonical response commit`,
              });
            }
            const requestedPayload = subagentState.requested.get(toolCallId);
            const startedPayload = subagentState.started.get(toolCallId);

            if (requestedPayload === undefined || startedPayload === undefined) {
              return yield* RunJournalError.make({
                message: `Delegation Tool Call ${toolCallId} joined without canonical establishment records`,
              });
            }
            const reservationId = decodeChildReservationIdSync(requestedPayload.reservationId);
            const joinedRecordId = subagentJoinedRecordId(runId, toolCallId);
            let finalAccounting: PersistedJson;
            const existingJoined = subagentState.joined.get(toolCallId);

            if (existingJoined !== undefined) {
              // The atomic join batch is already canonical: only the reservation release below
              // may remain (spec §12 join step 6 — the canonical record is the replay source).
              finalAccounting = existingJoined.finalAccounting;
            } else {
              const encodedResult = yield* decodePersisted(request.encodedResult).pipe(
                Effect.mapError((cause) =>
                  RunJournalError.make({
                    message: `The projected result of Tool Call ${toolCallId} exceeds canonical persistence bounds`,
                    cause,
                  }),
                ),
              );

              const accounting = yield* decodePersisted(request.encodedAccounting).pipe(
                Effect.mapError((cause) =>
                  RunJournalError.make({
                    message: `The join accounting of Tool Call ${toolCallId} exceeds canonical persistence bounds`,
                    cause,
                  }),
                ),
              );

              const verification = yield* verifySettledChild(
                submission,
                requestedPayload,
                startedPayload.childSubmissionId,
              );

              if (verification._tag === "mismatch") {
                return yield* LedgerError.make({
                  operation: "joinSubagent",
                  message: `Child Settlement verification failed for Tool Call ${toolCallId}: ${verification.message}`,
                });
              }
              const verified = verification.value;
              const toolName = declaredNamesByCallId.get(toolCallId);

              if (toolName === undefined) {
                return yield* RunJournalError.make({
                  message: `Delegation Tool Call ${toolCallId} joined without a declared Tool name`,
                });
              }

              yield* hit("subagent:before-join-append");

              const joinedPayload = SubagentJoined.make({
                runId,
                toolCallId,
                childSubmissionId: startedPayload.childSubmissionId,
                childSettlementId: verified.settlement.settlementId,
                childOutcome: verified.outcome,
                childResultDigest: yield* withCrypto(
                  digestJson(verified.settlement.result ?? null),
                ),
                projectedResultDigest: yield* withCrypto(digestJson(encodedResult)),
                usageSummary: yield* childUsageSummaryOf(
                  verified.childRecords,
                  startedPayload.childRunId,
                ),
                ...(yield* verifiedChildUsage(verified, startedPayload.childRunId)),
                reservationId: requestedPayload.reservationId,
                finalAccounting: accounting,
              });

              const settledRecordId = toolCallSettledRecordId(runId, turnInfo.turn, toolCallId);
              const joinedEnvelope = yield* makeEnvelope(joinedRecordId, joinedPayload);

              const settledEnvelope = yield* makeEnvelope(
                settledRecordId,
                ToolCallSettled.make({
                  runId,
                  toolCallId,
                  toolName,
                  result: encodedResult,
                  isFailure: request.isFailure,
                }),
              );

              yield* appendBatch(
                ctx,
                CanonicalBatch.make({
                  batchId: subagentJoinBatchId(runId, toolCallId),
                  producerId: config.producerId,
                  records: [joinedEnvelope, settledEnvelope],
                }),
              ).pipe(
                Effect.catchTag("AppendConflict", () => Effect.void),
                Effect.asVoid,
              );
              knownIds.add(joinedRecordId);
              knownIds.add(settledRecordId);
              subagentState.joined.set(toolCallId, joinedPayload);
              finalAccounting = accounting;
              yield* hit("subagent:after-join-append");
            }
            yield* applyReservationRelease(reservationId, finalAccounting, {
              threadId: submission.threadId,
              runId,
              toolCallId,
            });
          }),
        );

      const subagent: RunSubagentHook<CoordinatorHalt | AgentPersistenceCapacityError, never> = {
        establish: establishSubagent,
        join: joinSubagent,
      };

      const toolAuthorization: RunToolAuthorizationHook<AgentToolAuthorizationCheckError> = {
        authorize: (request: RunToolAuthorizationRequest) =>
          inheritedGrant !== undefined &&
          !inheritedGrant.allowedToolNames.includes(request.call.toolName)
            ? Effect.succeed({
                _tag: "denied",
                reason: "Tool exceeds the worker's immutable grant",
              })
            : runToolAuthorization.authorize({
                ...request,
                // Preserve the admitted wire value even when an Agent Schema's decode/encode
                // pair normalizes differently on a second pass.
                input: submission.inputPayload,
                ...(Schema.is(FrameworkMessage)(submission.messageAdmission)
                  ? { frameworkMessage: submission.messageAdmission }
                  : {}),
              }),
      };

      const options: RunOptions<
        | CoordinatorHalt
        | AgentPersistenceCapacityError
        | AgentToolAuthorizationCheckError
        | CompactionError
        | RunContextPreparationError
        | Agent.Failure<typeof agent>,
        Agent.DefinitionRequirements<(typeof agent)["definition"]>
      > = {
        threadId: submission.threadId,
        runId,
        history: pending === undefined ? journal.historyBefore : resumeProjection.historyBefore,
        input,
        // Ready corrections share the next model Turn, bounded by MAX_JOIN_DRAIN.
        commandDrainPolicy: "all",
        ...(priorContext !== undefined ||
        journal.committedTurns > 0 ||
        Schema.is(FrameworkMessage)(submission.messageAdmission)
          ? { retainedInput: submission.inputPayload }
          : {}),
        ...(priorContext === undefined
          ? {}
          : {
              retainedContext: Prompt.fromMessages(
                priorContext.prompt.content.slice(priorContext.priorHistoryLength),
              ),
            }),
        ...(Schema.is(FrameworkMessage)(submission.messageAdmission)
          ? { frameworkMessage: submission.messageAdmission }
          : {}),
        approval,
        toolAuthorization,
        ...(journal.toolSelection === undefined
          ? {}
          : {
              toolSelection: {
                // A settled discovery receipt survives deployment; its selection only
                // names current tools. Keep validation of fresh selections fail-closed.
                toolNames: journal.toolSelection.toolNames.filter((name) =>
                  Object.hasOwn(agent.definition.toolkit.tools, name),
                ),
              },
            }),
        durability,
        subagent,
        delegationDepth,
        ...(inheritedGrant === undefined ? {} : { subagentGrant: inheritedGrant }),
        ...(inheritedBudget === undefined ? {} : { subagentBudget: inheritedBudget }),
        ...(submission.workerAdmission?.origin.budgetScope === undefined
          ? {}
          : { subagentBudgetScope: submission.workerAdmission.origin.budgetScope }),
        runStartedAt: runTiming.startedAt,
        durationDeadline: runTiming.deadline,
        ...(submission.workerAdmission === undefined
          ? {}
          : {
              toolCallAllowance: submission.workerAdmission.origin.toolCallAllowance,
            }),
        ...(submission.parentLinkage !== undefined &&
        childLineage?._tag === "SubagentLineageRecorded"
          ? { toolCallAllowance: childLineage.toolCallAllowance }
          : {}),
        ...(pending === undefined
          ? {}
          : {
              resume: {
                ...(settledCompletion === undefined ? {} : { settledCompletion }),
                turn: pending.turn,
                toolResultMaxBytes: pending.toolResultMaxBytes,
                turnId: pending.turnId,
                calls: pending.calls,
                toolParameterRejections: pending.toolParameterRejections,
                settled: pending.settled,
                // Legacy eager requests have no exposure snapshot. Their validated canonical
                // declarations establish the minimal original exposure needed for batch resume.
                toolExposure:
                  pending.toolExposure ??
                  Snapshot.make({
                    exposedToolNames: [...new Set(pending.calls.map((call) => call.name))],
                  }),
                ...(resumeLeadingMessages === undefined
                  ? {}
                  : { leadingMessages: resumeLeadingMessages }),
                ...(resumeResponseMessages === undefined
                  ? {}
                  : { responseMessages: resumeResponseMessages }),
              },
            }),
        ...(preparedContext === undefined ? {} : { context: preparedContext }),
        beforeTurn: () =>
          Effect.gen(function* () {
            if (
              yieldAfter !== undefined &&
              (yield* Clock.currentTimeMillis) >= DateTime.toEpochMillis(yieldAfter)
            ) {
              yield* Deferred.succeed(yieldSignal, undefined);

              // The successful signal wins the outer race and closes the execution Scope;
              // no synthetic engine RunFailed event or terminal Settlement is produced.
              return yield* Effect.never;
            }
            if (submissionScheduling.yieldTo !== undefined && (yield* Ref.get(turnCounter)) > 0) {
              const candidate = yield* recordHalt(
                eligibleThreadHead(submission.threadId, submission.queueSequence),
              );

              if (
                Option.isSome(candidate) &&
                (candidate.value.state === "ready" ||
                  candidate.value.state === "running" ||
                  candidate.value.state === "input-applied")
              ) {
                const next = yield* recordHalt(
                  ledger.lookup(
                    SubmissionLookupById.make({
                      submissionId: candidate.value.submissionId,
                    }),
                  ),
                );

                if (
                  Option.isSome(next) &&
                  next.value.agentId === submission.agentId &&
                  (next.value.state === "ready" ||
                    next.value.state === "running" ||
                    next.value.state === "input-applied") &&
                  (yield* recordHalt(
                    submissionScheduling.yieldTo({ active: submission, next: next.value }),
                  ))
                ) {
                  yield* Deferred.succeed(yieldSignal, next.value.submissionId);

                  return yield* Effect.never;
                }
              }
            }
          }),
        ...(runContextPreparation.transientContext === undefined
          ? {}
          : { transientContext: runContextPreparation.transientContext }),
        ...(config.estimateCostMicrousd === undefined
          ? {}
          : { estimateCostMicrousd: config.estimateCostMicrousd }),
        resumeUsage: {
          ...journal.usage,
          ...journal.policyUsage,
          ...(journal.usage.modelCalls === 0 &&
          (journal.usage.unobservedModelCalls ?? 0) === 0 &&
          !records.some(
            ({ record }) =>
              record.payload._tag === "ModelResponseInterrupted" && record.payload.runId === runId,
          )
            ? emptyRunTotals()
            : runTotalsFromSummary(yield* currentUsageSummary())),
          children: childUsageReportsOf(subagentState),
        },
        ...(journal.contextWindowId === undefined
          ? {}
          : { initialContextWindowId: journal.contextWindowId }),
        ...(journal.protectedContext === undefined
          ? {}
          : { protectedContext: journal.protectedContext }),
        ...(pendingContextToolCallId === undefined ? {} : { pendingContextToolCallId }),
      };

      // Durability §9: the superseding Attempt records the interruption BEFORE re-invoking the
      // model. A batch resume never re-invokes the model for the pending Turn, so it is exempt.
      if (
        pending === undefined &&
        lineage.inputWasRecorded &&
        lineage.supersededEpoch >= 1 &&
        lineage.supersededEpoch < ctx.producerEpoch
      ) {
        yield* appendInterruptedAudit(ctx, runId, lineage, knownIds);
        interruptedUsage = true;
      }

      const consume = AgentRuntime.executeWithUsageAccountingUnknown(
        agent,
        submission.inputPayload,
        options,
      ).pipe(
        Effect.provideService(AgentUpdateAcceptance, {
          accept: (update) =>
            flushDeferredResponse.pipe(
              Effect.catch((failure) =>
                Ref.set(haltRef, failure).pipe(
                  Effect.andThen(Effect.fail(UpdateError.make({ reason: "storage" }))),
                ),
              ),
              Effect.andThen(() =>
                updateRuntime.emit({
                  updateId: update.updateId,
                  value: update.value,
                  submission,
                  runId,
                  producerEpoch: ctx.producerEpoch,
                  definitions: submission.agentDigests,
                }),
              ),
              Effect.catchTag(["LedgerError", "DurableRuntimeFailpointError"], (failure) =>
                // Preserve the original infrastructure failure at the coordinator boundary;
                // the next semantic checkpoint must not commit this Tool failure as an outcome.
                Ref.set(haltRef, failure).pipe(
                  Effect.andThen(Effect.fail(UpdateError.make({ reason: "storage" }))),
                ),
              ),
            ),
        }),
        Effect.provideService(ModelUsageAccounting, {
          noteIncompleteUsage: (turn) =>
            Effect.sync(() => {
              stagedUnobservedCalls.set(turn, (stagedUnobservedCalls.get(turn) ?? 0) + 1);
            }),
        }),
        Effect.provide(ThreadHistory.layer),
        Effect.provideService(SubagentHost.forTool, (source) =>
          source.threadId !== submission.threadId ||
          source.agentId !== submission.agentId ||
          source.runId !== runId
            ? SubagentHost.unavailable
            : workerRuntime.facet(
                {
                  source,
                  policy: agent.definition.policy,
                  depth: delegationDepth,
                  ...(inheritedGrant === undefined ? {} : { grant: inheritedGrant }),
                },
                submission.principal,
                submission.submissionId,
              ),
        ),
        Effect.provideService(MessagingHost.forTool, (source) =>
          source.threadId !== submission.threadId ||
          source.agentId !== submission.agentId ||
          source.runId !== runId
            ? MessagingHost.unavailable
            : messagingRuntime.forTool(source, submission.principal),
        ),
        Effect.provideService(CurrentToolFailureObserver, toolFailureObserver),
        Effect.provideService(RunToolVisibility, runToolVisibility),
        Effect.provideService(RunToolScheduling, runToolScheduling),
        Effect.provideService(ContextCompactor, compactor),
        Effect.provideService(RunContextPreparation, runContextPreparation),
        Effect.andThen(checkpoint),
        // A wrapped Tool/approval failure cannot outrank retained infrastructure authority.
        // Preserve independent defects, interruptions and already-marked coordinator failures.
        Effect.catchCause((cause) =>
          Ref.get(haltRef).pipe(
            Effect.flatMap((failure) =>
              failure === undefined
                ? Effect.failCause(cause)
                : Effect.failCause(
                    Cause.map(cause, (error) =>
                      error instanceof CoordinatorHalt ? error : new CoordinatorHalt(failure),
                    ),
                  ),
            ),
          ),
        ),
        // Retain while the Attempt's services, claim renewal and abort watcher are still live.
        // A failed retention halts the coordinator; it is not a failed Tool or a safe suspension.
        Effect.tapCause((cause) =>
          Option.isSome(agentApprovalPendingOption(cause)) &&
          cause.reasons.every(Cause.isFailReason)
            ? Effect.flatMap(DurableApprovalSuspension, (retain) =>
                retain === undefined ? Effect.void : halt(retain),
              )
            : Effect.void,
        ),
        Effect.as({ _tag: "run" as const }),
      );

      // Durable §13: the abort command becomes canonical (serialized on the append gate) BEFORE
      // the Run fiber is interrupted by losing the race.
      const abortWatcher = halt(
        Effect.gen(function* () {
          while (true) {
            yield* Effect.sleep(config.abortPollInterval);

            const intent = yield* ledger.readAbortIntent(AbortIntentRequest.make({ submissionId }));

            if (intent === undefined) continue;
            yield* appendAbortRecord(ctx, intent);

            return { _tag: "aborted" as const };
          }
        }),
      );

      // Liveness only: the lease keeps the claim visible; correctness stays with the epoch fence.
      // OwnershipLost ends the race and interrupts the Run fiber cleanly.
      const renewal = halt(session.maintain(config.leaseRenewalInterval));

      const execution = Effect.raceFirst(consume, Effect.raceFirst(abortWatcher, renewal));

      const raced = (
        yieldAfter === undefined && submissionScheduling.yieldTo === undefined
          ? execution
          : Effect.raceFirst(
              execution,
              Deferred.await(yieldSignal).pipe(
                Effect.map((nextSubmissionId) => ({
                  _tag: "yielded" as const,
                  ...(nextSubmissionId === undefined ? {} : { nextSubmissionId }),
                })),
              ),
            )
      ).pipe(Effect.provideService(IdGenerator, idGenerator));

      const result = yield* raced.pipe(
        Effect.catchCauseFilter(
          Cause.findError,
          (
            error,
            cause,
          ): Effect.Effect<
            | { readonly _tag: "failedRun"; readonly outcome: AttemptOutcome }
            | { readonly _tag: "aborted" }
            | { readonly _tag: "suspendedRun"; readonly toolCallId: ToolCallId }
            | {
                readonly _tag: "suspendedChildRun";
                readonly children: AgentChildPending["children"];
              },
            DurableWorkerFailure | DurableBindingFailure
          > => {
            if (isCoordinatorHaltCause(cause)) {
              return Effect.failCause(Cause.map(cause, (halt) => halt.failure));
            }
            if (
              cause.reasons.some(
                (reason) => reason._tag === "Fail" && reason.error instanceof AgentInputDecodeError,
              )
            ) {
              // Accepted input is still owed. A current codec refusal is a Binding problem,
              // while coexisting defects/interruption remain in their original Cause channels.
              return Effect.failCause(
                Cause.map(cause, () =>
                  BindingUnavailable.make({
                    agentId: agent.definition.id,
                    message: "The current Binding cannot decode accepted input",
                  }),
                ),
              );
            }
            const approvalPending = agentApprovalPendingOption(cause);

            if (Option.isSome(approvalPending)) {
              // The canonical request preserves the pending control marker. A coexisting defect
              // or interruption must escape intact, without retention or a safe suspension.
              const residual = Cause.fromReasons<never>(
                cause.reasons.filter(
                  (reason): reason is Cause.Die | Cause.Interrupt => reason._tag !== "Fail",
                ),
              );

              if (residual.reasons.length > 0) return Effect.failCause(residual);

              // Durable approval suspension (plan §2.6): the approval hook already made the
              // request canonical; `runAttempt` owns the ledger transition. The engine decoded
              // the declared call id before raising the suspension, so a failure is a defect.
              return decodeToolCallIdUnknown(approvalPending.value.toolCallId).pipe(
                Effect.orDie,
                Effect.map((toolCallId) => ({ _tag: "suspendedRun" as const, toolCallId })),
              );
            }
            const childPending = agentChildPendingOption(error);

            if (Option.isSome(childPending)) {
              // The interpreter committed every closed sibling before reporting suspension.
              // The Attempt now owns only the ledger transition.
              return Effect.succeed({
                _tag: "suspendedChildRun" as const,
                children: childPending.value.children,
              });
            }

            return Effect.gen(function* () {
              // A recorded halt means a coordinator mutation failed inside a Tool handler
              // (the engine re-wraps step-hook and subagent-hook errors): abort the Attempt
              // with the original infrastructure failure instead of settling the Run failed.
              const halted = yield* Ref.get(haltRef);

              if (halted !== undefined) {
                return yield* halted;
              }
              // A semantic failure retains the validated response and its usage. Ownership loss
              // and coordinator faults escape above without retrying a failed canonical append.
              yield* flushDeferredResponse;

              // Host authorization can observe cancellation before the abort watcher ticks.
              // Only an authorization denial with this Submission's durable intent is an abort.
              if (error instanceof AgentToolAuthorizationDenied) {
                const intent = yield* ledger.readAbortIntent(
                  AbortIntentRequest.make({ submissionId }),
                );

                if (intent !== undefined) {
                  yield* appendAbortRecord(ctx, intent);

                  return { _tag: "aborted" as const };
                }
              }

              // Report the original live Cause here, before its private diagnostic projection,
              // after excluding suspensions, so hosts retain its cause and stack exactly
              // once per failed Run; reading or retrying its receipt never reports again.
              if (
                cause.reasons.some(
                  (reason) =>
                    reason._tag === "Die" ||
                    (reason._tag === "Fail" &&
                      !(reason.error instanceof AgentApprovalDenied) &&
                      !(reason.error instanceof AgentToolAuthorizationDenied)),
                )
              )
                yield* Effect.logError("Agent run failed", cause).pipe(
                  Effect.annotateLogs({
                    agentId: agent.definition.id,
                    runId,
                    threadId: ctx.threadId,
                    submissionId,
                    attemptId: lineage.attemptId,
                  }),
                );

              return {
                _tag: "failedRun" as const,
                outcome: yield* failureOutcome(error, cause, {
                  agentId: agent.definition.id,
                  runId,
                  threadId: ctx.threadId,
                  submissionId,
                  attemptId: lineage.attemptId,
                }),
              };
            });
          },
        ),
      );

      if (result._tag === "yielded") return result;
      if (result._tag === "suspendedRun") return approvalSuspension(result.toolCallId);
      if (result._tag === "suspendedChildRun") return childSuspension(result.children);
      if (result._tag === "aborted") {
        yield* flushDeferredResponse;

        return {
          ...abortedRunPhase(yield* currentUsageSummary()),
          uncommittedModelUsage: uncommittedModelUsage(),
        };
      }
      if (result._tag === "failedRun") {
        const failed: RunPhaseOutcome = {
          ...result.outcome,
          usageSummary: yield* currentUsageSummary(),
          uncommittedModelUsage: uncommittedModelUsage(),
        };

        return failed;
      }
      const committedCompletion = completionState.committed;

      if (committedCompletion === undefined) {
        return yield* LedgerError.make({
          operation: "runModel",
          message: "Agent Run ended without a committed completion",
        });
      }

      const completed: RunPhaseOutcome = {
        _tag: "completed",
        result: committedCompletion.output,
        ...(committedCompletion.runDisposition === undefined
          ? {}
          : { runDisposition: committedCompletion.runDisposition }),
        ...(committedCompletion.finishReason === undefined
          ? {}
          : { finishReason: committedCompletion.finishReason }),
        ...(committedCompletion.exhausted === undefined
          ? {}
          : { exhausted: committedCompletion.exhausted }),
        usageSummary: yield* currentUsageSummary(),
        uncommittedModelUsage: uncommittedModelUsage(),
      };

      return completed;
    });

  /**
   * One ownership period over the claimed lane head. The step order mirrors the pure recovery
   * classifier's decision table, so a fresh Attempt and a recovering Attempt take the same path
   * through the same idempotent steps. Returns `Option.none` when the lane is durably blocked
   * (Unknown Outcomes, or a durable approval suspension) — no settlement occurs and the
   * obligation stays owed.
   */
  const runAttempt = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    RunDispositionValue extends
      | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
      | undefined = undefined,
    InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
      undefined,
    UpdatesSchema extends Schema.Top | undefined = undefined,
  >(
    registeredAgent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue,
      InputPromptValue,
      UpdatesSchema
    >,
    session: RunStorageSession,
    resumeAfterRetention: () => void,
    onHandoff: (nextSubmissionId: SubmissionId) => void,
    yieldAfter?: DateTime.Utc,
  ) =>
    Effect.gen(function* () {
      session = yield* withProgressSession(session);
      const { claim, threadId } = session;
      const submissionId = claim.submissionId;

      // Claim atomically advances the lane fence. SQL/DO share that fence with ThreadStore,
      // so reading the tail here already observes this claim, not its predecessor. Use the
      // granted generation; canonical input and response evidence below decide whether a
      // conservative interruption audit is needed before inference can repeat.
      const supersededEpoch = Schema.decodeSync(ProducerEpoch)(
        Math.max(0, claim.producerEpoch - 1),
      );

      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId }),
      );

      const submission = snapshot.submission;

      yield* ensureThreadCreated(threadId, submission.agentId, submission.agentDigests);
      if (submission.workerAdmission?.origin.reporting?.mode === "standard")
        yield* updateRuntime.repairRun(threadId, submissionId);
      if (submission.workerAdmission !== undefined) {
        yield* workerRuntime
          .ensureOrigin(submission.workerAdmission.origin)
          .pipe(
            Effect.mapError((cause) =>
              LedgerError.make({ operation: "worker-origin", message: cause.reason, cause }),
            ),
          );
      }
      if (submission.state === "admitted") {
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId }));
      }
      yield* session.refresh;
      const ctx: AttemptAppendContext = session;
      let controlThrough = (yield* session.tail).sequence;

      const initialThrough = controlThrough;

      const initialView = yield* recoveryView(
        threadId,
        initialThrough,
        [submissionId],
        runIdForSubmission(submissionId),
      );

      let journalMetadata: ReturnType<typeof makeJournalMetadata> | undefined;

      const retainControl = controlRecords([submissionId]);

      const collectControl = (record: CanonicalRecordEnvelope): boolean => {
        journalMetadata?.add(record);

        return retainControl(record);
      };

      const takeJournalMetadata = (): JournalMetadata | undefined => {
        const metadata = journalMetadata?.snapshot();

        // Release admission metadata before model or Tool waits; it is not a warm context cache.
        journalMetadata = undefined;

        return metadata;
      };

      let records: ReadonlyArray<CanonicalRecordEnvelope> = yield* Stream.runCollect(
        initialView.canonical.pipe(Stream.filter(collectControl)),
      );

      // The append-only prefix remains valid for this Attempt. Retain only this Run's control
      // evidence and validate each newly visible suffix against its own captured tail.
      const refreshControl = Effect.fnUntraced(function* (throughSequence?: CanonicalSequence) {
        const through =
          throughSequence ??
          (yield* store.inspectTail(ThreadTailRequest.make({ threadId }))).tailSequence;

        if (through > controlThrough) {
          const suffix = yield* selectedRange(
            threadId,
            [submissionId],
            through,
            controlThrough,
          ).pipe(Effect.map((entries) => entries.filter(collectControl)));

          records = [...records, ...suffix];
          controlThrough = through;
        }

        return records;
      });

      const workerOrigin = records
        .map((envelope) => envelope.record.payload)
        .find((payload) => payload._tag === "WorkerOriginRecorded")?.origin;

      if (
        workerOrigin !== undefined &&
        (submission.workerAdmission === undefined ||
          !Schema.toEquivalence(WorkerAdmission.fields.origin)(
            workerOrigin,
            submission.workerAdmission.origin,
          ))
      ) {
        return yield* RunJournalError.make({
          message: "Worker input does not match its immutable Thread origin",
        });
      }

      const childPolicy =
        workerOrigin?.policy ??
        records
          .map((envelope) => envelope.record.payload)
          .find((payload) => payload._tag === "SubagentLineageRecorded")?.policy;

      const agent =
        childPolicy === undefined
          ? registeredAgent
          : {
              ...registeredAgent,
              definition: { ...registeredAgent.definition, policy: childPolicy },
            };

      // Restoring the accepted child policy changes no Tool declarations. Retain the
      // selected registration's contracts instead of resolving the copied Definition.
      const currentContracts = yield* contractsFor(registeredAgent.definition);

      const evidence = yield* evidenceFor(records, submissionId, true, snapshot.hostSubmissionId);
      const knownIds = knownRecordIdsOf(records);

      if (evidence.recordedSettlementOutcome !== undefined) {
        const record = yield* canonicalSettlementRecord(records, submissionId);
        const settlement = yield* finalizeFromHistory(submission, record);
        const canonicalSettlement = yield* settlementPayloadFromRecord(record, submissionId);

        yield* settleJoinedSubmissions(ctx, canonicalSettlement);

        return Option.some(settlement);
      }
      if (snapshot.abortIntent !== undefined) {
        // request-abort-and-join (spec §13.1, SUB-022): propagate the durable abort to every
        // nonterminal attached child, join every settled child coordinator-side, and settle
        // aborted ONLY once no child obligation stays open. A waiting/blocked disposition ends
        // the ownership period without settling — the obligation stays owed.
        const disposition = yield* abortAttachedChildren(ctx, submission, session, knownIds);

        if (disposition === "waiting") {
          return Option.none<Settlement>();
        }
        if (disposition === "blocked") {
          yield* session.release.pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

          return Option.none<Settlement>();
        }

        return Option.some(
          yield* settleAborted(
            ctx,
            submission,
            session.publishSettlement,
            snapshot.abortIntent,
            evidence,
            knownIds,
          ),
        );
      }
      yield* applyCanonicalInput(ctx, submission, session, records, snapshot.inputApplied);

      const start = yield* makeRunStart(
        submission,
        Duration.toMillis(agent.definition.policy.maxDuration),
        yield* refreshControl(),
      ).pipe(Effect.provideService(CurrentRunWriter, ctx));

      const savedRunTiming = start.timing;

      const commitStart = Effect.flatMap(CurrentRunStart, (publication) =>
        publication.commit(),
      ).pipe(Effect.provideService(CurrentRunStart, start.publication));

      const runTiming =
        workerOrigin === undefined
          ? savedRunTiming
          : {
              ...savedRunTiming,
              deadline: DateTime.makeUnsafe(
                Math.min(
                  DateTime.toEpochMillis(savedRunTiming.deadline),
                  workerOrigin.expiresAtMillis,
                ),
              ),
            };

      let expiredChildObligation = false;

      if ((yield* Clock.currentTimeMillis) >= DateTime.toEpochMillis(runTiming.deadline)) {
        yield* commitStart;
        yield* reconcileRetainedChildren(ctx, submission, session);
        expiredChildObligation = yield* completeJoinedReleases(submission);
      }

      // Recheck after duration interruption too: that Attempt may have declared new ordinary calls.
      const ordinaryCallsResolved = Effect.gen(function* () {
        const reconciliationRecords = yield* refreshControl();

        const reconciliationEvidence = yield* evidenceFor(
          reconciliationRecords,
          submissionId,
          true,
          snapshot.hostSubmissionId,
        );

        if (reconciliationEvidence.openToolCalls.length === 0) return true;

        const reconciliationSnapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId }),
        );

        for (const envelope of reconciliationRecords) knownIds.add(envelope.record.recordId);

        const review = yield* reconcileOpenCalls(
          ctx,
          submission,
          reconciliationSnapshot,
          reconciliationRecords,
          reconciliationEvidence.openToolCalls,
          knownIds,
          { definition: agent.definition, contracts: currentContracts },
        );

        if (review.uncertain.length > 0 || review.unproven.length > 0) {
          if (review.uncertain.length > 0) {
            yield* markCallsUnknown(
              ctx,
              submissionId,
              knownIds,
              review.uncertain,
              "An ordinary Tool call may have executed without a canonical outcome",
            );
          }
          // This Submission is parked (marked Unknown, or reconciliation itself failed):
          // release the claim without settling — the accepted-work obligation stays owed.
          yield* session.release.pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

          return false;
        }

        return true;
      });

      if (!expiredChildObligation && !(yield* ordinaryCallsResolved))
        return Option.none<Settlement>();

      const continuationRecords = yield* refreshControl();

      const continuation = yield* pendingToolBatchFor(
        continuationRecords,
        runIdForSubmission(submissionId),
      );

      if (continuation !== undefined) {
        const current = {
          definition: agent.definition,
          contracts: currentContracts,
        };

        const operations = operationsFor(continuationRecords, runIdForSubmission(submissionId));
        const settled = new Set(continuation.settled.map((call) => call.id));

        const rejected = new Set(
          continuation.toolParameterRejections?.map((call) => call.toolCallId),
        );

        const unsupported: Array<OpenToolCallEvidence> = [];
        const unavailableDelegations = new Set<ToolCallId>();

        const operationStates = toolOperationStates(
          continuationRecords,
          runIdForSubmission(submissionId),
        );

        for (const call of continuation.calls) {
          const operation = operations.get(call.id);

          if (
            operation?.executionKind === "delegation" &&
            !settled.has(call.id) &&
            !supportsOperation(
              OpenToolCallEvidence.make({
                toolCallId: operation.toolCallId,
                toolName: operation.toolName,
                turn: continuation.turn,
              }),
              operation,
              current,
            )
          )
            unavailableDelegations.add(operation.toolCallId);
        }

        const children =
          unavailableDelegations.size === 0
            ? undefined
            : yield* reconcileRetainedChildren(ctx, submission, session, unavailableDelegations);

        for (const envelope of yield* refreshControl()) knownIds.add(envelope.record.recordId);

        for (const call of continuation.calls) {
          if (
            call.providerExecuted ||
            settled.has(call.id) ||
            rejected.has(Schema.decodeSync(ToolCallId)(call.id))
          )
            continue;

          const evidence = OpenToolCallEvidence.make({
            toolCallId: Schema.decodeSync(ToolCallId)(call.id),
            toolName: call.name,
            turn: continuation.turn,
          });

          const operation = operations.get(call.id);

          if (supportsOperation(evidence, operation, current)) continue;

          const blocked =
            operationStates.find(
              (state) =>
                state.turn === continuation.turn &&
                state.operation.toolCallId === evidence.toolCallId,
            )?.dispatchBlocked === true;

          const notExecuted = blocked || children?.notExecuted.has(evidence.toolCallId) === true;

          const readonly =
            operation?.executionClass === "readonly" && operation.executionKind === "ordinary";

          if (!notExecuted && !readonly) {
            if (!unavailableDelegations.has(evidence.toolCallId)) unsupported.push(evidence);
            continue;
          }
          const execution = notExecuted ? "not-executed" : "unavailable";

          const result = yield* Schema.encodeEffect(ToolUnavailable)(
            ToolUnavailable.make({
              toolName: call.name,
              execution,
              message:
                execution === "not-executed"
                  ? "This operation was not executed and its original implementation is unavailable. Continue with the current tools."
                  : "The original readonly operation is unavailable and may have run without a recorded result.",
            }),
          ).pipe(Effect.flatMap(decodePersisted), Effect.orDie);

          yield* hit("tools:before-unavailable-append");
          yield* appendClosedCall(ctx, submissionId, knownIds, evidence, {
            result: { value: result, isFailure: true },
            resolution: execution === "not-executed" ? "never-started" : "failed-with-error",
            author: "runtime",
            reason: "The declared operation cannot execute under its original contract",
          });
          yield* hit("tools:after-unavailable-append");
        }
        const firstWaiting = children?.waiting[0];

        if (children !== undefined && firstWaiting !== undefined) {
          const disposition = yield* session.suspend(
            WaitingForChildSuspension.make({
              children: [firstWaiting, ...children.waiting.slice(1)],
            }),
          );

          yield* hit("subagent:after-suspend");
          if (disposition === "resume-immediately") yield* wake.notify(threadId);

          return Option.none<Settlement>();
        }
        if ((children?.indeterminate.size ?? 0) > 0) return Option.none<Settlement>();
        if (unsupported.length > 0) {
          yield* markCallsUnknown(
            ctx,
            submissionId,
            knownIds,
            unsupported,
            "The unfinished operation has no supported recovery implementation; its original identity and evidence remain unresolved",
          );

          return Option.none<Settlement>();
        }
      }

      const lineage: AttemptLineage = {
        attemptId: claim.attemptId,
        supersededEpoch,
        inputWasRecorded: evidence.inputRecorded,
      };

      let approvalDecisionIntents = snapshot.approvalDecisions;

      while (true) {
        yield* session.refresh;
        const tail = yield* session.tail;

        const currentRecords = yield* refreshControl(tail.sequence);

        const view = yield* recoveryView(
          threadId,
          tail.sequence,
          [submissionId],
          runIdForSubmission(submissionId),
          currentRecords,
        );

        let canonical: Stream.Stream<
          JournalRecordEnvelope,
          ThreadStoreError | ThreadNotMaterialized
        > = view.canonical;

        if (view.context === undefined) {
          const original = currentRecords.find(
            ({ record }) =>
              record.payload._tag === "UserInputRecorded" &&
              record.payload.kind === "user" &&
              record.payload.submissionId === submissionId,
          );

          if (original === undefined)
            return yield* RunJournalError.make({
              message: "Run has no exact original input context boundary",
            });

          // Context assembly is paid once, bounded at the ORIGINAL admission, never the later
          // Thread tail. Subsequent recovery uses the immutable saved context and selected Run.
          const history = yield* initialContext(original).pipe(
            Effect.provideService(ThreadReader, reader),
            Effect.provideService(Crypto.Crypto, crypto),
          );

          const suffix = currentRecords.filter((entry) => entry.sequence > original.sequence);

          journalMetadata = makeJournalMetadata(runIdForSubmission(submissionId));
          for (const entry of history) journalMetadata.add(entry);
          for (const entry of suffix) journalMetadata.add(entry);
          canonical = Stream.fromIterable<JournalRecordEnvelope>([...history, ...suffix]);
        }

        const outcome = yield* runModel(
          agent,
          ctx,
          submission,
          session,
          currentRecords,
          canonical,
          tail.sequence,
          view.progressThrough,
          view.progress,
          view.context,
          takeJournalMetadata(),
          lineage,
          approvalDecisionIntents,
          currentContracts,
          runTiming,
          yieldAfter,
        ).pipe(Effect.provideService(CurrentRunStart, start.publication));

        if (outcome._tag === "yielded") {
          if (outcome.nextSubmissionId !== undefined) onHandoff(outcome.nextSubmissionId);

          return Option.none();
        }
        if (outcome._tag === "suspendedChild") {
          // Durable waitingForChild suspension (spec §12 step 10, SUB-030): the sibling
          // late-settles are already canonical; the ledger transition ends the ownership
          // period WITHOUT settling and the lane consumes no worker permit while each listed
          // child runs on its own Thread lane. A child settlement racing ahead of the
          // suspend transaction returns `resume-immediately`: the declared batch replays under
          // this same claim and the handler joins the settled child.
          const [firstChild, ...restChildren] = outcome.children;

          const waitingChildren: readonly [WaitingChild, ...Array<WaitingChild>] = [
            WaitingChild.make({
              toolCallId: firstChild.toolCallId,
              childSubmissionId: firstChild.childSubmissionId,
            }),
            ...restChildren.map((child) =>
              WaitingChild.make({
                toolCallId: child.toolCallId,
                childSubmissionId: child.childSubmissionId,
              }),
            ),
          ];

          const suspension = yield* session.suspend(
            WaitingForChildSuspension.make({ children: waitingChildren }),
          );

          yield* hit("subagent:after-suspend");
          for (const child of outcome.children) {
            yield* wake.notify(child.childThreadId);
          }
          if (suspension === "suspended") {
            return Option.none<Settlement>();
          }
          continue;
        }
        if (outcome._tag === "suspended") {
          // Durable approval suspension (plan §2.6): the ledger transition ends the ownership
          // period WITHOUT settling — the accepted-work obligation stays owed while the lane
          // consumes no worker permit. A decision that raced ahead of the suspend transaction
          // returns `resume-immediately`. Retained resources require a fresh Attempt; otherwise
          // the declared batch replays under this claim with the fresh decision intents.
          const suspension = yield* session.suspend(
            ApprovalPendingSuspension.make({ toolCallIds: [outcome.toolCallId] }),
          );

          yield* hit("approval:after-suspend");
          if (suspension === "suspended") {
            return Option.none<Settlement>();
          }
          if ((yield* DurableApprovalSuspension) !== undefined) {
            // Retention completed inside runModel. Leave both the service and claim scopes
            // before reacquiring: a transferred attachment cannot execute the approved batch.
            resumeAfterRetention();

            return Option.none<Settlement>();
          }
          approvalDecisionIntents = (yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId }),
          )).approvalDecisions;
          continue;
        }
        yield* commitStart;
        if (outcome._tag === "aborted") {
          // Durable abort ended the Run while attached children may still be open:
          // request-abort-and-join before the aborted settlement (spec §13.1).
          const disposition = yield* abortAttachedChildren(ctx, submission, session, knownIds);

          if (disposition === "waiting") {
            return Option.none<Settlement>();
          }
          if (disposition === "blocked") {
            yield* session.release.pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

            return Option.none<Settlement>();
          }

          return Option.some(
            yield* terminalize(ctx, submission, session.publishSettlement, outcome, true),
          );
        }
        // Canonical joins drive their reservation release BEFORE the parent settles (a settled
        // lane would strand the repair); a reserved row WITHOUT a canonical join is an open
        // attached-child obligation and the parent never settles across it (spec §13).
        if (outcome._tag === "failed" && outcome.policyLimit === "duration") {
          const exhaustedId = runDurationRecordId(runIdForSubmission(submissionId));

          if (!(yield* refreshControl()).some(({ record }) => record.recordId === exhaustedId)) {
            yield* hit("run:before-duration-append");
            yield* appendBatch(
              ctx,
              CanonicalBatch.make({
                batchId: runDurationBatchId(runIdForSubmission(submissionId)),
                producerId: config.producerId,
                records: [
                  yield* makeEnvelope(
                    exhaustedId,
                    RunDurationExhausted.make({
                      runId: runIdForSubmission(submissionId),
                    }),
                  ),
                ],
              }),
            );
            yield* hit("run:after-duration-append");
          }
          yield* reconcileRetainedChildren(ctx, submission, session);
        }
        const openObligation = yield* completeJoinedReleases(submission);

        if (openObligation) {
          if (outcome._tag === "failed") {
            // Release the claim and leave the lane to recovery classification.
            yield* session.release.pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

            return Option.none<Settlement>();
          }

          // A completed Run with an unjoined reservation is structurally unreachable (the
          // delegation Tool Call settles only through the atomic join batch): fail closed
          // with the obligation visibly owed instead of settling across it.
          return yield* LedgerError.make({
            operation: "runAttempt",
            message: `Submission ${submissionId} completed with an unjoined child budget reservation; the settlement is withheld fail-closed (spec 13)`,
          });
        }
        if (
          outcome._tag === "failed" &&
          outcome.policyLimit === "duration" &&
          !(yield* ordinaryCallsResolved)
        ) {
          return Option.none<Settlement>();
        }

        return Option.some(
          yield* terminalize(ctx, submission, session.publishSettlement, outcome, true),
        );
      }
    }).pipe(Effect.scoped);

  /** A missing current binding leaves both roots and children owed, with their claims released. */
  type CapturedWorkerBinding = Effect.Success<ReturnType<typeof makeLegacyWorkerBinding>>;

  // Administrative repairs acquire separately from active Run sessions and never renew.
  // Register cleanup before any interruptible work can observe the granted claim.
  const acquireAdministrativeClaim = Effect.fnUntraced(function* (
    threadId: ThreadId,
    handoff?: ClaimHandoff,
  ) {
    const claimed = yield* ledger.claim(
      ClaimRequest.make({
        threadId,
        producerId: config.producerId,
        ...(handoff === undefined ? {} : { handoff }),
      }),
    );

    if (Option.isNone(claimed)) return Option.none();
    const claim = claimed.value;

    yield* Effect.addFinalizer(() =>
      ledger
        .releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: claim.submissionId,
            ownershipToken: claim.ownershipToken,
          }),
        )
        .pipe(
          Effect.catchTag("OwnershipLost", () => Effect.void),
          Effect.catchTag("LedgerError", () =>
            Effect.logWarning(
              "Attempt ownership release failed; lease recovery remains required",
            ).pipe(Effect.annotateLogs({ submissionId: claim.submissionId })),
          ),
        ),
    );

    if (handoff !== undefined && claim.submissionId !== handoff.submissionId)
      return yield* LedgerError.make({
        operation: "claim handoff",
        message: "The submission adapter did not honor the requested handoff",
      });

    return Option.some({ claim });
  }, Effect.uninterruptible);

  const processThreadHead = (
    resolve: (
      submission: SubmissionSnapshot,
    ) => Effect.Effect<CapturedWorkerBinding, DurableBindingFailure | DurableWorkerFailure>,
    threadId: ThreadId,
    options?: { readonly yieldAfter?: DateTime.Utc },
  ): Effect.Effect<
    {
      readonly settlement: Option.Option<Settlement>;
      readonly firstClaimedSubmissionId: SubmissionId | undefined;
    },
    DurableWorkerFailure | DurableBindingFailure
  > =>
    Effect.gen(function* () {
      let resumeAfterRetention = false;
      let handoff: ClaimHandoff | undefined;
      let nextHandoff: ClaimHandoff | undefined;
      let firstClaimedSubmissionId: SubmissionId | undefined;

      const attempt = Effect.scoped(
        Effect.gen(function* () {
          const claimed = yield* runStorage.claim(
            ClaimRequest.make({
              threadId,
              producerId: config.producerId,
              ...(handoff === undefined ? {} : { handoff }),
            }),
          );

          if (Option.isNone(claimed)) return Option.none();
          const session = claimed.value;
          const { claim } = session;

          firstClaimedSubmissionId ??= claim.submissionId;

          const attributes = {
            threadId,
            submissionId: claim.submissionId,
            attemptId: claim.attemptId,
          };

          yield* Effect.annotateCurrentSpan(attributes);
          yield* Effect.annotateLogsScoped(attributes);
          yield* hit("claim:after-claim");

          const found = yield* ledger.lookup(
            SubmissionLookupById.make({ submissionId: claim.submissionId }),
          );

          if (Option.isNone(found)) {
            return yield* LedgerError.make({
              operation: "processThreadHead",
              message: `Claimed unknown Submission ${claim.submissionId}`,
            });
          }
          const submission = found.value;

          // The claim head rule legally grants an `admitted` head, so the worker path
          // enforces the same AwaitParentEstablishment discipline as the recovery classifier —
          // a parent-linked child whose Thread lacks its canonical lineage record is not
          // runnable yet (the parent's idempotent establishment appends lineage BEFORE
          // readiness, SUB-016).
          // Release the claim, nudge the parent lane, and leave the child to establishment.
          if (submission.parentLinkage !== undefined && submission.state === "admitted") {
            const read = yield* readAllTolerant(threadId, []);

            const lineageRecorded = read.records.some(
              (envelope) => envelope.record.payload._tag === "SubagentLineageRecorded",
            );

            if (!lineageRecorded) {
              yield* session.release.pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

              const parent = yield* ledger.lookup(
                SubmissionLookupById.make({
                  submissionId: submission.parentLinkage.parentSubmissionId,
                }),
              );

              if (Option.isSome(parent)) {
                yield* wake.notify(parent.value.threadId);
              }

              return Option.none();
            }
          }

          const resolution = yield* resolve(submission).pipe(
            Effect.map((binding) => ({ _tag: "resolved" as const, binding })),
            Effect.catchTags({
              BindingUnavailable: (failure) =>
                Effect.succeed({ _tag: "refused" as const, failure }),
            }),
          );

          if (resolution._tag === "refused") {
            return yield* resolution.failure;
          }

          return yield* resolution.binding.attempt(
            (agent) =>
              runAttempt(
                agent,
                session,
                () => {
                  resumeAfterRetention = true;
                },
                (nextSubmissionId) => {
                  nextHandoff = ClaimHandoff.make({
                    producerEpoch: claim.producerEpoch,
                    deferredSubmissionIds: [
                      ...(handoff?.deferredSubmissionIds ?? []),
                      claim.submissionId,
                    ],
                    submissionId: nextSubmissionId,
                  });
                },
                options?.yieldAfter,
              ),
            threadId,
            claim,
          );
        }),
      );

      while (true) {
        resumeAfterRetention = false;
        nextHandoff = undefined;
        const settlement = yield* attempt;

        // The previous Attempt's entire Scope has closed before the next authority is built.
        if (nextHandoff !== undefined) {
          handoff = nextHandoff;
          continue;
        }
        if (!resumeAfterRetention) return { settlement, firstClaimedSubmissionId };
      }
    }).pipe(withThreadHeadSpan);

  const eligibleThreadHead = (threadId: ThreadId, afterQueueSequence?: number) =>
    Stream.runHead(
      ledger.scanNonterminal.pipe(
        Stream.filter(
          (entry) =>
            entry.threadId === threadId &&
            (afterQueueSequence === undefined || entry.queueSequence > afterQueueSequence),
        ),
        Stream.filterEffect((entry) =>
          entry.state !== "unknown"
            ? Effect.succeed(true)
            : ledger
                .loadRecoverySnapshot(
                  RecoverySnapshotRequest.make({ submissionId: entry.submissionId }),
                )
                .pipe(Effect.map((snapshot) => snapshot.abortIntent !== undefined)),
        ),
      ),
    );

  const drainThread = (
    resolve: (
      submission: SubmissionSnapshot,
    ) => Effect.Effect<CapturedWorkerBinding, DurableBindingFailure | DurableWorkerFailure>,
    threadId: ThreadId,
  ): Effect.Effect<ReadonlyArray<Settlement>, DurableWorkerFailure | DurableBindingFailure> =>
    Effect.gen(function* () {
      const settlements: Array<Settlement> = [];

      while (true) {
        const { settlement, firstClaimedSubmissionId } = yield* processThreadHead(
          resolve,
          threadId,
        );

        if (Option.isNone(settlement)) {
          if (firstClaimedSubmissionId === undefined) return settlements;
          const after = yield* eligibleThreadHead(threadId);

          // Parking an unknown operation exposes later runnable work. Other suspensions,
          // live ownership and a transiently unproven operation retain the existing barrier.
          // Keep the first claim across handoffs: its unfinished lane may still be the head.
          if (Option.isSome(after) && firstClaimedSubmissionId !== after.value.submissionId)
            continue;

          return settlements;
        }
        settlements.push(settlement.value);
      }
    }).pipe((body) => withProcessing(threadId, body));

  const processThreadImpl = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    RunDispositionValue extends
      | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
      | undefined = undefined,
    InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
      undefined,
    UpdatesSchema extends Schema.Top | undefined = undefined,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue,
      InputPromptValue,
      UpdatesSchema
    >,
    threadId: ThreadId,
  ) =>
    Effect.gen(function* () {
      // The legacy single-binding worker is a singleton resolver (plan §1.7): identity-exact —
      // a claimed head with a different Agent never runs against this binding (the latent P4
      // gap) — and digest-transparent, because this call site registers no digest authority.
      const binding = yield* makeLegacyWorkerBinding(agent);

      return yield* drainThread(
        (submission) =>
          submission.agentId === binding.agentId
            ? Effect.succeed(binding)
            : Effect.fail(
                BindingUnavailable.make({
                  agentId: submission.agentId,
                  message: `This worker is bound to Agent ${binding.agentId}; the claimed head belongs to Agent ${submission.agentId} (SUB-023)`,
                }),
              ),
        threadId,
      );
    });

  const processThreadResolvedImpl = (
    threadId: ThreadId,
  ): Effect.Effect<ReadonlyArray<Settlement>, DurableWorkerFailure | DurableBindingFailure> =>
    drainThread(resolveCurrentBinding, threadId);

  const processThreadHeadImpl = (
    threadId: ThreadId,
    options?: { readonly yieldAfter?: DateTime.Utc },
  ): Effect.Effect<Option.Option<Settlement>, DurableWorkerFailure | DurableBindingFailure> =>
    processThreadHead(resolveCurrentBinding, threadId, options).pipe(
      Effect.map(({ settlement }) => settlement),
      (body) => withProcessing(threadId, body),
    );

  const claimFor = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
    decision: RecoveryDecision,
  ): Effect.fn.Return<Option.Option<Claim>, LedgerError | OwnershipLost, Scope.Scope> {
    const head = yield* eligibleThreadHead(submission.threadId);

    if (Option.isNone(head) || head.value.submissionId !== submission.submissionId) {
      return Option.none();
    }
    const claimed = yield* acquireAdministrativeClaim(submission.threadId);

    if (Option.isNone(claimed)) return Option.none();
    if (claimed.value.claim.submissionId !== submission.submissionId) {
      // Only the first eligible Submission is claimable; selection may have changed since the read.
      yield* ledger
        .releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: claimed.value.claim.submissionId,
            ownershipToken: claimed.value.claim.ownershipToken,
          }),
        )
        .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

      return Option.none();
    }

    yield* annotateRepair(submission.threadId, decision, claimed.value.claim.producerEpoch);

    return Option.some(claimed.value.claim);
  });

  /**
   * Best-effort DUR-013 audit of an owned repair attempt, before it can release its claim.
   * The fixed granted epoch never borrows a later writer's authority. State-only repairs
   * omit canonical annotations; their decision and operator author/reason remain in telemetry.
   */
  const annotateRepair = (
    threadId: ThreadId,
    decision: RecoveryDecision,
    producerEpoch: ProducerEpoch,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch }));
      const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

      // ThreadCreated must remain the first canonical record of a new Thread.
      if (tail.tailSequence === ZERO_SEQUENCE) return;

      const encodedDecision = yield* Schema.encodeEffect(RecoveryDecision)(decision).pipe(
        Effect.orDie,
      );

      const details = yield* Schema.decodeEffect(PersistedJson)(encodedDecision).pipe(Effect.orDie);

      const envelope = yield* makeEnvelope(
        recoveryRepairRecordId(decision.submissionId, decision._tag),
        RepairAnnotated.make({ reason: `recovery:${decision._tag}`, details }),
      );

      yield* store.append(
        FencedAppendRequest.make({
          threadId,
          batch: CanonicalBatch.make({
            batchId: recoveryRepairBatchId(decision.submissionId, decision._tag),
            producerId: config.producerId,
            records: [envelope],
          }),
          expectedTailSequence: tail.tailSequence,
          expectedTailDigest: tail.tailDigest,
          producerEpoch,
        }),
      );
    }).pipe(Effect.ignore);

  const settleAbortedForRecovery = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
    evidence: RecoveryEvidence,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    decision: SettleAbortedDecision,
  ): Effect.fn.Return<"repaired" | "deferred", DurableWorkerFailure, Scope.Scope> {
    const intent = snapshot.abortIntent;

    if (intent === undefined) return "deferred";
    const submission = snapshot.submission;

    if (submission.state === "suspended" && snapshot.suspension !== undefined) {
      if (snapshot.suspension.reason._tag === "WaitingForChild") {
        // The classifier reaches SettleAborted only when every attached-child obligation is
        // closed (open ones route to PropagateChildAbort/ResumeWaitingParent, spec §13.1), so
        // every listed child is provably settled: replay the idempotent wake to make the
        // suspended lane claimable, then settle aborted below. A wake the adapter cannot yet
        // verify defers honestly instead of guessing.
        const woken = yield* Effect.gen(function* () {
          for (const child of snapshot.suspension?.reason._tag === "WaitingForChild"
            ? snapshot.suspension.reason.children
            : []) {
            yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: submission.submissionId,
                childSubmissionId: child.childSubmissionId,
              }),
            );
          }

          return true;
        }).pipe(Effect.catchTag("LedgerError", () => Effect.succeed(false)));

        if (!woken) return "deferred";
      } else {
        // A suspended head is never worker-claimable (WP2 claim rule), so the aborted
        // settlement first closes the suspension: every undecided call of the stored reason
        // gets a durable DENIED decision, which wakes the lane (`suspended → input-applied`)
        // without ever resuming the batch — the abort intent settles the Submission before
        // any Run resumes. A raced real decision also covers the reason, so its conflict is
        // absorbed.
        const decided = new Set(snapshot.approvalDecisions.map((decision) => decision.toolCallId));

        for (const toolCallId of snapshot.suspension.reason.toolCallIds) {
          if (decided.has(toolCallId)) continue;
          yield* ledger
            .recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: submission.submissionId,
                toolCallId,
                decision: "denied",
                resolver: RECOVERY_RESOLVER,
                reason:
                  "The Submission was aborted while durably suspended; the pending approval closes denied so the aborted settlement can commit",
              }),
            )
            .pipe(
              Effect.catchTag("ApprovalConflict", () => Effect.void),
              Effect.asVoid,
            );
        }
      }
    }
    const claimed = yield* claimFor(submission, decision);

    if (Option.isNone(claimed)) {
      // P7 §7(c): an aborted, never-claimed, still-queued `ready` Submission settles NOW
      // instead of waiting to head the lane — settlement order of never-run work is not
      // execution order (DUR-004 bounds execution; DUR-012 allows settling inactive
      // accepted work without an Attempt). The appends run at the current tail with the
      // durable abort intent as publication authority; any racing owner's fence
      // advance (or a concurrent joining claim) defers honestly to the next pass.
      if (submission.state === "ready" && snapshot.ownership === undefined) {
        return yield* Effect.gen(function* () {
          yield* materializeAtLeast(submission.threadId, ZERO_EPOCH);
          yield* ensureThreadCreated(
            submission.threadId,
            submission.agentId,
            submission.agentDigests,
          );
          const ctx = yield* attemptContextAtTail(submission.threadId);

          yield* settleAborted(
            ctx,
            submission,
            publicationFor(ctx, submission.submissionId, { _tag: "QueuedAbort" }),
            intent,
            evidence,
            knownRecordIdsOf(records),
          );

          return "repaired" as const;
        }).pipe(
          Effect.catchTags({
            FenceRejected: () => Effect.succeed("deferred" as const),
            AppendConflict: () => Effect.succeed("deferred" as const),
            OwnershipLost: () => Effect.succeed("deferred" as const),
          }),
        );
      }

      return "deferred";
    }
    const claim = claimed.value;

    yield* store.materialize(
      ThreadMaterialization.make({
        threadId: submission.threadId,
        producerEpoch: claim.producerEpoch,
      }),
    );
    yield* ensureThreadCreated(submission.threadId, submission.agentId, submission.agentDigests);
    const ctx = yield* attemptContextFor(submission.threadId, claim.producerEpoch);

    yield* settleAborted(
      ctx,
      submission,
      publicationFor(ctx, submission.submissionId, {
        _tag: "Owned",
        ownershipToken: claim.ownershipToken,
      }),
      intent,
      evidence,
      knownRecordIdsOf(records),
    );

    return "repaired";
  });

  /**
   * Execute the reconcile-then-mark flow for a `MarkUnknown` decision (plan §2.2). Current
   * registration metadata checks retry support when available. Low-level drivers supply their
   * Agent at claim time, so retry proofs defer that check without permitting execution. Calls
   * without a durable intent, original idempotent declaration, or reconciler proof stay uncertain.
   * Closed calls report `repaired`; deferred checks report `deferred`; uncertain calls become Unknown.
   */
  const markUnknownForRecovery = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
    evidence: RecoveryEvidence,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    decision: MarkUnknownDecision,
  ): Effect.fn.Return<"repaired" | "deferred" | "unknown", DurableWorkerFailure, Scope.Scope> {
    const submission = snapshot.submission;
    const claimed = yield* claimFor(submission, decision);

    if (Option.isNone(claimed)) return "deferred";
    const claim = claimed.value;

    yield* store.materialize(
      ThreadMaterialization.make({
        threadId: submission.threadId,
        producerEpoch: claim.producerEpoch,
      }),
    );
    const ctx = yield* attemptContextFor(submission.threadId, claim.producerEpoch);
    const knownIds = knownRecordIdsOf(records);
    // The classifier's decision lists ONLY ordinary open calls (S2: an open delegation call
    // never marks Unknown — its establishment is idempotent and routes through the Subagent
    // rows), so reconciliation is scoped to exactly those ids.
    const markableIds = new Set<string>(decision.openToolCallIds);

    const review = yield* reconcileOpenCalls(
      ctx,
      submission,
      snapshot,
      records,
      evidence.openToolCalls.filter((call) => markableIds.has(call.toolCallId)),
      knownIds,
      yield* currentOperationsFor(submission),
    );

    let disposition: "repaired" | "deferred" | "unknown";

    if (review.uncertain.length > 0) {
      yield* markCallsUnknown(
        ctx,
        submission.submissionId,
        knownIds,
        review.uncertain,
        decision.reason,
      );
      disposition = "unknown";
    } else if (review.unproven.length > 0 || review.retryable.length > 0) {
      disposition = "deferred";
    } else {
      disposition = "repaired";
    }
    yield* ledger
      .releaseOwnership(
        ReleaseOwnershipRequest.make({
          submissionId: submission.submissionId,
          ownershipToken: claim.ownershipToken,
        }),
      )
      .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

    return disposition;
  });

  /** A retry intent permits only the saved operation under its supported current contract. */
  const hasUnsupportedRetry = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
  ) {
    const intents = [...resolutionIntentsFor(snapshot).values()].filter(
      (intent) => intent.resolution._tag === "SafeToRetry",
    );

    if (intents.length === 0) return false;
    const submission = snapshot.submission;
    const current = yield* currentOperationsFor(submission);
    const operations = operationsFor(records, runIdForSubmission(submission.submissionId));

    const declared = yield* declaredCallsFor(
      records,
      runIdForSubmission(submission.submissionId),
      new Set(intents.map((intent) => intent.toolCallId)),
    );

    for (const intent of intents) {
      const original = declared.get(intent.toolCallId);

      if (
        original === undefined ||
        (current !== undefined &&
          !supportsOperation(
            OpenToolCallEvidence.make(original),
            operations.get(intent.toolCallId),
            current,
          ))
      )
        return true;
    }

    return false;
  });

  /**
   * Replay covering resolution intents as state-only wakes. Canonical outcomes wait for an
   * actual claim, so resolving an older request cannot append using a later writer's epoch.
   */
  const applyUnknownResolutionsForRecovery = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
    _evidence: RecoveryEvidence,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
  ): Effect.fn.Return<"repaired" | "deferred" | "unknown", DurableWorkerFailure> {
    if (yield* hasUnsupportedRetry(snapshot, records)) return "unknown";
    const submission = snapshot.submission;

    for (const intent of resolutionIntentsFor(snapshot).values()) {
      yield* ledger
        .recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: intent.submissionId,
            toolCallId: intent.toolCallId,
            author: intent.author,
            reason: intent.reason,
            resolution: intent.resolution,
          }),
        )
        .pipe(
          Effect.catchTag("UnknownResolutionConflict", (error) =>
            LedgerError.make({
              operation: "applyUnknownResolutions",
              message: `Replaying the stored resolution intent for ${error.toolCallId} diverged`,
              cause: error,
            }),
          ),
        );
    }
    // Canonical outcomes are applied by the next claim. A wake never takes an active epoch.
    yield* wake.notify(submission.threadId);

    return "repaired";
  });

  /**
   * Repair a lost durable suspension from canonical history (plan §2.6, crash between the
   * canonical `ToolApprovalRequested` append and the ledger `suspend` transition): the lane is
   * claimable but the Run cannot proceed without a decision, so the executor claims it and moves
   * it to `suspended` — the ledger op ends the ownership period itself — reporting `repaired`.
   * Nothing executes and nothing settles. A lane that is already suspended has nothing to
   * repair: it waits durably for the authorized `resolveApproval` path, consuming no worker
   * permit, and stays `deferred`. Decisions that raced ahead of the repair leave the lane to a
   * worker's batch resume (`deferred`).
   */
  const awaitApprovalForRecovery = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
    evidence: RecoveryEvidence,
    decision: RecoveryDecision,
  ): Effect.fn.Return<"repaired" | "deferred", DurableWorkerFailure, Scope.Scope> {
    const submission = snapshot.submission;

    if (submission.state === "suspended") return "deferred";
    const first = pendingApprovalForRecovery(snapshot, evidence);

    if (first === undefined) return "deferred";
    const claimed = yield* claimFor(submission, decision);

    if (Option.isNone(claimed)) return "deferred";
    const claim = claimed.value;

    const outcome = yield* ledger.suspend(
      SuspendRequest.make({
        submissionId: submission.submissionId,
        ownershipToken: claim.ownershipToken,
        reason: ApprovalPendingSuspension.make({
          toolCallIds: [first.toolCallId],
        }),
      }),
    );

    yield* hit("approval:after-suspend");
    if (outcome === "resume-immediately") {
      // Decisions raced in between the snapshot read and the suspend transaction: nothing to
      // repair — release the claim so a worker resumes the declared batch.
      yield* ledger
        .releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: submission.submissionId,
            ownershipToken: claim.ownershipToken,
          }),
        )
        .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

      return "deferred";
    }

    return "repaired";
  });

  /**
   * Defensive branch for a `suspended` lane whose canonical approval requests are all decided
   * (the classifier's `ResumeSuspended`). Under this coordinator the state is unreachable: the
   * covering `recordApprovalDecision` wakes the lane atomically inside the adapter (WP2), and a
   * decision racing ahead of `suspend` returns `resume-immediately` without ever suspending. The
   * executor therefore only re-hints the lane and reports `deferred`, keeping the obligation
   * visible instead of guessing at a wake the ledger port does not offer.
   */
  const resumeSuspendedForRecovery = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
  ): Effect.fn.Return<"deferred", DurableWorkerFailure> {
    yield* wake.notify(snapshot.submission.threadId);

    return "deferred";
  });

  const executeRecoveryDecision = Effect.fnUntraced(function* (
    snapshot: RecoverySnapshot,
    evidence: RecoveryEvidence,
    decision: RecoveryDecision,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    history: RecoveryHistorySnapshot,
  ): Effect.fn.Return<
    "repaired" | "deferred" | "none" | "unknown",
    DurableWorkerFailure,
    Scope.Scope
  > {
    const submission = snapshot.submission;
    const runId = runIdForSubmission(submission.submissionId);
    const start = yield* canonicalRunStartFromRecords(records, runId);

    if (
      start?.payload._tag === "RunStarted" &&
      start.payload.runId === runId &&
      (records.some(
        ({ record }) =>
          record.payload._tag === "RunDurationExhausted" && record.payload.runId === runId,
      ) ||
        (submission.workerAdmission !== undefined &&
          (yield* Clock.currentTimeMillis) >= submission.workerAdmission.origin.expiresAtMillis)) &&
      (decision._tag === "CompleteChildAdmission" ||
        decision._tag === "RepairSubagentStartLink" ||
        decision._tag === "AwaitChildAdmissionResolution" ||
        (decision._tag === "MarkUnknown" &&
          snapshot.childReservations.some((reservation) => reservation.status !== "released")))
    ) {
      // A closed admission window must release provably-unused reservations or restore the
      // existing child's link. Do this before ordinary Unknown Outcomes make the lane
      // unclaimable, and never let binding-free recovery admit new work after expiry.
      const claimed = yield* claimFor(submission, decision);

      if (Option.isNone(claimed)) return "deferred";
      const claim = claimed.value;

      yield* store.materialize(
        ThreadMaterialization.make({
          threadId: submission.threadId,
          producerEpoch: claim.producerEpoch,
        }),
      );
      const ctx = yield* attemptContextFor(submission.threadId, claim.producerEpoch);

      yield* reconcileRetainedChildren(
        ctx,
        submission,
        yield* recoveryOwnership(
          submission.threadId,
          submission.submissionId,
          claim.ownershipToken,
        ),
      );
      const open = yield* completeJoinedReleases(submission);

      yield* ledger
        .releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: submission.submissionId,
            ownershipToken: claim.ownershipToken,
          }),
        )
        .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

      return open ? "deferred" : "repaired";
    }
    switch (decision._tag) {
      case "NoAction": {
        return "none";
      }
      case "ResumeFromTurnBoundary":
      case "ResumePendingToolBatch": {
        // Resumption needs the Agent Binding: a claiming worker resumes from the committed
        // boundary (the declared batch resumes without model re-invocation, durability §15).
        return "deferred";
      }
      case "MarkUnknown": {
        return yield* markUnknownForRecovery(snapshot, evidence, records, decision);
      }
      case "ApplyUnknownResolutions": {
        return yield* applyUnknownResolutionsForRecovery(snapshot, evidence, records);
      }
      case "AwaitUnknownResolution": {
        // This Submission stays parked awaiting the authorized DUR-017 resolution path;
        // the settlement obligation stays visible, nothing replays.
        return "unknown";
      }
      case "AwaitApprovalDecision": {
        return yield* awaitApprovalForRecovery(snapshot, evidence, decision);
      }
      case "ResumeSuspended": {
        return yield* resumeSuspendedForRecovery(snapshot);
      }
      case "RevertJoining":
      case "RepairJoinMarker": {
        const hostSubmissionId = snapshot.hostSubmissionId;

        if (hostSubmissionId === undefined) return "deferred";

        const host = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: hostSubmissionId }),
        );

        if (Option.isNone(host)) return "deferred";

        // Joining belongs to the host's ownership period, including the interval before
        // its input append. Recovery must not revert claims a live host is still consuming.
        const claimed =
          host.value.state === "settled"
            ? Option.none<Claim>()
            : yield* claimFor(host.value, decision);

        if (host.value.state !== "settled" && Option.isNone(claimed)) return "deferred";
        if (Option.isSome(claimed))
          yield* store.materialize(
            ThreadMaterialization.make({
              threadId: submission.threadId,
              producerEpoch: claimed.value.producerEpoch,
            }),
          );

        // The previous owner may have appended after the pass snapshot, before this claim.
        // Re-read that suffix and the join link before deciding whether the input is absent.
        const currentRecords = yield* refreshRecoveryHistory(
          submission.threadId,
          records,
          history.throughSequence,
          history.submissionIds,
        );

        const current = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
        );

        if (current.submission.state !== "joining" || current.hostSubmissionId !== hostSubmissionId)
          return "deferred";

        const currentEvidence = yield* evidenceFor(
          currentRecords,
          submission.submissionId,
          history.materialized,
          hostSubmissionId,
        );

        const inputEnvelope = currentRecords.find(
          (envelope) =>
            envelope.record.recordId === submissionInputRecordId(submission.submissionId),
        );

        if (currentEvidence.hostSettlementOutcome !== undefined) {
          // A covered terminal input stays visible: markJoined must precede delivery, so
          // recovery cannot guess at that unreachable prefix. Uncovered input returns ready.
          if (currentEvidence.joinedInputCovered) return "deferred";
        } else if (Option.isNone(claimed)) {
          // A ledger-settled host without canonical settlement evidence is not repair authority.
          return "deferred";
        } else if (inputEnvelope !== undefined) {
          yield* ledger.markJoined(
            MarkJoinedRequest.make({
              submissionId: submission.submissionId,
              ownershipToken: claimed.value.ownershipToken,
              recordId: inputEnvelope.record.recordId,
              sequence: inputEnvelope.sequence,
            }),
          );
        }
        if (currentEvidence.hostSettlementOutcome !== undefined || inputEnvelope === undefined)
          yield* ledger.revertJoining(
            RevertJoiningRequest.make({
              submissionId: submission.submissionId,
              guard: {
                hostSubmissionId,
                ...(Option.isNone(claimed) ? {} : { ownershipToken: claimed.value.ownershipToken }),
              },
            }),
          );
        if (Option.isSome(claimed))
          yield* ledger
            .releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: hostSubmissionId,
                ownershipToken: claimed.value.ownershipToken,
              }),
            )
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
        yield* wake.notify(submission.threadId);

        return "repaired";
      }
      case "SettleJoinedWithHost": {
        const hostSubmissionId = snapshot.hostSubmissionId;

        if (hostSubmissionId === undefined) return "deferred";
        const hostRecord = yield* canonicalSettlementRecord(records, hostSubmissionId);
        const hostSettlement = yield* settlementPayloadFromRecord(hostRecord, hostSubmissionId);
        // The host settled canonically, so no live owner can exist for this lane (a joined
        // head is never claimable): the joined settlement completes unfenced at the current
        // tail, deferring to any racing fence advance.
        const ctx = yield* attemptContextAtTail(submission.threadId);

        const applied = yield* settleOneJoined(ctx, hostSettlement, snapshot).pipe(
          Effect.as(true),
          Effect.catchTag("FenceRejected", () => Effect.succeed(false)),
        );

        return applied ? "repaired" : "deferred";
      }
      case "AwaitHostSettlement": {
        // The joined input reattaches through the host Run's resume (prompt-coverage rule);
        // hint the shared lane and keep the obligation visible.
        yield* wake.notify(submission.threadId);

        return "deferred";
      }
      case "CompleteMaterialization":
      case "RepairReadiness": {
        yield* materializeAtLeast(submission.threadId, ZERO_EPOCH);
        yield* ensureThreadCreated(
          submission.threadId,
          submission.agentId,
          submission.agentDigests,
        );
        if (submission.workerAdmission !== undefined) {
          yield* workerRuntime
            .ensureOrigin(submission.workerAdmission.origin)
            .pipe(
              Effect.mapError((cause) =>
                LedgerError.make({ operation: "worker-origin", message: cause.reason, cause }),
              ),
            );
        }
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: submission.submissionId }));

        return "repaired";
      }
      case "ApplyInput":
      case "RepairInputMarker": {
        const claimed = yield* claimFor(submission, decision);

        if (Option.isNone(claimed)) return "deferred";
        const claim = claimed.value;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId: submission.threadId,
            producerEpoch: claim.producerEpoch,
          }),
        );
        yield* ensureThreadCreated(
          submission.threadId,
          submission.agentId,
          submission.agentDigests,
        );
        const ctx = yield* attemptContextFor(submission.threadId, claim.producerEpoch);

        const ownership = yield* recoveryOwnership(
          submission.threadId,
          submission.submissionId,
          claim.ownershipToken,
        );

        const currentRecords = yield* refreshRecoveryHistory(
          submission.threadId,
          records,
          history.throughSequence,
          history.submissionIds,
        );

        yield* applyCanonicalInput(
          ctx,
          submission,
          ownership,
          currentRecords,
          snapshot.inputApplied,
        );
        const ownershipToken = claim.ownershipToken;

        yield* ledger
          .releaseOwnership(
            ReleaseOwnershipRequest.make({
              submissionId: submission.submissionId,
              ownershipToken,
            }),
          )
          .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

        return "repaired";
      }
      case "FinalizeLedgerFromHistory": {
        const record = yield* canonicalSettlementRecord(records, submission.submissionId);

        yield* finalizeFromHistory(submission, record);

        return "repaired";
      }
      case "SettleAborted": {
        return yield* settleAbortedForRecovery(snapshot, evidence, records, decision);
      }
      case "CompleteChildAdmission": {
        // Binding-free (D3): the canonical `SubagentRequested` payload carries the encoded
        // child input, intended identity, and every digest, so admission completes without a
        // live delegation handler — one child, same Receipt on every replay (SUB-016).
        const subagent = subagentRecordsOf(records, runIdForSubmission(submission.submissionId));
        const requestedPayload = subagent.requested.get(decision.toolCallId);

        if (requestedPayload === undefined) return "deferred";
        const admission = yield* establishChildFromRequest(submission, requestedPayload);

        return admission._tag === "indeterminate" ? "deferred" : "repaired";
      }
      case "RepairSubagentStartLink": {
        // Resolve the SAME child by its deterministic idempotency key, complete any missing
        // materialization/lineage/readiness, then append the exact deterministic
        // `SubagentStarted` link and reattach the reservation under the parent fence
        // (spec §13, SUB-016/SUB-017).
        const runId = runIdForSubmission(submission.submissionId);
        const subagent = subagentRecordsOf(records, runId);
        const requestedPayload = subagent.requested.get(decision.toolCallId);

        if (requestedPayload === undefined) return "deferred";
        const admission = yield* establishChildFromRequest(submission, requestedPayload);

        if (admission._tag === "indeterminate") return "deferred";
        const claimed = yield* claimFor(submission, decision);

        if (Option.isNone(claimed)) return "deferred";
        const claim = claimed.value;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId: submission.threadId,
            producerEpoch: claim.producerEpoch,
          }),
        );
        const ctx = yield* attemptContextFor(submission.threadId, claim.producerEpoch);
        const knownIds = knownRecordIdsOf(records);
        const startRecordId = subagentStartedRecordId(runId, decision.toolCallId);

        if (!knownIds.has(startRecordId)) {
          const envelope = yield* makeEnvelope(
            startRecordId,
            SubagentStarted.make({
              runId,
              toolCallId: decision.toolCallId,
              childThreadId: requestedPayload.childThreadId,
              childSubmissionId: admission.childSubmissionId,
              childReceiptId: admission.receiptId,
              childRunId: runIdForSubmission(admission.childSubmissionId),
            }),
          );

          yield* appendBatch(
            ctx,
            CanonicalBatch.make({
              batchId: subagentStartedBatchId(runId, decision.toolCallId),
              producerId: config.producerId,
              records: [envelope],
            }),
          ).pipe(
            Effect.catchTag("AppendConflict", () => Effect.void),
            Effect.asVoid,
          );
          yield* hit("subagent:after-start-append");
        }
        yield* ledger
          .attachChildToReservation(
            AttachChildToReservationRequest.make({
              reservationId: decodeChildReservationIdSync(requestedPayload.reservationId),
              ownershipToken: claim.ownershipToken,
              childSubmissionId: admission.childSubmissionId,
            }),
          )
          .pipe(
            Effect.catchTag(
              "ChildReservationConflict",
              conflictToLedgerError("attachChildToReservation"),
            ),
          );
        yield* ledger
          .releaseOwnership(
            ReleaseOwnershipRequest.make({
              submissionId: submission.submissionId,
              ownershipToken: claim.ownershipToken,
            }),
          )
          .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
        yield* wake.notify(requestedPayload.childThreadId);
        yield* wake.notify(submission.threadId);

        return "repaired";
      }
      case "EnsureWaitingForChild": {
        // Restore the lost `waitingForChild` checkpoint (spec §14 "after parent start, before
        // waitingForChild checkpoint"): claim the lane and suspend it — the ledger op ends the
        // ownership period itself, so the lane holds no worker permit while each child runs
        // on its own lane. Never spawns a replacement invocation (SUB-018/SUB-030).
        const claimed = yield* claimFor(submission, decision);

        if (Option.isNone(claimed)) return "deferred";
        const claim = claimed.value;

        const suspension = yield* ledger.suspend(
          SuspendRequest.make({
            submissionId: submission.submissionId,
            ownershipToken: claim.ownershipToken,
            reason: WaitingForChildSuspension.make({ children: decision.children }),
          }),
        );

        yield* hit("subagent:after-suspend");
        if (suspension === "resume-immediately") {
          // Every listed child already settled: leave the joins to a claiming worker's batch
          // resume (they need the parent Binding and its result projection).
          yield* ledger
            .releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: submission.submissionId,
                ownershipToken: claim.ownershipToken,
              }),
            )
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));

          return "deferred";
        }

        return "repaired";
      }
      case "ResumeWaitingParent": {
        // Every relevant child is provably settled: replay the idempotent ownership-free wake
        // (a dropped wake is never a lost obligation) so a claiming worker resumes the
        // declared batch and joins each child's canonical Settlement (spec §13).
        for (const child of decision.children) {
          yield* ledger.recordChildSettled(
            ChildSettledNotification.make({
              parentSubmissionId: submission.submissionId,
              childSubmissionId: child.childSubmissionId,
            }),
          );
        }
        yield* wake.notify(submission.threadId);

        return "repaired";
      }
      case "ApplyJoinAccounting": {
        // Budget release incomplete after a canonical join: replay the accounting decision
        // FROM the canonical `SubagentJoined` record — budget stays unavailable until repair,
        // never available twice (spec §12 join step 6, DUR-015).
        const reservation = snapshot.childReservations.find(
          (row) => row.reservationId === decision.reservationId,
        );

        if (reservation === undefined) return "deferred";
        if (reservation.status === "released") return "repaired";
        const subagent = subagentRecordsOf(records, runIdForSubmission(submission.submissionId));
        const joinedPayload = subagent.joined.get(decision.toolCallId);

        if (joinedPayload !== undefined) {
          yield* applyReservationRelease(decision.reservationId, joinedPayload.finalAccounting, {
            threadId: submission.threadId,
            runId: runIdForSubmission(submission.submissionId),
            toolCallId: decision.toolCallId,
          });

          return "repaired";
        }
        if (reservation.status === "releasePending" && reservation.accounting !== undefined) {
          // The decision is already frozen: finish the idempotent release — never re-freeze.
          yield* applyReservationRelease(decision.reservationId, reservation.accounting, {
            threadId: submission.threadId,
            runId: runIdForSubmission(submission.submissionId),
            toolCallId: decision.toolCallId,
          });

          return "repaired";
        }

        return "deferred";
      }
      case "PropagateChildAbort": {
        // Request-abort-and-join (spec §13.1): the ONE idempotent durable abort command per
        // nonterminal child — the recorded child `AbortIntent` row IS the propagation marker,
        // so the replayed command returns it unchanged (DUR-012) — while the parent stays (or
        // becomes) suspended `waitingForChild` for the joins.
        for (const child of decision.children) {
          yield* ledger
            .requestAbort(
              AbortCommand.make({
                submissionId: child.childSubmissionId,
                author: SUBAGENT_ABORT_AUTHOR,
                reason: SUBAGENT_ABORT_REASON,
              }),
            )
            .pipe(
              // The child settled concurrently: its one winning Settlement joins next pass.
              Effect.catchTags({
                SettlementConflict: () => Effect.void,
                JoinedToHost: conflictToLedgerError("PropagateChildAbort"),
              }),
              Effect.asVoid,
            );
          yield* hit("subagent:after-child-abort-intent");

          const childRow = yield* ledger.lookup(
            SubmissionLookupById.make({ submissionId: child.childSubmissionId }),
          );

          if (Option.isSome(childRow)) {
            yield* wake.notify(childRow.value.threadId);
          }
        }
        if (submission.state !== "suspended") {
          const claimed = yield* claimFor(submission, decision);

          if (Option.isNone(claimed)) return "deferred";
          const claim = claimed.value;

          const suspension = yield* ledger.suspend(
            SuspendRequest.make({
              submissionId: submission.submissionId,
              ownershipToken: claim.ownershipToken,
              reason: WaitingForChildSuspension.make({ children: decision.children }),
            }),
          );

          yield* hit("subagent:after-suspend");
          if (suspension === "resume-immediately") {
            // Every child settled while suspending: the next pass joins the winners.
            yield* ledger
              .releaseOwnership(
                ReleaseOwnershipRequest.make({
                  submissionId: submission.submissionId,
                  ownershipToken: claim.ownershipToken,
                }),
              )
              .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          }
        }

        return "repaired";
      }
      case "ReleaseOrphanChildReservation": {
        // Provably childless reservations release exactly once (spec §13/§14): freeze the
        // deterministic zero-consumed decision, then apply it idempotently.
        for (const reservationId of decision.reservationIds) {
          yield* releaseOrphanReservation(reservationId);
        }

        return "repaired";
      }
      case "AwaitChildAdmissionResolution": {
        // Wait-and-retry (SUB-031): the evidence assembler re-queries the authoritative owner
        // with the deterministic idempotency key on every recovery pass; an indeterminate
        // answer never permits a second admission and holds no worker permit.
        return "deferred";
      }
      case "AwaitChildSettlement": {
        // The parent lane stays dormant `waitingForChild` (SUB-030): the child's Settlement
        // wakes it durably through `recordChildSettled`; an unresolved ordinary Tool inside
        // the child keeps the parent here honestly with the obligation visible (SUB-021).
        return "deferred";
      }
      case "AwaitParentEstablishment": {
        // The child lane defers its own materialization/readiness repair until the
        // parent's idempotent establishment appends the immutable lineage record — a child
        // never runs a Turn before its lineage is canonical. A droppable wake hint nudges the
        // parent lane, whose own recovery re-drives establishment (CompleteChildAdmission /
        // RepairSubagentStartLink); the deterministic child identity makes every replay
        // converge on this one child (SUB-016).
        const parent = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: decision.parentSubmissionId }),
        );

        if (Option.isSome(parent)) {
          yield* wake.notify(parent.value.threadId);
        }

        return "deferred";
      }
    }
  });

  const recoverSnapshot = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
    history: RecoveryHistorySnapshot,
  ): Effect.fn.Return<RecoveryReport, DurableWorkerFailure> {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );

    // A racing steering link can name a host outside the pass's nonterminal group. Read that
    // host from the same verified prefix, without changing this snapshot's visibility.
    if (
      snapshot.hostSubmissionId !== undefined &&
      !history.submissionIds.includes(snapshot.hostSubmissionId)
    ) {
      const hostId = snapshot.hostSubmissionId;
      const hostView = yield* recoveryView(submission.threadId, history.throughSequence, [hostId]);

      const hostRecords = yield* Stream.runCollect(
        hostView.canonical.pipe(Stream.filter(controlRecords([hostId]))),
      );

      const merged = new Map(history.records.map((entry) => [entry.sequence, entry]));

      for (const entry of hostRecords) merged.set(entry.sequence, entry);
      history = {
        ...history,
        records: [...merged.values()].sort((left, right) => left.sequence - right.sequence),
        submissionIds: [...history.submissionIds, snapshot.hostSubmissionId],
      };
    }

    const evidence = yield* evidenceFor(
      history.records,
      submission.submissionId,
      history.materialized,
      snapshot.hostSubmissionId,
    );

    const decision = classifyRecovery(snapshot, evidence);

    // Untouched ready input belongs to the worker's first claim. Running attempts and
    // canonical appends with a missing marker still take their classified repair path.
    const disposition =
      decision._tag === "ApplyInput" && snapshot.submission.state === "ready"
        ? "deferred"
        : yield* Effect.scoped(
            executeRecoveryDecision(snapshot, evidence, decision, history.records, history),
          );

    return RecoveryReport.make({
      submissionId: submission.submissionId,
      threadId: submission.threadId,
      decision,
      disposition,
    });
  });

  const recoverSubmission = Effect.fn("DurableAgentRuntime.recoverSubmission")(function* (
    submissionId: SubmissionId,
  ): Effect.fn.Return<RecoveryReport, DurableWorkerFailure> {
    const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    if (Option.isNone(found)) {
      return yield* LedgerError.make({
        operation: "recoverSubmission",
        message: `Unknown Submission ${submissionId}`,
      });
    }

    if (found.value.state === "settled")
      yield* workerRuntime.completeInput(found.value).pipe(
        Effect.mapError((cause) =>
          LedgerError.make({
            operation: "recoverSubmission",
            message: "Worker completion repair failed",
            cause,
          }),
        ),
      );
    const history = yield* readRecoveryHistory(found.value.threadId, [submissionId]);

    if (history.materialized && found.value.workerAdmission?.origin.reporting?.mode === "standard")
      yield* updateRuntime.repairRun(found.value.threadId, submissionId);

    return yield* recoverSnapshot(found.value, history);
  });

  const discoverWork = Effect.fn("DurableAgentRuntime.discoverWork")(function* (
    request: ThreadWorkRequest,
  ): Effect.fn.Return<ThreadWorkPage, ThreadStoreError | WorkDiscoveryUnavailable> {
    const decoded = yield* Schema.decodeEffect(ThreadWorkRequest)(request).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "discover work",
          message: "Invalid work page request",
          cause,
        }),
      ),
    );

    return yield* validateWorkPage(yield* workDiscovery.page(decoded), decoded.limit);
  });

  const rebuildWorkIndex = workDiscovery.rebuild;
  const discoverWorkThreads = workDiscovery.threads;

  const originalSubmission = Effect.fnUntraced(function* (threadId: ThreadId, runId: RunId) {
    const input = yield* getRunInput({ threadId, runId }).pipe(
      Effect.provideService(ThreadReader, reader),
    );

    const payload = Option.isSome(input) ? input.value.record.payload : undefined;

    if (
      payload?._tag !== "UserInputRecorded" ||
      payload.submissionId === undefined ||
      runIdForSubmission(payload.submissionId) !== runId ||
      (Option.isSome(input) &&
        input.value.record.recordId !== submissionInputRecordId(payload.submissionId))
    )
      return yield* ThreadStoreError.make({
        operation: "recover work",
        message: "The original Run input is missing or incompatible",
      });
    const submission = yield* lookupKnownSubmission("recover work", payload.submissionId);

    if (submission.threadId !== threadId)
      return yield* ThreadStoreError.make({
        operation: "recover work",
        message: "The original admission belongs to another Thread",
      });

    return submission;
  });

  const readOperation = Effect.fnUntraced(function* (threadId: ThreadId, entry: ThreadWorkEntry) {
    if (
      entry.owner._tag !== "Operation" ||
      entry.originRecordId === undefined ||
      entry.originDigest === undefined
    )
      return yield* ThreadStoreError.make({
        operation: "recover operation",
        message: "The original declaration reference is missing",
      });
    const owner = entry.owner;

    const current = yield* withCrypto(
      resolveWorkEvidence(threadId, entry).pipe(Effect.provideService(ThreadReader, reader)),
    );

    const origin = yield* withCrypto(
      resolveEvidence(threadId, {
        recordId: entry.originRecordId,
        digest: entry.originDigest,
      }).pipe(Effect.provideService(ThreadReader, reader)),
    );

    const declaration = origin.record.payload;

    if (
      declaration._tag !== "ModelResponseRecorded" ||
      declaration.runId !== owner.runId ||
      !declaration.toolOperations.some((call) => call.toolCallId === owner.toolCallId) ||
      current.sequence < origin.sequence
    )
      return yield* ThreadStoreError.make({
        operation: "recover operation",
        message: "The selected operation does not match its declaration",
      });
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

    const records = yield* Stream.runCollect(
      operationEvidence(threadId, owner, entry.originRecordId, tail.tailSequence, declaration),
    ).pipe(Effect.provideService(ThreadReader, reader));

    if (
      !records.some(({ record }) => record.recordId === origin.record.recordId) ||
      !records.some(({ record }) => record.recordId === current.record.recordId)
    )
      return yield* ThreadStoreError.make({
        operation: "recover operation",
        message: "Selected operation evidence is incomplete",
      });

    const declared = (yield* declaredCallsFor(
      records,
      owner.runId,
      new Set([owner.toolCallId]),
    )).get(owner.toolCallId);

    if (declared === undefined)
      return yield* RunJournalError.make({
        message: "The selected call has no original parameters and replay contract",
      });

    const state = toolOperationStates(records, owner.runId).find(
      (state) => state.operation.toolCallId === owner.toolCallId,
    );

    if (state === undefined)
      return yield* RunJournalError.make({ message: "Selected operation state is missing" });

    return { records, declared, state, declaration };
  });

  const recoverTerminalOperation = Effect.fnUntraced(function* (
    threadId: ThreadId,
    entry: ThreadWorkEntry,
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<WorkRecoveryReport["disposition"], DurableWorkerFailure> {
    const selected = yield* readOperation(threadId, entry);

    if (selected.state.settled || selected.state.resolved) return "none";

    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );

    const intent = snapshot.unknownResolutions.find(
      (intent) =>
        intent.toolCallId === selected.declared.toolCallId &&
        unknownResolutionKind(intent.resolution) === "factual",
    );

    const factual = intent?.resolution;

    const reconciliation =
      factual === undefined && selected.declared.executionKind === "ordinary"
        ? yield* reconciler
            .reconcile(
              DeclaredToolCallEvidence.make({
                threadId,
                submissionId: submission.submissionId,
                ...selected.declared,
              }),
            )
            .pipe(
              Effect.map(Option.some),
              Effect.catchTag("ToolReconcilerError", () => Effect.succeed(Option.none())),
            )
        : Option.none();

    const completed =
      factual?._tag === "CompletedWithResult"
        ? factual
        : Option.isSome(reconciliation) && reconciliation.value._tag === "CompletedWithResult"
          ? reconciliation.value
          : undefined;

    const neverStarted =
      factual?._tag === "NeverHappened" ||
      selected.state.dispatchBlocked ||
      (Option.isSome(reconciliation) && reconciliation.value._tag === "NeverStarted");

    if (completed === undefined && !neverStarted) return "unknown";

    const result =
      completed === undefined
        ? yield* Schema.encodeEffect(ToolUnavailable)(
            ToolUnavailable.make({
              toolName: selected.declared.toolName,
              execution: "not-executed",
              message: "The Run is terminal and this original operation did not execute.",
            }),
          ).pipe(Effect.flatMap(decodePersisted), Effect.orDie)
        : completed.result;

    if (utf8ByteLength(JSON.stringify(result)) > selected.declaration.toolResultMaxBytes)
      return yield* RunJournalError.make({
        message: "The recovered result exceeds the original operation's result bound",
      });
    const ctx = yield* attemptContextAtTail(threadId);

    yield* appendClosedCall(
      ctx,
      submission.submissionId,
      new Set(selected.records.map(({ record }) => record.recordId)),
      OpenToolCallEvidence.make({
        toolCallId: selected.declared.toolCallId,
        toolName: selected.declared.toolName,
        turn: selected.declared.turn,
      }),
      {
        result: { value: result, isFailure: completed?.isFailure ?? true },
        resolution:
          completed === undefined
            ? "never-started"
            : completed.isFailure
              ? "failed-with-error"
              : "completed-with-result",
        author: intent?.author ?? RECONCILER_AUTHOR,
        reason:
          intent?.reason ??
          "Original operation evidence established the factual outcome after Run closure",
      },
    );
    yield* workerRuntime.completeInput(submission).pipe(
      Effect.mapError((cause) =>
        LedgerError.make({
          operation: "recover operation",
          message: "Worker effect acknowledgement remains unavailable",
          cause,
        }),
      ),
    );

    return "repaired";
  });

  const repairFrozenDelivery = Effect.fnUntraced(function* (
    threadId: ThreadId,
    frozen: {
      readonly messageId: IdempotencyKey;
      readonly envelope: PersistedJson;
      readonly createdAtMillis: number;
      readonly deadlineAtMillis: number;
      readonly predecessor?: IdempotencyKey;
    },
  ) {
    if (Option.isNone(deliveries))
      return yield* ThreadStoreError.make({
        operation: "recover delivery",
        message: "The owning delivery port is unavailable",
      });

    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(frozen.envelope).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "recover delivery",
          message: "Frozen delivery evidence is incompatible",
          cause,
        }),
      ),
    );

    const record = yield* withCrypto(
      prepareMessageDelivery({
        key: { ownerThreadId: threadId, messageId: frozen.messageId },
        envelope,
        createdAtMillis: frozen.createdAtMillis,
        deadlineAtMillis: frozen.deadlineAtMillis,
        ...(frozen.predecessor === undefined ? {} : { predecessor: frozen.predecessor }),
      }),
    ).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "recover delivery",
          message: "Frozen delivery cannot be prepared",
          cause,
        }),
      ),
    );

    yield* deliveries.value.insert(record).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "recover delivery",
          message: "The durable handoff remains owed",
          cause,
        }),
      ),
    );
  });

  const recoverOwnedWork = Effect.fnUntraced(function* (
    request: WorkRecoveryRequest,
    recoveredAdmissions = new Map<SubmissionId, RecoveryReport>(),
  ): Effect.fn.Return<WorkRecoveryReport, DurableWorkerFailure> {
    const { threadId, work: entry } = yield* Schema.decodeEffect(WorkRecoveryRequest)(request).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "recover work",
          message: "Invalid selected work",
          cause,
        }),
      ),
    );

    const owner = entry.owner;

    const recoverAdmission = Effect.fnUntraced(function* (submissionId: SubmissionId) {
      const recovered = recoveredAdmissions.get(submissionId);

      if (recovered !== undefined) return recovered;
      const report = yield* recoverSubmission(submissionId);

      recoveredAdmissions.set(submissionId, report);

      return report;
    });

    const report = (
      disposition: WorkRecoveryReport["disposition"],
      submission?: RecoveryReport,
      retryAtMillis?: number,
    ) =>
      WorkRecoveryReport.make({
        threadId,
        workId: entry.id,
        disposition,
        ...(submission === undefined ? {} : { submission }),
        ...(retryAtMillis === undefined ? {} : { retryAtMillis: Math.ceil(retryAtMillis) }),
      });

    const invalid = () =>
      ThreadStoreError.make({
        operation: "recover work",
        message: "The selected owner does not match its state reference",
      });

    if (owner._tag === "Admission") {
      if (
        entry.id !== workId("admission", owner.submissionId) ||
        entry.stateReference._tag !== "Submission" ||
        entry.stateReference.submissionId !== owner.submissionId
      )
        return yield* invalid();
      const original = yield* lookupKnownSubmission("recover work", owner.submissionId);

      if (original.threadId !== threadId) return yield* invalid();
      const submission = yield* recoverAdmission(owner.submissionId);

      return report(submission.disposition, submission);
    }
    if (owner._tag === "Delivery") {
      if (
        entry.id !== workId("delivery", owner.messageId) ||
        entry.stateReference._tag !== "Delivery" ||
        entry.stateReference.messageId !== owner.messageId
      )
        return yield* invalid();
      if (Option.isNone(deliveries))
        return yield* WorkDiscoveryUnavailable.make({ threadId, reason: "unsupported" });

      const delivery = yield* deliveries.value
        .get({ ownerThreadId: threadId, messageId: owner.messageId })
        .pipe(
          Effect.mapError((cause) =>
            ThreadStoreError.make({
              operation: "recover work",
              message: "The owning delivery is unavailable",
              cause,
            }),
          ),
        );

      if (
        delivery === null ||
        delivery.key.ownerThreadId !== threadId ||
        delivery.key.messageId !== owner.messageId ||
        delivery.version < entry.stateReference.version
      )
        return yield* invalid();

      // The delivery driver owns due-time, claim, retry, destination authorization and parking.
      return report(
        delivery.status === "processed" || delivery.status === "refused" ? "none" : "deferred",
      );
    }

    const evidence = yield* withCrypto(
      resolveWorkEvidence(threadId, entry).pipe(Effect.provideService(ThreadReader, reader)),
    );

    const payload = evidence.record.payload;

    if (owner._tag === "Operation") {
      if (entry.id !== workId("operation", owner.runId, owner.toolCallId)) return yield* invalid();
      const submission = yield* originalSubmission(threadId, owner.runId);

      if (submission.state === "settled")
        return report(yield* recoverTerminalOperation(threadId, entry, submission));
      const recovered = yield* recoverAdmission(submission.submissionId);

      return report(recovered.disposition, recovered);
    }
    if (owner._tag === "WorkerInput") {
      if (
        entry.id !== workId("worker-input", owner.messageId) ||
        payload._tag !== "WorkerInputRequested" ||
        payload.admission.messageId !== owner.messageId ||
        payload.admission.origin.worker.threadId !== owner.workerThreadId ||
        (payload.admission.reportKind === "update") !== owner.update ||
        payload.admission.origin.source.threadId !== threadId
      )
        return yield* invalid();

      const repair = yield* workerRuntime.repairInput(threadId, payload).pipe(
        Effect.mapError((cause) =>
          ThreadStoreError.make({
            operation: "recover worker input",
            message: "Worker effect evidence remains owed",
            cause,
          }),
        ),
      );

      // A processed delivery no longer owns retry. Keep source acknowledgement recovery due
      // until the child publishes its factual evidence; remote wake hints may be lost.
      return report(
        repair === "repaired" ? "repaired" : "deferred",
        undefined,
        repair === "awaiting-effects"
          ? (yield* Clock.currentTimeMillis) + Duration.toMillis(config.settlementPollInterval)
          : undefined,
      );
    }
    if (owner._tag === "WorkerEffects") {
      if (
        entry.id !== workId("worker-effects", owner.submissionId) ||
        payload._tag !== "SubmissionSettled" ||
        payload.submissionId !== owner.submissionId
      )
        return yield* invalid();
      const submission = yield* lookupKnownSubmission("recover worker effects", owner.submissionId);
      const admission = submission.workerAdmission;

      if (
        submission.threadId !== threadId ||
        admission === undefined ||
        admission.origin.worker.threadId !== threadId ||
        payload.receiptId !== submission.receiptId
      )
        return yield* invalid();
      yield* workerRuntime.completeInput(submission).pipe(
        Effect.mapError((cause) =>
          ThreadStoreError.make({
            operation: "recover worker effects",
            message: "The owning worker acknowledgement remains owed",
            cause,
          }),
        ),
      );

      const acknowledgement = Option.getOrUndefined(
        yield* getRecord({
          threadId,
          recordId: RecordId.make(`worker-effects-resolved:${admission.messageId}`),
        }).pipe(Effect.provideService(ThreadReader, reader)),
      )?.record.payload;

      if (acknowledgement === undefined) return report("unknown");
      if (
        acknowledgement._tag !== "WorkerInputCompleted" ||
        acknowledgement.effectsResolved !== true ||
        acknowledgement.submissionId !== submission.submissionId ||
        acknowledgement.receiptId !== submission.receiptId ||
        acknowledgement.settlementId !== payload.settlementId ||
        acknowledgement.workerThreadId !== threadId ||
        acknowledgement.messageId !== admission.messageId
      )
        return yield* invalid();

      return report("repaired");
    }
    if (owner._tag === "Report") {
      if (
        entry.id !== workId("report", owner.runId) ||
        payload._tag !== "SubmissionSettled" ||
        payload.runId !== owner.runId
      )
        return yield* invalid();
      const submission = yield* originalSubmission(threadId, owner.runId);

      if (submission.state !== "settled") return report("deferred");
      if (submission.workerAdmission?.origin.reporting?.mode !== "standard")
        return yield* invalid();
      yield* workerRuntime.completeInput(submission).pipe(
        Effect.mapError((cause) =>
          ThreadStoreError.make({
            operation: "recover report",
            message: "The report obligation remains owed",
            cause,
          }),
        ),
      );

      return report("repaired");
    }
    if (owner._tag === "Child") {
      if (
        entry.id !== workId("child", owner.runId, owner.toolCallId) ||
        payload._tag !== "SubagentRequested" ||
        payload.runId !== owner.runId ||
        payload.toolCallId !== owner.toolCallId ||
        payload.childThreadId !== owner.childThreadId
      )
        return yield* invalid();
      const parent = yield* originalSubmission(threadId, owner.runId);

      if (parent.state !== "settled") {
        const recovered = yield* recoverAdmission(parent.submissionId);

        return report(recovered.disposition, recovered);
      }
      const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

      const records = yield* withCrypto(
        readRunEvidenceSnapshot(threadId, parent.submissionId, tail.tailSequence).pipe(
          Effect.provideService(ThreadReader, reader),
        ),
      );

      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
      );

      const reservation = snapshot.childReservations.find(
        (row) =>
          row.reservationId === payload.reservationId && row.parentToolCallId === owner.toolCallId,
      );

      if (reservation === undefined) return yield* invalid();
      const subagent = subagentRecordsOf(records, owner.runId);
      const joined = subagent.joined.get(owner.toolCallId);

      if (joined !== undefined) {
        yield* applyReservationRelease(reservation.reservationId, joined.finalAccounting, {
          threadId,
          runId: owner.runId,
          toolCallId: owner.toolCallId,
        });

        return report("repaired");
      }

      const childId =
        subagent.started.get(owner.toolCallId)?.childSubmissionId ?? reservation.childSubmissionId;

      if (childId === undefined) return report("deferred");
      const child = yield* lookupKnownSubmission("recover child", childId);

      if (
        child.threadId !== owner.childThreadId ||
        child.parentLinkage?.parentSubmissionId !== parent.submissionId ||
        child.parentLinkage.parentToolCallId !== owner.toolCallId
      )
        return yield* invalid();
      if (child.state === "settled") {
        yield* joinSettledChildWithoutHandler(
          yield* attemptContextAtTail(threadId),
          parent,
          new Set(records.map(({ record }) => record.recordId)),
          subagent,
          reservation,
          owner.toolCallId,
          childId,
          "unavailable",
        );

        return report("repaired");
      }
      yield* ledger
        .requestAbort(
          AbortCommand.make({
            submissionId: childId,
            author: SUBAGENT_ABORT_AUTHOR,
            reason: SUBAGENT_ABORT_REASON,
          }),
        )
        .pipe(
          Effect.catchTags({
            SettlementConflict: () => Effect.void,
            JoinedToHost: conflictToLedgerError("recover child"),
          }),
        );
      yield* wake.notify(child.threadId);

      return report("deferred");
    }
    if (
      owner._tag !== "Handoff" ||
      owner.recordId !== evidence.record.recordId ||
      entry.id !==
        (owner.kind === "reservation"
          ? payload._tag === "SubtreeBudgetReserved"
            ? workId("reservation", payload.reservationId)
            : ""
          : workId("handoff", owner.recordId))
    )
      return yield* invalid();
    switch (owner.kind) {
      case "peer":
        if (
          payload._tag !== "PeerMessagePrepared" ||
          owner.messageId !== payload.messageId ||
          payload.source.threadId !== threadId
        )
          return yield* invalid();
        yield* repairFrozenDelivery(threadId, {
          messageId: payload.messageId,
          envelope: payload.encodedEnvelope,
          createdAtMillis: DateTime.toEpochMillis(evidence.record.createdAt),
          deadlineAtMillis: payload.deadlineAtMillis,
        });

        return report("repaired");
      case "report":
        if (payload._tag !== "WorkerReportPrepared" || owner.messageId !== payload.messageId)
          return yield* invalid();
        yield* repairFrozenDelivery(threadId, payload);

        return report("repaired");
      case "update":
        if (
          payload._tag !== "AgentUpdateEmitted" ||
          payload.delivery === undefined ||
          owner.messageId !== payload.delivery.messageId ||
          payload.update.threadId !== threadId
        )
          return yield* invalid();
        yield* repairFrozenDelivery(threadId, payload.delivery);

        return report("repaired");
      case "worker-stop":
        if (
          payload._tag !== "WorkerStopRequested" ||
          owner.childThreadId !== payload.command.worker.threadId
        )
          return yield* invalid();

        const stopped = yield* workerRuntime.repairStop(threadId, owner.recordId, payload).pipe(
          Effect.mapError((cause) =>
            ThreadStoreError.make({
              operation: "recover worker stop",
              message: "Destination sealing remains owed",
              cause,
            }),
          ),
        );

        return report(
          stopped ? "repaired" : "deferred",
          undefined,
          stopped
            ? undefined
            : (yield* Clock.currentTimeMillis) + Duration.toMillis(config.settlementPollInterval),
        );
      case "child-accounting": {
        if (payload._tag !== "SubagentJoined") return yield* invalid();
        const parent = yield* originalSubmission(threadId, payload.runId);

        const snapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
        );

        const reservation = snapshot.childReservations.find(
          (row) =>
            row.reservationId === payload.reservationId &&
            row.parentToolCallId === payload.toolCallId,
        );

        if (
          reservation === undefined ||
          reservation.childSubmissionId !== payload.childSubmissionId
        )
          return yield* invalid();
        yield* applyReservationRelease(reservation.reservationId, payload.finalAccounting, {
          threadId,
          runId: payload.runId,
          toolCallId: payload.toolCallId,
        });

        return report("repaired");
      }
      case "reservation": {
        if (
          payload._tag !== "SubtreeBudgetReserved" ||
          owner.childThreadId !== payload.childThreadId
        )
          return yield* invalid();
        if (payload.lifetime === "background") {
          const repaired = yield* workerRuntime.repairReservation(threadId, evidence).pipe(
            Effect.mapError((cause) =>
              ThreadStoreError.make({
                operation: "recover background reservation",
                message: "Worker admission closure remains owed",
                cause,
              }),
            ),
          );

          return report(repaired ? "repaired" : "deferred");
        }
        if (payload.executionRunId === null) return report("deferred");
        const parent = yield* originalSubmission(threadId, payload.executionRunId);

        if (parent.state !== "settled") {
          const recovered = yield* recoverAdmission(parent.submissionId);

          return report(recovered.disposition, recovered);
        }
        const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

        const records = yield* withCrypto(
          readRunEvidenceSnapshot(threadId, parent.submissionId, tail.tailSequence).pipe(
            Effect.provideService(ThreadReader, reader),
          ),
        );

        if (
          records.some(
            ({ record: { payload: fact } }) =>
              fact._tag === "SubagentRequested" && fact.childThreadId === payload.childThreadId,
          )
        )
          return report("deferred");
        // No dispatch remains owed by a terminal Run. The monotonic subtree charge is retained.
        yield* completeWorkHandoff(threadId, owner.recordId, entry.id);

        return report("repaired");
      }
    }
  });

  const recoverWork = Effect.fn("DurableAgentRuntime.recoverWork")((request: WorkRecoveryRequest) =>
    recoverOwnedWork(request),
  );

  const runRecovery = Effect.fn("DurableAgentRuntime.runRecovery")(function* (
    options?: RecoverySweepOptions,
  ): Effect.fn.Return<RecoverySweepResult, DurableWorkerFailure> {
    const selected = options?.threadId;

    if (selected !== undefined && options?.cursor !== undefined)
      yield* withCrypto(
        decodeWorkCursor({
          threadId: selected,
          limit: MAX_RECOVERY_WORK_ITEMS,
          cursor: options.cursor,
        }),
      );

    const cursor =
      selected !== undefined || options?.cursor === undefined
        ? undefined
        : yield* Schema.decodeEffect(RecoveryCursor)(options.cursor).pipe(
            Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(RecoverySweepCursor))),
            Effect.mapError((cause) =>
              ThreadStoreError.make({
                operation: "recover work cursor",
                message: "Invalid recovery cursor",
                cause,
              }),
            ),
          );

    if (
      cursor !== undefined &&
      ((cursor.threadId === undefined) === (cursor.afterThreadId === undefined) ||
        (cursor.workCursor !== undefined && cursor.threadId === undefined))
    )
      return yield* ThreadStoreError.make({
        operation: "recover work cursor",
        message: "Invalid global recovery position",
      });
    const reports: Array<RecoveryReport> = [];
    const workReports: Array<WorkRecoveryReport> = [];
    const blocked: Array<RecoveryBlocked> = [];
    // Several native owners can point at one active Submission. Advance it only once per
    // bounded pass; terminal obligations still repair independently from their own evidence.
    const recoveredAdmissions = new Map<SubmissionId, RecoveryReport>();
    const reportedAdmissions = new Set<SubmissionId>();

    const page =
      selected === undefined
        ? yield* workDiscovery.threads({
            limit: cursor?.threadId === undefined ? 32 : 31,
            ...((cursor?.threadId ?? cursor?.afterThreadId) === undefined
              ? {}
              : { afterThreadId: cursor?.threadId ?? cursor?.afterThreadId }),
          })
        : { threadIds: [selected], afterThreadId: undefined };

    const threadIds =
      cursor?.threadId !== undefined && selected === undefined
        ? [cursor.threadId, ...page.threadIds]
        : page.threadIds;

    let continuation: string | undefined;

    for (let index = 0; index < threadIds.length; index++) {
      const threadId = threadIds[index];

      if (threadId === undefined) break;

      let workCursor =
        selected !== undefined
          ? options?.cursor
          : cursor?.threadId === threadId
            ? cursor.workCursor
            : undefined;

      let complete = false;
      let phase: RecoveryFailure["phase"] = "history";

      const outcome = yield* isolateRecovery(
        Effect.gen(function* () {
          for (
            let pages = 0;
            pages < MAX_RECOVERY_PAGES && workReports.length < MAX_RECOVERY_WORK_ITEMS;
            pages++
          ) {
            phase = "history";

            const workPage: ThreadWorkPage = yield* discoverWork({
              threadId,
              limit: MAX_RECOVERY_WORK_ITEMS - workReports.length,
              ...(workCursor === undefined ? {} : { cursor: workCursor }),
            });

            phase = "recovery";
            for (const work of workPage.entries) {
              const repaired = yield* recoverOwnedWork({ threadId, work }, recoveredAdmissions);

              workReports.push(repaired);
              if (
                repaired.submission !== undefined &&
                !reportedAdmissions.has(repaired.submission.submissionId)
              ) {
                reportedAdmissions.add(repaired.submission.submissionId);
                reports.push(repaired.submission);
              }
            }
            workCursor = workPage.cursor;
            if (workCursor === undefined) {
              complete = true;
              break;
            }
          }
        }),
        { timeout: config.recoveryTimeout, phase: () => phase, operation: "recover Thread work" },
      );

      if (Result.isFailure(outcome)) {
        blocked.push(RecoveryBlocked.make({ threadId, failure: outcome.failure }));
        complete = true;
      }
      if (!complete) {
        continuation =
          selected !== undefined
            ? workCursor
            : encodeRecoveryCursor({
                version: 2,
                threadId,
                ...(workCursor === undefined ? {} : { workCursor }),
              });
        break;
      }
      if (workReports.length >= MAX_RECOVERY_WORK_ITEMS || index === threadIds.length - 1) {
        if (
          selected === undefined &&
          (index < threadIds.length - 1 || page.afterThreadId !== undefined)
        )
          continuation = encodeRecoveryCursor({ version: 2, afterThreadId: threadId });
        break;
      }
    }

    return RecoverySweepResult.make({
      reports,
      blocked,
      workReports,
      ...(continuation === undefined ? {} : { cursor: continuation }),
    });
  });

  const submit = Effect.fnUntraced(function* <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: DurableSubmitOptions,
  ): Effect.fn.Return<Receipt, DurableSubmitFailure, InputSchema["EncodingServices"]> {
    const encodedInput = yield* Schema.encodeEffect(agent.definition.input)(input).pipe(
      Effect.mapError((cause) =>
        AgentInputError.make({ message: `Unable to encode Agent input: ${cause.message}` }),
      ),
    );

    const inputPayload = yield* Schema.decodeUnknownEffect(PersistedJson)(encodedInput).pipe(
      Effect.mapError(() =>
        AgentInputError.make({
          message: "Agent input does not satisfy the canonical persistence bounds",
        }),
      ),
    );

    const inputDigest = yield* withCrypto(digestJson(inputPayload));

    const workerAdmission =
      options.workerAdmission === undefined
        ? undefined
        : yield* workerRuntime
            .validateAdmission(
              options.workerAdmission,
              options,
              agent.definition.id,
              inputDigest,
              inputPayload,
            )
            .pipe(
              Effect.mapError((cause) =>
                AdmissionPolicyError.make({
                  reason:
                    cause.reason === "storage" || cause.reason === "unavailable"
                      ? "unavailable"
                      : cause.reason === "capacity" &&
                          cause.retryable === true &&
                          Schema.is(FrameworkMessage)(options.messageAdmission)
                        ? "occupied"
                        : "refused",
                  code: `worker-${cause.reason}`,
                  cause,
                }),
              ),
            );

    const pendingMessage = options.messageAdmission;

    const messageAdmission =
      pendingMessage === undefined
        ? undefined
        : yield* Effect.suspend((): Effect.Effect<InputMessage, MessagingError | WorkerError> =>
            Schema.is(MessageAdmission)(pendingMessage)
              ? messagingRuntime.validateAdmission(
                  pendingMessage,
                  options,
                  agent.definition.id,
                  inputDigest,
                )
              : workerRuntime.validateCompletion(
                  pendingMessage,
                  options,
                  agent.definition.id,
                  inputDigest,
                ),
          ).pipe(
            Effect.mapError((cause) =>
              AdmissionPolicyError.make({
                reason:
                  cause.reason === "storage" || cause.reason === "unavailable"
                    ? "unavailable"
                    : "refused",
                code: `message-${cause.reason}`,
                cause,
              }),
            ),
          );

    // Replay metadata is registration authority, never a caller's compatibility assertion.
    // Frozen native deliveries already proved their original envelope above. Root retries may
    // reuse only the original row; a new root must name a host-owned contract.
    if (
      options.definitions.replay !== undefined &&
      workerAdmission === undefined &&
      messageAdmission === undefined &&
      !registeredBindings.some(
        (binding) =>
          binding.agentId === agent.definition.id &&
          definitionDigestsEqual(binding.digests, options.definitions),
      )
    ) {
      const retained = yield* ledger.lookup(
        SubmissionLookupByKey.make({
          threadId: options.threadId,
          principal: options.principal,
          idempotencyKey: options.idempotencyKey,
        }),
      );

      if (
        Option.isNone(retained) ||
        retained.value.agentId !== agent.definition.id ||
        !definitionDigestsEqual(retained.value.agentDigests, options.definitions)
      )
        return yield* AdmissionPolicyError.make({
          reason: "refused",
          code: "unregistered-replay-contract",
        });
    }

    const request = yield* Schema.decodeEffect(AdmissionRequest)({
      threadId: options.threadId,
      principal: options.principal,
      idempotencyKey: options.idempotencyKey,
      ...(options.admissionGroup === undefined ? {} : { admissionGroup: options.admissionGroup }),
      ...(options.admissionFence === undefined ? {} : { admissionFence: options.admissionFence }),
      agentId: agent.definition.id,
      agentDigests: options.definitions,
      deploymentId: config.deploymentId,
      inputPayload,
      inputDigest,
      ...(workerAdmission === undefined
        ? {}
        : {
            workerAdmission: yield* Schema.encodeEffect(WorkerAdmission)(workerAdmission).pipe(
              Effect.mapError(() =>
                LedgerError.make({
                  operation: "submit",
                  message: "Worker admission cannot be encoded",
                }),
              ),
            ),
          }),
      ...(messageAdmission === undefined ? {} : { messageAdmission }),
    }).pipe(
      Effect.mapError(() =>
        LedgerError.make({
          operation: "submit",
          message: "Admission fields do not satisfy the ledger contract",
        }),
      ),
    );

    if (workerAdmission !== undefined) {
      const admitted = yield* workerAdmissionPort
        .admit(
          WorkerAdmissionRequest.make({
            ...request,
            workerAdmission,
            producerId: config.producerId,
          }),
        )
        .pipe(
          Effect.provideService(SubmissionLedger, ledger),
          Effect.provideService(ThreadStore, store),
          Effect.provideService(ThreadReader, reader),
          Effect.provideService(WakeScheduler, wake),
          Effect.provideService(DurableRuntimeFailpoint, failpoint),
        );

      return Receipt.make({
        receiptId: admitted.receiptId,
        submissionId: admitted.submissionId,
        threadId: request.threadId,
        queueSequence: admitted.queueSequence,
      });
    }
    const admitted = yield* ledger.admit(request);

    yield* hit("submit:after-admit");

    const receipt = Receipt.make({
      receiptId: admitted.receiptId,
      submissionId: admitted.submissionId,
      threadId: options.threadId,
      queueSequence: admitted.queueSequence,
    });

    // A replay of an already-ready Submission resumes by returning the original Receipt;
    // an admitted-but-not-ready Submission (ours or a crashed predecessor's) is completed here.
    if (admitted.replayed && admitted.state !== "admitted") {
      return receipt;
    }
    yield* materializeAtLeast(options.threadId, ZERO_EPOCH);
    yield* ensureThreadCreated(options.threadId, agent.definition.id, options.definitions);
    yield* hit("submit:after-materialize");
    yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
    yield* wake.notify(options.threadId);

    return receipt;
  });

  const authorizeSettlement = (receipt: Receipt) =>
    operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "awaitSettlement",
        threadId: receipt.threadId,
        submissionId: receipt.submissionId,
      }),
    );

  const submitRegistered = Effect.fnUntraced(function* <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: Omit<DurableSubmitOptions, "definitions">,
  ) {
    const binding = yield* resolveDefinitionBinding(registeredBindings, agent.definition);

    return yield* submit(agent, input, { ...options, definitions: binding.digests });
  });

  const readFinalizedSubmission = Effect.fnUntraced(function* (receipt: Receipt): Effect.fn.Return<
    Option.Option<{
      readonly settlement: Settlement;
      readonly record: SubmissionSettledRecord;
    }>,
    DurableAwaitFailure
  > {
    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: receipt.submissionId }),
    );

    if (Option.isNone(snapshot)) {
      return yield* LedgerError.make({
        operation: "awaitSettlement",
        message: `Unknown Submission ${receipt.submissionId}`,
      });
    }
    if (
      snapshot.value.threadId !== receipt.threadId ||
      snapshot.value.receiptId !== receipt.receiptId ||
      snapshot.value.queueSequence !== receipt.queueSequence
    ) {
      return yield* OperationDenied.make({
        operation: "awaitSettlement",
        reason: "Receipt does not match the authorized Submission",
        threadId: receipt.threadId,
        submissionId: receipt.submissionId,
      });
    }
    if (snapshot.value.state !== "settled") return Option.none();

    // Finalization replay reads the canonical outcome without changing settledAt.
    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({
        submissionId: receipt.submissionId,
        settlementId: submissionSettlementId(receipt.submissionId),
      }),
    );

    const canonical = yield* getRecord({
      threadId: receipt.threadId,
      recordId: submissionSettlementRecordId(receipt.submissionId),
    }).pipe(
      Effect.provideService(ThreadReader, reader),
      Effect.mapError((cause) =>
        LedgerError.make({
          operation: "awaitSettlement",
          message: "Cannot read the finalized canonical Settlement",
          cause,
        }),
      ),
    );

    if (Option.isNone(canonical)) {
      return yield* LedgerError.make({
        operation: "awaitSettlement",
        message: "Finalized Submission has no canonical Settlement",
      });
    }

    const record = yield* settlementPayloadFromRecord(canonical.value.record, receipt.submissionId);

    if (
      record.receiptId !== receipt.receiptId ||
      record.settlementId !== settlement.settlementId ||
      record.outcome !== settlement.outcome
    ) {
      return yield* LedgerError.make({
        operation: "awaitSettlement",
        message: "Canonical Settlement disagrees with the receipt or finalized outcome",
      });
    }

    return Option.some({
      settlement: materializeSettlement(settlement, canonical.value.record),
      record,
    });
  });

  const readSubmissionStatus = Effect.fnUntraced(function* (
    receipt: Receipt,
  ): Effect.fn.Return<SubmissionStatus, DurableAwaitFailure> {
    const finalized = yield* readFinalizedSubmission(receipt);

    return Option.isNone(finalized)
      ? PendingSubmission.make({})
      : SettledSubmission.make({ settlement: finalized.value.settlement });
  });

  const submissionStatus = Effect.fnUntraced(function* (
    receipt: Receipt,
  ): Effect.fn.Return<SubmissionStatus, DurableAwaitFailure> {
    yield* authorizeSettlement(receipt);

    return yield* readSubmissionStatus(receipt);
  });

  const settlementRecord = Effect.fnUntraced(function* (receipt: Receipt) {
    yield* authorizeSettlement(receipt);
    const finalized = yield* readFinalizedSubmission(receipt);

    if (Option.isNone(finalized)) {
      return yield* LedgerError.make({
        operation: "settlementRecord",
        message: "Submission has not settled",
      });
    }

    return finalized.value.record;
  });

  const awaitFinalizedSubmission = Effect.fnUntraced(function* (receipt: Receipt) {
    // Authorization lasts for this wait, as it does for one observe subscription.
    yield* authorizeSettlement(receipt);
    while (true) {
      const finalized = yield* Effect.scoped(
        Effect.gen(function* () {
          // Register before reading the ledger so settlement between the read and
          // parking cannot be lost. Hints never replace the authoritative re-read.
          const awaitHint = yield* wake.subscribe(receipt.threadId, "settlement");
          const finalized = yield* readFinalizedSubmission(receipt);

          if (Option.isNone(finalized))
            yield* Effect.raceFirst(awaitHint, Effect.sleep(config.settlementPollInterval));

          return finalized;
        }),
      );

      if (Option.isSome(finalized)) return finalized.value;
    }
  });

  const awaitSettlement = Effect.fnUntraced(function* (receipt: Receipt) {
    return (yield* awaitFinalizedSubmission(receipt)).settlement;
  });

  const awaitSettlementRecord = Effect.fnUntraced(function* (receipt: Receipt) {
    return (yield* awaitFinalizedSubmission(receipt)).record;
  });

  const awaitProgress = Effect.fnUntraced(function* (
    threadId: ThreadId,
    afterSequence: CanonicalSequence,
  ): Effect.fn.Return<void, DurableProgressFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "observe",
        threadId,
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        // Registration MUST precede the authoritative read. A notify between this acquisition
        // and parking completes the returned one-shot Effect, so neither subscribe/check nor
        // check/park can lose progress.
        const awaitHint = yield* wake.subscribe(threadId);

        const committed = yield* Stream.runHead(
          store.read(
            ThreadRead.make({
              threadId,
              afterSequence,
              limit: 1,
            }),
          ),
        );

        if (Option.isSome(committed)) return;
        // A wake is only a hint. The caller re-reads canonical records after this returns.
        yield* awaitHint;
      }),
    );
  });

  const observe = (receipt: Receipt, options?: DurableObserveOptions) =>
    Stream.unwrap(
      operationAuthorizer
        .authorize(
          OperationAuthorizationRequest.make({
            operation: "observe",
            threadId: receipt.threadId,
            submissionId: receipt.submissionId,
          }),
        )
        .pipe(
          Effect.as(
            store.observe(
              ThreadObservation.make({
                threadId: receipt.threadId,
                ...(options?.after === undefined ? {} : { afterOffset: options.after }),
              }),
            ),
          ),
        ),
    );

  const abort = Effect.fnUntraced(function* (
    command: AbortCommand,
  ): Effect.fn.Return<AbortIntent, DurableAbortFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "abort",
        submissionId: command.submissionId,
      }),
    );
    const intent = yield* ledger.requestAbort(command);

    yield* hit("abort:after-intent");

    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );

    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.threadId);
    }

    return intent;
  });

  /**
   * DUR-017 resolution surface (abort-shaped, plan §2.2): the durable ledger intent commits
   * first; the canonical `ToolCallResolved` (+ `ToolCallSettled` for a recovered result) is
   * appended by the recovery pass or the next owning Attempt. An `AbortSubmission` resolution
   * routes into the existing abort path — the unknown calls stay recorded and abort never
   * asserts external rollback (durability §13). Possession of this service plus the mandatory
   * author/reason audit fields is the Phase 5 authorization boundary, identical to `abort`; the
   * authenticated operator surface is a P7 deliverable.
   */
  const resolveUnknown = Effect.fn("DurableAgentRuntime.resolveUnknown")(function* (
    command: UnknownResolutionCommand,
  ): Effect.fn.Return<UnknownResolutionIntent, DurableResolveFailure | OperationDenied> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "resolveUnknown",
        submissionId: command.submissionId,
      }),
    );

    const original = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );

    if (
      Option.isSome(original) &&
      original.value.state === "settled" &&
      (command.resolution._tag === "CompletedWithResult" ||
        command.resolution._tag === "NeverHappened")
    ) {
      yield* Effect.gen(function* () {
        const submission = original.value;

        const snapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: command.submissionId }),
        );

        // Identical intent retries remain valid after the factual closure commits.
        if (
          snapshot.unknownResolutions.some(
            (intent) =>
              intent.toolCallId === command.toolCallId &&
              unknownResolutionKind(intent.resolution) === "factual",
          )
        )
          return;

        const tail = yield* store.inspectTail(
          ThreadTailRequest.make({ threadId: submission.threadId }),
        );

        const records = yield* withCrypto(
          readRunEvidenceSnapshot(
            submission.threadId,
            submission.submissionId,
            tail.tailSequence,
          ).pipe(Effect.provideService(ThreadReader, reader)),
        );

        const runId = runIdForSubmission(submission.submissionId);

        const state = toolOperationStates(records, runId).find(
          (state) => state.operation.toolCallId === command.toolCallId,
        );

        const declared = yield* declaredCallsFor(records, runId, new Set([command.toolCallId]));

        if (
          state === undefined ||
          state.settled ||
          state.resolved ||
          !declared.has(command.toolCallId)
        )
          return yield* LedgerError.make({
            operation: "resolveUnknown",
            message: "No unresolved original operation exists on this terminal Run",
          });

        const response = records.find(
          ({ record: { payload } }) =>
            payload._tag === "ModelResponseRecorded" &&
            payload.runId === runId &&
            payload.turn === state.turn,
        )?.record.payload;

        if (
          response?._tag !== "ModelResponseRecorded" ||
          (command.resolution._tag === "CompletedWithResult" &&
            utf8ByteLength(JSON.stringify(command.resolution.result)) > response.toolResultMaxBytes)
        )
          return yield* LedgerError.make({
            operation: "resolveUnknown",
            message: "The factual result exceeds the original result contract",
          });
      }).pipe(
        Effect.mapError((cause) =>
          cause._tag === "LedgerError"
            ? cause
            : LedgerError.make({
                operation: "resolveUnknown",
                message: "Original operation evidence is unavailable",
                cause,
              }),
        ),
      );
    }
    const intent = yield* ledger.recordUnknownResolution(command);

    yield* hit("resolve:after-intent");
    if (command.resolution._tag === "AbortSubmission") {
      yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: command.submissionId,
          author: command.author,
          reason: command.reason,
        }),
      );
    }

    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );

    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.threadId);
    }

    return intent;
  });

  /**
   * Durable approval decision surface (plan §2.6, abort-shaped): the ledger intent commits
   * first — idempotent per (submission, tool call), with a typed `ApprovalConflict` on a
   * divergent re-decision — and the adapter transitions `suspended → input-applied` atomically
   * once every pending call of the stored suspension reason is decided; the wake hint then lets
   * a worker resume the declared batch through the batch-resume seam (no model re-invocation).
   * The canonical `ToolApprovalDecided` record is appended by the resuming Attempt's approval
   * hook before the decision is honored — never here. A denied decision fails the Run through
   * the engine's `AgentApprovalDenied` path (denial-terminal, P2 policy default). Possession of
   * this service plus the mandatory resolver/reason audit fields is the Phase 5 authorization
   * boundary, identical to `abort`; the authenticated operator surface is a P7 deliverable.
   */
  const resolveApproval = Effect.fn("DurableAgentRuntime.resolveApproval")(function* (
    command: ApprovalDecisionCommand,
  ): Effect.fn.Return<ApprovalDecisionIntent, DurableApprovalFailure | OperationDenied> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "resolveApproval",
        submissionId: command.submissionId,
      }),
    );
    const intent = yield* ledger.recordApprovalDecision(command);

    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );

    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.threadId);
    }

    return intent;
  });

  // -------------------------------------------------------------------------
  // P7 administrative operations (plan §3): explain/verify/retry/wake/scanObligations over the
  // SAME two ports the coordinator already owns, so they behave identically on DN and DC.
  // -------------------------------------------------------------------------

  /** Whole non-negative seconds between a recorded instant and Effect Clock-now. */
  const ageSecondsSince = (instant: DateTime.Utc, nowMillis: number): number =>
    Math.max(0, Math.floor((nowMillis - DateTime.toEpochMillis(instant)) / 1_000));

  const lookupKnownSubmission = Effect.fnUntraced(function* (
    operation: string,
    submissionId: SubmissionId,
  ): Effect.fn.Return<SubmissionSnapshot, LedgerError> {
    const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    if (Option.isNone(found)) {
      return yield* LedgerError.make({
        operation,
        message: `Unknown Submission ${submissionId}`,
      });
    }

    return found.value;
  });

  /**
   * Read-only explanation of one Submission: the same snapshot + tolerant canonical read +
   * pure classification the recovery pass performs, packaged WITHOUT executing anything —
   * assembling it performs zero writes (P7 exit gate: operators explain recovery state without
   * editing storage).
   */
  const explainSubmission = Effect.fnUntraced(function* (
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<RecoveryExplanation, LedgerError | ThreadStoreError | RunJournalError> {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );

    const read = yield* readRecoveryHistory(submission.threadId, [
      submission.submissionId,
      ...(snapshot.hostSubmissionId === undefined ? [] : [snapshot.hostSubmissionId]),
    ]);

    const evidence = yield* evidenceFor(
      read.records,
      submission.submissionId,
      read.materialized,
      snapshot.hostSubmissionId,
    );

    const decision = classifyRecovery(snapshot, evidence);
    const nowMillis = yield* Clock.currentTimeMillis;
    const explainedAt = yield* nowUtc;
    const runId = runIdForSubmission(submission.submissionId);

    const resolvedIds = new Set(
      read.records.flatMap(({ record }) =>
        record.payload._tag === "ToolCallSettled" && record.payload.runId === runId
          ? [record.payload.toolCallId]
          : [],
      ),
    );

    const unknownCalls: Array<ExplainedUnknownCall> = [];

    for (const envelope of read.records) {
      const payload = envelope.record.payload;

      if (payload._tag !== "ToolCallUnknown" || payload.runId !== runId) continue;
      unknownCalls.push(
        ExplainedUnknownCall.make({
          toolCallId: payload.toolCallId,
          toolName: payload.toolName,
          reason: payload.reason,
          recordedAt: envelope.record.createdAt,
          resolved: resolvedIds.has(payload.toolCallId),
        }),
      );
    }

    const pendingIds = new Set(
      toolOperationStates(read.records, runId)
        .filter((state) => !state.settled && !state.resolved)
        .map((state) => state.operation.toolCallId),
    );

    const declared = yield* declaredCallsFor(read.records, runId, pendingIds);

    const row = snapshot.submission;

    return RecoveryExplanation.make({
      submission: ExplainedSubmission.make({
        submissionId: row.submissionId,
        threadId: row.threadId,
        state: row.state,
        queueSequence: row.queueSequence,
        createdAt: row.createdAt,
        ageSeconds: ageSecondsSince(row.createdAt, nowMillis),
        ...(row.readyAt === undefined
          ? {}
          : { readyAt: row.readyAt, readyAgeSeconds: ageSecondsSince(row.readyAt, nowMillis) }),
        ...(row.parentLinkage === undefined ? {} : { parentLinkage: row.parentLinkage }),
      }),
      evidence: ExplainedEvidence.make({
        threadMaterialized: evidence.threadMaterialized,
        inputRecorded: evidence.inputRecorded,
        abortRecorded: evidence.abortRecorded,
        openToolCalls: evidence.openToolCalls,
        pendingOperations: [...declared.values()],
        openDelegationCalls: evidence.openDelegationCalls,
        approvalsPending: evidence.approvalsPending,
        unknownCalls,
        approvalDecisions: snapshot.approvalDecisions,
        unknownResolutions: snapshot.unknownResolutions,
        childAttachments: snapshot.childAttachments,
        joins: snapshot.joins,
        ...(evidence.recordedSettlementOutcome === undefined
          ? {}
          : { recordedSettlementOutcome: evidence.recordedSettlementOutcome }),
        ...(snapshot.hostSubmissionId === undefined
          ? {}
          : { hostSubmissionId: snapshot.hostSubmissionId }),
        ...(snapshot.suspension === undefined ? {} : { suspension: snapshot.suspension }),
        ...(snapshot.abortIntent === undefined ? {} : { abortIntent: snapshot.abortIntent }),
      }),
      decision,
      decisionMeaning: recoveryDecisionMeaning(decision._tag),
      disposition:
        decision._tag === "ApplyUnknownResolutions" &&
        (yield* hasUnsupportedRetry(snapshot, read.records).pipe(
          Effect.mapError((cause) =>
            RunJournalError.make({ message: "Cannot verify the pending retry contract", cause }),
          ),
        ))
          ? "unknown"
          : predictRecoveryDisposition(decision, snapshot),
      explainedAt,
    });
  });

  const explain = Effect.fnUntraced(function* (
    submissionId: SubmissionId,
  ): Effect.fn.Return<RecoveryExplanation, DurableExplainFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "explain", submissionId }),
    );
    const submission = yield* lookupKnownSubmission("explain", submissionId);

    return yield* explainSubmission(submission);
  });

  const explainThread = Effect.fnUntraced(function* (
    threadId: ThreadId,
  ): Effect.fn.Return<ReadonlyArray<RecoveryExplanation>, DurableExplainFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "explain", threadId }),
    );

    const nonterminal = yield* Stream.runCollect(
      ledger.scanNonterminal.pipe(Stream.filter((submission) => submission.threadId === threadId)),
    );

    const explanations: Array<RecoveryExplanation> = [];

    for (const submission of nonterminal) {
      if (submission.threadId !== threadId) continue;
      explanations.push(
        yield* explainSubmission(
          yield* lookupKnownSubmission("explain Thread", submission.submissionId),
        ),
      );
    }

    return explanations;
  });

  const verifyImpl = Effect.fnUntraced(function* (
    threadId: ThreadId,
  ): Effect.fn.Return<IntegrityReport, DurableVerifyFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "verify", threadId }),
    );
    if (store.verification === undefined)
      return yield* ThreadStoreError.make({
        operation: "verify Thread",
        message: "This adapter does not provide snapshot-bound streaming verification",
      });

    return yield* store.verification.verify({ threadId });
  });

  /**
   * Safe re-drive of exactly one Submission's recovery decision (plan §3): classify, execute
   * the ONE repair the classifier names, annotate owned repair attempts with their actual
   * claim epoch (DUR-013), and wake the lane. Typed refusals
   * protect the paths that own their own operations: settled work (`NoAction`), requests parked
   * on Unknown Outcomes (`resolveUnknown`, DUR-017), and lanes awaiting approval decisions
   * (`resolveApproval`). The mandatory `author`/`reason` audit fields (SEC-011) annotate the
   * structured operator log.
   */
  const retryImpl = Effect.fn("DurableAgentRuntime.retry")(function* (
    command: RetryCommand,
  ): Effect.fn.Return<RecoveryReport, DurableRetryFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "retry",
        submissionId: command.submissionId,
      }),
    );
    const submission = yield* lookupKnownSubmission("retry", command.submissionId);

    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: command.submissionId }),
    );

    const read = yield* readRecoveryHistory(submission.threadId, [
      submission.submissionId,
      ...(snapshot.hostSubmissionId === undefined ? [] : [snapshot.hostSubmissionId]),
    ]);

    const evidence = yield* evidenceFor(
      read.records,
      command.submissionId,
      read.materialized,
      snapshot.hostSubmissionId,
    );

    const decision = classifyRecovery(snapshot, evidence);

    if (decision._tag === "NoAction") {
      return yield* RetryRefused.make({
        submissionId: command.submissionId,
        refusal: "settled",
        decisionTag: decision._tag,
        message: "The Submission is settled; terminal outcomes are never revisited (DUR-002).",
      });
    }
    if (decision._tag === "AwaitUnknownResolution") {
      return yield* RetryRefused.make({
        submissionId: command.submissionId,
        refusal: "await-unknown-resolution",
        decisionTag: decision._tag,
        message:
          "This Submission is parked on Unknown Outcomes; resolve them through the authorized resolveUnknown path (DUR-017) instead of retrying.",
      });
    }
    if (decision._tag === "AwaitApprovalDecision") {
      return yield* RetryRefused.make({
        submissionId: command.submissionId,
        refusal: "await-approval-decision",
        decisionTag: decision._tag,
        message:
          "The lane is durably waiting for approval decisions; decide them through resolveApproval instead of retrying.",
      });
    }
    yield* Effect.logInfo("DurableAgentRuntime.retry executed an operator re-drive").pipe(
      Effect.annotateLogs({
        submissionId: command.submissionId,
        threadId: submission.threadId,
        author: command.author,
        reason: command.reason,
        decision: decision._tag,
      }),
    );

    const disposition = yield* Effect.scoped(
      executeRecoveryDecision(snapshot, evidence, decision, read.records, read),
    );

    yield* wake.notify(submission.threadId);

    return RecoveryReport.make({
      submissionId: command.submissionId,
      threadId: submission.threadId,
      decision,
      disposition,
    });
  });

  /** The documented operator liveness nudge: a droppable wake hint for one lane. */
  const wakeImpl = Effect.fnUntraced(function* (
    threadId: ThreadId,
  ): Effect.fn.Return<void, OperationDenied> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "wake", threadId }),
    );
    yield* wake.notify(threadId);
  });

  /**
   * Scan-based DUR-017/OPS-001 obligation report — never a daemon: one ledger scan folded into
   * aged, severity-classified rows. Ages come from timestamps that already exist: `readyAt`/
   * `createdAt` for queued and running work, `suspendedAt` for durable suspensions, and the
   * oldest unresolved canonical `ToolCallUnknown` record for DUR-017 blocks (OPS-002).
   */
  const scanObligationsImpl = Effect.fnUntraced(function* (
    thresholds: ObligationThresholds,
  ): Effect.fn.Return<ObligationReport, DurableObligationFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "scanObligations" }),
    );
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    const nowMillis = yield* Clock.currentTimeMillis;
    const generatedAt = yield* nowUtc;
    const entries: Array<ObligationEntry> = [];

    for (const entry of nonterminal) {
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: entry.submissionId }),
      );

      const submission = snapshot.submission;

      let blockedOn: ObligationBlockedOn;
      let since: DateTime.Utc = submission.readyAt ?? submission.createdAt;

      switch (submission.state) {
        case "unknown": {
          blockedOn = "unknown";

          const records = (yield* readAllTolerant(submission.threadId, [submission.submissionId]))
            .records;

          const runId = runIdForSubmission(submission.submissionId);

          const resolvedIds = new Set(
            records.flatMap(({ record }) =>
              record.payload._tag === "ToolCallSettled" && record.payload.runId === runId
                ? [record.payload.toolCallId]
                : [],
            ),
          );

          let earliest: DateTime.Utc | undefined;

          for (const envelope of records) {
            const payload = envelope.record.payload;

            if (payload._tag !== "ToolCallUnknown" || payload.runId !== runId) continue;
            if (resolvedIds.has(payload.toolCallId)) continue;
            const recordedAt = envelope.record.createdAt;

            if (
              earliest === undefined ||
              DateTime.toEpochMillis(recordedAt) < DateTime.toEpochMillis(earliest)
            ) {
              earliest = recordedAt;
            }
          }
          since = earliest ?? snapshot.suspension?.suspendedAt ?? since;
          break;
        }
        case "suspended": {
          blockedOn =
            snapshot.suspension?.reason._tag === "WaitingForChild" ? "waitingForChild" : "approval";
          since = snapshot.suspension?.suspendedAt ?? since;
          break;
        }
        case "admitted":
        case "ready": {
          blockedOn = "ready-aged";
          break;
        }
        default: {
          blockedOn = "running-aged";
          break;
        }
      }
      const ageSeconds = ageSecondsSince(since, nowMillis);

      entries.push(
        ObligationEntry.make({
          submissionId: submission.submissionId,
          threadId: submission.threadId,
          state: submission.state,
          blockedOn,
          ageSeconds,
          severity: obligationSeverityOf(ageSeconds, thresholds),
        }),
      );
    }

    return ObligationReport.make({ thresholds, entries, generatedAt });
  });

  const runResolvedWorkerImpl = Effect.gen(function* () {
    // Each claimed head selects a current Binding by stable Agent ID and optional host routing.
    // Original per-operation replay contracts still govern unfinished handlers, while one worker
    // pool serves both parent and child lanes.
    const initial = Stream.paginate(undefined, (afterThreadId: ThreadId | undefined) =>
      workDiscovery
        .threads({ limit: 32, ...(afterThreadId === undefined ? {} : { afterThreadId }) })
        .pipe(
          Effect.map(
            (page): readonly [ReadonlyArray<ThreadId>, Option.Option<ThreadId | undefined>] => [
              page.threadIds,
              page.afterThreadId === undefined ? Option.none() : Option.some(page.afterThreadId),
            ],
          ),
        ),
    );

    yield* Stream.merge(initial, wake.wakes, { haltStrategy: "right" }).pipe(
      Stream.runForEach(
        Effect.fnUntraced(function* (threadId) {
          let cursor: string | undefined;

          do {
            const recovered = yield* runRecovery({
              threadId,
              ...(cursor === undefined ? {} : { cursor }),
            });

            const blocked = recovered.blocked[0];

            if (blocked !== undefined) return yield* blocked;
            cursor = recovered.cursor;
            yield* processThreadResolvedImpl(threadId);
          } while (cursor !== undefined);
        }),
      ),
    );
  });

  const messagingRuntime = yield* makeMessagingRuntime({
    bindings: registeredBindings,
    deploymentId: config.deploymentId,
    producerId: config.producerId,
  }).pipe(Effect.provideService(ThreadStore, progressStore));

  const workerRuntime = yield* makeWorkerRuntime({
    bindings: registeredBindings,
    deploymentId: config.deploymentId,
    producerId: config.producerId,
    settlementPollInterval: config.settlementPollInterval,
  }).pipe(
    Effect.provideService(ThreadStore, progressStore),
    Effect.provideService(WorkerInputControl, {
      status: readSubmissionStatus,
      abort,
      submit: (envelope) =>
        submit({ definition: { id: envelope.agentId, input: PersistedJson } }, envelope.input, {
          threadId: envelope.threadId,
          principal: envelope.deliveryPrincipal,
          idempotencyKey: envelope.admissionKey,
          definitions: envelope.definitions,
          ...(envelope.workerAdmission === undefined
            ? {}
            : { workerAdmission: envelope.workerAdmission }),
          ...(envelope.messageAdmission === undefined
            ? {}
            : { messageAdmission: envelope.messageAdmission }),
        }),
    }),
  );

  const updateRuntime = yield* makeAgentUpdateRuntime({
    deploymentId: config.deploymentId,
    producerId: config.producerId,
  }).pipe(
    Effect.provideService(WorkerRuntime, workerRuntime),
    Effect.provideService(ThreadStore, progressStore),
  );

  return DurableAgentRuntime.of({
    bindingRegistryKey: yield* withCrypto(
      digestJson({
        selection: bindingSelection?.key ?? null,
        bindings: (yield* Effect.forEach(registeredBindings, (binding) =>
          withCrypto(
            digestJson({
              agentId: binding.agentId,
              agent: binding.digests.agent,
              model: binding.digests.model,
              tools: binding.digests.tools,
              replay:
                binding.digests.replay === undefined
                  ? null
                  : {
                      agent: binding.digests.replay.agent,
                      agentBehavior: binding.digests.replay.agentBehavior ?? null,
                      tools: binding.digests.replay.tools,
                    },
            }),
          ),
        ).pipe(Effect.orDie)).sort(),
      }),
    ).pipe(Effect.orDie),
    workerHost: workerRuntime.acquire,
    messagingHost: messagingRuntime.acquire,
    submitRegistered,
    settlementRecord,
    submit,
    submissionStatus,
    inspectSubmissionStatus: readSubmissionStatus,
    awaitSettlement,
    awaitSettlementRecord,
    awaitProgress,
    observe,
    abort,
    resolveUnknown,
    resolveApproval,
    explain,
    explainThread,
    verify: verifyImpl,
    retry: retryImpl,
    wake: wakeImpl,
    scanObligations: scanObligationsImpl,
    processThread: processThreadImpl,
    processThreadResolved: processThreadResolvedImpl,
    processThreadHead: processThreadHeadImpl,
    runResolvedWorker: runResolvedWorkerImpl,
    runRecovery,
    recoverSubmission,
    discoverWork,
    discoverWorkThreads,
    rebuildWorkIndex,
    recoverWork,
  });
});

/**
 * Durable Agent Runtime coordinator (deployment class DN; D1/D2). It coordinates the
 * SubmissionLedger, ThreadStore, and WakeScheduler ports so that once `submit` returns a
 * Receipt, the Submission settles exactly once (DUR-001/DUR-002) while every external effect
 * remains at-least-once (DUR-003) — this runtime never claims exactly-once side effects.
 *
 * - `submit(agent, input, options)` — durable admission → Thread materialization →
 *   `ThreadCreated` → readiness → Receipt, with failpoints between the steps; a retry with
 *   the same (thread, principal, idempotencyKey) resumes and returns the same Receipt.
 * - `awaitSettlement(receipt)` — authorized once before the first ledger read, for the lifetime
 *   of this wait; interrupting it detaches the caller only and never cancels accepted work.
 * - `observe(receipt, {after})` — canonical record observation from a stored offset.
 * - `abort(command)` — authorized before ledger access; durable idempotent abort intent; inactive work settles aborted through
 *   recovery, an active worker makes the command canonical before interrupting its Run (§13),
 *   settled work fails with `SettlementConflict` (DUR-012). A `joined` Submission fails with a
 *   typed `JoinedToHost` conflict carrying the host identity — it settles with its host, so the
 *   abort target is the host; aborting a `joining` Submission records the intent, honored only
 *   if the host has not consumed the input (revert-then-abort, plan §2.5).
 * - `resolveUnknown(command)` — the authorized DUR-017 resolution path for Unknown Outcomes:
 *   the durable intent is idempotent per (submission, tool call) and conflicts typed on
 *   divergence; the canonical resolution records are applied by recovery or the next Attempt,
 *   and the lane wakes once every marked call is covered.
 * - `resolveApproval(command)` — the durable approval decision path (plan §2.6): the intent is
 *   idempotent per (submission, tool call) with a typed `ApprovalConflict` on divergence; once
 *   every pending call of the suspension reason is decided the lane wakes
 *   (`suspended → input-applied`) and the next Attempt resumes the declared batch without model
 *   re-invocation, appending the canonical `ToolApprovalDecided` before honoring the decision.
 * - `processThread(agent, threadId)` — drain one lane: fenced FIFO-head claims,
 *   canonical input apply, split response/results Turn commits (plan §2.1),
 *   reconcile-then-mark for open ordinary Tool Calls (DUR-009, never an automatic replay),
 *   declared-batch resume without model re-invocation (§15), and terminalization. An active
 *   host Run claims the contiguous ready prefix of later queued Submissions at every safe Turn
 *   seam (Joining/Joined, plan §2.5): the queued input becomes canonical (`input:{sid}`) before
 *   the next model request, reattaches through the prompt-coverage rule after a crash, and the
 *   joined Submissions settle with the host outcome (DUR-002/DUR-016).
 * - `processThreadResolved(threadId)` / `runResolvedWorker` — drain or continuously process
 *   lanes using registrations owned by the runtime Layer: each stable `agentId` selects
 *   one current Binding. Unfinished operation contracts gate handler execution independently
 *   of immutable admission evidence; missing bindings release the claim with a typed refusal.
 * - `discoverWork()` / `discoverWorkThreads()` — enumerate native metadata without granting
 *   execution authority. Missing or incomplete indexes fail explicitly; `rebuildWorkIndex()`
 *   reconstructs them in bounded, resumable pages independently of ordinary recovery.
 * - `runRecovery()` — repair at most 32 native owners per pass, returning a cursor for remaining
 *   work. Follow that cursor to finish the sweep. Selected admissions use `classifyRecovery`;
 *   ready input and model work remain `deferred` for a fenced worker claim. Terminal Runs can
 *   retain factual tool closures, frozen deliveries, child accounting and worker acknowledgements.
 *   These repairs use original evidence and never invoke a Tool handler or reopen a settled Run.
 * - `explain`/`explainThread`, `verify`, `retry`, `wake`, `scanObligations` — the P7
 *   administrative operations (plan §3) over the same two ports, identical on DN and DC.
 *   `explain` and `verify` are strictly read-only; `retry` re-drives exactly one classified
 *   repair with mandatory author/reason audit and typed refusals; `scanObligations` is the
 *   scan-based DUR-017/OPS-001 obligation surface. Every one of them (plus `observe`,
 *   `resolveUnknown`, `resolveApproval`) consults the `OperationAuthorizer` reference
 *   fail-closed — the default Layer preserves the service-possession behavior, and a
 *   host-supplied authorizer turns denials into the typed `OperationDenied`.
 */
export class DurableAgentRuntime extends Context.Service<
  DurableAgentRuntime,
  {
    /** Stable registration identity for hosts parking unavailable bindings; never an admission authority. */
    readonly bindingRegistryKey: string;
    /** Acquire an authenticated source Thread facet; its References confer no authority. */
    readonly workerHost: (request: {
      readonly sourceThreadId: ThreadId;
      readonly principal: Principal;
      /** Exact retained owner input for captured source policy; never selects the latest input. */
      readonly sourceSubmissionId?: SubmissionId;
    }) => Effect.Effect<SubagentHost["Service"], WorkerError>;
    readonly messagingHost: (request: {
      readonly sourceThreadId: ThreadId;
      readonly principal: Principal;
    }) => Effect.Effect<MessagingHost["Service"], MessagingError>;
    /** Admit the exact registered Definition instance; reject missing, ambiguous, or different definitions. */
    readonly submitRegistered: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: Omit<DurableSubmitOptions, "definitions">,
    ) => Effect.Effect<
      Receipt,
      DurableSubmitFailure | BindingUnavailable,
      InputSchema["EncodingServices"]
    >;
    /** Authorized exact canonical terminal record, including the encoded output. Reject pending work. */
    readonly settlementRecord: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionSettledRecord, DurableAwaitFailure>;
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<Receipt, DurableSubmitFailure, InputSchema["EncodingServices"]>;
    readonly awaitSettlement: (receipt: Receipt) => Effect.Effect<Settlement, DurableAwaitFailure>;
    /** Wait under settlement authority for the exact canonical terminal record, including encoded output. */
    readonly awaitSettlementRecord: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionSettledRecord, DurableAwaitFailure>;
    /** Authorized, nonblocking read. Only the durable Settlement marks work complete. */
    readonly submissionStatus: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionStatus, DurableAwaitFailure>;
    /**
     * Trusted worker inspection under the same runtime authority as processThread/recoverSubmission.
     * Caller-facing observations must use submissionStatus or awaitSettlement for authorization.
     */
    readonly inspectSubmissionStatus: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionStatus, DurableAwaitFailure>;
    /**
     * Wait for an already-committed record or one incarnation-local progress hint after
     * `afterSequence`. Canonical records remain authoritative: callers re-read after return.
     */
    readonly awaitProgress: (
      threadId: ThreadId,
      afterSequence: CanonicalSequence,
    ) => Effect.Effect<void, DurableProgressFailure>;
    readonly observe: (
      receipt: Receipt,
      options?: DurableObserveOptions,
    ) => Stream.Stream<
      CanonicalRecordEnvelope,
      ThreadStoreError | ThreadNotMaterialized | OperationDenied
    >;
    readonly abort: (command: AbortCommand) => Effect.Effect<AbortIntent, DurableAbortFailure>;
    readonly resolveUnknown: (
      command: UnknownResolutionCommand,
    ) => Effect.Effect<UnknownResolutionIntent, DurableResolveFailure | OperationDenied>;
    readonly resolveApproval: (
      command: ApprovalDecisionCommand,
    ) => Effect.Effect<ApprovalDecisionIntent, DurableApprovalFailure | OperationDenied>;
    /**
     * Read-only recovery explanation of one Submission (P7 plan §3): snapshot + tolerant
     * canonical read + the pure classifier, packaged with the decision's operator meaning and
     * predicted disposition. Performs ZERO writes — the canonical log and every ledger row are
     * byte-identical before and after.
     */
    readonly explain: (
      submissionId: SubmissionId,
    ) => Effect.Effect<RecoveryExplanation, DurableExplainFailure>;
    /** `explain` for every nonterminal lane member of one Thread, in queue order. */
    readonly explainThread: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<RecoveryExplanation>, DurableExplainFailure>;
    /**
     * Read-only integrity verification of one Thread (P7 plan §3): Schema round-trips,
     * record-identity uniqueness, sequence contiguity, FIFO input/settlement order,
     * ledger-terminal vs canonical-settlement agreement (DUR-015), and checkpoint binding —
     * typed per-check results, never a repair. Current exports carry the batch identities and
     * original record wire needed to recompute the digest chain. Older exports without those
     * identities report `skipped`; adapter-level `verifyOnOpen` audits storage directly.
     */
    readonly verify: (threadId: ThreadId) => Effect.Effect<IntegrityReport, DurableVerifyFailure>;
    /**
     * Safe re-drive of one Submission's recovery decision with mandatory author/reason audit
     * (SEC-011): executes exactly the repair the classifier names; owned repair attempts
     * append a best-effort `RepairAnnotated` with their actual claim epoch (DUR-013). Typed `RetryRefused`
     * for settled work and for lanes owned by the resolveUnknown/resolveApproval paths.
     */
    readonly retry: (command: RetryCommand) => Effect.Effect<RecoveryReport, DurableRetryFailure>;
    /** The documented operator liveness nudge: a droppable wake hint for one lane. */
    readonly wake: (threadId: ThreadId) => Effect.Effect<void, OperationDenied>;
    /**
     * Scan-based DUR-017/OPS-001 obligation report: every nonterminal Submission with what it
     * is visibly blocked on, its age, and a threshold-classified severity. Never a daemon —
     * hosts run it periodically and own the alert loop (OPS-002).
     */
    readonly scanObligations: (
      thresholds: ObligationThresholds,
    ) => Effect.Effect<ObligationReport, DurableObligationFailure>;
    /** Drain a Thread's claimable heads under the host's WakeScheduler processing boundary. */
    readonly processThread: <
      InputSchema extends Schema.Top,
      OutputSchema extends Schema.Top,
      Instructions,
      Tools extends Record<string, Tool.Any>,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
      InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
      RunDispositionValue extends
        | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
        | undefined = undefined,
      InputPromptValue extends
        | InputPromptSource<InputSchema["Type"], unknown, unknown>
        | undefined = undefined,
      UpdatesSchema extends Schema.Top | undefined = undefined,
    >(
      agent: RuntimeBinding<
        InputSchema,
        OutputSchema,
        Instructions,
        Tools,
        Provider,
        ModelProvides,
        ModelRequires,
        InstructionError,
        InstructionRequirements,
        RunDispositionValue,
        InputPromptValue,
        UpdatesSchema
      >,
      threadId: ThreadId,
    ) => Effect.Effect<
      ReadonlyArray<Settlement>,
      DurableWorkerFailure | DurableBindingFailure,
      DurableWorkerRequirements<
        RuntimeBinding<
          InputSchema,
          OutputSchema,
          Instructions,
          Tools,
          Provider,
          ModelProvides,
          ModelRequires,
          InstructionError,
          InstructionRequirements,
          RunDispositionValue,
          InputPromptValue,
          UpdatesSchema
        >,
        InstructionRequirements
      >
    >;
    /** Drain a Thread using registered bindings under the same host processing boundary. */
    readonly processThreadResolved: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<Settlement>, DurableWorkerFailure | DurableBindingFailure>;
    /**
     * Advance the FIFO head under the host's WakeScheduler processing boundary, closing its
     * Attempt resources before returning. With an opted-in SubmissionScheduling policy, a complete
     * Turn may hand off to the next same-Agent input;
     * the returned Settlement identifies the Submission actually completed. Each Attempt closes
     * before the next is claimed, and deferred Runs retain their original obligations. A vacant,
     * owned, unknown, or suspended head returns None and leaves accepted work pending.
     * Ready input may join this Run through the existing Turn seams and settle with its head.
     * Interruption ends only this Attempt; ownership cleanup uses its latest renewed token.
     * A trusted host may supply `yieldAfter` to return None before the next Turn's context
     * preparation, committing any completed Turn first. This releases ownership without settling or resetting the durable
     * Run deadline. It is a cooperative scheduling deadline, not a hard interruption timeout.
     */
    readonly processThreadHead: (
      threadId: ThreadId,
      options?: { readonly yieldAfter?: DateTime.Utc },
    ) => Effect.Effect<Option.Option<Settlement>, DurableWorkerFailure | DurableBindingFailure>;
    readonly runResolvedWorker: Effect.Effect<void, DurableWorkerFailure | DurableBindingFailure>;
    /**
     * Recover each pending Thread independently. History/child failures, defects and the
     * configured timeout return one RecoveryBlocked per Thread; hosts must retain visibility
     * outside execution history and exclude those Threads from claims. Ledger scan failures,
     * owner interruption and injected crash boundaries still fail the whole sweep.
     * Optional Thread selection lets a host recover its selected dispatch lane independently
     * of old cleanup. Unselected Threads are neither read nor reported by this call.
     */
    readonly runRecovery: (
      options?: RecoverySweepOptions,
    ) => Effect.Effect<RecoverySweepResult, DurableWorkerFailure>;
    /** Apply one recovery decision; untouched ready input is deferred to its worker claim. */
    readonly recoverSubmission: (
      submissionId: SubmissionId,
    ) => Effect.Effect<RecoveryReport, DurableWorkerFailure>;
    /** Trusted host inventory; discovery grants no execution or disclosure authority. */
    readonly discoverWork: (
      request: ThreadWorkRequest,
    ) => Effect.Effect<ThreadWorkPage, ThreadStoreError | WorkDiscoveryUnavailable>;
    readonly discoverWorkThreads: (
      request: WorkThreadsRequest,
    ) => Effect.Effect<WorkThreadsPage, ThreadStoreError>;
    /** Explicit, resumable maintenance. Missing indexes never trigger implicit history reads. */
    readonly rebuildWorkIndex: (
      request: WorkIndexRebuildRequest,
    ) => Effect.Effect<WorkIndexProgress, ThreadStoreError | WorkDiscoveryUnavailable>;
    readonly recoverWork: (
      request: WorkRecoveryRequest,
    ) => Effect.Effect<WorkRecoveryReport, DurableWorkerFailure>;
  }
>()("@effect-agent/thread/DurableAgentRuntime") {
  /**
   * Resolve typed registrations and capture their services once in the runtime's Layer Scope.
   * Every claimed head must select exactly one current registered Definition. Worker calls
   * cannot replace these registrations or their captured services. Tool authorization is required.
   */
  static layerRegistered<const Entries extends ReadonlyArray<AgentRegistration>>(
    registrations: Entries,
  ) {
    return Layer.effect(DurableAgentRuntime)(
      Effect.flatMap(compileRegistrations(registrations), make),
    );
  }

  /** Construct from precompiled registrations whose captured resources belong to the caller's Scope. */
  static layerWithBindings(bindings: ReadonlyArray<ResolvedBinding>) {
    return Layer.effect(DurableAgentRuntime)(make(bindings));
  }

  /** Captures optional context preparation; Tool authorization remains required in `R`. */
  static readonly layerWithServices: Layer.Layer<
    DurableAgentRuntime,
    never,
    | SubmissionLedger
    | ThreadStore
    | RunStorage
    | WakeScheduler
    | DurableRuntimeFailpoint
    | DurableRuntimeConfig
    | ToolReconciler
    | RunToolAuthorization
    | Crypto.Crypto
  > = DurableAgentRuntime.layerWithBindings([]);

  /** Compatible defaults: no host prompt transformation and allow-all Tool authorization. */
  static readonly layer: Layer.Layer<
    DurableAgentRuntime,
    never,
    | SubmissionLedger
    | ThreadStore
    | RunStorage
    | WakeScheduler
    | DurableRuntimeFailpoint
    | DurableRuntimeConfig
    | ToolReconciler
    | Crypto.Crypto
  > = DurableAgentRuntime.layerWithServices.pipe(
    Layer.provide([RunContextPreparationPassthrough, RunToolAuthorization.allowAll]),
  );
}
