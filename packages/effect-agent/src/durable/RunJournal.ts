import {
  Brand,
  type Crypto,
  Effect,
  Predicate,
  Result,
  Schema,
  Stream,
  type DateTime,
} from "effect";
import { Prompt } from "effect/ai";

import {
  type RunId,
  type TurnId,
  ThreadId,
  ToolCallId,
  type SubmissionId,
} from "../core/Identifiers.ts";
import { copyJson } from "../core/internal/json.ts";
import { summarizeModelUsageResult } from "../core/internal/usage.ts";
import { type ExhaustedLimit } from "../core/RunEvent.ts";
import { RunPolicyUsage } from "../core/RunPolicyUsage.ts";
import { type Selection, type Snapshot } from "../core/ToolExposure.ts";
import type { ToolParameterRejection } from "../core/ToolResult.ts";
import { ModelCallUsage, type RunUsageSummary } from "../core/Usage.ts";
import type { WorkerRef } from "../core/Worker.ts";
import {
  CLEARED_TOOL_RESULT,
  COMPACTION_SUMMARY_PREFIX,
  contextWindowId,
  contextWindowMessage,
} from "../engine/Compaction.ts";
import type { RunTurnToolResult } from "../engine/RunOptions.ts";
import { digestJson, type DigestError } from "./Digest.ts";
import {
  makeJournalMetadata,
  toolCallSettledRecordId,
  type JournalMetadata,
  type JournalRecordEnvelope,
} from "./internal/journal-metadata.ts";
import {
  type BatchId,
  type RecordId,
  type CompactionCreated,
  CanonicalBatch,
  ModelResponseRecorded,
  MAX_PERSISTED_JSON_BYTES,
  PersistedJson,
  RecordEnvelope,
  RunCompleted,
  ToolCallSettled,
  type CanonicalRecordEnvelope,
  CanonicalSequence,
  type DeploymentId,
  type ProducerId,
} from "./Records.ts";
import { IdempotencyKey } from "./SubmissionLedger.ts";

/** A canonical record could not be projected into Run/Prompt state. */
export class RunJournalError extends Schema.TaggedError<RunJournalError>()("RunJournalError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const journalError = (message: string, cause?: unknown): RunJournalError =>
  cause === undefined
    ? RunJournalError.make({ message })
    : RunJournalError.make({ message, cause });

// These constructors produce nonempty strings from fixed prefixes or validated nonempty IDs.
// Keep the canonical Schemas as the validation boundary for incoming and persisted values.
const makeRunId = Brand.nominal<RunId>();
const makeTurnId = Brand.nominal<TurnId>();
const makeBatchId = Brand.nominal<BatchId>();
const makeRecordId = Brand.nominal<RecordId>();

/**
 * Deterministic Run identity pinned per Submission (plan §Coordinator flow). Every Attempt of one
 * Submission shares this Run identity, so canonical per-Turn records from different Attempts
 * belong to one logical Run.
 */
export const runIdForSubmission = (submissionId: SubmissionId): RunId =>
  makeRunId(`run:${submissionId}`);

/** Stable canonical clock identity shared by every Attempt of one Run. */
export const runStartedRecordId = (runId: RunId): RecordId => makeRecordId(`run-start:${runId}`);

export const runStartedBatchId = (runId: RunId): BatchId => makeBatchId(`run-start:${runId}`);

export const runDurationRecordId = (runId: RunId): RecordId =>
  makeRecordId(`run-duration:${runId}`);

export const runDurationBatchId = (runId: RunId): BatchId => makeBatchId(`run-duration:${runId}`);

/** Deterministic Turn identity: Attempt-independent for one (Run, canonical turn) pair. */
export const turnIdForRun = (runId: RunId, turn: number): TurnId =>
  makeTurnId(`turn:${runId}:${turn}`);

/** Deterministic batch identity of a response committed atomically with its closed Turn outcomes. */
export const turnBatchId = (runId: RunId, turn: number): BatchId =>
  makeBatchId(`turn:${runId}:${turn}`);

/**
 * Deterministic batch identity of a tool-declaring Turn's RESPONSE commit (plan §2.1 commit 1):
 * the assistant response plus pending steering becomes canonical before approval preflight and
 * dispatch fencing, or when readonly execution first requires a persisted call-scoped capability.
 * Unsettled declarations conservatively record possible execution.
 */
export const turnResponseBatchId = (runId: RunId, turn: number): BatchId =>
  makeBatchId(`turn-response:${runId}:${turn}`);

/**
 * Deterministic batch identity of a tool-declaring Turn's RESULTS commit (plan §2.1 commit 5):
 * every `ToolCallSettled` record of the Turn, in declaration order, model-visible atomically.
 */
export const turnResultsBatchId = (runId: RunId, turn: number): BatchId =>
  makeBatchId(`turn-results:${runId}:${turn}`);

/**
 * Deterministic per-call late-settle batch identity used by the resolution path when one
 * recovered/resolved call settles outside its Turn's results batch. Record identity
 * (`tool-settled:{runId}:{turn}:{toolCallId}`) dedupes double-settles across both paths.
 */
export const toolCallResultBatchId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): BatchId => makeBatchId(`turn-results:${runId}:${turn}:${toolCallId}`);

/**
 * Deterministic identity of one pre-Turn compaction record (RUN-026,
 * RUN-026). Keyed by Run, Turn, and kind — the engine performs at most one
 * threshold compaction per Turn plus at most one overflow-forced summarize,
 * so a superseding Attempt that re-decides the same compaction replays the
 * batch identity instead of duplicating the record.
 */
export const compactionRecordId = (
  runId: RunId,
  turn: number,
  kind: "clear-tool-results" | "summarize" | "rollover",
): RecordId => makeRecordId(`compaction:${runId}:${turn}:${kind}`);

/** Deterministic batch identity of one compaction append (same string as its record id). */
export const compactionBatchId = (
  runId: RunId,
  turn: number,
  kind: "clear-tool-results" | "summarize" | "rollover",
): BatchId => makeBatchId(`compaction:${runId}:${turn}:${kind}`);

/** Deterministic canonical record identity of one Turn's `ModelResponseRecorded` record. */
export const modelResponseRecordId = (runId: RunId, turn: number): RecordId =>
  makeRecordId(`model-response:${runId}:${turn}`);

/** Terminal Tool completion marker committed atomically with its settled Tool result. */
export const runCompletedRecordId = (runId: RunId): RecordId =>
  makeRecordId(`run-completed:${runId}`);

/** Native source reservation locator, including the first admission of a continuing worker. */
export const workerInputRecordId = (messageId: IdempotencyKey): RecordId =>
  makeRecordId(`worker-input:${messageId}`);

/** Source-owned first reservation; native worker identity is minted from its first message. */
export const firstWorkerInputRecordId = (worker: WorkerRef): RecordId =>
  makeRecordId(`worker-input:${worker.threadId}`);

export const workerOriginRecordId = (threadId: ThreadId): RecordId =>
  makeRecordId(`worker-origin:${threadId}`);

export const workerReportRecordId = (messageId: IdempotencyKey): RecordId =>
  makeRecordId(`worker-report:${messageId}`);

export const peerMessageRecordId = (messageId: IdempotencyKey): RecordId => makeRecordId(messageId);

/** Same immutable tuple used by native update admission; callers never parse its hash. */
export const agentUpdateRecordId = Effect.fnUntraced(function* (
  threadId: ThreadId,
  runId: RunId,
  updateId: IdempotencyKey,
) {
  return makeRecordId(`agent-update:${yield* digestJson([threadId, runId, updateId])}`);
});

/** Deterministic canonical record identity of one Turn's `ToolCallSettled` record. */
export { toolCallSettledRecordId };

/** Deterministic batch identity of one Turn's `ToolCallUnknown` marking append. */
export const markUnknownBatchId = (submissionId: SubmissionId, turn: number): BatchId =>
  makeBatchId(`mark-unknown:${submissionId}:${turn}`);

/** Deterministic canonical record identity of one Tool Call's `ToolCallUnknown` record. */
export const toolCallUnknownRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => makeRecordId(`tool-unknown:${runId}:${turn}:${toolCallId}`);

/** Deterministic batch identity of one Tool Call's resolution append (DUR-017). */
export const toolCallResolutionBatchId = (
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
): BatchId => makeBatchId(`resolve:${submissionId}:${toolCallId}`);

/** Deterministic canonical record identity of one Tool Call's `ToolCallResolved` record. */
export const toolCallResolvedRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => makeRecordId(`tool-resolved:${runId}:${turn}:${toolCallId}`);

/**
 * Deterministic identity of one accepted Durable Step result. The one-record batch reuses the
 * SAME string, so batch idempotency plus the epoch fence realize the durability §11
 * racing-writers rule: only the fenced winner's record commits; the loser replays it.
 * A versioned JSON tuple keeps separator characters in each identity component distinct.
 * Replay derives this key from the structured payload, including records with legacy IDs.
 */
export const toolStepSettledRecordId = (
  runId: RunId,
  toolCallId: ToolCallId,
  stepName: string,
): RecordId => makeRecordId(JSON.stringify(["step@2", runId, toolCallId, stepName]));

/** Deterministic batch identity of one Durable Step commit (same string as its record id). */
export const toolStepSettledBatchId = (
  runId: RunId,
  toolCallId: ToolCallId,
  stepName: string,
): BatchId => makeBatchId(toolStepSettledRecordId(runId, toolCallId, stepName));

/** Deterministic batch identity of one Turn's canonical approval-request append (plan §2.6). */
export const turnApprovalsBatchId = (runId: RunId, turn: number): BatchId =>
  makeBatchId(`turn-approvals:${runId}:${turn}`);

/** Deterministic canonical record identity of one Tool Call's `ToolApprovalRequested` record. */
export const toolApprovalRequestRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => makeRecordId(`approval-request:${runId}:${turn}:${toolCallId}`);

/** Deterministic batch identity of one Tool Call's canonical approval-decision append. */
export const approvalDecisionBatchId = (
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
): BatchId => makeBatchId(`approval-decision:${submissionId}:${toolCallId}`);

/** Deterministic canonical record identity of one Tool Call's `ToolApprovalDecided` record. */
export const toolApprovalDecisionRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => makeRecordId(`approval-decision:${runId}:${turn}:${toolCallId}`);

/**
 * Deterministic identity of a conservative `ModelResponseInterrupted` audit, keyed by the
 * preceding fencing generation. Retirement and repair can also advance generations; the key
 * does not identify an exact missing model call. The one-record batch reuses the same string.
 */
export const modelResponseInterruptedRecordId = (runId: RunId, supersededEpoch: number): RecordId =>
  makeRecordId(`interrupted:${runId}:${supersededEpoch}`);

/** Deterministic batch identity of one `ModelResponseInterrupted` append (same string). */
export const modelResponseInterruptedBatchId = (runId: RunId, supersededEpoch: number): BatchId =>
  makeBatchId(`interrupted:${runId}:${supersededEpoch}`);

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

/**
 * Deterministic canonical record identity of one parent Tool Call's `SubagentRequested` record.
 * The one-record batch reuses the same string, so batch
 * idempotency plus the parent epoch fence make the request append exactly-once-canonical.
 */
export const subagentRequestedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  makeRecordId(`subagent-requested:${runId}:${toolCallId}`);

/** Deterministic batch identity of one `SubagentRequested` append (same string). */
export const subagentRequestedBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  makeBatchId(`subagent-requested:${runId}:${toolCallId}`);

/**
 * Deterministic canonical record identity of one parent Tool Call's `SubagentStarted` record.
 * Recovery's start-link repair appends the EXACT same record
 * under this identity, so a raced repair replays instead of duplicating (SUB-016/SUB-017).
 */
export const subagentStartedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  makeRecordId(`subagent-started:${runId}:${toolCallId}`);

/** Deterministic batch identity of one `SubagentStarted` append (same string). */
export const subagentStartedBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  makeBatchId(`subagent-started:${runId}:${toolCallId}`);

/**
 * Deterministic batch identity of one parent Tool Call's atomic settlement join: the
 * `SubagentJoined` record plus the parent's `ToolCallSettled` record (its existing
 * `tool-settled:{runId}:{turn}:{toolCallId}` identity) commit as ONE canonical batch (SUB-019).
 */
export const subagentJoinBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  makeBatchId(`subagent-join:${runId}:${toolCallId}`);

/** Deterministic canonical record identity of one parent Tool Call's `SubagentJoined` record. */
export const subagentJoinedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  makeRecordId(`subagent-joined:${runId}:${toolCallId}`);

/**
 * Deterministic identity of one child Thread's `SubagentLineageRecorded` record.
 * Its own single-record batch reuses the same string so the generic
 * `thread-created:{cid}` batch identity is never contradicted.
 */
export const subagentLineageRecordId = (threadId: ThreadId): RecordId =>
  makeRecordId(`subagent-lineage:${threadId}`);

/** Deterministic batch identity of one `SubagentLineageRecorded` append (same string). */
export const subagentLineageBatchId = (threadId: ThreadId): BatchId =>
  makeBatchId(`subagent-lineage:${threadId}`);

/**
 * Deterministic intended child Thread identity: the
 * parent Submission and Tool Call pair addresses exactly one child Thread, so a replayed
 * establishment converges on the one existing child (SUB-016).
 */
export const childThreadIdFor = (
  parentSubmissionId: SubmissionId,
  toolCallId: ToolCallId,
): ThreadId => decodeThreadId(`subagent:${parentSubmissionId}:${toolCallId}`);

/**
 * Deterministic child admission idempotency key: scoped to
 * the parent Run and Tool Call identity, so duplicate admission attempts resolve through the
 * ledger's idempotency contract to one child Receipt (SUB-016, SUB-031). The ledger key is
 * bounded (256); coordinator-minted Run and Tool Call identities stay far below that bound.
 */
export const childIdempotencyKeyFor = (
  parentRunId: RunId,
  toolCallId: ToolCallId,
): IdempotencyKey => decodeIdempotencyKey(`subagent:${parentRunId}:${toolCallId}`);

const decodePersistedPrompt = Schema.decodeUnknownResult(Prompt.Prompt);

// Canonical payloads are immutable. Weak keys let retained storage rows own the lifetime
// while repeated projections of an advancing tail reuse their validated Prompt values.
const decodedMessages = new WeakMap<object, Prompt.Prompt>();

const decodePromptMessages = (
  messages: PersistedJson | Prompt.Prompt,
): Result.Result<Prompt.Prompt, RunJournalError> => {
  if (Prompt.isPrompt(messages)) return Result.succeed(messages);
  const cached = Predicate.isObject(messages) ? decodedMessages.get(messages) : undefined;

  if (cached !== undefined) return Result.succeed(cached);
  const decoded = decodePersistedPrompt(messages);

  if (Result.isFailure(decoded))
    return Result.fail(
      journalError("Canonical messages are not Schema-encoded Prompt messages", decoded.failure),
    );
  if (Predicate.isObject(messages)) decodedMessages.set(messages, decoded.success);

  return Result.succeed(decoded.success);
};

interface PendingSettledTool {
  readonly record: Pick<ToolCallSettled, "toolCallId" | "toolName" | "result" | "isFailure">;
  /** RUN-026: a `clear-tool-results` compaction covers this record's sequence. */
  readonly cleared: boolean;
}

const toolMessageFromSettled = (settled: ReadonlyArray<PendingSettledTool>): Prompt.Message =>
  Prompt.makeMessage("tool", {
    content: settled.map(({ record, cleared }) =>
      Prompt.makePart("tool-result", {
        id: record.toolCallId,
        name: record.toolName,
        result: cleared ? CLEARED_TOOL_RESULT : record.result,
        isFailure: record.isFailure,
        providerExecuted: false,
      }),
    ),
  });

/**
 * Pure canonical projection of one Run's durable execution state.
 *
 * Prompt reconstruction contract (D8):
 *
 * - `ModelResponseRecorded.messages` carries the Schema-encoded Prompt messages the Turn appended
 *   to the model-visible prompt — for the Run's FIRST committed Turn that includes the evaluated
 *   instruction and user-input messages, for later Turns only the assistant response messages.
 *   Tool messages are excluded from `messages`.
 * - `ToolCallSettled` records (committed in the same per-Turn batch, in declaration order)
 *   deterministically rebuild the Turn's single Tool message.
 * - `UserInputRecorded` records input, with a Submission identity only for durable admission. Its
 *   Prompt-visible form (instructions + user message) becomes canonical inside the owning Run's
 *   first `ModelResponseRecorded`. The projection consumes it only for Run correlation.
 * - Immediate retained history uses `ModelCompleted.history` as a full retained model-context
 *   snapshot. Its `messages` separately preserve the successful Run's exact source contribution
 *   for recall. It has no resumable Turn state.
 */
/** Cumulative committed usage of the projected Run (RUN-023 resume re-seed). */
export interface RunJournalUsage {
  readonly modelCalls: number;
  /** Known accounting gaps retained across Attempts without changing numeric usage totals. */
  readonly unobservedModelCalls?: number | undefined;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly lastInputTokens: number;
  readonly lastOutputTokens: number;
  /** Cumulative persisted spend of the projected Run's committed calls (RUN-023). */
  readonly costMicrousd: number;
  /** Canonical per-call detail used for settlement aggregation and recovery. */
  readonly modelUsage: ReadonlyArray<ModelCallUsage>;
  /** Detailed usage of retired records, grouped without retaining per-call payloads. */
  readonly summarizedModelUsage?: RunUsageSummary | undefined;
}

export interface RunJournalProjection {
  readonly toolSelection?: Selection | undefined;
  readonly policyUsage: RunPolicyUsage;
  /** Canonical projection for the requested Run; may end at its resumable Tool declaration. */
  readonly prompt: Prompt.Prompt;
  /** Prior-Run history with model-only unknown results closing incomplete application calls. */
  readonly historyBefore: Prompt.Prompt;
  /** Inclusive range after effective summarize/rollover coverage; earlier used facts stay retained. */
  readonly historyFrom: CanonicalSequence;
  /** Number of canonical Turns already committed for the projected Run. */
  readonly committedTurns: number;
  /** Summed per-call usage of the projected Run's committed responses; zeros for records predating usage capture. */
  readonly usage: RunJournalUsage;
  /** Latest committed context window identity, retained across ownership changes. */
  readonly contextWindowId?: string | undefined;
  /** Canonical evaluated instructions and initial input for this Run, independent of the view. */
  readonly protectedContext?: Prompt.Prompt | undefined;
  /** Last successful singleton Tool awaiting possible context-control interpretation by the engine. */
  readonly pendingContextToolCallId?: string | undefined;
}

interface ProjectedResponseUsage {
  readonly modelCalls: number;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly costMicrousd: number;
  readonly modelUsage: ReadonlyArray<ModelCallUsage>;
}

const projectedResponseUsage = (
  response: ModelResponseRecorded,
): Result.Result<ProjectedResponseUsage, RunJournalError> => {
  const calls = response.modelUsage;

  if (calls === undefined) {
    return Result.succeed({
      modelCalls: 1,
      inputTokens: response.inputTokens ?? 0,
      outputTokens: response.outputTokens ?? 0,
      costMicrousd: response.costMicrousd ?? 0,
      modelUsage: [],
    });
  }
  if (calls.length === 0)
    return Result.fail(journalError("A canonical modelUsage list must not be empty"));
  const summarized = summarizeModelUsageResult(calls);

  if (Result.isFailure(summarized))
    return Result.fail(
      journalError("Canonical model usage exceeds accounting bounds", summarized.failure),
    );
  const summary = summarized.success;

  if (
    (response.inputTokens !== undefined && response.inputTokens !== summary.inputTokens.total) ||
    (response.outputTokens !== undefined && response.outputTokens !== summary.outputTokens.total) ||
    (response.costMicrousd !== undefined && response.costMicrousd !== summary.costMicrousd)
  )
    return Result.fail(journalError("Canonical detailed and aggregate model usage disagree"));

  return Result.succeed({
    modelCalls: summary.modelCalls,
    inputTokens: summary.inputTokens.total,
    outputTokens: summary.outputTokens.total,
    costMicrousd: summary.costMicrousd,
    modelUsage: calls,
  });
};

const decodeUsageNatural = Schema.decodeResult(Schema.Natural);

const addProjectedUsage = (
  field: string,
  left: number,
  right: number,
): number | RunJournalError => {
  const total = left + right;

  if (Number.isSafeInteger(total) && total >= 0) return total;
  const invalid = decodeUsageNatural(total);

  return journalError(
    `Canonical projected usage exceeds safe-integer bounds at ${field}`,
    Result.isFailure(invalid) ? invalid.failure : undefined,
  );
};

interface FoldState {
  readonly all: Array<Prompt.Message>;
  readonly before: Array<Prompt.Message>;
  readonly pendingTools: Array<PendingSettledTool>;
  readonly pendingToolsForRun: boolean;
  readonly committedTurns: number;
}

const decodeToolCallId = Schema.decodeResult(ToolCallId);

const declaredApplicationToolCallIds = (prompt: Prompt.Prompt): ReadonlyArray<string> => {
  const ids: Array<string> = [];

  for (const message of prompt.content) {
    if (message.role !== "assistant") continue;
    for (const part of message.content) {
      if (part.type === "tool-call" && !part.providerExecuted) ids.push(part.id);
    }
  }

  return ids;
};

/**
 * Phase 5 audit tags that are prompt-transparent: they carry durability evidence (approval,
 * unknown marking, resolution, Step results, approvals, interruption) but contribute nothing to
 * the model-visible Prompt, and — unlike the P4 tags — they do NOT split a contiguous
 * `ToolCallSettled` group into separate Tool messages, so a late-settled call's audit records
 * cannot change the replayed prompt shape.
 *
 * The four S2 Subagent lifecycle tags are prompt-transparent for the same reason (spec §5/§11):
 * the parent prompt never carries child transcript or establishment evidence — the joined child
 * result reaches the model exclusively through the paired `ToolCallSettled` record, and the
 * child-log lineage record is not model input.
 */
const PROMPT_TRANSPARENT_TAGS: ReadonlySet<string> = new Set([
  "RunPolicyUsageReserved",
  "RunStarted",
  "RunContextRecorded",
  "RunContinuation",
  "RunDurationExhausted",
  "ToolCallUnknown",
  "ToolCallResolved",
  "ToolStepSettled",
  "ToolApprovalRequested",
  "ToolApprovalDecided",
  "ModelResponseInterrupted",
  "ModelCallAborted",
  "SubagentRequested",
  "SubagentStarted",
  "SubagentJoined",
  "SubagentLineageRecorded",
  "WorkerInputRequested",
  "WorkerOriginRecorded",
  "WorkerInputCompleted",
  "WorkerInputRefused",
  "WorkerReportPrepared",
  "AgentUpdateEmitted",
  "WorkerReportRefused",
  "PeerMessagePrepared",
  "SubtreeBudgetReserved",
]);

/**
 * Pure projection: rebuild one Run's resume state from canonical records (DUR-015). Canonical
 * order is authoritative; the fold projects each `ModelResponseRecorded` Turn and the
 * owning Run's resumable incomplete Tool Turn. It flushes each contiguous group of valid `ToolCallSettled` records into one Tool message,
 * exactly mirroring the per-Turn commit shape produced by `turnCanonicalBatch` and the
 * `turnResponseBatch`/`turnResultsBatch` split. The Phase 5 audit tags
 * are skipped transparently, so split-batch commits replay to the same prompt as P4 single-batch
 * commits.
 *
 * An incomplete application Tool turn remains visible while projecting its owning Run so active
 * recovery can resume the declared batch. It is not a valid model-visible Turn boundary for a
 * later Run until model-only unknown results close its missing calls. Those explanatory results
 * are never canonical settlements or evidence for compaction, recovery or accounting. Real
 * results replace them at the original declaration, including results appended after another Run.
 * Prior user intent and assistant text remain visible; prior system instructions do not.
 * Each Run retains the history preceding its start and its own continuation. Other Runs'
 * subsequent Turns cannot replace its input. Compaction covers only its creator's Run view;
 * a projection without an owner retains interleaved exchanges from the complete Thread history.
 */
/** @internal Lightweight canonical boundaries collected without retaining record payloads. */
export interface JournalBoundary {
  readonly sequence: CanonicalSequence;
  readonly tag:
    | "ModelCompleted"
    | "ModelResponseRecorded"
    | "ToolCallSettled"
    | "CompactionCreated";
  readonly promptLength: number;
  /** A declaration without all settled results requires terminal-prior-Run proof for coverage. */
  readonly incomplete?: true | undefined;
  /** The Run terminated after its last response and differs from the projection's owner. */
  readonly terminalPriorRun?: true | undefined;
}

/** Resolved immutable context. Persistence keeps a canonical range and digest, not this Prompt. */
export interface RunJournalContext {
  readonly runId: RunId;
  readonly prompt: Prompt.Prompt;
  readonly priorHistoryLength: number;
  readonly historyFrom: CanonicalSequence;
  readonly boundaries: ReadonlyArray<JournalBoundary>;
  readonly contextWindowId?: string | undefined;
  readonly digest: string;
}

/** Bound resident reduction even when a caller supplies one large stream chunk. */
const foldJournalRecords = <A, E, R>(
  records: Stream.Stream<A, E, R>,
  visit: (record: A) => RunJournalError | undefined,
): Effect.Effect<void, E | RunJournalError, R> =>
  Stream.runForEachArray(
    records,
    Effect.fnUntraced(function* (chunk) {
      for (let start = 0; start < chunk.length; start += 8) {
        yield* Effect.suspend(() => {
          const end = Math.min(start + 8, chunk.length);

          for (let index = start; index < end; index++) {
            const error = visit(chunk[index]);

            if (error !== undefined) return Effect.fail(error);
          }

          return Effect.void;
        });
      }
    }),
  );

/**
 * Reconstruct a fixed canonical prefix from a re-readable stream. The caller must keep the same
 * records visible on every traversal. The first collects compaction and settlement metadata;
 * rollovers additionally validate covered Tool batches before rebuilding Prompt and usage. Metadata and the
 * live Prompt remain resident; covered historical message/tool payloads do not. An uncompacted Prompt still grows
 * with its conversation, so hosts must configure an appropriate context compaction policy.
 * Prepared metadata must describe exactly this stream through the same fixed tail;
 * it skips only the metadata scan, never covered-Tool validation or the canonical fold.
 * @internal
 */
export const projectRunJournalStream = Effect.fnUntraced(function* <
  E,
  R,
  A extends JournalRecordEnvelope,
>(
  records: Stream.Stream<A, E, R>,
  ownerRunId: RunId | undefined,
  onBoundary?: (boundary: JournalBoundary) => void,
  savedContext?: RunJournalContext,
  preparedMetadata?: JournalMetadata,
  onEvidence?: (envelope: A) => void,
): Effect.fn.Return<RunJournalProjection, RunJournalError | E, R> {
  if (savedContext !== undefined && savedContext.runId !== ownerRunId)
    return yield* journalError("Saved original context belongs to another Run");

  const initialContext = savedContext?.prompt;

  if (
    initialContext !== undefined &&
    savedContext !== undefined &&
    savedContext.priorHistoryLength > initialContext.content.length
  )
    return yield* journalError("Saved original context has an invalid ownership boundary");
  const historical = initialContext?.content.slice(0, savedContext?.priorHistoryLength) ?? [];

  let state: FoldState = {
    all: [],
    before: [],
    pendingTools: [],
    pendingToolsForRun: false,
    committedTurns: 0,
  };

  // RUN-026 pre-scan: valid compactions retire only their creator's Run view. A bound
  // stays below its own sequence and cannot split a visible response from its settled results.
  // Pruning and rollovers may cover complete owner-Run batches; a summarize record carries
  // its summary. Invalid records leave full history authoritative. A wider containing view
  // replaces earlier coverage, with equal bounds preferring the later record.
  // Exact declaration spans are validated at each compaction's own captured prefix.
  // Later results preserve committed coverage and close retained or summarized calls below.
  if (preparedMetadata !== undefined && preparedMetadata.ownerRunId !== ownerRunId)
    return yield* journalError("Prepared journal metadata belongs to another replay");

  let metadata = preparedMetadata;

  if (metadata === undefined) {
    const collected = makeJournalMetadata(ownerRunId);

    yield* foldJournalRecords(records, (envelope) => {
      collected.add(envelope);

      return undefined;
    });
    metadata = collected.snapshot();
  }

  const {
    firstSequenceByRun,
    responseSequencesByRun,
    terminalSequenceByRun,
    settledSpans,
    settledSequenceById,
    settledToolCallRecordIds,
    settledById,
  } = metadata;

  const declarationByResultSequence = new Map(settledSpans.map(({ from, to }) => [to, from]));
  const spansByDeclaration = new Map<number, Array<(typeof settledSpans)[number]>>();

  for (const span of settledSpans) {
    const spans = spansByDeclaration.get(span.from) ?? [];

    spans.push(span);
    spansByDeclaration.set(span.from, spans);
  }

  const isInRunView = (
    sequence: number,
    payload: { readonly _tag: string; readonly runId?: RunId | undefined },
    runId: RunId | undefined,
  ): boolean => {
    if (runId === undefined || payload.runId === undefined || payload.runId === runId) return true;
    const first = firstSequenceByRun.get(runId) ?? Number.POSITIVE_INFINITY;

    return (
      sequence < first ||
      (payload._tag === "ToolCallSettled" &&
        (declarationByResultSequence.get(sequence) ?? Number.POSITIVE_INFINITY) < first)
    );
  };

  const compactions = metadata.compactions.filter(({ sequence, payload }) =>
    isInRunView(sequence, payload, ownerRunId),
  );

  const recordsForRun = records.pipe(
    Stream.filter(({ sequence, record: { payload } }) =>
      isInRunView(sequence, payload, ownerRunId),
    ),
  );

  const isCovered = (envelope: JournalRecordEnvelope, compaction: CompactionCreated): boolean =>
    envelope.sequence <= compaction.coversThrough &&
    isInRunView(envelope.sequence, envelope.record.payload, compaction.runId);

  const settledCoverage = compactions.reduce(
    (through, { payload }) =>
      payload.kind !== "summarize" ? Math.max(through, payload.coversThrough) : through,
    0,
  );

  // Terminality changes only prompt coverage; it never settles or resolves the Tool Call.
  // Compare against the compaction's Run and sequence, independent of the replay's owner.
  const isTerminalPriorRun = (
    candidateRunId: RunId,
    compactionRunId: RunId | undefined,
    beforeSequence: number,
  ): boolean => {
    const terminal = terminalSequenceByRun.get(candidateRunId);

    return (
      candidateRunId !== compactionRunId &&
      terminal !== undefined &&
      terminal < beforeSequence &&
      terminal >
        (responseSequencesByRun
          .get(candidateRunId)
          ?.findLast((sequence) => sequence < beforeSequence) ?? 0)
    );
  };

  const incompleteResponseSequences: Array<{
    readonly sequence: number;
    readonly runId: RunId;
    readonly lastSettlementSequence: number;
  }> = [];

  let ownerPrefixSequence = firstSequenceByRun.get(ownerRunId ?? "") ?? Number.POSITIVE_INFINITY;

  let protectedContext: Prompt.Prompt | undefined =
    initialContext === undefined
      ? undefined
      : Prompt.fromMessages(initialContext.content.slice(savedContext?.priorHistoryLength));

  if (settledCoverage > 0) {
    yield* foldJournalRecords(recordsForRun, (envelope) => {
      const payload = envelope.record.payload;

      if (payload._tag !== "ModelResponseRecorded" || envelope.sequence > settledCoverage) return;
      const decoded = decodePromptMessages(payload.messages);

      if (Result.isFailure(decoded)) return decoded.failure;
      const messages = decoded.success;
      const declared = declaredApplicationToolCallIds(messages);

      let lastSettlementSequence = 0;

      for (const id of declared) {
        const decodedId = decodeToolCallId(id);

        if (Result.isFailure(decodedId))
          return journalError("Failed to decode a declared Tool Call ID", decodedId.failure);
        const callId = decodedId.success;

        lastSettlementSequence = Math.max(
          lastSettlementSequence,
          settledSequenceById.get(toolCallSettledRecordId(payload.runId, payload.turn, callId)) ??
            Infinity,
        );
      }
      if (declared.length > 0)
        incompleteResponseSequences.push({
          sequence: envelope.sequence,
          runId: payload.runId,
          lastSettlementSequence,
        });
      if (
        payload.runId === ownerRunId &&
        payload.turn === 1 &&
        payload.runScopedPrefixLength !== undefined
      ) {
        ownerPrefixSequence = envelope.sequence;
        protectedContext = Prompt.fromMessages(
          messages.content.slice(0, payload.runScopedPrefixLength),
        );
      }
    });
  }

  const boundIsValid = (
    payload: CompactionCreated,
    ownSequence: number,
    evidence?: Set<number>,
  ): boolean => {
    const { runId, coversThrough } = payload;

    if (coversThrough <= 0 || coversThrough >= ownSequence) return false;
    const ownerFirst = firstSequenceByRun.get(runId);

    if (payload.kind === "summarize" && ownerFirst !== undefined && coversThrough >= ownerFirst)
      return false;
    if (
      payload.kind !== "summarize" &&
      savedContext?.boundaries.some(
        (boundary) =>
          boundary.sequence <= coversThrough &&
          boundary.incomplete === true &&
          boundary.terminalPriorRun !== true,
      )
    )
      return false;
    if (payload.kind !== "summarize") {
      for (const response of incompleteResponseSequences) {
        if (
          response.sequence > coversThrough ||
          response.lastSettlementSequence < ownSequence ||
          !isInRunView(
            response.sequence,
            { _tag: "ModelResponseRecorded", runId: response.runId },
            runId,
          )
        )
          continue;
        if (evidence !== undefined) {
          evidence.add(response.sequence);
          const terminal = terminalSequenceByRun.get(response.runId);

          const lastResponse = responseSequencesByRun
            .get(response.runId)
            ?.findLast((sequence) => sequence < ownSequence);

          if (terminal !== undefined) evidence.add(terminal);
          if (lastResponse !== undefined) evidence.add(lastResponse);
        }
        if (!isTerminalPriorRun(response.runId, runId, ownSequence)) return false;
      }
    }
    for (const span of settledSpans) {
      if (
        span.from <= coversThrough &&
        coversThrough < span.to &&
        span.to < ownSequence &&
        isInRunView(span.from, { _tag: "ModelResponseRecorded", runId: span.runId }, runId)
      ) {
        evidence?.add(span.from);
        evidence?.add(span.to);

        return false;
      }
    }

    return true;
  };

  type CompactionView = (typeof compactions)[number];

  let replacements: ReadonlyArray<CompactionView> = [];
  let clearings: ReadonlyArray<CompactionView> = [];

  const retainCompaction = (views: ReadonlyArray<CompactionView>, next: CompactionView) => {
    // Independent Run views overlap without containing one another. Retire an overlay only
    // when the replacement actually contains its view, not merely a larger sequence number.
    if (
      views.some(
        (prior) =>
          prior.payload.coversThrough > next.payload.coversThrough &&
          isInRunView(next.sequence, next.payload, prior.payload.runId),
      )
    )
      return views;

    return [
      ...views.filter(
        (prior) =>
          next.payload.coversThrough < prior.payload.coversThrough ||
          !isInRunView(prior.sequence, prior.payload, next.payload.runId),
      ),
      next,
    ];
  };

  let latestWindowId: string | undefined = savedContext?.contextWindowId;
  let latestWindowSequence = -1;
  let latestWindow: CompactionView | undefined;
  let rolloverCoveredThrough = 0;

  for (const { payload, sequence } of compactions) {
    if (!boundIsValid(payload, sequence)) continue;
    if (payload.kind === "rollover" && sequence > latestWindowSequence) {
      latestWindowId = contextWindowId(payload.runId, payload.turn);
      latestWindowSequence = sequence;
      latestWindow = { payload, sequence };
    }
    if (payload.kind === "rollover")
      rolloverCoveredThrough = Math.max(rolloverCoveredThrough, payload.coversThrough);
    if (payload.kind === "summarize" || payload.kind === "rollover") {
      if (payload.kind === "summarize" && payload.summary === undefined) continue;
      replacements = retainCompaction(replacements, { payload, sequence });
    } else clearings = retainCompaction(clearings, { payload, sequence });
  }

  // A superseding summary retires the overlay, but the latest rollover still owns the window.
  const contextViews = [
    ...replacements,
    ...clearings,
    ...(latestWindow === undefined ? [] : [latestWindow]),
  ];

  const contextCompactions = new Set(contextViews.map(({ sequence }) => sequence));

  const historyFrom = CanonicalSequence.make(
    replacements.reduce(
      (from, { payload }) => Math.max(from, payload.coversThrough + 1),
      savedContext?.historyFrom ?? 1,
    ),
  );

  const contextValidationViews = compactions.filter(
    ({ sequence }) => sequence >= historyFrom || contextCompactions.has(sequence),
  );

  const contextEvidence = new Set(
    contextValidationViews.flatMap(({ payload }) => {
      const sequence = firstSequenceByRun.get(payload.runId);

      return sequence === undefined ? [] : [sequence];
    }),
  );

  // Reread compactions must keep the facts that accepted or rejected their original bounds.
  if (onEvidence !== undefined)
    for (const { payload, sequence } of contextValidationViews)
      boundIsValid(payload, sequence, contextEvidence);

  const retainEvidence = (envelope: A): void => {
    if (onEvidence === undefined) return;
    onEvidence(envelope);
    if (envelope.sequence < historyFrom && envelope.record.payload._tag === "ModelResponseRecorded")
      // A retained declaration must preserve its already-settled siblings for later coverage checks.
      for (const span of spansByDeclaration.get(envelope.sequence) ?? [])
        if (span.to < historyFrom) contextEvidence.add(span.to);
  };

  let summaryEmitted = false;

  const retainedPrefix = replacements.some(
    ({ payload }) =>
      payload.kind === "rollover" &&
      payload.runId === ownerRunId &&
      ownerPrefixSequence <= payload.coversThrough,
  )
    ? (protectedContext?.content ?? [])
    : [];

  const replacementLength = retainedPrefix.length + replacements.length;

  const emitSummary = () => {
    if (summaryEmitted || replacements.length === 0) return;
    summaryEmitted = true;
    state.all.push(...retainedPrefix);
    for (const { payload: replacement } of replacements) {
      const message =
        replacement.kind === "rollover"
          ? contextWindowMessage(
              contextWindowId(replacement.runId, replacement.turn),
              replacement.handoff,
            )
          : Prompt.makeMessage("user", {
              content: [
                Prompt.makePart("text", {
                  text: `${COMPACTION_SUMMARY_PREFIX}${replacement.summary}`,
                }),
              ],
            });

      state.all.push(message);
      if (replacement.kind !== "rollover" || replacement.runId !== ownerRunId)
        state.before.push(message);
      // Marker coverage comes from its compaction, even when covered facts leave the saved range.
      onBoundary?.({
        sequence: replacement.coversThrough,
        tag: "CompactionCreated",
        promptLength: state.all.length,
      });
    }
  };

  // The original context is canonical model input, not a reconstruction of later Thread
  // traffic. Its bounded mapping lets compaction cover exactly the same historical prefix.
  if (savedContext !== undefined) {
    emitSummary();
    for (const [index, message] of historical.entries()) {
      const boundary = savedContext.boundaries.find((entry) => entry.promptLength > index);

      if (boundary === undefined && replacements.length > 0)
        return yield* journalError("Saved context lacks its canonical compaction boundary");
      if (
        boundary !== undefined &&
        replacements.some(({ payload }) => boundary.sequence <= payload.coversThrough)
      )
        continue;

      const visible =
        message.role === "tool" &&
        boundary !== undefined &&
        clearings.some(({ payload }) => boundary.sequence <= payload.coversThrough)
          ? Prompt.makeMessage("tool", {
              content: message.content.map((part) =>
                part.type === "tool-result"
                  ? Prompt.makePart("tool-result", { ...part, result: CLEARED_TOOL_RESULT })
                  : part,
              ),
            })
          : message;

      state.all.push(visible);
      state.before.push(visible);
    }
    for (const boundary of savedContext.boundaries) {
      const removed = savedContext.boundaries.reduce(
        (length, candidate) =>
          candidate.sequence <= boundary.sequence &&
          replacements.some(({ payload }) => candidate.sequence <= payload.coversThrough)
            ? Math.max(length, candidate.promptLength)
            : length,
        0,
      );

      onBoundary?.({
        ...boundary,
        promptLength: replacementLength + Math.max(0, boundary.promptLength - removed),
      });
    }
  }

  const modelUsage: Array<ModelCallUsage> = [];
  let unobservedModelCalls = 0;

  const usage = {
    modelCalls: 0,
    inputTokens: 0,
    outputTokens: 0,
    lastInputTokens: 0,
    lastOutputTokens: 0,
    costMicrousd: 0,
    modelUsage,
  };

  let toolSelection: Selection | undefined;
  let usageTurn = 0;

  const incompleteToolTurns = new Set<string>();
  const incompleteToolCalls = new Set<string>();
  const incompleteContextRuns = new Set<RunId>();

  for (const response of incompleteResponseSequences)
    if (
      contextViews.some(
        (view) =>
          view.payload.kind !== "summarize" &&
          response.sequence <= view.payload.coversThrough &&
          response.lastSettlementSequence >= view.sequence,
      )
    )
      incompleteContextRuns.add(response.runId);
  let pendingContextToolCallId: string | undefined;
  let ownerTerminated = false;

  const policyUsage = {
    committedTurns: 0,
    toolCalls: 0,
    programmaticToolCalls: 0,
    consecutiveToolFailures: 0,
    finalizationUsed: false,
    modelRestarts: 0,
  };

  const accountResponse = (
    envelope: JournalRecordEnvelope,
    payload: Extract<
      JournalRecordEnvelope["record"]["payload"],
      { readonly _tag: "ModelResponseRecorded" }
    >,
    messages: Prompt.Prompt,
  ): RunJournalError | undefined => {
    const record = envelope.record;

    const declared = declaredApplicationToolCallIds(messages);
    const actualResultIds: Array<RecordId> = [];
    let incomplete = false;

    for (const id of declared) {
      const decodedId = decodeToolCallId(id);

      if (Result.isFailure(decodedId))
        return journalError("Failed to decode a declared Tool Call ID", decodedId.failure);
      const toolCallId = decodedId.success;

      const resultId = toolCallSettledRecordId(payload.runId, payload.turn, toolCallId);

      if (settledToolCallRecordIds.has(resultId)) actualResultIds.push(resultId);
      else incomplete = true;
    }
    if (incomplete) {
      incompleteToolTurns.add(envelope.record.recordId);
      incompleteContextRuns.add(payload.runId);
      for (const recordId of actualResultIds) incompleteToolCalls.add(recordId);
    }
    if (payload.runId !== ownerRunId) return;
    if (!("toolOperations" in payload))
      return journalError("Owning Run execution requires full canonical evidence");
    if (payload.turn === 1 && payload.runScopedPrefixLength !== undefined) {
      protectedContext = Prompt.fromMessages(
        messages.content.slice(0, payload.runScopedPrefixLength),
      );
    }

    const projected = projectedResponseUsage(payload);

    if (Result.isFailure(projected)) return projected.failure;
    const responseUsage = projected.success;

    const nextUnobservedModelCalls = addProjectedUsage(
      "unobservedModelCalls",
      unobservedModelCalls,
      payload.unobservedModelCalls ?? 0,
    );

    if (typeof nextUnobservedModelCalls !== "number") return nextUnobservedModelCalls;
    unobservedModelCalls = nextUnobservedModelCalls;

    const nextUsageModelCalls = addProjectedUsage(
      "modelCalls",
      usage.modelCalls,
      responseUsage.modelCalls,
    );

    if (typeof nextUsageModelCalls !== "number") return nextUsageModelCalls;
    usage.modelCalls = nextUsageModelCalls;

    const nextUsageInputTokens = addProjectedUsage(
      "inputTokens",
      usage.inputTokens,
      responseUsage.inputTokens,
    );

    if (typeof nextUsageInputTokens !== "number") return nextUsageInputTokens;
    usage.inputTokens = nextUsageInputTokens;

    const nextUsageOutputTokens = addProjectedUsage(
      "outputTokens",
      usage.outputTokens,
      responseUsage.outputTokens,
    );

    if (typeof nextUsageOutputTokens !== "number") return nextUsageOutputTokens;
    usage.outputTokens = nextUsageOutputTokens;

    const nextUsageCostMicrousd = addProjectedUsage(
      "costMicrousd",
      usage.costMicrousd,
      responseUsage.costMicrousd,
    );

    if (typeof nextUsageCostMicrousd !== "number") return nextUsageCostMicrousd;
    usage.costMicrousd = nextUsageCostMicrousd;
    usage.modelUsage.push(...responseUsage.modelUsage);
    if (payload.turn > usageTurn) {
      usageTurn = payload.turn;
      usage.lastInputTokens = responseUsage.inputTokens;
      usage.lastOutputTokens = responseUsage.outputTokens;
    }

    policyUsage.committedTurns = Math.max(policyUsage.committedTurns, payload.turn);

    const calls = messages.content.flatMap((message) =>
      message.role === "assistant"
        ? message.content.filter((part) => part.type === "tool-call")
        : [],
    );

    const candidate = calls.length === 1 ? calls[0] : undefined;

    const candidateResult =
      candidate === undefined
        ? undefined
        : settledById.get(`tool-settled:${ownerRunId}:${payload.turn}:${candidate.id}`);

    pendingContextToolCallId =
      candidate !== undefined &&
      !candidate.providerExecuted &&
      candidateResult?.isFailure === false &&
      candidateResult.budgetRejected !== true &&
      envelope.sequence > rolloverCoveredThrough
        ? candidate.id
        : undefined;

    policyUsage.toolCalls += calls.length;
    if (payload.toolExposure !== undefined) toolSelection = payload.toolExposure.selection;
    if (incompleteToolTurns.has(record.recordId)) return;
    for (const call of calls) {
      const result = call.providerExecuted
        ? messages.content
            .flatMap((message) => (message.role === "assistant" ? message.content : []))
            .find(
              (part) => part.type === "tool-result" && part.providerExecuted && part.id === call.id,
            )
        : settledById.get(`tool-settled:${ownerRunId}:${payload.turn}:${call.id}`);

      if (result === undefined || ("budgetRejected" in result && result.budgetRejected === true))
        continue;
      if (!("isFailure" in result)) continue;
      if (!result.isFailure && "toolSelection" in result && result.toolSelection !== undefined)
        toolSelection = result.toolSelection;
      policyUsage.consecutiveToolFailures = result.isFailure
        ? policyUsage.consecutiveToolFailures + 1
        : 0;
    }
  };

  // Slots refer to the retained Prompt itself, not an additional history payload index.
  // Only still-unseen results retain a slot; late results fill the original declaration in place.
  const historicalResults = new Map<
    string,
    {
      readonly allIndex: number;
      readonly beforeIndex: number;
      readonly parts: Array<Prompt.ToolResultPart>;
      readonly partIndex: number;
    }
  >();

  // Only exact calls needed by late results survive retirement, never their old messages.
  const retiredLateCalls = new Map<string, Prompt.ToolCallPart>();

  let pendingToolOrder = new Map<string, number>();

  const flushTools = (current: FoldState): FoldState => {
    if (current.pendingTools.length === 0) return current;

    // Suspension can persist an ordinary sibling before a delegated call joins. Model context
    // still uses declaration order, matching the live interpreter's completed results batch.
    const ordered = current.pendingTools.toSorted(
      (left, right) =>
        (pendingToolOrder.get(left.record.toolCallId) ?? Number.MAX_SAFE_INTEGER) -
        (pendingToolOrder.get(right.record.toolCallId) ?? Number.MAX_SAFE_INTEGER),
    );

    const toolMessage = toolMessageFromSettled(ordered);

    current.all.push(toolMessage);
    if (!current.pendingToolsForRun) current.before.push(toolMessage);

    return {
      ...current,
      pendingTools: [],
      pendingToolsForRun: false,
    };
  };

  yield* foldJournalRecords(recordsForRun, (envelope) => {
    const payload = envelope.record.payload;

    if (contextEvidence.has(envelope.sequence)) retainEvidence(envelope);
    if (
      (payload._tag === "RunCompleted" ||
        payload._tag === "RunFailed" ||
        payload._tag === "SubmissionSettled") &&
      payload.runId !== undefined &&
      incompleteContextRuns.has(payload.runId)
    )
      retainEvidence(envelope);

    if (
      ((payload._tag === "RunCompleted" || payload._tag === "RunFailed") &&
        payload.runId === ownerRunId) ||
      (payload._tag === "SubmissionSettled" &&
        runIdForSubmission(payload.submissionId) === ownerRunId)
    )
      ownerTerminated = true;

    if (payload._tag === "RunPolicyUsageReserved" && payload.runId === ownerRunId) {
      if (
        payload.programmaticToolCalls < policyUsage.programmaticToolCalls ||
        (policyUsage.finalizationUsed && !payload.finalizationUsed)
      ) {
        return journalError("Run policy reservations must be monotonic");
      }
      policyUsage.programmaticToolCalls = payload.programmaticToolCalls;
      policyUsage.finalizationUsed = payload.finalizationUsed;
    }
    if (payload._tag === "ModelCallAborted" && payload.runId === ownerRunId) {
      if (payload.restart !== policyUsage.modelRestarts + 1)
        return journalError("Model restart reservations must advance once");
      policyUsage.modelRestarts = payload.restart;

      const summarized = summarizeModelUsageResult(payload.modelUsage);

      if (Result.isFailure(summarized))
        return journalError("Aborted model usage exceeds accounting bounds", summarized.failure);
      const summary = summarized.success;

      const nextUsageModelCalls = addProjectedUsage(
        "modelCalls",
        usage.modelCalls,
        summary.modelCalls,
      );

      if (typeof nextUsageModelCalls !== "number") return nextUsageModelCalls;
      usage.modelCalls = nextUsageModelCalls;

      const nextUsageInputTokens = addProjectedUsage(
        "inputTokens",
        usage.inputTokens,
        summary.inputTokens.total,
      );

      if (typeof nextUsageInputTokens !== "number") return nextUsageInputTokens;
      usage.inputTokens = nextUsageInputTokens;

      const nextUsageOutputTokens = addProjectedUsage(
        "outputTokens",
        usage.outputTokens,
        summary.outputTokens.total,
      );

      if (typeof nextUsageOutputTokens !== "number") return nextUsageOutputTokens;
      usage.outputTokens = nextUsageOutputTokens;

      const nextUsageCostMicrousd = addProjectedUsage(
        "costMicrousd",
        usage.costMicrousd,
        summary.costMicrousd,
      );

      if (typeof nextUsageCostMicrousd !== "number") return nextUsageCostMicrousd;
      usage.costMicrousd = nextUsageCostMicrousd;
      usage.modelUsage.push(...payload.modelUsage);

      const nextUnobservedModelCalls = addProjectedUsage(
        "unobservedModelCalls",
        unobservedModelCalls,
        payload.unobservedModelCalls,
      );

      if (typeof nextUnobservedModelCalls !== "number") return nextUnobservedModelCalls;
      unobservedModelCalls = nextUnobservedModelCalls;
    }
    if (PROMPT_TRANSPARENT_TAGS.has(payload._tag)) return;
    // The compaction record governs the fold (pre-scan) and contributes no
    // message of its own; records at or below the summarize bound render as
    // the one summary message emitted at the covered/kept transition.
    if (payload._tag === "CompactionCreated") {
      if (contextCompactions.has(envelope.sequence)) retainEvidence(envelope);

      return;
    }
    if (replacements.some(({ payload }) => isCovered(envelope, payload))) {
      if (payload._tag === "ModelResponseRecorded" && payload.runId !== ownerRunId) {
        const late = (spansByDeclaration.get(envelope.sequence) ?? []).filter((span) =>
          replacements.some((view) => isCovered(envelope, view.payload) && span.to > view.sequence),
        );

        if (late.length > 0) {
          // Even a rollover-hidden late result needs its exact declaration to stay hidden.
          retainEvidence(envelope);
          if (latestWindow !== undefined && isCovered(envelope, latestWindow.payload)) return;
          const decoded = decodePromptMessages(payload.messages);

          if (Result.isFailure(decoded)) return decoded.failure;
          const messages = decoded.success;

          for (const message of messages.content) {
            if (message.role !== "assistant") continue;
            for (const call of message.content) {
              if (call.type !== "tool-call" || call.providerExecuted) continue;

              const decodedId = decodeToolCallId(call.id);

              if (Result.isFailure(decodedId))
                return journalError("Invalid retired Tool Call ID", decodedId.failure);
              const callId = decodedId.success;

              const id = toolCallSettledRecordId(payload.runId, payload.turn, callId);

              if (late.some((span) => span.recordId === id)) retiredLateCalls.set(id, call);
            }
          }
        }
      }
      // Retiring Prompt payloads does not retire the owning Run's policy or usage accounting.
      if (payload._tag === "ModelResponseRecorded" && payload.runId === ownerRunId) {
        const decoded = decodePromptMessages(payload.messages);

        if (Result.isFailure(decoded)) return decoded.failure;
        const messages = decoded.success;

        const accountingError = accountResponse(envelope, payload, messages);

        if (accountingError !== undefined) return accountingError;
        state = { ...state, committedTurns: Math.max(state.committedTurns, payload.turn) };
      }

      return;
    }
    emitSummary();
    if (payload._tag === "ToolCallSettled") {
      const declaration = declarationByResultSequence.get(envelope.sequence);

      const declarationFact = {
        sequence: declaration ?? Infinity,
        record: { payload: { _tag: "ModelResponseRecorded", runId: payload.runId } },
      };

      const declarationCovered = (compaction: CompactionCreated) =>
        declarationFact.sequence <= compaction.coversThrough &&
        isInRunView(declarationFact.sequence, declarationFact.record.payload, compaction.runId);

      if (latestWindow !== undefined && declarationCovered(latestWindow.payload)) return;
      retainEvidence(envelope);
      if (payload.runId !== ownerRunId) {
        const slot = historicalResults.get(envelope.record.recordId);

        if (slot === undefined) {
          const call = retiredLateCalls.get(envelope.record.recordId);

          if (
            call === undefined ||
            call.id !== payload.toolCallId ||
            call.name !== payload.toolName
          )
            return journalError("Historical Tool result has no matching declaration");

          const pair = [
            Prompt.makeMessage("assistant", { content: [call] }),
            Prompt.makeMessage("tool", {
              content: [
                Prompt.makePart("tool-result", {
                  id: call.id,
                  name: call.name,
                  result: clearings.some(({ payload }) => declarationCovered(payload))
                    ? CLEARED_TOOL_RESULT
                    : payload.result,
                  isFailure: payload.isFailure,
                  providerExecuted: false,
                }),
              ],
            }),
          ];

          state.all.push(...pair);
          state.before.push(...pair);
          retiredLateCalls.delete(envelope.record.recordId);
          onBoundary?.({
            sequence: envelope.sequence,
            tag: payload._tag,
            promptLength: state.all.length,
          });

          return;
        }
        const declared = slot.parts[slot.partIndex];

        if (declared?.id !== payload.toolCallId || declared.name !== payload.toolName)
          return journalError("Historical Tool result differs from its declaration");
        slot.parts[slot.partIndex] = Prompt.makePart("tool-result", {
          id: payload.toolCallId,
          name: payload.toolName,
          result: clearings.some(
            ({ payload }) => isCovered(envelope, payload) || declarationCovered(payload),
          )
            ? CLEARED_TOOL_RESULT
            : payload.result,
          isFailure: payload.isFailure,
          providerExecuted: false,
        });
        const message = Prompt.makeMessage("tool", { content: [...slot.parts] });

        state.all[slot.allIndex] = message;
        state.before[slot.beforeIndex] = message;
        historicalResults.delete(envelope.record.recordId);
        onBoundary?.({
          sequence: envelope.sequence,
          tag: payload._tag,
          promptLength: state.all.length,
          ...(incompleteToolCalls.has(envelope.record.recordId) ? { incomplete: true } : {}),
          ...(isTerminalPriorRun(payload.runId, ownerRunId, Number.POSITIVE_INFINITY)
            ? { terminalPriorRun: true }
            : {}),
        });

        return;
      }
      if (
        state.pendingTools.length > 0 &&
        state.pendingToolsForRun !== (payload.runId === ownerRunId)
      ) {
        state = flushTools(state);
      }
      state.pendingTools.push({
        record: payload,
        cleared: clearings.some(({ payload }) => isCovered(envelope, payload)),
      });
      state = {
        ...state,
        pendingToolsForRun: payload.runId === ownerRunId,
      };
      onBoundary?.({
        sequence: envelope.sequence,
        tag: payload._tag,
        promptLength: state.all.length + 1,
        ...(isTerminalPriorRun(payload.runId, ownerRunId, Number.POSITIVE_INFINITY)
          ? { terminalPriorRun: true }
          : {}),
        ...(incompleteToolCalls.has(envelope.record.recordId) ? { incomplete: true } : {}),
      });

      return;
    }
    state = flushTools(state);
    if (payload._tag === "ModelCompleted" && payload.history !== undefined) {
      retainEvidence(envelope);
      const decoded = decodePromptMessages(payload.history);

      if (Result.isFailure(decoded)) return decoded.failure;
      const messages = decoded.success;

      state.all.length = 0;
      state.before.length = 0;
      historicalResults.clear();

      for (const message of messages.content) {
        state.all.push(message);
        if (payload.runId !== ownerRunId) state.before.push(message);
      }
      // Persistent, non-durable model history needs the same original compaction mapping
      // as durable responses; cold selected-Run recovery cannot infer it from later history.
      onBoundary?.({
        sequence: envelope.sequence,
        tag: payload._tag,
        promptLength: state.all.length,
      });

      return;
    }
    if (payload._tag !== "ModelResponseRecorded") return;
    retainEvidence(envelope);
    const decoded = decodePromptMessages(payload.messages);

    if (Result.isFailure(decoded)) return decoded.failure;
    const messages = decoded.success;
    const forRun = payload.runId === ownerRunId;

    pendingToolOrder = new Map(
      declaredApplicationToolCallIds(messages).map((id, index) => [id, index]),
    );

    const accountingError = accountResponse(envelope, payload, messages);

    if (accountingError !== undefined) return accountingError;

    const visibleMessages = forRun
      ? messages.content
      : messages.content.filter((message) => message.role !== "system");

    for (const message of visibleMessages) {
      state.all.push(message);
      if (!forRun) state.before.push(message);
    }
    if (!forRun) {
      const calls = messages.content.flatMap((message) =>
        message.role === "assistant"
          ? message.content.filter(
              (part): part is Prompt.ToolCallPart =>
                part.type === "tool-call" && !part.providerExecuted,
            )
          : [],
      );

      if (calls.length > 0) {
        const parts = calls.map((call) =>
          Prompt.makePart("tool-result", {
            id: call.id,
            name: call.name,
            result: {
              _tag: "ToolOutcomeUnknown",
              message:
                "This earlier operation has no recorded outcome. It may have executed. Do not assume success or retry it; its original operation remains unresolved.",
            },
            isFailure: true,
            providerExecuted: false,
          }),
        );

        const allIndex = state.all.length;
        const beforeIndex = state.before.length;
        const message = Prompt.makeMessage("tool", { content: parts });

        state.all.push(message);
        state.before.push(message);
        for (const [partIndex, call] of calls.entries()) {
          const decodedId = decodeToolCallId(call.id);

          if (Result.isFailure(decodedId))
            return journalError("Invalid historical Tool Call ID", decodedId.failure);
          const callId = decodedId.success;

          const resultId = toolCallSettledRecordId(payload.runId, payload.turn, callId);

          // A fixed replay can fill only a result present in its captured metadata. Missing
          // calls retain their visible Unknown part without an expanded identity directory.
          if (!settledToolCallRecordIds.has(resultId)) continue;
          historicalResults.set(resultId, {
            allIndex,
            beforeIndex,
            parts,
            partIndex,
          });
        }
      }
    }
    state = {
      ...state,
      committedTurns: forRun ? Math.max(state.committedTurns, payload.turn) : state.committedTurns,
    };
    onBoundary?.({
      sequence: envelope.sequence,
      tag: payload._tag,
      promptLength: state.all.length,
      ...(isTerminalPriorRun(payload.runId, ownerRunId, Number.POSITIVE_INFINITY)
        ? { terminalPriorRun: true }
        : {}),
      ...(incompleteToolTurns.has(envelope.record.recordId) ? { incomplete: true } : {}),
    });
  });
  emitSummary();
  state = flushTools(state);

  const validatedPolicyUsage = yield* Schema.decodeEffect(RunPolicyUsage)(policyUsage).pipe(
    Effect.mapError((cause) =>
      journalError("Run policy accounting exceeds its Schema bounds", cause),
    ),
  );

  return {
    ...(toolSelection === undefined ? {} : { toolSelection }),
    policyUsage: validatedPolicyUsage,
    prompt: Prompt.fromMessages(state.all),
    historyBefore: Prompt.fromMessages(state.before),
    historyFrom,
    committedTurns: state.committedTurns,
    usage: unobservedModelCalls === 0 ? usage : { ...usage, unobservedModelCalls },
    ...(latestWindowId === undefined ? {} : { contextWindowId: latestWindowId }),
    ...(protectedContext === undefined ? {} : { protectedContext }),
    ...(ownerTerminated || pendingContextToolCallId === undefined
      ? {}
      : { pendingContextToolCallId }),
  };
});

/** Pure projection of one Run's durable recovery state from canonical records. */
export const projectRunJournal = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId: RunId,
): Effect.Effect<RunJournalProjection, RunJournalError> =>
  projectRunJournalStream(Stream.fromIterable(records), runId);

/**
 * Pure valid-prompt projection from canonical records: `UserInputRecorded` +
 * `ModelResponseRecorded` + complete `ToolCallSettled` batches → the deterministic model-visible
 * Prompt (plan §Coordinator flow step 3).
 */
export const promptFromCanonicalRecords = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): Effect.Effect<Prompt.Prompt, RunJournalError> =>
  projectRunJournalStream(Stream.fromIterable(records), undefined).pipe(
    Effect.map((projection) => projection.prompt),
  );

/** Everything one committed Turn contributes to its canonical batch. */
/**
 * Staged usage is validated, never repaired: clamping negatives or truncating
 * fractions would under-record canonical usage, and NaN/Infinity must fail
 * typed instead of escaping as a record-construction defect (RUN-023).
 */
const validStagedUsage = (label: string, value: number): RunJournalError | undefined =>
  Number.isSafeInteger(value) && value >= 0
    ? undefined
    : journalError(`Staged ${label} must be a non-negative safe integer, got ${String(value)}`);

export interface TurnCommitInput {
  readonly toolResultMaxBytes?: number | undefined;
  readonly toolSelectionMaxBytes?: number | undefined;
  readonly toolOperations?: ModelResponseRecorded["toolOperations"] | undefined;
  readonly toolParameterRejections?: ReadonlyArray<ToolParameterRejection> | undefined;
  readonly toolExposure?: Snapshot | undefined;
  readonly runId: RunId;
  /** Canonical (Run-relative, Attempt-independent) Turn number; must be positive. */
  readonly turn: number;
  readonly turnId: TurnId;
  /**
   * Exact response and leading input/instruction messages. Application Tool outcomes
   * are supplied separately; the journal does not reconstruct them from Prompt history.
   */
  readonly responseMessages: ReadonlyArray<Prompt.Message>;
  readonly toolResults: ReadonlyArray<RunTurnToolResult>;
  readonly producerId: ProducerId;
  readonly deploymentId: DeploymentId;
  readonly createdAt: DateTime.Utc;
  /** Leading instruction/wake messages that stay canonical but are hidden from later Runs. */
  readonly runScopedPrefixLength?: number | undefined;
  /** Terminal output committed atomically with this Turn's final canonical batch. */
  readonly runCompletion?:
    | {
        readonly output: PersistedJson;
        readonly runDisposition?: PersistedJson | undefined;
        readonly finishReason?: "budget-exhausted" | undefined;
        readonly exhausted?: ExhaustedLimit | undefined;
      }
    | undefined;
  /** Known missing-accounting invocations staged before this canonical response. */
  readonly unobservedModelCalls?: number | undefined;
  /** Per-call provider usage staged by the engine's `noteTurnUsage` (RUN-023). */
  readonly usage?:
    | {
        readonly inputTokens: number;
        readonly outputTokens: number;
        readonly costMicrousd?: number | undefined;
        readonly modelUsage?: ReadonlyArray<ModelCallUsage> | undefined;
      }
    | undefined;
}

const decodePersistedJson = Schema.decodeUnknownResult(PersistedJson);
const encodePrompt = Schema.encodeResult(Prompt.Prompt);
const decodeModelUsage = Schema.decodeResult(Schema.Array(ModelCallUsage));
const decodeModelResponse = Schema.decodeResult(ModelResponseRecorded);
const decodeSettledTool = Schema.decodeResult(ToolCallSettled);

const requireCanonicalTurn = (turn: number): RunJournalError | undefined =>
  !Number.isInteger(turn) || turn <= 0
    ? journalError(`Canonical turn number must be a positive integer: ${turn}`)
    : undefined;

const prepareModelResponse = (
  input: TurnCommitInput,
): Result.Result<
  {
    readonly messages: PersistedJson;
    readonly runScopedPrefixLength: number | undefined;
  },
  RunJournalError
> => {
  const promptMessages = input.responseMessages;

  if (promptMessages.length === 0)
    return Result.fail(
      journalError(`Turn ${input.turn} appended no model-visible Prompt messages`),
    );
  if (promptMessages.some((message) => message.role === "tool"))
    return Result.fail(
      journalError("Application Tool outcomes must be supplied as terminal facts"),
    );
  const runScopedPrefixLength = input.runScopedPrefixLength;

  if (
    runScopedPrefixLength !== undefined &&
    (input.turn !== 1 ||
      !Number.isSafeInteger(runScopedPrefixLength) ||
      runScopedPrefixLength <= 0 ||
      runScopedPrefixLength >= promptMessages.length ||
      promptMessages
        .slice(0, runScopedPrefixLength)
        .some((message) => message.role !== "system" && message.role !== "user"))
  )
    return Result.fail(
      journalError(
        "Run-scoped Prompt provenance must identify a non-empty system/user prefix of Turn 1 before its assistant response",
      ),
    );
  const encodedMessages = encodePrompt(Prompt.fromMessages([...promptMessages]));

  if (Result.isFailure(encodedMessages))
    return Result.fail(
      journalError("Turn Prompt messages failed to encode", encodedMessages.failure),
    );
  const boundedMessages = decodePersistedJson(encodedMessages.success);

  if (Result.isFailure(boundedMessages))
    return Result.fail(
      journalError(
        "Turn Prompt messages exceed canonical persistence bounds",
        boundedMessages.failure,
      ),
    );

  return Result.succeed({ messages: copyJson(boundedMessages.success), runScopedPrefixLength });
};

const prepareResponseUsage = (
  input: TurnCommitInput,
): Result.Result<ReadonlyArray<ModelCallUsage> | undefined, RunJournalError> => {
  let modelUsage: ReadonlyArray<ModelCallUsage> | undefined;

  if (input.usage?.modelUsage !== undefined) {
    const decoded = decodeModelUsage(input.usage.modelUsage);

    if (Result.isFailure(decoded))
      return Result.fail(journalError("Turn model usage failed to decode", decoded.failure));
    modelUsage = decoded.success;
    if (modelUsage.length === 0)
      return Result.fail(journalError("Turn model usage must contain at least one completed call"));
    const summarized = summarizeModelUsageResult(modelUsage);

    if (Result.isFailure(summarized))
      return Result.fail(
        journalError("Turn model usage exceeds accounting bounds", summarized.failure),
      );
    const summary = summarized.success;

    if (
      input.usage.inputTokens !== summary.inputTokens.total ||
      input.usage.outputTokens !== summary.outputTokens.total ||
      (input.usage.costMicrousd ?? 0) !== summary.costMicrousd
    )
      return Result.fail(journalError("Turn detailed and aggregate model usage disagree"));
  }

  for (const [label, value] of [
    ["unobservedModelCalls", input.unobservedModelCalls],
    ["inputTokens", input.usage?.inputTokens],
    ["outputTokens", input.usage?.outputTokens],
    ["costMicrousd", input.usage?.costMicrousd],
  ] as const) {
    if (value === undefined) continue;
    const error = validStagedUsage(label, value);

    if (error !== undefined) return Result.fail(error);
  }

  return Result.succeed(modelUsage);
};

const modelResponseRecord = Effect.fnUntraced(function* (
  input: TurnCommitInput,
): Effect.fn.Return<RecordEnvelope, RunJournalError | DigestError, Crypto.Crypto> {
  const prepared = prepareModelResponse(input);

  if (Result.isFailure(prepared)) return yield* prepared.failure;
  const { messages, runScopedPrefixLength } = prepared.success;
  const messagesDigest = yield* digestJson(messages);
  const usage = prepareResponseUsage(input);

  if (Result.isFailure(usage)) return yield* usage.failure;
  const modelUsage = usage.success;

  const decoded = decodeModelResponse({
    _tag: "ModelResponseRecorded",
    toolResultMaxBytes: input.toolResultMaxBytes ?? MAX_PERSISTED_JSON_BYTES,
    toolSelectionMaxBytes: input.toolSelectionMaxBytes ?? 0,
    toolOperations: input.toolOperations ?? [],
    ...(input.toolParameterRejections === undefined || input.toolParameterRejections.length === 0
      ? {}
      : { toolParameterRejections: input.toolParameterRejections }),
    ...(input.toolExposure === undefined ? {} : { toolExposure: input.toolExposure }),
    runId: input.runId,
    turnId: input.turnId,
    turn: input.turn,
    messages,
    messagesDigest,
    ...(runScopedPrefixLength === undefined ? {} : { runScopedPrefixLength }),
    ...(modelUsage === undefined ? {} : { modelUsage }),
    ...(input.unobservedModelCalls === undefined || input.unobservedModelCalls === 0
      ? {}
      : { unobservedModelCalls: input.unobservedModelCalls }),
    ...(input.usage === undefined
      ? {}
      : {
          inputTokens: input.usage.inputTokens,
          outputTokens: input.usage.outputTokens,
          // Absent cost re-seeds as zero without changing the canonical wire.
          ...(input.usage.costMicrousd === undefined || input.usage.costMicrousd === 0
            ? {}
            : { costMicrousd: input.usage.costMicrousd }),
        }),
  });

  if (Result.isFailure(decoded))
    return yield* journalError("Invalid model response operation evidence", decoded.failure.issue);

  return RecordEnvelope.make({
    recordId: modelResponseRecordId(input.runId, input.turn),
    family: "thread",
    schemaVersion: 1,
    createdAt: input.createdAt,
    deploymentId: input.deploymentId,
    payload: decoded.success,
  });
});

const toolSettledRecords = (
  input: TurnCommitInput,
): Result.Result<Array<RecordEnvelope>, RunJournalError> => {
  const toolRecords: Array<RecordEnvelope> = [];

  for (const part of input.toolResults) {
    const result = decodePersistedJson(part.result);

    if (Result.isFailure(result))
      return Result.fail(
        journalError(
          `Tool result ${part.toolCallId} exceeds canonical persistence bounds`,
          result.failure,
        ),
      );
    const decodedId = decodeToolCallId(part.toolCallId);

    if (Result.isFailure(decodedId))
      return Result.fail(
        journalError(`Invalid Tool Call ID ${part.toolCallId}`, decodedId.failure),
      );
    const toolCallId = decodedId.success;

    const payload = decodeSettledTool({
      _tag: "ToolCallSettled",
      ...(part.toolSelection === undefined ? {} : { toolSelection: part.toolSelection }),
      runId: input.runId,
      toolCallId,
      toolName: part.toolName,
      result: result.success,
      isFailure: part.isFailure,
      ...(part.budgetRejected === true ? { budgetRejected: true } : {}),
    });

    if (Result.isFailure(payload))
      return Result.fail(journalError(`Invalid Tool result ${part.toolCallId}`, payload.failure));
    toolRecords.push(
      RecordEnvelope.make({
        recordId: toolCallSettledRecordId(input.runId, input.turn, toolCallId),
        family: "thread",
        schemaVersion: 1,
        createdAt: input.createdAt,
        deploymentId: input.deploymentId,
        payload: payload.success,
      }),
    );
  }

  return Result.succeed(toolRecords);
};

/** Integrity of the original validated terminal values; never a current-code projection. */
export const runCompletionDigest = (
  completion: Pick<
    RunCompleted,
    "runId" | "output" | "runDisposition" | "finishReason" | "exhausted"
  >,
) =>
  digestJson({
    runId: completion.runId,
    output: completion.output,
    runDisposition: completion.runDisposition ?? null,
    finishReason: completion.finishReason ?? null,
    exhausted: completion.exhausted ?? null,
  });

const runCompletionRecord = Effect.fnUntraced(function* (input: TurnCommitInput) {
  if (input.runCompletion === undefined) return undefined;
  const completion = { runId: input.runId, ...input.runCompletion };

  return RecordEnvelope.make({
    recordId: runCompletedRecordId(input.runId),
    family: "thread",
    schemaVersion: 1,
    createdAt: input.createdAt,
    deploymentId: input.deploymentId,
    payload: RunCompleted.make({
      ...completion,
      resultDigest: yield* runCompletionDigest(completion),
    }),
  });
});

/**
 * Build a canonical batch from interpreter-validated Turn facts: one
 * `ModelResponseRecorded` record plus one `ToolCallSettled` record per terminal Tool result, all
 * under the WP0-style deterministic identities, committed as ONE atomic batch. The same input
 * always yields byte-identical content, so an in-Attempt append retry is an honest batch replay.
 *
 * No-tool Turns and eligible readonly Turns use this shape; a terminal `RunCompleted` marker
 * joins the response in the same atomic batch. Other application Turns split into
 * `turnResponseBatch` + `turnResultsBatch` before execution.
 */
export const turnCanonicalBatch = Effect.fnUntraced(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  const invalidTurn = requireCanonicalTurn(input.turn);

  if (invalidTurn !== undefined) return yield* invalidTurn;
  const modelResponse = yield* modelResponseRecord(input);
  const settled = toolSettledRecords(input);

  if (Result.isFailure(settled)) return yield* settled.failure;
  const toolRecords = settled.success;

  const completionRecord =
    input.runCompletion === undefined ? undefined : yield* runCompletionRecord(input);

  return CanonicalBatch.make({
    batchId: turnBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records:
      completionRecord === undefined
        ? [modelResponse, ...toolRecords]
        : [modelResponse, ...toolRecords, completionRecord],
  });
});

/**
 * Commit the normalized response and original operation contracts before application Tools
 * execute, or promote a deferred readonly response before a persisted call-scoped capability.
 * An unsettled application declaration is conservative uncertainty after ownership
 * loss; approvals and the dispatch fence remain separate execution requirements. Provider
 * results stay in assistant content. Application outcomes belong to the results commit.
 */
export const turnResponseBatch = Effect.fnUntraced(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  const invalidTurn = requireCanonicalTurn(input.turn);

  if (invalidTurn !== undefined) return yield* invalidTurn;
  const modelResponse = yield* modelResponseRecord(input);

  return CanonicalBatch.make({
    batchId: turnResponseBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records: [modelResponse],
  });
});

/**
 * Commit 5 of a tool-declaring Turn (plan §2.1): the Turn's `ToolCallSettled` records in
 * declaration order under batch identity `turn-results:{runId}:{turn}` — the batch becomes
 * model-visible atomically. A completion after already committed Tool results gets its own
 * terminal batch; it never rewrites or repeats the earlier Tool outcomes.
 */
export const turnResultsBatch = Effect.fnUntraced(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  const invalidTurn = requireCanonicalTurn(input.turn);

  if (invalidTurn !== undefined) return yield* invalidTurn;
  const settled = toolSettledRecords(input);

  if (Result.isFailure(settled)) return yield* settled.failure;
  const toolRecords = settled.success;
  const first = toolRecords[0];

  const completionRecord =
    input.runCompletion === undefined ? undefined : yield* runCompletionRecord(input);

  if (first === undefined) {
    if (completionRecord === undefined) {
      return yield* journalError(`Turn ${input.turn} has no terminal Tool results to commit`);
    }

    return CanonicalBatch.make({
      batchId: makeBatchId(runCompletedRecordId(input.runId)),
      producerId: input.producerId,
      records: [completionRecord],
    });
  }
  if (input.runCompletion !== undefined && toolRecords.length !== 1) {
    return yield* journalError("A terminal Tool completion requires exactly one settled result");
  }

  return CanonicalBatch.make({
    batchId: turnResultsBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records:
      completionRecord === undefined
        ? [first, ...toolRecords.slice(1)]
        : [first, ...toolRecords.slice(1), completionRecord],
  });
});
