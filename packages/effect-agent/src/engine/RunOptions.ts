import type * as LanguageModel from "effect/ai/LanguageModel";
import type * as Model from "effect/ai/Model";
import type * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import type * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { type AgentInputError, type AgentToolAuthorizationCheckError } from "../core/AgentError.ts";
import {
  type AgentId,
  type ThreadId,
  type RunId,
  type ToolCallId,
  type TurnId,
} from "../core/Identifiers.ts";
import { type MemoryRecallError } from "../core/MemoryReference.ts";
import { type ToolExecutionKind } from "../core/SubagentContract.ts";
import type { Selection } from "../core/ToolExposure.ts";
import type { ModelCallUsage } from "../core/Usage.ts";
import type { FrameworkMessage } from "../core/Worker.ts";
import type { CompactionError, ContextMessageTokenEstimator } from "./ContextCompactor.ts";
import type { ContextRolloverSelection, ModelCallContext } from "./ContextWindow.ts";
import type { ToolExecutionClassValue } from "./DurableStep.ts";

/** Live, trusted application diagnostics. Never persisted, transported, or automatically logged. */
interface ToolFailureIdentity {
  readonly agentId: AgentId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly toolName: string;
  /** Best-effort error tag, with the existing `UnknownError` fallback. */
  readonly tag: string;
}

/** A model-declared Handler returned a declared failure instead of failing the Run. */
export interface ModelToolFailure extends ToolFailureIdentity {
  readonly _tag: "ModelToolFailure";
  readonly kind: "declared-failure";
  /** Raw provider identity, without the telemetry ID filter. */
  readonly toolCallId: ToolCallId;
  readonly executionClass: ToolExecutionClassValue;
  readonly message?: never;
  readonly cause?: never;
}

interface ProgrammaticToolFailureIdentity extends ToolFailureIdentity {
  readonly _tag: "ProgrammaticToolFailure";
  /** `${parentToolCallId}#${sequenceIndex}`, raw and unique only within this in-memory pass. */
  readonly toolCallId: string;
  readonly parentToolCallId: ToolCallId;
  /** Presence means the Handler started and consumed budget; side effects may exist. */
  readonly sequenceIndex: number;
  readonly executionClass: ToolExecutionClassValue;
}

interface ProgrammaticDeclaredFailure extends ProgrammaticToolFailureIdentity {
  readonly kind: "declared-failure";
  readonly message?: never;
  readonly cause?: never;
}

interface ProgrammaticHandlerFailure extends ProgrammaticToolFailureIdentity {
  readonly kind: "handler-error";
  readonly message?: never;
  /** The original, uncollapsed Cause captured before the broker's diagnostic projection. */
  readonly cause: Cause.Cause<unknown>;
}

interface ProgrammaticDiagnosticFailure extends ProgrammaticToolFailureIdentity {
  readonly kind: "infrastructure" | "protocol";
  /** At most 4096 UTF-8 bytes. Never a declared payload. */
  readonly message: string;
  /** Original Cause when one exists; never fabricated from a source-less rejection. */
  readonly cause?: Cause.Cause<unknown> | undefined;
}

/** A programmatic Handler started and its failure became a broker outcome. */
export type ProgrammaticToolFailure =
  | ProgrammaticDeclaredFailure
  | ProgrammaticHandlerFailure
  | ProgrammaticDiagnosticFailure;

/** A programmatic invocation was rejected before its Handler started. No inner identity exists. */
export interface ProgrammaticPreflightFailure extends ToolFailureIdentity {
  readonly _tag: "ProgrammaticPreflightFailure";
  readonly kind: "infrastructure" | "protocol";
  readonly parentToolCallId: ToolCallId;
  /** Absent if the Tool could not be resolved. */
  readonly executionClass?: ToolExecutionClassValue | undefined;
  /** At most 4096 UTF-8 bytes. */
  readonly message: string;
  /** Original Cause only for Cause-backed rejection, never fabricated from an outcome. */
  readonly cause?: Cause.Cause<unknown> | undefined;
}

/** Plain readonly interfaces, intentionally not persisted or transported Schemas (RUN-036). */
export type ToolFailureObservation =
  | ModelToolFailure
  | ProgrammaticToolFailure
  | ProgrammaticPreflightFailure;

/**
 * Trusted in-process observation of non-propagating application Tool failures (RUN-036).
 * Capture reporting dependencies before installation. Delivery is inline under the existing
 * Tool permit for started calls; preflight reporting is serialized per broker. Delivery is at
 * most once per in-memory attempt, with isolated observer/reporter defects.
 * External interruption may end delivery. Replacement Attempts may repeat IDs and observations.
 * Never reenter ToolBroker, RunEventSink, or Agent execution, or intentionally self-interrupt.
 */
export interface RunToolFailureObserver {
  readonly observe: (observation: ToolFailureObservation) => Effect.Effect<void>;
}

/** Resolved once per Run; durable coordinators capture it at Layer acquisition. Default absent. */
export const CurrentToolFailureObserver = Context.Reference<RunToolFailureObserver | undefined>(
  "@effect-agent/engine/CurrentToolFailureObserver",
  { defaultValue: () => undefined },
);

/** The sole installation seam, shared by ephemeral Runs and durable platform options. */
export const toolFailureObserverLayer = (observer: RunToolFailureObserver): Layer.Layer<never> =>
  Layer.succeed(CurrentToolFailureObserver)(observer);

/** Number of queued inputs consumed at one documented Turn seam. */
export const CommandDrainPolicy = Schema.Literals(["one", "all"]);
export type CommandDrainPolicy = typeof CommandDrainPolicy.Type;

/** Engine-normalized input command. Capability packages may adapt richer audit records to it. */
export interface RunInputCommand {
  readonly kind: "steering" | "follow-up";
  readonly input: Prompt.RawInput;
}

/**
 * Dependency-neutral input seam used by the interpreter.
 *
 * A capability adapter owns queue bounds and Scope finalization. The engine calls
 * `start` before the first drain, `drain` only at safe Turn seams (including after
 * cancellation of a disposable model call), and `end`
 * exactly once when the Run leaves its Scope.
 */
export interface RunInputHook<Error = never, Requirements = never> {
  /**
   * Wait for an eligible join without appending it to history. Must register before checking
   * authority and retain any claimed input for `drain`, even when this wait is interrupted.
   * Used only with `restartOnJoinedInput`; each wait belongs to the model stream's Scope.
   */
  readonly awaitJoin?: Effect.Effect<void, Error, Requirements> | undefined;
  readonly start?: (() => Effect.Effect<void, Error, Requirements>) | undefined;
  readonly drain: (
    policy: CommandDrainPolicy,
  ) => Effect.Effect<ReadonlyArray<RunInputCommand>, Error, Requirements>;
  readonly end?: (() => Effect.Effect<void, never, Requirements>) | undefined;
}

/** Decision returned by an ephemeral approval adapter. */
export type RunApprovalDecision =
  | {
      readonly _tag: "approved";
      readonly reason?: string | undefined;
    }
  | {
      readonly _tag: "denied";
      readonly reason?: string | undefined;
    }
  | {
      readonly _tag: "unresolved";
      readonly reason?: string | undefined;
    };

/** Native Effect AI approval request enriched with stable Run identities. */
export interface RunApprovalRequest {
  readonly request: Response.ToolApprovalRequestPart;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly toolCallId: ToolCallId;
  readonly toolName: string;
  readonly parameters: unknown;
}

/** Approval policy preparation is deadline-bounded; canonical acceptance stays host-owned. */
export interface RunApprovalHook<Error = never, Requirements = never> {
  /** Retain a nonempty batch's required requests before any decision can release dispatch proof. */
  readonly prepareBatch?:
    | ((requests: ReadonlyArray<RunApprovalRequest>) => Effect.Effect<void, Error, Requirements>)
    | undefined;
  /** Prepare a decision without publishing canonical facts; this shares the Run duration deadline. */
  readonly request: (
    request: RunApprovalRequest,
  ) => Effect.Effect<RunApprovalDecision, Error, Requirements>;
  /** Accept the prepared decision canonically before the engine honors it, outside preparation timers. */
  readonly commit?:
    | ((
        request: RunApprovalRequest,
        decision: RunApprovalDecision,
      ) => Effect.Effect<void, Error, Requirements>)
    | undefined;
}

/** Input passed to ordered context transformation and compaction adapters. */
export interface RunContextRequest {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  /**
   * Official engine history for this Attempt before context preparation. The
   * engine never replaces or mutates this value. Durable prompt
   * reconstruction can produce a different model-visible basis afterward, so
   * adapters that need canonical retrieval should key it by the supplied Run
   * identities rather than infer durable state from this Prompt alone.
   */
  readonly source: Prompt.Prompt;
  /**
   * Durable coordinator's exact prior-Run prefix length in `source`. A preparation hook
   * can replace `source.content.slice(0, priorRunPrefixLength)` with application context
   * while preserving this Run's instructions, input, updates and recovered Tool exchanges.
   * This is a model-only transformation: canonical records and retry receipts are retained.
   * Absent for non-durable Runs; never infer this boundary from message roles or text.
   */
  readonly priorRunPrefixLength?: number | undefined;
  /**
   * The exact model-visible final-output contract the engine appends to the
   * prepared prompt after this hook returns (RUN-028), or
   * undefined when the definition's output Schema cannot render to JSON
   * Schema. Exposed so a limit-targeting adapter can reserve the contract's
   * overhead in its own window calculation; the hook can size for the
   * contract but cannot remove or alter it.
   */
  readonly outputContract?: string | undefined;
}

/**
 * One captured provider configuration and its context bounds. Build the native Model Layer
 * from the same resolved values as context; do not defer route selection inside its requests.
 * Capture provider requirements in the preparation Layer. The engine acquires this closed
 * Layer once per Turn and keeps it through compaction, dispatch, accounting, and overflow retry.
 */
export interface ResolvedModelCall {
  readonly model: Layer.Layer<LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName>;
  readonly context: ModelCallContext;
  /**
   * The selected provider's native Tool schema transformer, such as toCodecOpenAI or
   * toCodecAnthropic. Omission uses generic JSON Schema and is suitable only when the provider
   * does not rewrite schemas. Select it from the same captured configuration as the Model Layer.
   * Provider-defined Tools count their native name/configuration arguments instead; their
   * provider-generated call parameter schemas are never transformed into function declarations.
   */
  readonly toolSchemaTransformer?: LanguageModel.CodecTransformer | undefined;
  /**
   * Replace the entire estimate of a message, including its framing, with a non-negative finite
   * integer. Undefined uses the native structural estimate for that message. Capture any model
   * or content-specific reservation before returning this callback; it must be deterministic for
   * copied message content and same-Turn retries, without consulting mutable provider state.
   * The engine shares it across admission, built-in compaction sizing, and default summaries.
   * Do not also charge a replaced message through uncountedOverheadTokens.
   */
  readonly estimateMessageTokens?: ContextMessageTokenEstimator | undefined;
}

/** Prepared model-only context returned by a context adapter. */
export interface PreparedRunContext {
  /** Replace non-pinned registered native Tools at this Turn boundary. */
  readonly toolSelection?: Selection | undefined;
  readonly prompt: Prompt.Prompt;
  /** Resolves actual model selection and admission together, once at this Turn boundary. */
  readonly modelCall?: ResolvedModelCall | undefined;
  /**
   * Start a fresh native context window even below capacity. Coverage must map to a complete
   * canonical prefix for durable Runs and cannot discard protected input or split Tool pairs.
   * Omit through to select prior history before this Run's protected instructions/input; this
   * is a no-op when no prior prefix remains, including after a committed reset is recovered.
   * Leave the source prefix intact; the engine commits and applies the selected rollover.
   */
  readonly rollover?: ContextRolloverSelection | undefined;
}

/**
 * Model-only prompt transformation. Compaction requires a safely mapped original
 * instruction/input block and content-equivalent covered prefixes across Turns.
 * Incompatible changes fail with CompactionError before compaction or model I/O.
 */
export interface RunContextHook<Error = never, Requirements = never> {
  /**
   * Resources acquired by `prepare` belong to the current Turn and close before
   * the next Turn starts. Acquire resources shared across Turns in a surrounding
   * Run Layer or Scope instead.
   */
  readonly prepare: (
    request: RunContextRequest,
  ) => Effect.Effect<PreparedRunContext, Error, Requirements>;
}

/**
 * Supplies model-visible reference context for one Turn without changing the
 * prompt that compaction covers or the history that the engine commits.
 *
 * The engine treats the returned input as untrusted, validates it before
 * provider I/O, and includes it in the Turn's context and completion-reserve
 * admission. It never passes this input to the compaction summary Model. A
 * same-Turn provider-overflow retry reuses the loaded snapshot. Return an
 * empty Prompt when the Turn needs no references.
 */
export interface RunTransientContextHook<Error = never, Requirements = never> {
  readonly load: (
    request: RunContextRequest,
  ) => Effect.Effect<Prompt.RawInput, Error, Requirements>;
}

/** Context service failures retain their concrete tags and structured fields. */
export type RunContextPreparationError = AgentInputError | MemoryRecallError | CompactionError;

/**
 * One usage delta. Turn-boundary consumption charges `modelCalls: 1` after a
 * complete response and before any Tool starts; the programmatic Tool broker
 * charges `modelCalls: 0, toolCalls: 1` before each inner handler starts
 * (RUN-017).
 */
export interface RunUsageDelta {
  readonly modelCalls: 0 | 1;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly totalTokens: number;
  readonly toolCalls: number;
  readonly costMicrousd: number;
  readonly usage: Response.Usage;
  /** Present for model-call deltas; absent for programmatic Tool-only charges. */
  readonly modelUsage?: ModelCallUsage | undefined;
}

/** Stable pricing identity returned alongside a host's microdollar estimate. */
export interface RunCostEstimate {
  readonly costMicrousd: number;
  readonly serviceTier?: string | undefined;
  readonly pricingVersion?: string | undefined;
  /** Unknown estimates do not prove a free call and fail an explicit cost budget after usage is retained. */
  readonly pricingStatus?: "estimated" | "unknown" | undefined;
}

/** Provider usage and identity supplied for one model-call estimate. */
export interface RunCostEstimateRequest {
  /** Observed hosted web search calls, separately billed from tokens. */
  readonly webSearchCalls?: number | undefined;
  readonly provider: string;
  /** Configured binding identity; only response.model identifies the returned model. */
  readonly model: string;
  readonly usage: Response.Usage;
  /** Actual provider response fields, never the configured binding name. */
  readonly response?:
    | {
        readonly id?: string | undefined;
        readonly model?: string | undefined;
      }
    | undefined;
  /** Native Effect AI provider metadata, runtime-only; HTTP details are excluded. */
  readonly finishMetadata?: Response.FinishPart["metadata"] | undefined;
  readonly purpose?: "turn" | "summary" | undefined;
}

/**
 * Host-owned price lookup; durable hosts close every dependency before installing it.
 */
export type RunCostEstimator<Error = never, Requirements = never> = (
  request: RunCostEstimateRequest,
) => Effect.Effect<RunCostEstimate, Error, Requirements>;

/**
 * Dependency-neutral hierarchical budget hook. A typed failure at a Turn-seam
 * consumption or a stream-guard pull stops the Run. A mid-pass programmatic
 * consumption failure instead becomes that call's outcome (RUN-017 —
 * exhaustion prevents the call): the Run still stops at the next Turn seam,
 * because the following model call's `consume` and the guarded stream pulls
 * re-enforce the same budget.
 */
export interface RunBudgetHook<Error = never, Requirements = never> {
  /**
   * Guard one active model or Tool stream pull. Hierarchical budget adapters
   * use an absolute deadline so repeated pulls share one duration allowance.
   */
  readonly guard: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | Error, R | Requirements>;
  readonly consume: (delta: RunUsageDelta) => Effect.Effect<void, Error, Requirements>;
}

/**
 * One application Tool Call of a completed Turn as the durable runtime sees
 * it: stable identity, the encoded (wire-form) parameters exactly as official
 * history carries them, and the Tool's declared execution class (fail-closed
 * `"uncertain"` for unannotated Tools).
 */
export interface RunToolCallDescriptor {
  readonly toolCallId: ToolCallId;
  readonly toolName: string;
  /** Encoded JSON parameters — the same value official history and canonical records carry. */
  readonly parameters: unknown;
  readonly executionClass: ToolExecutionClassValue;
  /** Definition-owned classification; never inferred from the Tool name. */
  readonly executionKind: ToolExecutionKind;
}

/** Decision returned by a host's action-time Tool authorization policy. */
export type RunToolAuthorizationDecision =
  | { readonly _tag: "allowed" }
  | {
      readonly _tag: "denied";
      /** Safe model-facing explanation. */
      readonly reason: string;
      /** Original local policy evidence; retained privately at native Run settlement. */
      readonly cause?: Cause.Cause<unknown>;
    };

/**
 * Exact authority presented before one application Tool Handler may start.
 *
 * `input` is the Agent Schema's encoded Run input. Durable coordinators replace it with the exact
 * canonical Submission input admitted for the logical Run on every Attempt.
 * `call` is the exact still-executable call being authorized. Recorded settled calls are never
 * reauthorized because no Handler can start for them.
 */
export interface RunToolAuthorizationRequest {
  readonly frameworkMessage?: FrameworkMessage;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  readonly input: unknown;
  readonly call: RunToolCallDescriptor;
  /** Present for an ephemeral inner invocation; the outer Tool's approval grants no inner authority. */
  readonly programmatic?:
    | {
        readonly parentToolCallId: ToolCallId;
        readonly sequenceIndex: number;
      }
    | undefined;
}

/** Host policy invoked before each executable native or programmatic application call. */
export interface RunToolAuthorizationHook<Error = never, Requirements = never> {
  readonly authorize: (
    request: RunToolAuthorizationRequest,
  ) => Effect.Effect<RunToolAuthorizationDecision, Error, Requirements>;
}

/**
 * Host-owned model-context preparation, independent of action-time Tool authorization.
 *
 * Runs use this service when provided; durable coordinators capture it while their runtime
 * Layer is acquired. Implementations acquire dependencies in their Layer and preserve the
 * declared error tags. Layer acquisition failures belong to the providing Effect, not this union.
 * Without this service, Runs apply no host context loading. Provide `ContextCompactor`
 * separately to select native compaction; neither service replaces `RunToolAuthorization`.
 */
export class RunContextPreparation extends Context.Service<
  RunContextPreparation,
  {
    /** Optional transformation of the model-visible prompt. */
    readonly hook?: RunContextHook<RunContextPreparationError, never> | undefined;
    /** Optional per-Turn reference context, excluded from history and compaction coverage. */
    readonly transientContext?:
      | RunTransientContextHook<RunContextPreparationError, never>
      | undefined;
  }
>()("@effect-agent/engine/RunContextPreparation") {}

/** Explicit no-preparer Layer used by compatible runtime assemblies. */
export const RunContextPreparationPassthrough: Layer.Layer<RunContextPreparation> = Layer.succeed(
  RunContextPreparation,
)({});

/**
 * Host action-time authority for native and programmatic application Tools. Implementations close over
 * their dependencies at Layer construction and return a denial when execution is not authorized.
 * A dependency or validation failure instead fails with AgentToolAuthorizationCheckError and
 * retains its original Cause. Defects and interruption remain in the Effect Cause channel.
 * Durable coordinators capture this service once and retain it across replacement Attempts.
 * Ephemeral Runs also resolve this service at their Run boundary. A typed per-run
 * `RunOptions.toolAuthorization` overrides it while retaining its own error and requirement channel.
 */
export class RunToolAuthorization extends Context.Service<
  RunToolAuthorization,
  RunToolAuthorizationHook<AgentToolAuthorizationCheckError>
>()("@effect-agent/engine/RunToolAuthorization") {
  /** Immutable built-in policy; its identity lets the interpreter omit absent host checks. */
  static readonly allowAllPolicy: RunToolAuthorizationHook = Object.freeze({
    authorize: () => Effect.succeed({ _tag: "allowed" as const }),
  });

  /** Explicit compatibility policy: Tool execution requires no additional host authorization. */
  static readonly allowAll: Layer.Layer<RunToolAuthorization> = Layer.succeed(
    RunToolAuthorization,
    RunToolAuthorization.allowAllPolicy,
  );
}

/** Positive concurrency ceiling for one Run. One is sequential; the Agent's bound can only tighten. */
export const RunSchedulingOverride = Schema.Int.check(Schema.isGreaterThan(0));

export type RunSchedulingOverride = typeof RunSchedulingOverride.Type;

/** Dependency-neutral scheduling policy surrounding native Effect AI Tool handlers. */
export interface RunSchedulingHook {
  readonly runOverride?: RunSchedulingOverride | undefined;
  readonly toolRequiresSequential?: ((toolName: string) => boolean) | undefined;
}

/**
 * Host Tool scheduling, captured by durable runtimes across replacement Attempts.
 * The Agent's admitted concurrency remains the upper bound. A sequential Tool
 * forms a barrier around neighboring parallel batches, including on recovery.
 * Ephemeral Runs may override this reference with `RunOptions.scheduling`.
 */
export const RunToolScheduling = Context.Reference<RunSchedulingHook>(
  "@effect-agent/engine/RunToolScheduling",
  { defaultValue: () => ({}) },
);

/**
 * Tightening-only memory limits for one Run. The engine supplies finite ceilings for every field;
 * callers may lower them for a deployment or test but cannot widen the engine defaults.
 */
export interface RunBufferLimits {
  /** Maximum decoded response parts retained from one model call, including compaction calls. */
  readonly maxModelResponseParts?: number | undefined;
  /** Maximum conservative retained-byte estimate for one model response. */
  readonly maxModelResponseBytes?: number | undefined;
  /** Maximum observed Run events, including the terminal event; headless execution has no event cap. */
  readonly maxRunEvents?: number | undefined;
  /**
   * Maximum cumulative UTF-8 JSON bytes in Tool progress, including unobserved and provider
   * progress. Defaults to 8 MiB. Invalid or oversized application progress
   * fails with ModelProtocolError; terminal Tool results use the Agent's toolResultBounds.
   */
  readonly maxToolProgressBytes?: number | undefined;
  /** Maximum events buffered by the public stream before its execution producer waits. */
  readonly maxBufferedEvents?: number | undefined;
}

/**
 * Application-owned values and hooks for one Run. ThreadHistory.layer retains history
 * incrementally in memory, including completed updates before a failure or interruption.
 * PersistentHistory.layer commits only successful Runs to a ThreadStore. Hook failures and
 * requirements stay visible in the returned Stream / Effect through the generic parameters.
 */
export interface RunOptions<HookError = never, HookRequirements = never> {
  /** Finite per-Run limits: accepted update count and cumulative UTF-8 JSON value bytes. Defaults: 32 and 16384. */
  readonly updates?: { readonly maxCount?: number; readonly maxBytes?: number };

  /** Initial or canonically restored run-scoped native selection. */
  readonly toolSelection?: Selection | undefined;
  /**
   * Host preparation boundary before each new or replacement model call, including context preparation
   * and compaction calls. The preceding Tool batch and history advance have finished. A resumed
   * canonical Tool batch bypasses this hook until it continues to a new Turn. This hook does not
   * reset the Run deadline or change prompt protection, and is not automatically inherited by spawned children.
   * Resources acquired here close with the current Turn, before the next Turn starts.
   */
  readonly beforeTurn?: (() => Effect.Effect<void, HookError, HookRequirements>) | undefined;
  /** Reuse a Thread identity, including retained history, instead of allocating one. */
  readonly threadId?: ThreadId | undefined;
  /**
   * Preallocated Run identity used instead of `IdGenerator` when supplied.
   * The S1 Subagent seam preallocates child Run identity through this option
   * so `SubagentRequested` can carry the intended child identity.
   */
  readonly runId?: RunId | undefined;

  /**
   * Seed or append-only extension for an in-memory Thread. ThreadHistory.layer retains this
   * prefix before execution and rejects replacement of existing messages. The engine appends
   * this Run's evaluated instructions and rendered input. PersistentHistory rejects this option;
   * durable hosts supply the history reconstructed from their journal.
   */
  readonly history?: Prompt.Prompt | undefined;
  readonly commandDrainPolicy?: CommandDrainPolicy | undefined;
  readonly input?: RunInputHook<HookError, HookRequirements> | undefined;
  readonly approval?: RunApprovalHook<HookError, HookRequirements> | undefined;
  /** Per-Run override of RunContextPreparation.hook, preserving application-specific E/R. */
  readonly context?: RunContextHook<HookError, HookRequirements> | undefined;
  /**
   * Per-Run override of RunContextPreparation.transientContext. Use the service
   * for host configuration; this generic hook preserves application-specific E/R.
   * Model-visible reference context is loaded for each Turn, including a grace
   * finalization Turn and a Turn reconstructed by durable recovery. The engine
   * validates and budgets the result but never adds it to official history or
   * compaction coverage. A same-Turn provider-overflow retry reuses the Turn's
   * loaded snapshot.
   */
  readonly transientContext?: RunTransientContextHook<HookError, HookRequirements> | undefined;
  readonly budget?: RunBudgetHook<HookError, HookRequirements> | undefined;
  /**
   * Host-owned action-time authorization for model-declared application Tool batches. The engine
   * uses this per-Run override when present, otherwise the provided RunToolAuthorization service.
   * It invokes the policy for every still-executable call after complete-batch validation and approval, but
   * before the durable dispatch fence or any Handler permit. A resumed durable batch invokes it again
   * with the same canonical Run/Turn/input authority and Tool Call identity. Programmatic
   * `ToolBroker` calls invoke it after schema/visibility checks and before budget reservation or
   * execution, with their parent identity in `programmatic`. Inner denials become catchable
   * outcomes; other independent calls may already have completed.
   */
  readonly toolAuthorization?: RunToolAuthorizationHook<HookError, HookRequirements> | undefined;
  /**
   * Optional absolute deadline for the Run's `maxDuration` rail. The engine
   * uses the earlier of this value and the fresh policy deadline, so callers
   * may preserve or tighten an existing Run allowance but can never widen it.
   * Durable coordinators reuse the canonical per-Attempt duration allowance;
   * deployment downtime does not consume it. An immutable worker grant can
   * impose an earlier absolute expiry. Turn, Tool and cost budgets remain Run-wide.
   */
  readonly durationDeadline?: DateTime.Utc | undefined;
  /**
   * Per-Run Tool Call allowance (RUN-021): a TIGHTENING-ONLY bound below the
   * Agent Policy's `maxToolCalls` — the effective limit is
   * `min(policy.maxToolCalls, max(1, floor(toolCallAllowance)))`, so an
   * allowance can never widen the Definition's ceiling. The `onExhaustion`
   * resolution (RUN-018) keys off the effective limit, which is how an
   * orchestrator grants a delegated child a budget extension by re-invoking
   * with a larger allowance up to the child Definition's policy.
   */
  readonly toolCallAllowance?: number | undefined;
  /**
   * Per-Run Turn allowance (RUN-021): tightening-only below the Agent
   * Policy's `maxTurns`, with the same normalization and the same
   * `onExhaustion` resolution (RUN-019 grace) at the effective limit.
   */
  readonly turnAllowance?: number | undefined;
  /** Required when the core policy declares `costBudgetMicrousd`. */
  readonly estimateCostMicrousd?: RunCostEstimator<HookError, HookRequirements> | undefined;
  readonly scheduling?: RunSchedulingHook | undefined;
  /** Optional tightening-only overrides for the engine's finite in-memory buffer ceilings. */
  readonly bufferLimits?: RunBufferLimits | undefined;
  /**
   * Incremental history observer. ThreadHistory.layer retains each complete update first.
   * Invoked inline with the source Prompt and its retained compacted model context whenever
   * official history advances, including initial
   * instructions/input before the first model call. It can write before the Run succeeds or its
   * resources close. Failure stops execution through HookError; defects and interruption propagate.
   * Earlier callback writes are caller-owned and are not rolled back if this or a later step fails.
   * PersistentHistory's on-success adapter rejects this hook;
   * the durable coordinator uses it for live Prompt state while its journal owns durable commits.
   */
  readonly onHistory?:
    | ((
        history: Prompt.Prompt,
        modelContext: Prompt.Prompt,
      ) => Effect.Effect<void, HookError, HookRequirements>)
    | undefined;
}

export type RunOptionsError<Options> =
  Options extends RunOptions<infer Error, infer _Requirements> ? Error : never;

export type RunOptionsRequirements<Options> =
  Options extends RunOptions<infer _Error, infer Requirements> ? Requirements : never;
