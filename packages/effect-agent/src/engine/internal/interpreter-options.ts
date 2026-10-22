import type * as Prompt from "effect/ai/Prompt";
import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import type { Update, UpdateError } from "../../core/AgentUpdates.ts";
import type { ToolCallId, TurnId } from "../../core/Identifiers.ts";
import type { CompletionMetadata } from "../../core/RunEvent.ts";
import { RunPolicyUsage } from "../../core/RunPolicyUsage.ts";
import type {
  SubagentParentLink,
  SubagentGrant,
  SubagentBudgetReservation,
} from "../../core/SubagentContract.ts";
import { Selection, type Snapshot } from "../../core/ToolExposure.ts";
import type { ToolParameterRejection } from "../../core/ToolResult.ts";
import { ChildRunUsage, UsageCompleteness, type ModelCallUsage } from "../../core/Usage.ts";
import type { FrameworkMessage, WorkerBudgetScope } from "../../core/Worker.ts";
import type {
  RunTurnToolResult,
  RunSubagentEstablishRequest,
  ChildEstablishStatus,
  RunSubagentJoinRequest,
} from "../AgentRuntime.ts";
import type { RunStepHook } from "../DurableStep.ts";
import type { RunOptions, RunToolCallDescriptor } from "../RunOptions.ts";

/**
 * Validated response facts supplied by the interpreter. Messages include only this
 * response and its new leading instructions/inputs, never a reconstructed history suffix.
 * These are in-process values; the durable owner admits and captures them through its Schemas.
 * @internal
 */
export interface RunTurnResponse {
  /** Rejected fresh arguments; persist atomically with the response and restore on resume. */
  readonly toolParameterRejections?: ReadonlyArray<ToolParameterRejection> | undefined;
  readonly toolExposure?: Snapshot | undefined;
  readonly messages: ReadonlyArray<Prompt.Message>;
  readonly runScopedPrefixLength?: number | undefined;
  readonly calls: ReadonlyArray<RunToolCallDescriptor>;
}

/**
 * Encoded terminal values. They become authoritative after canonical acceptance.
 * @internal
 */
export interface RunTurnCompletion extends CompletionMetadata {
  readonly output: unknown;
  readonly runDisposition?: unknown;
}

interface RunTurnIdentity {
  readonly turn: number;
  readonly turnId: TurnId;
}

/**
 * The interpreter owns Turn progress; observations do not drive durable state.
 * Response acceptance precedes Tool dispatch. Settled commits contain closed outcomes;
 * an omitted response was already committed or restored. Partial commits preserve
 * closed siblings before child suspension, without declaring the whole Turn settled.
 * @internal
 */
export type RunTurnCommit = RunTurnIdentity &
  (
    | {
        readonly _tag: "Response";
        readonly response: RunTurnResponse;
        /**
         * The interpreter proved that the model's hosted Tools and every declared ordinary
         * application call are readonly, with no approval preflight. The coordinator may retain
         * an owned response until settlement, but must promote it before any persisted call-scoped
         * capability.
         */
        readonly defer?: true | undefined;
      }
    | {
        readonly _tag: "Settled";
        readonly response?: RunTurnResponse | undefined;
        readonly results: ReadonlyArray<RunTurnToolResult>;
        readonly completion?: RunTurnCompletion | undefined;
      }
    | { readonly _tag: "Partial"; readonly results: ReadonlyArray<RunTurnToolResult> }
  );

/**
 * One compaction decision the engine applied to its model-visible view
 * (RUN-026). The durable coordinator maps the covered source prefix to complete canonical
 * records, including settled current-Run batches for rollover. It must never infer a wider cutoff
 * from policy or token estimates.
 * @internal
 */
export interface RunCompactionCommit {
  readonly turn: number;
  /** Exact pre-compaction source and exclusive message bound; live values, never persisted. */
  readonly source: Prompt.Prompt;
  readonly through: number;
  readonly kind: "clear-tool-results" | "summarize" | "rollover";
  /** Present exactly when `kind` is `"summarize"`. */
  readonly summary?: string | undefined;
  /** Optional continuation state for a rollover; never a generated summary. */
  readonly handoff?: string | undefined;
  readonly tokensBeforeEstimate: number;
  readonly tokensAfterEstimate: number;
}

/**
 * One completed model call's provider-reported usage, staged for the Turn's canonical commit.
 * @internal
 */
export interface RunTurnUsage {
  readonly turn: number;
  readonly usage: ModelCallUsage;
}

/**
 * Attempt-local accounting for provider invocations without retained usage.
 * Staging is infallible and does not itself persist records. Durable runtime
 * composition supplies the canonical Turn accumulator; ephemeral entry points
 * explicitly supply the no-op implementation.
 * @internal
 */
export class ModelUsageAccounting extends Context.Service<
  ModelUsageAccounting,
  { readonly noteIncompleteUsage: (turn: number) => Effect.Effect<void> }
>()("@effect-agent/engine/ModelUsageAccounting") {
  static readonly layerEphemeral = Layer.succeed(ModelUsageAccounting, {
    noteIncompleteUsage: () => Effect.void,
  });
}

/**
 * Accepts an interpreter-validated update before semantic publication. Durable hosts bind this
 * port to the emitting Attempt and return its canonical identity and sequence. Infrastructure
 * failures must also halt that Attempt before any subsequent event is committed.
 * Ephemeral entry points explicitly accept only into their Run-local event stream.
 * @internal
 */
export class AgentUpdateAcceptance extends Context.Service<
  AgentUpdateAcceptance,
  { readonly accept: (update: Update) => Effect.Effect<Update, UpdateError> }
>()("@effect-agent/engine/AgentUpdateAcceptance") {
  static readonly layerEphemeral = Layer.succeed(AgentUpdateAcceptance, {
    accept: (update) => Effect.succeed(update),
  });
}

/**
 * Dependency-neutral durability seam implemented by a durable coordinator.
 *
 * Invocation ordering inside one Tool-declaring Turn is normative:
 * The Response commit fires after the provider stream closes and continuation validates,
 * but before approval preflight. Unless the interpreter permits deferral, the response becomes
 * canonical before any Tool work and conservatively records possible execution;
 * `checkToolDispatch` checks the writer fence after every approval and host authorization resolved
 * allowed and before any handler acquires a scheduler permit. It is skipped when no unfinished
 * call is a delegation or non-`readonly` ordinary call; `step` persists Durable Step
 * results mid-flight. When the hook is absent the engine behaves exactly as
 * the ephemeral runtime always has.
 * @internal
 */
export interface RunDurabilityHook<Error = never, Requirements = never> {
  /** Upper bound the coordinator can durably retain for one application Tool result. */
  readonly toolResultMaxBytes: number;
  /** Fail with retained infrastructure errors before the next execution or commit boundary. */
  readonly checkpoint: Effect.Effect<void, Error, Requirements>;
  /** Capture initial instruction/projection metadata once, before input/context preparation. */
  readonly initialize: (initial: {
    readonly initialHistory: Prompt.Prompt;
    readonly priorHistoryLength: number;
  }) => Effect.Effect<void, Error, Requirements>;
  /** Persist replacement count and staged usage after cancellation, before starting its successor. */
  readonly commitModelRestart?:
    | ((restart: {
        readonly turn: number;
        readonly turnId: TurnId;
        readonly restart: number;
      }) => Effect.Effect<void, Error, Requirements>)
    | undefined;
  /**
   * Reserve cumulative programmatic calls and grace finalization before external execution.
   * Calls are serialized across the Run. A committed reservation
   * is never refunded, even if ownership is lost before the Handler or model starts.
   * Coordinators must append fenced, schema-backed records before returning.
   */
  readonly reservePolicyUsage?:
    | ((
        usage: Pick<RunPolicyUsage, "programmaticToolCalls" | "finalizationUsed">,
      ) => Effect.Effect<void, Error, Requirements>)
    | undefined;
  /**
   * `committed` accepts the supplied facts canonically. `deferred` is valid only for a Response
   * with `defer: true`: validation and ownership succeeded, but no canonical acceptance occurred.
   * The interpreter retains its response and inputs until the Settled commit is accepted.
   * `capacity-refused` is valid only for a fresh Response rejected before any storage append or
   * Tool work. The interpreter settles every application call as not dispatched, atomically with
   * that response. A failed append or an already accepted response must never return this value.
   */
  readonly commitTurn: (
    commit: RunTurnCommit,
  ) => Effect.Effect<"committed" | "deferred" | "capacity-refused", Error, Requirements>;
  /** Check the writer fence after approval/authorization and before handler permits; no write. */
  readonly checkToolDispatch: Effect.Effect<void, Error, Requirements>;
  readonly step: RunStepHook<Error, Requirements>;
  /**
   * RUN-026: called at the pre-Turn seam BEFORE the engine applies a
   * compaction to its model-visible view or starts the model call whose
   * prompt reflects it, so a crash between the two resumes onto the compacted
   * projection. Required by the durability protocol: a coordinator that
   * silently dropped the record would let the engine use a compacted prompt
   * that recovery cannot reproduce. Reject unpersistable decisions in the typed
   * error channel; success means the same replacement and coverage are durable.
   */
  readonly commitCompaction: (
    commit: RunCompactionCommit,
  ) => Effect.Effect<void, Error, Requirements>;
  /**
   * RUN-023: stage one completed model call's usage for the Turn's canonical
   * commit (the response record carries it for resume re-seeding). Staging is
   * not itself a durable mutation, but the member is required by the
   * durability protocol: dropping it writes response records without the
   * usage a later Attempt needs, so ownership changes would silently reset
   * token budgets instead of failing closed.
   */
  readonly noteTurnUsage: (usage: RunTurnUsage) => Effect.Effect<void, Error, Requirements>;
}

/**
 * `DefinitionDigests`-shaped digests of one child Agent Binding in plain
 * string form. The durable coordinator's thread-owned digest Schema never
 * crosses inward: the coordinator captures these digests from the admitted target
 * and verifies them byte-for-byte as immutable provenance. Current executable
 * selection uses the stable Agent identity, independently of historical definitions.
 * @internal
 */
export interface RunSubagentDigests {
  readonly agent: string;
  readonly model: string;
  readonly tools: string;
}

/**
 * Dependency-neutral durable-Subagent seam implemented by a durable
 * coordinator (S2 plan §2 option c) and consumed by the delegation Tool
 * handler through the engine-provided per-batch `SubagentDurability` service.
 *
 * `establish` performs (or replays) child establishment
 * under the parent's ownership fence and reports where
 * the one child stands; `join` atomically commits the verified settlement
 * join. When the hook is absent the engine provides the explicit
 * ephemeral-mode service and the S1 in-process spawn semantics apply honestly
 * — absence means no durable claim is being made.
 * @internal
 */
export interface RunSubagentHook<Error = never, Requirements = never> {
  readonly establish: (
    request: RunSubagentEstablishRequest,
  ) => Effect.Effect<ChildEstablishStatus, Error, Requirements>;
  readonly join: (request: RunSubagentJoinRequest) => Effect.Effect<void, Error, Requirements>;
}

/**
 * One declared Tool Call of a Turn being resumed, in canonical encoded form.
 * @internal
 */
export interface RunTurnResumeCall {
  readonly id: string;
  readonly name: string;
  /** Canonical encoded parameters; re-validated through the Tool's parameter Schema before anything executes. */
  readonly params: unknown;
  /** Provider calls must have a canonical settled result and are never dispatched locally. */
  readonly providerExecuted?: boolean;
}

/**
 * One already-settled Tool Call of a Turn being resumed; injected without execution.
 *
 * The engine decodes this Schema at the recovery boundary before the value can
 * enter official history. The result remains the exact canonical JSON value
 * carried by the Tool message.
 * @internal
 */
export const RunTurnResumeSettledCallSchema = Schema.Struct({
  toolSelection: Schema.optionalKey(Selection),
  id: Schema.NonEmptyString,
  result: Schema.Json,
  isFailure: Schema.Boolean,
  /** Engine-owned rejection evidence; never inferred from the encoded result. */
  budgetRejected: Schema.optionalKey(Schema.Literal(true)),
});

/** @internal */
export type RunTurnResumeSettledCall = typeof RunTurnResumeSettledCallSchema.Type;

/**
 * Canonical cumulative usage and Stop Policy accounting restored from prior Attempts.
 * Turns and declared Tool calls include a pending resumed batch. The failure
 * streak excludes that entire batch, whose terminal outcomes the engine folds once.
 * Programmatic reservations include pending work and are never refunded.
 *
 * Counts and microdollars are non-negative safe integers. The most recent
 * call cannot exceed its cumulative total. Every committed Turn has a model
 * call, and a failure streak cannot exceed the declared Tool call count.
 * @internal
 */
export const RunResumeUsageSchema = Schema.Struct({
  ...RunPolicyUsage.fields,
  modelCalls: Schema.Natural,
  webSearchCalls: Schema.optionalKey(Schema.Natural),
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  lastInputTokens: Schema.Natural,
  lastOutputTokens: Schema.Natural,
  costMicrousd: Schema.Natural,
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(UsageCompleteness),
  unobservedModelCalls: Schema.optionalKey(Schema.Natural),
  /** Verified direct-child reports, deduplicated by Run ID across Attempts. */
  children: Schema.optionalKey(Schema.Array(ChildRunUsage)),
}).check(
  Schema.makeFilter(
    (usage) =>
      usage.lastInputTokens <= usage.inputTokens && usage.lastOutputTokens <= usage.outputTokens,
    {
      expected: "last-call token usage no greater than cumulative token usage",
    },
  ),
  Schema.makeFilter(
    (usage) =>
      usage.modelCalls >= usage.committedTurns && usage.consecutiveToolFailures <= usage.toolCalls,
    {
      expected:
        "model calls covering committed Turns and a failure streak within declared Tool calls",
    },
  ),
);

/** @internal */
export type RunResumeUsage = typeof RunResumeUsageSchema.Type;

/**
 * Resume one canonically declared Tool batch without re-invoking the model.
 *
 * When present, the engine's first Turn skips the model request entirely: the
 * executable calls are re-validated through their Tool parameter Schemas (a
 * decode failure executes nothing). Canonically rejected calls retain their native failure;
 * the rejection must match their exact arguments and any settled result. Approval preflight
 * for executable calls uses recorded decisions, host Tool authorization is re-evaluated,
 * and the writer fence is checked before dispatch. Calls listed in `settled` use final results without starting
 * their handlers, and only the remaining open calls execute. The Run then
 * proceeds through the normal continuation.
 * @internal
 */
export interface RunTurnResume {
  /** Original application-result bound promised by this canonical response. */
  readonly toolResultMaxBytes?: number | undefined;
  /** A successful settled call whose original operation contract still supports completion projection. */
  readonly settledCompletion?: ToolCallId | undefined;
  /** Canonical rejection evidence, matched to the exact call; never permits handler execution. */
  readonly toolParameterRejections?: ReadonlyArray<ToolParameterRejection> | undefined;
  readonly toolExposure?: Snapshot | undefined;
  readonly turn: number;
  readonly turnId: TurnId;
  readonly calls: ReadonlyArray<RunTurnResumeCall>;
  readonly settled: ReadonlyArray<RunTurnResumeSettledCall>;
  /**
   * The pending Turn's committed LEADING messages — the messages the durable
   * coordinator committed inside the pending Turn's canonical response record
   * BEFORE the assistant tool-call message (Turn-1 evaluated instructions +
   * input, or steering drained at the prior seam). A resumed Attempt's
   * canonical prompt boundary excludes the pending Turn entirely, so without
   * this field those messages would be absent from the resumed Run's live
   * model context. When present the engine threads them into official history
   * between the re-evaluated initial prompt and the rebuilt assistant
   * tool-call message. Optional: absent keeps the prior behavior byte-for-byte.
   */
  readonly leadingMessages?: Prompt.Prompt | undefined;
  /**
   * The pending Turn's canonical assistant response, including text, reasoning and provider
   * options. Durable hosts pass the decoded response after its leading messages. Execution
   * still uses the independently validated `calls`; this exact response enters history instead
   * of reconstructing it from call descriptors that cannot preserve every provider field.
   */
  readonly responseMessages?: Prompt.Prompt | undefined;
}

/**
 * Interpreter protocol supplied by the owning durable coordinator or delegation capability.
 * @internal
 */
export interface InterpreterOptions<HookError = never, HookRequirements = never> extends RunOptions<
  HookError,
  HookRequirements
> {
  /** Host-validated worker message; application input still supplies instructions and policy context. */
  readonly frameworkMessage?: FrameworkMessage;

  /**
   * Original admitted JSON for a durable continuation or verified framework message. Static
   * instructions can resume without decoding a future input Schema; input-dependent instructions
   * still require that Schema. The original value remains the policy input, and a saved prompt
   * is restored through `context` instead of rerendering the application's input prompt.
   */
  readonly retainedInput?: Schema.Json | undefined;

  /**
   * Canonical evaluated instructions and input messages of an unfinished Run. Reuse their
   * exact model context under a compatible current Binding; do not reevaluate instructions
   * or render the original input again. Input-dependent Bindings still decode retainedInput.
   */
  readonly retainedContext?: Prompt.Prompt | undefined;

  /** Canonically reconstructed window identity for a resumed durable Run. */
  readonly initialContextWindowId?: string | undefined;
  /** Last unconsumed, settled singleton application Tool in this Run; the engine verifies its control annotation. */
  readonly pendingContextToolCallId?: string | undefined;
  /** Canonical instruction/input block for durable recovery; not re-evaluated Attempt input. */
  readonly protectedContext?: Prompt.Prompt | undefined;
  /**
   * Non-model-visible Parent Link for a delegated child Run (S1 seam). It
   * never enters the model prompt or the Run's event payloads directly; the
   * engine uses it only to fix the Run's delegation depth (`parentLink.depth`
   * for a child, `0` when absent) exposed through the locally provided
   * `AgentSpawner`, and future durable work persists it as child lineage.
   */
  readonly parentLink?: SubagentParentLink | undefined;
  /** Durable background provenance supplies depth independently of attached parent linkage. */
  readonly delegationDepth?: number | undefined;
  /** Immutable narrowed authority restored for every delegated Attempt. */
  readonly subagentGrant?: SubagentGrant | undefined;
  /** Reserved subtree frame; descendants may spend only the residual after this Run's own ceiling. */
  readonly subagentBudget?: SubagentBudgetReservation | undefined;
  /** Native host funding provenance; never inferred from absent allocation amounts. */
  readonly subagentBudgetScope?: WorkerBudgetScope | undefined;
  /**
   * Actual wall-clock start of the logical Run, used for elapsed-time status.
   * Durable coordinators supply the canonical `RunStarted` record timestamp on
   * every replacement Attempt. It is independent from `durationDeadline`,
   * which may tighten the remaining allowance without changing how long the
   * Run has existed (RUN-024/RUN-030).
   */
  readonly runStartedAt?: DateTime.Utc | undefined;
  /**
   * Durable turn-commit seam (P5). When absent the engine behaves exactly as
   * the ephemeral runtime: no response/prepared commits, and `DurableStep`
   * executes pass-through.
   */
  readonly durability?: RunDurabilityHook<HookError, HookRequirements> | undefined;
  /**
   * Durable-Subagent establishment/join seam (S2). When present the engine's
   * per-batch `SubagentDurability` service runs in durable mode over this
   * hook; when absent the service is the explicit ephemeral-mode default and
   * delegation Tools keep their S1 in-process spawn semantics unchanged.
   */
  readonly subagent?: RunSubagentHook<HookError, HookRequirements> | undefined;
  /**
   * Resume a declared, canonically committed Tool batch without re-invoking
   * the model (durable batch-resume seam). Consumed by the Run's first Turn.
   * Requires `resumeUsage`; missing or contradictory accounting fails with
   * `ModelProtocolError` before input, model, or Tool execution.
   */
  readonly resume?: RunTurnResume | undefined;
  /**
   * Cumulative usage and Stop Policy accounting of the Run's prior Attempts,
   * projected from canonical records. A fresh continuation starts at
   * `committedTurns + 1`; a pending batch uses the already-counted declarations
   * and folds its terminal outcomes onto the prior failure streak once.
   * Omit only for fresh Runs. Incomplete or invalid seeds fail typed before execution.
   * For a pending batch, `committedTurns` must equal `resume.turn`, `toolCalls`
   * must include every pending declaration, and the failure streak cannot
   * exceed the declared calls before that batch.
   */
  readonly resumeUsage?: RunResumeUsage | undefined;
}
