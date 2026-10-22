/**
 * Execute Agent definitions and bindings as scoped Effects or event streams, preserving typed
 * failures and service requirements.
 *
 * @since 0.1.0
 */

import type * as LanguageModel from "effect/ai/LanguageModel";
import type * as Model from "effect/ai/Model";
import type * as Tool from "effect/ai/Tool";
import type * as Toolkit from "effect/ai/Toolkit";
import type * as DateTime from "effect/DateTime";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import type * as Agent from "../core/Agent.ts";
import type {
  Definition,
  InputPromptSource,
  InstructionSource,
  RunDispositionDeclaration,
} from "../core/Agent.ts";
import type {
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentPersistenceCapacityError,
  AgentToolAuthorizationDenied,
  AgentToolAuthorizationCheckError,
  AgentPolicyError,
  ContextBudgetError,
  ContextOverflowError,
  ModelProtocolError,
} from "../core/AgentError.ts";
import type { AgentPolicy } from "../core/AgentPolicy.ts";
import type { Emitter, UpdateError } from "../core/AgentUpdates.ts";
import type {
  AgentId,
  ThreadId,
  DelegationId,
  ReceiptId,
  SubmissionId,
  RunId,
  ToolCallId,
} from "../core/Identifiers.ts";
import type { RunEvent } from "../core/RunEvent.ts";
import type {
  DelegationDepth,
  SubagentGrant,
  SubagentBudgetReservation,
  SubagentParentLink,
} from "../core/SubagentContract.ts";
import type { Selection } from "../core/ToolExposure.ts";
import type { RunTotals, RunUsageReport } from "../core/Usage.ts";
import type { WorkerBudgetScope } from "../core/Worker.ts";
import type { CompactionError } from "./ContextCompactor.ts";
import type { ContextWindow } from "./ContextWindow.ts";
import type { DurableStep } from "./DurableStep.ts";
import * as Internal from "./internal/agent-runtime.ts";
import type {
  AgentResultSchema,
  AgentChildPending,
  AgentSpawner,
  SubagentDurability,
  SubagentDurabilityError,
  ToolCallWaiting,
} from "./internal/agent-runtime.ts";
import type { MessagingHost } from "./MessagingHost.ts";
import type { RunEventSink } from "./RunEventSink.ts";
import type {
  RunOptions,
  RunContextPreparation,
  RunContextPreparationError,
} from "./RunOptions.ts";
import type { SubagentHost } from "./SubagentHost.ts";
import type { ThreadHistory, ThreadHistoryError } from "./ThreadHistory.ts";
import type { ToolBroker } from "./ToolBroker.ts";
import type { CurrentToolCatalog } from "./ToolExposure.ts";

export {
  /**
   * Schema factory for the terminal value produced by reducing a completed agent event stream.
   *
   * @category operations
   * @since 0.1.0
   */
  AgentResultSchema,
} from "./internal/agent-runtime.ts";

export {
  /**
   * The Run suspended `waitingForChild`: at least one durable delegation call
   * of the last Tool batch is waiting on its attached child.
   * Mirrors `AgentApprovalPending`: the engine emits
   * `RunSuspended` and then fails the Run stream with this error; the durable
   * coordinator catches it and ends the Attempt's ownership period without
   * settling. `children` is listed in declaration order, deterministically.
   *
   * @category errors
   * @since 0.1.0
   */
  AgentChildPending,
} from "./internal/agent-runtime.ts";

export {
  /**
   * Engine-owned service contract through which a declared delegation Tool
   * runs an Attached Child on the same interpreter.
   *
   * The engine provides this service locally to every Run, bound to the Run's
   * immutable identity and delegation depth; it is never satisfied from an
   * application Layer, is excluded from the runtime's public requirements, and
   * exposes neither the engine's mutable Run state nor any root Layer Context.
   *
   * @category services
   * @since 0.1.0
   */
  AgentSpawner,
} from "./internal/agent-runtime.ts";

export {
  /**
   * Engine-owned durable-Subagent seam provided locally to every Tool batch,
   * constructed from `InterpreterOptions.subagent` when present and the explicit
   * ephemeral-mode default when absent. Like the other engine-provided Tool
   * services it is excluded from the runtime's public requirements and MUST NOT
   * be satisfied from an application Layer; handlers resolve it per call (the
   * S1 innermost re-provide pattern) so a Layer built inside another Run's
   * batch never captures a stale mode.
   *
   * @category services
   * @since 0.1.0
   */
  SubagentDurability,
} from "./internal/agent-runtime.ts";

export {
  /**
   * Typed failure of the engine-provided `SubagentDurability` operations.
   * `hook-failed` wraps a coordinator hook failure without leaking the hook's
   * error type into handler signatures (the `DurableStepError` precedent);
   * `no-active-tool-batch` is the fail-closed Run-level default — establishment
   * outside an active Tool batch never silently degrades to ephemeral spawning.
   *
   * @category errors
   * @since 0.1.0
   */
  SubagentDurabilityError,
} from "./internal/agent-runtime.ts";

export {
  /**
   * Typed engine-owned suspension signal raised by a delegation Tool handler
   * through the per-batch `SubagentDurability` service when its durable child
   * is established but not settled (S2 plan §2). It is NOT a Tool failure: the
   * batch executor treats the raising call as "stays open" — no terminal
   * result, no `ToolCallFailed`, no batch failure policy, no sibling
   * interruption — and terminates the Run with `AgentChildPending` after every
   * non-waiting sibling handler settled.
   *
   * @category errors
   * @since 0.1.0
   */
  ToolCallWaiting,
} from "./internal/agent-runtime.ts";

export {
  /**
   * Render decoded Agent input for model visibility, preserving the legacy JSON default.
   *
   * @category operations
   * @since 0.1.0
   */
  renderInputPrompt,
} from "./internal/agent-runtime.ts";

/**
 * A runtime-ready Agent definition preserving the instruction source’s typed errors and service
 * requirements.
 *
 * @category utility types
 * @since 0.1.0
 */
export type RuntimeProgram<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
  InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
    undefined,
  UpdatesSchema extends Schema.Top | undefined = undefined,
> = {
  readonly definition: Definition<
    InputSchema,
    OutputSchema,
    Instructions,
    Toolkit.Toolkit<Tools>,
    RunDispositionValue,
    InputPromptValue,
    UpdatesSchema
  > & {
    readonly instructions: InstructionSource<
      InputSchema["Type"],
      NoInfer<InstructionError>,
      NoInfer<InstructionRequirements>
    >;
    readonly inputPrompt?: InputPromptValue | undefined;
  };
};

/**
 * Structural shape of an Agent Binding accepted by the interpreter and by
 * `AgentSpawner.spawn`: a model-agnostic Definition paired with an explicit
 * native model Layer whose requirements stay visible. The Layer must provide
 * LanguageModel, ProviderName, and ModelName, including after requirement capture.
 *
 * @category utility types
 * @since 0.1.0
 */
export type RuntimeBinding<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
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
> = RuntimeProgram<
  InputSchema,
  OutputSchema,
  Instructions,
  Tools,
  InstructionError,
  InstructionRequirements,
  RunDispositionValue,
  InputPromptValue,
  UpdatesSchema
> & {
  readonly model: Layer.Layer<
    LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName | ModelProvides,
    never,
    ModelRequires
  >;
};

type InstructionResultOf<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;

/**
 * Extract the typed failure of an effectful instruction source; pure instructions contribute no
 * failure.
 *
 * @category utility types
 * @since 0.1.0
 */
export type InstructionErrorOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer Error,
    infer _Requirements
  >
    ? Error
    : never;

/**
 * Extract the services required by an effectful instruction source; pure instructions require none.
 *
 * @category utility types
 * @since 0.1.0
 */
export type InstructionRequirementsOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer _Error,
    infer Requirements
  >
    ? Requirements
    : never;

/**
 * Decoded terminal value produced by reducing a completed agent event stream.
 *
 * @category utility types
 * @since 0.1.0
 */
export type AgentResult<Output> = ReturnType<
  typeof AgentResultSchema<Schema.Schema<Output>>
>["Type"];

/**
 * Expected agent, policy, and model-protocol failures exposed by the runtime.
 *
 * @category utility types
 * @since 0.1.0
 */
export type AgentRuntimeFailure<
  AgentValue extends Agent.AnyDefinition | Agent.Any,
  HookError = never,
  InstructionError = never,
> =
  | Agent.Failure<AgentValue>
  | ([Agent.UpdatesSchema<AgentValue>] extends [never] ? never : UpdateError)
  | AgentPolicyError
  | ContextBudgetError
  | ContextOverflowError
  | CompactionError
  | RunContextPreparationError
  | AgentToolAuthorizationCheckError
  | ModelProtocolError
  | AgentPersistenceCapacityError
  | AgentApprovalDenied
  | AgentToolAuthorizationDenied
  | AgentApprovalPending
  | AgentChildPending
  | ThreadHistoryError
  | HookError
  | InstructionError;

/**
 * Services the engine itself provides locally to every Run: `AgentSpawner`
 * bound to the Run's immutable identity and delegation depth, `RunEventSink`
 * and `SubagentDurability` bound to the active Tool batch, `DurableStep`
 * bound to the active Tool Call. They are excluded from the runtime's public
 * requirements and must not be satisfied from an application Layer.
 *
 * @category models
 * @since 0.1.0
 */
export type EngineProvidedToolServices =
  | Emitter
  | AgentSpawner
  | RunEventSink
  | DurableStep
  | SubagentDurability
  | SubagentHost
  | MessagingHost
  | ToolBroker
  | ContextWindow
  | CurrentToolCatalog;

/**
 * Schema services needed to reconstruct a completion Tool's canonical Agent output.
 *
 * @category utility types
 * @since 0.1.0
 */
export type AgentCompletionProjectionRequirements<
  AgentValue extends Agent.AnyDefinition | Agent.Any,
> =
  | Agent.OutputSchema<AgentValue>["DecodingServices"]
  | Agent.OutputSchema<AgentValue>["EncodingServices"]
  | Tool.ParametersSchema<Agent.ToolUnion<AgentValue>>["DecodingServices"]
  | Tool.SuccessSchema<Agent.ToolUnion<AgentValue>>["DecodingServices"];

/**
 * Inferred agent services plus the runtime's identity and Thread history authorities.
 * Engine-provided Tool handler services are excluded because the interpreter
 * supplies them itself, bound to the current Run's identity. Tool parameter
 * encoding, output decoding, and completion-projection Schema services stay
 * listed unexcluded: the interpreter records canonical parameters before the
 * handler boundary, `run`/`start` re-decode terminal output, and durable recovery
 * re-decodes canonical completion Tool parameters/results.
 *
 * @category utility types
 * @since 0.1.0
 */
export type AgentRuntimeRequirements<
  AgentValue extends Agent.AnyDefinition | Agent.Any,
  HookRequirements = never,
  InstructionRequirements = never,
> =
  | Exclude<Agent.Requirements<AgentValue>, EngineProvidedToolServices>
  | Tool.ParametersSchema<Agent.ToolUnion<AgentValue>>["EncodingServices"]
  | AgentCompletionProjectionRequirements<AgentValue>
  | Agent.OutputSchema<AgentValue>["DecodingServices"]
  | ThreadHistory
  | HookRequirements
  | InstructionRequirements;

/**
 * Scoped detached execution whose observers cannot backpressure Run
 * completion.
 *
 * `observe` is a live multicast subscription: each subscription replays every
 * event the Run has already emitted, follows subsequent events as they occur,
 * and ends once the Run settles. The finite Run event ceiling sizes the replay
 * buffer so events are never dropped for a slow subscriber and publishing
 * never blocks the Run. `events` remains the complete bounded replay,
 * available after settlement. Both belong to the `start` Scope; observing
 * after that Scope closes interrupts the observer.
 *
 * @category models
 * @since 0.1.0
 */
export interface DetachedRun<Output, Error> {
  readonly await: Effect.Effect<AgentResult<Output>, Error>;
  readonly events: Effect.Effect<ReadonlyArray<RunEvent>>;
  readonly observe: Stream.Stream<RunEvent>;
  /**
   * Snapshot of observed usage, also available after failure, defect, or interruption. Read after
   * await settles (or the owning Scope closes) for a final report. In-flight work may be unpriced. */
  readonly usageReport: Effect.Effect<RunUsageReport>;
}

/** Executable definitions or explicit per-agent model Layers for registration and delegation. */
type ExecutableDefinition = Agent.AnyDefinition & {
  readonly instructions: InstructionSource<never, unknown, unknown>;
};

/**
 * Structural input accepted by runtime dispatch: an Agent definition or a definition paired with
 * its native model Layer.
 *
 * @category models
 * @since 0.1.0
 */
export type ExecutableAgent =
  | ExecutableDefinition
  | {
      readonly definition: ExecutableDefinition;
      readonly model: Layer.Layer<
        LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
        never,
        unknown
      >;
    };

/**
 * Immutable parent Run identity carried by the locally provided
 * `AgentSpawner`. It exposes no mutable engine state and no Layer Context.
 *
 * @category models
 * @since 0.1.0
 */
export interface AgentSpawnerParent {
  readonly agentId: AgentId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
}

/**
 * Explicit ephemeral mode: no durable coordinator supplied
 * `InterpreterOptions.subagent`, so no durable claim is being made and the delegation
 * handler keeps the S1 in-process spawn semantics honestly.
 *
 * @category models
 * @since 0.1.0
 */
export interface SubagentDurabilityEphemeral {
  readonly mode: "ephemeral";
}

/**
 * Durable mode: establishment and join run through the coordinator's
 * `RunSubagentHook` under the parent's ownership fence, and `waiting` raises
 * the engine-owned suspension signal for a still-running child.
 *
 * @category models
 * @since 0.1.0
 */
export interface SubagentDurabilityDurable {
  readonly mode: "durable";
  /** Idempotent durable child establishment under the parent ownership fence. */
  readonly establish: (
    request: RunSubagentEstablishRequest,
  ) => Effect.Effect<ChildEstablishStatus, SubagentDurabilityError>;
  /** Atomic settlement join: `SubagentJoined` + parent `ToolCallSettled` in one canonical batch (SUB-019). */
  readonly join: (request: RunSubagentJoinRequest) => Effect.Effect<void, SubagentDurabilityError>;
  /**
   * Raise the waiting suspension signal for one delegation call whose
   * established child has not settled. Never returns: the handler ends here,
   * siblings run to completion, the Run suspends, and the resumed batch
   * re-executes the handler idempotently.
   */
  readonly waiting: (
    toolCallId: ToolCallId,
    child: RunSubagentChildIdentity,
  ) => Effect.Effect<never, ToolCallWaiting | SubagentDurabilityError>;
}

/**
 * Mode-dispatched service value seen by delegation Tool handlers: an explicit
 * ephemeral default when the Run carries no durable coordinator, the durable
 * establish/join/waiting surface otherwise.
 *
 * @category models
 * @since 0.1.0
 */
export type SubagentDurabilityService = SubagentDurabilityEphemeral | SubagentDurabilityDurable;

/**
 * Stable delegation identity supplied by the invoking delegation Tool handler.
 *
 * @category models
 * @since 0.1.0
 */
export interface SpawnDelegation {
  readonly delegationId: DelegationId;
  readonly parentToolCallId: ToolCallId;
}

/**
 * Child Run options accepted by `AgentSpawner.spawn`. Child Thread/Run
 * identity and the Parent Link are spawner-owned and cannot be overridden.
 *
 * @category models
 * @since 0.1.0
 */
export interface SpawnRunOptions<HookError = never, HookRequirements = never> extends Omit<
  RunOptions<HookError, HookRequirements>,
  "threadId" | "runId"
> {}

/**
 * Handle to one spawned Attached Child: its preallocated child identity, its
 * immutable Parent Link, and the same observation surface as `DetachedRun`.
 * The child fiber belongs to the Scope the caller provided to `spawn`.
 *
 * @category models
 * @since 0.1.0
 */
export interface SpawnedChildRun<Output, Error> extends DetachedRun<Output, Error> {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly parentLink: SubagentParentLink;
}

/**
 * Narrow parent execution value visible to Tool handlers. `depth` is the
 * current Run's root-relative delegation depth: `0` for a root Run and
 * `parentLink.depth` for a child, which the delegation preflight uses to
 * enforce inherited nesting ceilings and conserved subtree allocations.
 *
 * @category models
 * @since 0.1.0
 */
export interface AgentSpawnerService {
  /** Owns parent-wide resources until all Tool handlers and attached children have closed. */
  readonly scope: Scope.Scope;
  /** Resolved defaults for this parent Run; never includes its tools or handlers. */
  readonly policy: AgentPolicy;
  /** Effective deadline of this active parent execution, including RunOptions.durationDeadline. */
  readonly durationDeadline: DateTime.Utc;
  readonly depth: number;
  readonly grant?: SubagentGrant;
  readonly budget?: SubagentBudgetReservation;
  readonly budgetScope?: WorkerBudgetScope;
  readonly parent: AgentSpawnerParent;
  readonly spawn: <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    ModelProvides,
    ModelRequires,
    HookError = never,
    HookRequirements = never,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    RunDispositionValue extends
      | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
      | undefined = undefined,
    InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
      undefined,
    UpdatesSchema extends Schema.Top | undefined = undefined,
  >(
    binding: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue,
      InputPromptValue,
      UpdatesSchema
    >,
    input: unknown,
    delegation: SpawnDelegation,
    options?: SpawnRunOptions<HookError, HookRequirements>,
  ) => Effect.Effect<
    SpawnedChildRun<
      OutputSchema["Type"],
      AgentRuntimeFailure<
        RuntimeBinding<
          InputSchema,
          OutputSchema,
          Instructions,
          Tools,
          ModelProvides,
          ModelRequires,
          InstructionError,
          InstructionRequirements,
          RunDispositionValue,
          InputPromptValue,
          UpdatesSchema
        >,
        HookError,
        InstructionError
      >
    >,
    never,
    | Scope.Scope
    | Exclude<
        AgentRuntimeRequirements<
          RuntimeBinding<
            InputSchema,
            OutputSchema,
            Instructions,
            Tools,
            ModelProvides,
            ModelRequires,
            InstructionError,
            InstructionRequirements,
            RunDispositionValue,
            InputPromptValue,
            UpdatesSchema
          >,
          HookRequirements,
          InstructionRequirements
        >,
        ThreadHistory | RunContextPreparation
      >
  >;
}

/**
 * Establishment request assembled by a delegation Tool handler for the
 * durable coordinator, expressed strictly
 * in core/engine vocabulary. The coordinator derives everything else
 * deterministically: reservation identity from `(parentRunId, toolCallId)`,
 * child Thread/Submission identity and the admission idempotency key
 * from the parent Run and Tool Call pair, the child principal from the parent
 * Submission, and digests of the encoded input/grant/allocation values (it
 * owns the canonical digest authority). Establishment is idempotent by
 * construction (SUB-016): replaying the identical request converges on the
 * one existing child.
 *
 * @category models
 * @since 0.1.0
 */
export interface RunSubagentEstablishRequest {
  /** Resolved child defaults and ceilings, fixed before admission and restored on recovery. */
  readonly policy?: AgentPolicy | undefined;
  readonly budget?: SubagentBudgetReservation | undefined;
  readonly toolCallId: ToolCallId;
  readonly delegationId: DelegationId;
  readonly targetAgentId: AgentId;
  /** The child Run's root-relative delegation depth, bounded by its inherited grant. */
  readonly depth: DelegationDepth;
  /** Exact target Definition from the capability; the host resolves its existing registration. */
  readonly target: Agent.AnyDefinition;
  /** The prepared child input in encoded (wire) form; it rides the canonical request record so recovery admission never needs a live handler. */
  readonly encodedChildInput: unknown;
  /** The delegation's authority ceiling in encoded form. */
  readonly encodedGrant: unknown;
  /** The per-invocation budget allocation in encoded form (opaque to the engine and to storage adapters). */
  readonly encodedAllocation: unknown;
  /** Tightening-only child Tool Call bound, clamped to the reservation and Definition by the caller. */
  readonly toolCallAllowance?: number | undefined;
}

/**
 * Durable identity of one established attached child, in core vocabulary.
 *
 * @category models
 * @since 0.1.0
 */
export interface RunSubagentChildIdentity {
  readonly childThreadId: ThreadId;
  readonly childSubmissionId: SubmissionId;
  readonly childRunId: RunId;
  /** The child Receipt: establishment is never durable-visible before it exists (SUB-017). */
  readonly receiptId: ReceiptId;
}

/**
 * The established child has not settled; the parent must suspend, never poll or respawn (SUB-018).
 *
 * @category models
 * @since 0.1.0
 */
export interface ChildEstablishWaiting extends RunSubagentChildIdentity {
  readonly _tag: "waiting";
}

/**
 * The established child already has a verified canonical Settlement. The
 * coordinator verified Parent Link, target, digests, and settlement identity
 * fail-closed before returning it; `encodedResult` is the Schema-encoded
 * child terminal output for `completed` and the coordinator's bounded
 * `{errorTag, message}` failure projection otherwise — never a raw Cause.
 *
 * @category models
 * @since 0.1.0
 */
export interface ChildEstablishSettled extends RunSubagentChildIdentity {
  readonly _tag: "settled";
  /** Verified canonical usage; absence denotes legacy or unavailable evidence. */
  readonly usage?: RunTotals;
  readonly delegatedUsage?: RunTotals;
  readonly outcome: "completed" | "failed" | "aborted";
  readonly encodedResult: unknown;
  /**
   * Present when the child's durable Settlement carries the honest
   * exhaustion marker (RUN-011/RUN-018): the child completed through the
   * final-answer resolution, so its output is a budget-truncated partial.
   */
  readonly finishReason?: "budget-exhausted" | undefined;
}

/**
 * Establishment was refused or failed verification fail-closed (typed, bounded;
 * never a raw Cause). No further establishment attempt is made for this call.
 *
 * @category models
 * @since 0.1.0
 */
export interface ChildEstablishDenied {
  readonly _tag: "denied";
  readonly errorTag: string;
  readonly message: string;
}

/**
 * Result of one idempotent durable child establishment attempt.
 *
 * @category models
 * @since 0.1.0
 */
export type ChildEstablishStatus =
  | ChildEstablishWaiting
  | ChildEstablishSettled
  | ChildEstablishDenied;

/**
 * One settlement join handed back to the coordinator by the delegation
 * handler after it decoded the verified child output and applied its bounded
 * result/failure projection. The coordinator appends `SubagentJoined` and the
 * parent `ToolCallSettled` as ONE atomic canonical batch (SUB-019) and then
 * applies the accounting decision through the idempotent reservation-release
 * transitions.
 *
 * @category models
 * @since 0.1.0
 */
export interface RunSubagentJoinRequest {
  readonly toolCallId: ToolCallId;
  /** The encoded parent Tool result exactly as the Tool message will carry it. */
  readonly encodedResult: unknown;
  readonly isFailure: boolean;
  /** Final consumed/released accounting decision per budget dimension (opaque encoded policy math owned by the delegation capability). */
  readonly encodedAccounting: unknown;
}

/**
 * Run an Agent to its terminal result using schema-encoded input.
 *
 * @category operations
 * @since 0.1.0
 */
export const run: <A extends ExecutableAgent, H = never, R = never>(
  agent: A,
  input: NoInfer<Agent.EncodedInput<A>>,
  options?: RunOptions<H, R>,
) => Effect.Effect<
  AgentResult<Agent.Output<A>>,
  AgentRuntimeFailure<A, H>,
  AgentRuntimeRequirements<A, R>
> = Internal.run;

/**
 * Decode external input and run an Agent to its terminal result.
 *
 * @category operations
 * @since 0.1.0
 */
export const runUnknown: <A extends ExecutableAgent, H = never, R = never>(
  agent: A,
  input: unknown,
  options?: RunOptions<H, R>,
) => Effect.Effect<
  AgentResult<Agent.Output<A>>,
  AgentRuntimeFailure<A, H>,
  AgentRuntimeRequirements<A, R>
> = Internal.runUnknown;

/**
 * Start an Agent in the caller’s Scope and return a handle for observing and awaiting it.
 * Execution continues independently of observers; the Scope owns execution and event replay.
 *
 * @category operations
 * @since 0.1.0
 */
export const start: <A extends ExecutableAgent, H = never, R = never>(
  agent: A,
  input: NoInfer<Agent.EncodedInput<A>>,
  options?: RunOptions<H, R>,
) => Effect.Effect<
  DetachedRun<Agent.Output<A>, AgentRuntimeFailure<A, H>>,
  never,
  AgentRuntimeRequirements<A, R> | Scope.Scope
> = Internal.start;

/**
 * Decode external input and start an Agent in the caller’s Scope.
 * The returned handle observes and awaits the scoped execution.
 *
 * @category operations
 * @since 0.1.0
 */
export const startUnknown: <A extends ExecutableAgent, H = never, R = never>(
  agent: A,
  input: unknown,
  options?: RunOptions<H, R>,
) => Effect.Effect<
  DetachedRun<Agent.Output<A>, AgentRuntimeFailure<A, H>>,
  never,
  AgentRuntimeRequirements<A, R> | Scope.Scope
> = Internal.startUnknown;

/**
 * Run an Agent from schema-encoded input and stream its progress and terminal events.
 *
 * @category operations
 * @since 0.1.0
 */
export const stream: <A extends ExecutableAgent, H = never, R = never>(
  agent: A,
  input: NoInfer<Agent.EncodedInput<A>>,
  options?: RunOptions<H, R>,
) => Stream.Stream<RunEvent, AgentRuntimeFailure<A, H>, AgentRuntimeRequirements<A, R>> =
  Internal.stream;

/**
 * Decode external input and stream the Agent’s progress and terminal events.
 *
 * @category operations
 * @since 0.1.0
 */
export const streamUnknown: <A extends ExecutableAgent, H = never, R = never>(
  agent: A,
  input: unknown,
  options?: RunOptions<H, R>,
) => Stream.Stream<RunEvent, AgentRuntimeFailure<A, H>, AgentRuntimeRequirements<A, R>> =
  Internal.streamUnknown;

/**
 * One validated, stream-closed application Tool outcome in declaration order.
 *
 * @category models
 * @since 0.1.0
 */
export interface RunTurnToolResult {
  readonly toolCallId: ToolCallId;
  readonly toolName: string;
  readonly result: unknown;
  readonly isFailure: boolean;
  readonly toolSelection?: Selection | undefined;
  readonly budgetRejected?: boolean | undefined;
}
