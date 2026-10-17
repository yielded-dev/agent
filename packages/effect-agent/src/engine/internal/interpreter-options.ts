import type * as Prompt from "effect/ai/Prompt";
import type * as DateTime from "effect/DateTime";
import type * as Schema from "effect/Schema";

import type {
  SubagentParentLink,
  SubagentGrant,
  SubagentBudgetReservation,
} from "../../core/SubagentContract.ts";
import type { FrameworkMessage, WorkerBudgetScope } from "../../core/Worker.ts";
import type {
  RunOptions,
  RunDurabilityHook,
  RunSubagentHook,
  RunTurnResume,
  RunResumeUsage,
} from "../RunOptions.ts";

/** Interpreter protocol supplied by the owning durable coordinator or delegation capability. */
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
