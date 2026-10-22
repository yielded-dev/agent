import type * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  type ThreadEncodingError,
  type ThreadHistoryDiverged,
  type ThreadLimitExceeded,
  type ThreadNotFound,
  type ThreadOwnershipError,
  toPrompt,
  Store as ConversationStore,
} from "../core/Thread.ts";
import {
  type PreparedRunContext,
  type RunBudgetHook,
  type RunContextHook,
  type RunContextRequest,
  type RunOptions,
  type RunSchedulingHook,
} from "../engine/RunOptions.ts";
import { type BudgetExceeded, UsageDelta, type UsageBudgetNode } from "./Budget.ts";

/** Engine usage data did not satisfy the non-negative budget delta Schema. */
export class BudgetAdapterError extends Schema.TaggedError<BudgetAdapterError>()(
  "BudgetAdapterError",
  { message: Schema.String },
) {}

/** Adapt one hierarchical budget node to the engine's usage accounting seam. */
export const toRunBudgetHook = (
  budget: UsageBudgetNode,
): RunBudgetHook<BudgetExceeded | BudgetAdapterError, never> => ({
  guard: budget.guard,
  consume: (delta) =>
    Schema.decodeEffect(UsageDelta)({
      modelCalls: delta.modelCalls,
      inputTokens: delta.inputTokens,
      outputTokens: delta.outputTokens,
      cacheReadInputTokens: Math.max(0, delta.usage.inputTokens.cacheRead ?? 0),
      cacheWriteInputTokens: Math.max(0, delta.usage.inputTokens.cacheWrite ?? 0),
      toolCalls: delta.toolCalls,
      costMicrousd: delta.costMicrousd,
    }).pipe(
      Effect.mapError((error) =>
        BudgetAdapterError.make({
          message: `Engine usage delta is invalid: ${error.message}`,
        }),
      ),
      Effect.flatMap((usage) => budget.consume(usage)),
      Effect.asVoid,
    ),
});

export type ThreadAdapterError =
  | ThreadNotFound
  | ThreadLimitExceeded
  | ThreadEncodingError
  | ThreadHistoryDiverged
  | ThreadOwnershipError;

/**
 * Advanced integration for an existing Thread.Store snapshot. Ordinary Runs automatically
 * retain history through ThreadHistory.layer; use this helper only for explicit snapshot hooks.
 * Share that Layer's Thread.Store owner. PersistentHistory rejects these competing hooks.
 * The snapshot is explicit initial Prompt data. Each inline onHistory call immediately records
 * its append-only suffix, including updates from Runs that later fail or are interrupted. Writes
 * already made remain in the snapshot. Callback errors stop the Run as ThreadAdapterError;
 * snapshot lookup can fail with ThreadNotFound or ThreadOwnershipError before execution.
 * Construct scoped hooks inside InMemory.scoped; captured callbacks retain that owner.
 * Native Effect AI parts and provider options are preserved without a role/text projection.
 * For retaining only successful Runs, provide ThreadHistory through PersistentHistory.layer
 * with a memory or SQLite store instead. This adapter does not provide durable recovery.
 */
export const toRunThreadOptions = Effect.fnUntraced(function* (
  threadId: import("../core/Identifiers.ts").ThreadId,
  runId: import("../core/Identifiers.ts").RunId,
): Effect.fn.Return<
  Pick<RunOptions<ThreadAdapterError>, "threadId" | "history" | "onHistory">,
  ThreadNotFound | ThreadOwnershipError,
  ConversationStore
> {
  const threads = yield* ConversationStore;
  const snapshot = yield* threads.snapshot(threadId);

  return {
    threadId,
    history: toPrompt(snapshot),
    onHistory: (history) => threads.recordHistory(threadId, runId, history).pipe(Effect.asVoid),
  };
});

/** Prompt transform used by the adapter; the engine retains the authoritative source separately. */
export interface EnginePromptTransform<Error = never, Requirements = never> {
  readonly prepare: (
    source: Prompt.Prompt,
    request: RunContextRequest,
  ) => Effect.Effect<Prompt.Prompt, Error, Requirements>;
}

/** Adapt a prompt-only context transform without granting it authority over engine source history. */
export const toRunContextHook = <Error, Requirements>(
  transform: EnginePromptTransform<Error, Requirements>,
): RunContextHook<Error, Requirements> => ({
  prepare: (request): Effect.Effect<PreparedRunContext, Error, Requirements> =>
    transform.prepare(request.source, request).pipe(Effect.map((prompt) => ({ prompt }))),
});

/** Scheduling values are structurally aligned and only reduce finite concurrency. */
export const toRunSchedulingHook = (
  runOverride: RunSchedulingHook["runOverride"],
  toolRequiresSequential?: (toolName: string) => boolean,
): RunSchedulingHook => ({
  runOverride,
  ...(toolRequiresSequential === undefined ? {} : { toolRequiresSequential }),
});
