import type * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";

import type { Selection } from "../../core/ToolExposure.ts";
import {
  summarizeModelUsage,
  UsageAggregationError,
  type ModelCallUsage,
} from "../../core/Usage.ts";
import type { ModelResponseRecorded, ToolCallSettled } from "../Records.ts";

/** Captured calls are additive and nonnegative; progress does not need attribution groups. */
export const continuationUsage = (calls: ReadonlyArray<ModelCallUsage>) =>
  Effect.suspend(() => {
    let inputTokens = 0;
    let outputTokens = 0;
    let costMicrousd = 0;
    let knownWebSearchCalls = 0;

    for (const call of calls) {
      inputTokens += call.inputTokens.total;
      outputTokens += call.outputTokens.total;
      costMicrousd += call.costMicrousd;
      knownWebSearchCalls += call.webSearchCalls ?? 0;
    }
    if (
      Number.isSafeInteger(inputTokens) &&
      Number.isSafeInteger(outputTokens) &&
      Number.isSafeInteger(costMicrousd) &&
      Number.isSafeInteger(knownWebSearchCalls)
    )
      return Effect.succeed({ modelCalls: calls.length, inputTokens, outputTokens, costMicrousd });

    // Safe global sums bound every component/group. On overflow retain the full validator,
    // including groups whose hosted-search coverage differs from the aggregate's coverage.
    return Effect.map(summarizeModelUsage(calls), (summary) => ({
      modelCalls: summary.modelCalls,
      inputTokens: summary.inputTokens.total,
      outputTokens: summary.outputTokens.total,
      costMicrousd: summary.costMicrousd,
    }));
  });

/** Per-call evidence is authoritative; aggregate-only records retain their original charges. */
export const recordedResponseUsage = Effect.fnUntraced(function* (
  response: Pick<
    ModelResponseRecorded,
    "modelUsage" | "inputTokens" | "outputTokens" | "costMicrousd"
  >,
) {
  const calls = response.modelUsage;

  if (calls === undefined)
    return {
      modelCalls: 1,
      inputTokens: response.inputTokens ?? 0,
      outputTokens: response.outputTokens ?? 0,
      costMicrousd: response.costMicrousd ?? 0,
      modelUsage: [],
    };
  if (calls.length === 0)
    return yield* UsageAggregationError.make({
      field: "modelUsage",
      message: "A canonical modelUsage list must not be empty",
    });

  const summary = yield* continuationUsage(calls);

  if (
    (response.inputTokens !== undefined && response.inputTokens !== summary.inputTokens) ||
    (response.outputTokens !== undefined && response.outputTokens !== summary.outputTokens) ||
    (response.costMicrousd !== undefined && response.costMicrousd !== summary.costMicrousd)
  )
    return yield* UsageAggregationError.make({
      field: "modelUsage",
      message: "Canonical detailed and aggregate model usage disagree",
    });

  return { ...summary, modelUsage: calls };
});

/**
 * Account a complete batch in declaration order. Provider outcomes stay with their response;
 * only canonical application settlements can exclude budget refusals or select Tools.
 */
export const completedToolBatch = (
  messages: ModelResponseRecorded["messages"] | Prompt.Prompt,
  applicationResult: (
    toolCallId: string,
  ) => Pick<ToolCallSettled, "isFailure" | "budgetRejected" | "toolSelection"> | undefined,
  previousFailures: number,
) => {
  const parts = messages.content.flatMap((message) =>
    message.role === "assistant" && typeof message.content !== "string" ? message.content : [],
  );

  let consecutiveToolFailures = previousFailures;
  let toolSelection: Selection | undefined;

  for (const call of parts) {
    if (call.type !== "tool-call") continue;

    const result = call.providerExecuted
      ? parts.find(
          (part) => part.type === "tool-result" && part.providerExecuted && part.id === call.id,
        )
      : applicationResult(call.id);

    if (
      result === undefined ||
      !("isFailure" in result) ||
      (!call.providerExecuted && "budgetRejected" in result && result.budgetRejected === true)
    )
      continue;
    if (
      !call.providerExecuted &&
      !result.isFailure &&
      "toolSelection" in result &&
      result.toolSelection !== undefined
    )
      toolSelection = result.toolSelection;
    consecutiveToolFailures = result.isFailure ? consecutiveToolFailures + 1 : 0;
  }

  return { consecutiveToolFailures, toolSelection };
};
