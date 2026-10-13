import { Effect } from "effect";

import { summarizeModelUsage, type ModelCallUsage } from "../../core/Usage.ts";

/** Captured calls are additive and nonnegative; progress does not need attribution groups. */
export const continuationUsage = Effect.fnUntraced(function* (
  calls: ReadonlyArray<ModelCallUsage>,
) {
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
    return { modelCalls: calls.length, inputTokens, outputTokens, costMicrousd };

  // Safe global sums bound every component/group. On overflow retain the full validator,
  // including groups whose hosted-search coverage differs from the aggregate's coverage.
  const summary = yield* summarizeModelUsage(calls);

  return {
    modelCalls: summary.modelCalls,
    inputTokens: summary.inputTokens.total,
    outputTokens: summary.outputTokens.total,
    costMicrousd: summary.costMicrousd,
  };
});
