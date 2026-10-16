import * as Effect from "effect/Effect";

import { summarizeModelUsage, type ModelCallUsage } from "../../core/Usage.ts";

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
