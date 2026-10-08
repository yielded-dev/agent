import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { RunId } from "../Identifiers.ts";

const UsageIdentity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** Bounded provider response identity. Request configuration is not response evidence. */
export class ModelResponseIdentity extends Schema.Class<ModelResponseIdentity>(
  "@effect-agent/core/ModelResponseIdentity",
)({
  id: Schema.optionalKey(UsageIdentity),
  model: Schema.optionalKey(UsageIdentity),
}) {}

/** Missing legacy status means unknown, never a free call or a complete report. */
export const UsageCompleteness = Schema.Literals(["complete", "partial", "unknown"]);

const hasAdditiveTotal = (total: number, components: ReadonlyArray<number>): boolean => {
  const sum = components.reduce((accumulator, component) => accumulator + component, 0);

  return Number.isSafeInteger(sum) && total === sum;
};

const InputTokenUsageFields = Schema.Struct({
  total: Schema.Natural,
  uncached: Schema.Natural,
  cacheRead: Schema.Natural,
  cacheWrite: Schema.Natural,
}).check(
  Schema.makeFilter(
    (usage) => hasAdditiveTotal(usage.total, [usage.uncached, usage.cacheRead, usage.cacheWrite]),
    { title: "Input token total equals uncached, cache-read, and cache-write components" },
  ),
);

/** Provider-reported input-token components for one completed model call. */
export class InputTokenUsage extends Schema.Class<InputTokenUsage>(
  "@effect-agent/core/InputTokenUsage",
)(InputTokenUsageFields) {}

const OutputTokenUsageFields = Schema.Struct({
  total: Schema.Natural,
  text: Schema.Natural,
  reasoning: Schema.Natural,
}).check(
  Schema.makeFilter((usage) => hasAdditiveTotal(usage.total, [usage.text, usage.reasoning]), {
    title: "Output token total equals text and reasoning components",
  }),
);

/** Provider-reported output-token components for one completed model call. */
export class OutputTokenUsage extends Schema.Class<OutputTokenUsage>(
  "@effect-agent/core/OutputTokenUsage",
)(OutputTokenUsageFields) {}

/** Canonical, provider-independent accounting for one completed model call. */
export class ModelCallUsage extends Schema.Class<ModelCallUsage>(
  "@effect-agent/core/ModelCallUsage",
)({
  provider: UsageIdentity,
  /** Configured binding identity. See response.model for provider-reported identity. */
  model: UsageIdentity,
  serviceTier: Schema.optionalKey(UsageIdentity),
  pricingVersion: Schema.optionalKey(UsageIdentity),
  response: Schema.optionalKey(ModelResponseIdentity),
  purpose: Schema.optionalKey(Schema.Literals(["turn", "summary"])),
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(Schema.Literals(["estimated", "unknown"])),
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  /** Observed hosted web searches; excludes OpenAI page/find actions. Absent in legacy records. */
  webSearchCalls: Schema.optionalKey(Schema.Natural),
  costMicrousd: Schema.Natural,
}) {}

/**
 * Observed model usage and estimated cost, without per-model attribution.
 * Numeric zero with unknown coverage never establishes free execution. Missing legacy
 * statuses mean unknown. Unobserved calls are excluded from the numeric call/token totals.
 */
export class RunTotals extends Schema.Class<RunTotals>("@effect-agent/core/RunTotals")({
  modelCalls: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  /** Observed hosted web searches; excludes OpenAI page/find actions. Absent in legacy records. */
  webSearchCalls: Schema.optionalKey(Schema.Natural),
  costMicrousd: Schema.Natural,
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(UsageCompleteness),
  unobservedModelCalls: Schema.optionalKey(Schema.Natural),
}) {}

/** Own Run usage and disjoint attached-descendant usage. Add each component once. */
export class RunUsageReport extends Schema.Class<RunUsageReport>(
  "@effect-agent/core/RunUsageReport",
)({
  usage: RunTotals,
  delegatedUsage: RunTotals,
}) {}

/** A verified child report retained across durable parent Attempts. */
export class ChildRunUsage extends Schema.Class<ChildRunUsage>("@effect-agent/core/ChildRunUsage")({
  runId: RunId,
  report: RunUsageReport,
}) {}

/** Known absence of observed work. */
export const emptyRunTotals = (): RunTotals =>
  RunTotals.make({
    modelCalls: 0,
    webSearchCalls: 0,
    inputTokens: 0,
    outputTokens: 0,
    costMicrousd: 0,
    usageStatus: "complete",
    pricingStatus: "complete",
    unobservedModelCalls: 0,
  });

/** No accounting evidence, including legacy child reports. This is not known zero spend. */
export const unknownRunTotals = (): RunTotals =>
  RunTotals.make({
    ...emptyRunTotals(),
    usageStatus: "unknown",
    pricingStatus: "unknown",
  });

/** Settlement-sized aggregate for calls sharing one pricing identity. */
export class ModelUsageGroup extends Schema.Class<ModelUsageGroup>(
  "@effect-agent/core/ModelUsageGroup",
)({
  provider: UsageIdentity,
  model: UsageIdentity,
  /** Actual returned model; absent when only the configured binding is known. */
  responseModel: Schema.optionalKey(UsageIdentity),
  serviceTier: Schema.optionalKey(UsageIdentity),
  pricingVersion: Schema.optionalKey(UsageIdentity),
  modelCalls: Schema.Natural.check(Schema.isGreaterThan(0)),
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  /** Observed hosted web searches; excludes OpenAI page/find actions. Absent in legacy records. */
  webSearchCalls: Schema.optionalKey(Schema.Natural),
  costMicrousd: Schema.Natural,
}) {}

const RunUsageSummaryFields = Schema.Struct({
  modelCalls: Schema.Natural,
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  /** Observed hosted web searches; excludes OpenAI page/find actions. Absent in legacy records. */
  webSearchCalls: Schema.optionalKey(Schema.Natural),
  costMicrousd: Schema.Natural,
  byModel: Schema.Array(ModelUsageGroup),
  /** Coverage of recorded calls only; interruptions can make the Run less complete. */
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(UsageCompleteness),
  /** Observed invocations without retained accounting, excluded from numeric call/token totals. */
  unobservedModelCalls: Schema.optionalKey(Schema.Natural),
}).check(
  Schema.makeFilter(
    (summary) => {
      const identities = summary.byModel.map((group) =>
        JSON.stringify([
          group.provider,
          group.model,
          group.responseModel ?? null,
          group.serviceTier ?? null,
          group.pricingVersion ?? null,
        ]),
      );

      return (
        new Set(identities).size === identities.length &&
        (summary.webSearchCalls === undefined ||
          hasAdditiveTotal(
            summary.webSearchCalls,
            summary.byModel.map((group) => group.webSearchCalls ?? 0),
          )) &&
        hasAdditiveTotal(
          summary.modelCalls,
          summary.byModel.map((group) => group.modelCalls),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.total,
          summary.byModel.map((group) => group.inputTokens.total),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.uncached,
          summary.byModel.map((group) => group.inputTokens.uncached),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.cacheRead,
          summary.byModel.map((group) => group.inputTokens.cacheRead),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.cacheWrite,
          summary.byModel.map((group) => group.inputTokens.cacheWrite),
        ) &&
        hasAdditiveTotal(
          summary.outputTokens.total,
          summary.byModel.map((group) => group.outputTokens.total),
        ) &&
        hasAdditiveTotal(
          summary.outputTokens.text,
          summary.byModel.map((group) => group.outputTokens.text),
        ) &&
        hasAdditiveTotal(
          summary.outputTokens.reasoning,
          summary.byModel.map((group) => group.outputTokens.reasoning),
        ) &&
        hasAdditiveTotal(
          summary.costMicrousd,
          summary.byModel.map((group) => group.costMicrousd),
        )
      );
    },
    {
      title:
        "Run usage totals equal unique per-model pricing groups within safe-integer accounting",
    },
  ),
);

/** Settlement aggregate included with a terminal Run settlement. */
export class RunUsageSummary extends Schema.Class<RunUsageSummary>(
  "@effect-agent/core/RunUsageSummary",
)(RunUsageSummaryFields) {}

interface MutableUsageGroup {
  readonly provider: string;
  readonly model: string;
  readonly responseModel?: string | undefined;
  readonly serviceTier?: string | undefined;
  readonly pricingVersion?: string | undefined;
  modelCalls: number;
  webSearchCalls: number | undefined;
  inputTokens: {
    total: number;
    uncached: number;
    cacheRead: number;
    cacheWrite: number;
  };
  outputTokens: { total: number; text: number; reasoning: number };
  costMicrousd: number;
}

const emptyInputTokens = () => ({ total: 0, uncached: 0, cacheRead: 0, cacheWrite: 0 });
const emptyOutputTokens = () => ({ total: 0, text: 0, reasoning: 0 });

/** Canonical usage could not be aggregated without exceeding safe-integer accounting. */
export class UsageAggregationError extends Schema.TaggedError<UsageAggregationError>()(
  "UsageAggregationError",
  {
    field: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

const checkedAdd = (field: string, left: number, right: number): number | UsageAggregationError => {
  const total = left + right;

  return Number.isSafeInteger(left) &&
    left >= 0 &&
    Number.isSafeInteger(right) &&
    right >= 0 &&
    Number.isSafeInteger(total)
    ? total
    : UsageAggregationError.make({
        field,
        message: `Canonical usage aggregation exceeded the safe-integer range at ${field}`,
      });
};

const decodeTotals = Schema.decodeResult(RunTotals);
const decodeSummary = Schema.decodeResult(RunUsageSummary);
const USAGE_BATCH_SIZE = 64;

/** Combine disjoint totals, retaining uncertainty and rejecting numeric overflow. */
const sumRunTotalsResult = (
  contributions: ReadonlyArray<RunTotals>,
): Result.Result<RunTotals, UsageAggregationError> => {
  let modelCalls = 0;
  let webSearchCalls: number | undefined = 0;
  let inputTokens = 0;
  let outputTokens = 0;
  let costMicrousd = 0;
  let unobservedModelCalls = 0;
  const usageStatuses: Array<typeof UsageCompleteness.Type> = [];
  const pricingStatuses: Array<typeof UsageCompleteness.Type> = [];

  for (const contribution of contributions) {
    const decoded = decodeTotals(contribution);

    if (Result.isFailure(decoded))
      return Result.fail(
        new UsageAggregationError({ field: "totals", message: "Invalid Run totals" }),
      );
    const value = decoded.success;

    const nextModelCalls = checkedAdd("modelCalls", modelCalls, value.modelCalls);

    if (typeof nextModelCalls !== "number") return Result.fail(nextModelCalls);

    modelCalls = nextModelCalls;
    if (value.webSearchCalls === undefined && value.modelCalls > 0) webSearchCalls = undefined;
    else if (webSearchCalls !== undefined) {
      const nextWebSearchCalls = checkedAdd(
        "webSearchCalls",
        webSearchCalls,
        value.webSearchCalls ?? 0,
      );

      if (typeof nextWebSearchCalls !== "number") return Result.fail(nextWebSearchCalls);
      webSearchCalls = nextWebSearchCalls;
    }
    const nextInputTokens = checkedAdd("inputTokens", inputTokens, value.inputTokens);

    if (typeof nextInputTokens !== "number") return Result.fail(nextInputTokens);
    inputTokens = nextInputTokens;
    const nextOutputTokens = checkedAdd("outputTokens", outputTokens, value.outputTokens);

    if (typeof nextOutputTokens !== "number") return Result.fail(nextOutputTokens);
    outputTokens = nextOutputTokens;
    const nextCostMicrousd = checkedAdd("costMicrousd", costMicrousd, value.costMicrousd);

    if (typeof nextCostMicrousd !== "number") return Result.fail(nextCostMicrousd);
    costMicrousd = nextCostMicrousd;

    const nextUnobservedModelCalls = checkedAdd(
      "unobservedModelCalls",
      unobservedModelCalls,
      value.unobservedModelCalls ?? 0,
    );

    if (typeof nextUnobservedModelCalls !== "number") return Result.fail(nextUnobservedModelCalls);
    unobservedModelCalls = nextUnobservedModelCalls;
    // Known empty contributions are identities; unknown empty contributions are evidence gaps.
    if (
      value.modelCalls > 0 ||
      (value.unobservedModelCalls ?? 0) > 0 ||
      value.usageStatus !== "complete"
    ) {
      usageStatuses.push(value.usageStatus ?? "unknown");
    }
    if (
      value.modelCalls > 0 ||
      (value.unobservedModelCalls ?? 0) > 0 ||
      value.pricingStatus !== "complete"
    ) {
      pricingStatuses.push(value.pricingStatus ?? "unknown");
    }
  }

  const coverage = (statuses: ReadonlyArray<typeof UsageCompleteness.Type>) =>
    statuses.every((status) => status === "complete")
      ? ("complete" as const)
      : statuses.every((status) => status === "unknown")
        ? ("unknown" as const)
        : ("partial" as const);

  return Result.mapError(
    decodeTotals({
      modelCalls,
      ...(webSearchCalls === undefined ? {} : { webSearchCalls }),
      inputTokens,
      outputTokens,
      costMicrousd,
      unobservedModelCalls,
      usageStatus: coverage(usageStatuses),
      pricingStatus: coverage(pricingStatuses),
    }),
    () => new UsageAggregationError({ field: "totals", message: "Invalid Run totals" }),
  );
};

/** Combine disjoint totals with cooperative boundaries for unrestricted public input arrays. */
export const sumRunTotals = Effect.fnUntraced(function* (
  contributions: ReadonlyArray<RunTotals>,
): Effect.fn.Return<RunTotals, UsageAggregationError> {
  let total: RunTotals | undefined;

  for (let start = 0; ; start += USAGE_BATCH_SIZE) {
    const batch = contributions.slice(start, start + USAGE_BATCH_SIZE);

    if (total !== undefined) batch.unshift(total);
    const result = sumRunTotalsResult(batch);

    if (Result.isFailure(result)) return yield* result.failure;
    if (start + USAGE_BATCH_SIZE >= contributions.length) return result.success;
    total = result.success;
    yield* Effect.yieldNow;
  }
});

/** Flatten a canonical settlement summary without discarding its coverage markers. */
export const runTotalsFromSummary = (summary: RunUsageSummary): RunTotals =>
  RunTotals.make({
    modelCalls: summary.modelCalls,
    ...(summary.webSearchCalls === undefined ? {} : { webSearchCalls: summary.webSearchCalls }),
    inputTokens: summary.inputTokens.total,
    outputTokens: summary.outputTokens.total,
    costMicrousd: summary.costMicrousd,
    usageStatus: summary.usageStatus ?? "unknown",
    pricingStatus: summary.pricingStatus ?? "unknown",
    unobservedModelCalls: summary.unobservedModelCalls ?? 0,
  });

/**
 * Deterministically aggregate canonical per-call usage without making cached tokens free.
 * A validated seed retains earlier call totals and pricing groups without retaining those calls.
 * Empty contributions do not change completeness; the seed is never mutated.
 */
export const summarizeModelUsageResult = (
  calls: ReadonlyArray<ModelCallUsage>,
  seed?: RunUsageSummary,
): Result.Result<RunUsageSummary, UsageAggregationError> => {
  let initial: RunUsageSummary | undefined;

  if (seed !== undefined) {
    const decoded = decodeSummary(seed);

    if (Result.isFailure(decoded))
      return Result.fail(
        new UsageAggregationError({
          field: "seed",
          message: "Canonical usage seed does not satisfy the RunUsageSummary Schema",
        }),
      );
    initial = decoded.success;
  }

  const inputTokens = initial === undefined ? emptyInputTokens() : { ...initial.inputTokens };
  const outputTokens = initial === undefined ? emptyOutputTokens() : { ...initial.outputTokens };
  const groups = new Map<string, MutableUsageGroup>();
  let modelCalls = initial?.modelCalls ?? 0;

  let webSearchCalls =
    initial === undefined || initial.modelCalls === 0 ? 0 : initial.webSearchCalls;

  let costMicrousd = initial?.costMicrousd ?? 0;
  const hasSeedCalls = modelCalls > 0;

  let allUsageUnknown =
    !hasSeedCalls || initial?.usageStatus === undefined || initial.usageStatus === "unknown";

  let allUsageComplete = !hasSeedCalls || initial?.usageStatus === "complete";

  let allPricingUnknown =
    !hasSeedCalls || initial?.pricingStatus === undefined || initial.pricingStatus === "unknown";

  let allPricingComplete = !hasSeedCalls || initial?.pricingStatus === "complete";

  for (const group of initial?.byModel ?? []) {
    const key = JSON.stringify([
      group.provider,
      group.model,
      group.responseModel ?? null,
      group.serviceTier ?? null,
      group.pricingVersion ?? null,
    ]);

    groups.set(key, {
      ...group,
      webSearchCalls: group.webSearchCalls,
      inputTokens: { ...group.inputTokens },
      outputTokens: { ...group.outputTokens },
    });
  }

  for (const call of calls) {
    allUsageUnknown &&= call.usageStatus === undefined || call.usageStatus === "unknown";
    allUsageComplete &&= call.usageStatus === "complete";
    allPricingUnknown &&= call.pricingStatus !== "estimated";
    allPricingComplete &&= call.pricingStatus === "estimated";
    const nextModelCalls = checkedAdd("modelCalls", modelCalls, 1);

    if (typeof nextModelCalls !== "number") return Result.fail(nextModelCalls);
    modelCalls = nextModelCalls;
    if (webSearchCalls === undefined || call.webSearchCalls === undefined)
      webSearchCalls = undefined;
    else {
      const nextWebSearchCalls = checkedAdd("webSearchCalls", webSearchCalls, call.webSearchCalls);

      if (typeof nextWebSearchCalls !== "number") return Result.fail(nextWebSearchCalls);
      webSearchCalls = nextWebSearchCalls;
    }

    const nextInputTokensTotal = checkedAdd(
      "inputTokens.total",
      inputTokens.total,
      call.inputTokens.total,
    );

    if (typeof nextInputTokensTotal !== "number") return Result.fail(nextInputTokensTotal);
    inputTokens.total = nextInputTokensTotal;

    const nextInputTokensUncached = checkedAdd(
      "inputTokens.uncached",
      inputTokens.uncached,
      call.inputTokens.uncached,
    );

    if (typeof nextInputTokensUncached !== "number") return Result.fail(nextInputTokensUncached);
    inputTokens.uncached = nextInputTokensUncached;

    const nextInputTokensCacheRead = checkedAdd(
      "inputTokens.cacheRead",
      inputTokens.cacheRead,
      call.inputTokens.cacheRead,
    );

    if (typeof nextInputTokensCacheRead !== "number") return Result.fail(nextInputTokensCacheRead);
    inputTokens.cacheRead = nextInputTokensCacheRead;

    const nextInputTokensCacheWrite = checkedAdd(
      "inputTokens.cacheWrite",
      inputTokens.cacheWrite,
      call.inputTokens.cacheWrite,
    );

    if (typeof nextInputTokensCacheWrite !== "number")
      return Result.fail(nextInputTokensCacheWrite);
    inputTokens.cacheWrite = nextInputTokensCacheWrite;

    const nextOutputTokensTotal = checkedAdd(
      "outputTokens.total",
      outputTokens.total,
      call.outputTokens.total,
    );

    if (typeof nextOutputTokensTotal !== "number") return Result.fail(nextOutputTokensTotal);
    outputTokens.total = nextOutputTokensTotal;

    const nextOutputTokensText = checkedAdd(
      "outputTokens.text",
      outputTokens.text,
      call.outputTokens.text,
    );

    if (typeof nextOutputTokensText !== "number") return Result.fail(nextOutputTokensText);
    outputTokens.text = nextOutputTokensText;

    const nextOutputTokensReasoning = checkedAdd(
      "outputTokens.reasoning",
      outputTokens.reasoning,
      call.outputTokens.reasoning,
    );

    if (typeof nextOutputTokensReasoning !== "number")
      return Result.fail(nextOutputTokensReasoning);
    outputTokens.reasoning = nextOutputTokensReasoning;
    const nextCostMicrousd = checkedAdd("costMicrousd", costMicrousd, call.costMicrousd);

    if (typeof nextCostMicrousd !== "number") return Result.fail(nextCostMicrousd);
    costMicrousd = nextCostMicrousd;

    const key = JSON.stringify([
      call.provider,
      call.model,
      call.response?.model ?? null,
      call.serviceTier ?? null,
      call.pricingVersion ?? null,
    ]);

    let group = groups.get(key);

    if (group === undefined) {
      group = {
        provider: call.provider,
        model: call.model,
        ...(call.response?.model === undefined ? {} : { responseModel: call.response.model }),
        ...(call.serviceTier === undefined ? {} : { serviceTier: call.serviceTier }),
        ...(call.pricingVersion === undefined ? {} : { pricingVersion: call.pricingVersion }),
        modelCalls: 0,
        webSearchCalls: 0,
        inputTokens: emptyInputTokens(),
        outputTokens: emptyOutputTokens(),
        costMicrousd: 0,
      };
      groups.set(key, group);
    }
    const nextByModelModelCalls = checkedAdd("byModel.modelCalls", group.modelCalls, 1);

    if (typeof nextByModelModelCalls !== "number") return Result.fail(nextByModelModelCalls);
    group.modelCalls = nextByModelModelCalls;
    if (group.webSearchCalls === undefined || call.webSearchCalls === undefined)
      group.webSearchCalls = undefined;
    else {
      const nextByModelWebSearchCalls = checkedAdd(
        "byModel.webSearchCalls",
        group.webSearchCalls,
        call.webSearchCalls,
      );

      if (typeof nextByModelWebSearchCalls !== "number")
        return Result.fail(nextByModelWebSearchCalls);
      group.webSearchCalls = nextByModelWebSearchCalls;
    }

    const nextByModelInputTokensTotal = checkedAdd(
      "byModel.inputTokens.total",
      group.inputTokens.total,
      call.inputTokens.total,
    );

    if (typeof nextByModelInputTokensTotal !== "number")
      return Result.fail(nextByModelInputTokensTotal);
    group.inputTokens.total = nextByModelInputTokensTotal;

    const nextByModelInputTokensUncached = checkedAdd(
      "byModel.inputTokens.uncached",
      group.inputTokens.uncached,
      call.inputTokens.uncached,
    );

    if (typeof nextByModelInputTokensUncached !== "number")
      return Result.fail(nextByModelInputTokensUncached);
    group.inputTokens.uncached = nextByModelInputTokensUncached;

    const nextByModelInputTokensCacheRead = checkedAdd(
      "byModel.inputTokens.cacheRead",
      group.inputTokens.cacheRead,
      call.inputTokens.cacheRead,
    );

    if (typeof nextByModelInputTokensCacheRead !== "number")
      return Result.fail(nextByModelInputTokensCacheRead);
    group.inputTokens.cacheRead = nextByModelInputTokensCacheRead;

    const nextByModelInputTokensCacheWrite = checkedAdd(
      "byModel.inputTokens.cacheWrite",
      group.inputTokens.cacheWrite,
      call.inputTokens.cacheWrite,
    );

    if (typeof nextByModelInputTokensCacheWrite !== "number")
      return Result.fail(nextByModelInputTokensCacheWrite);
    group.inputTokens.cacheWrite = nextByModelInputTokensCacheWrite;

    const nextByModelOutputTokensTotal = checkedAdd(
      "byModel.outputTokens.total",
      group.outputTokens.total,
      call.outputTokens.total,
    );

    if (typeof nextByModelOutputTokensTotal !== "number")
      return Result.fail(nextByModelOutputTokensTotal);
    group.outputTokens.total = nextByModelOutputTokensTotal;

    const nextByModelOutputTokensText = checkedAdd(
      "byModel.outputTokens.text",
      group.outputTokens.text,
      call.outputTokens.text,
    );

    if (typeof nextByModelOutputTokensText !== "number")
      return Result.fail(nextByModelOutputTokensText);
    group.outputTokens.text = nextByModelOutputTokensText;

    const nextByModelOutputTokensReasoning = checkedAdd(
      "byModel.outputTokens.reasoning",
      group.outputTokens.reasoning,
      call.outputTokens.reasoning,
    );

    if (typeof nextByModelOutputTokensReasoning !== "number")
      return Result.fail(nextByModelOutputTokensReasoning);
    group.outputTokens.reasoning = nextByModelOutputTokensReasoning;

    const nextByModelCostMicrousd = checkedAdd(
      "byModel.costMicrousd",
      group.costMicrousd,
      call.costMicrousd,
    );

    if (typeof nextByModelCostMicrousd !== "number") return Result.fail(nextByModelCostMicrousd);
    group.costMicrousd = nextByModelCostMicrousd;
  }

  return Result.mapError(
    decodeSummary({
      modelCalls,
      ...(webSearchCalls === undefined ? {} : { webSearchCalls }),
      inputTokens,
      outputTokens,
      costMicrousd,
      usageStatus: allUsageUnknown ? "unknown" : allUsageComplete ? "complete" : "partial",
      pricingStatus: allPricingUnknown ? "unknown" : allPricingComplete ? "complete" : "partial",
      ...(initial?.unobservedModelCalls === undefined
        ? {}
        : { unobservedModelCalls: initial.unobservedModelCalls }),
      byModel: [...groups.values()].map((group) => ({
        provider: group.provider,
        model: group.model,
        ...(group.responseModel === undefined ? {} : { responseModel: group.responseModel }),
        ...(group.serviceTier === undefined ? {} : { serviceTier: group.serviceTier }),
        ...(group.pricingVersion === undefined ? {} : { pricingVersion: group.pricingVersion }),
        modelCalls: group.modelCalls,
        ...(group.webSearchCalls === undefined ? {} : { webSearchCalls: group.webSearchCalls }),
        inputTokens: group.inputTokens,
        outputTokens: group.outputTokens,
        costMicrousd: group.costMicrousd,
      })),
    }),
    () =>
      new UsageAggregationError({
        field: "summary",
        message: "Canonical usage does not satisfy the RunUsageSummary Schema",
      }),
  );
};

/** Aggregate calls in cooperative batches without per-field Effects or mutation of the seed. */
export const summarizeModelUsage = Effect.fnUntraced(function* (
  calls: ReadonlyArray<ModelCallUsage>,
  seed?: RunUsageSummary,
): Effect.fn.Return<RunUsageSummary, UsageAggregationError> {
  let summary = seed;

  for (let start = 0; ; start += USAGE_BATCH_SIZE) {
    const result = summarizeModelUsageResult(calls.slice(start, start + USAGE_BATCH_SIZE), summary);

    if (Result.isFailure(result)) return yield* result.failure;
    if (start + USAGE_BATCH_SIZE >= calls.length) return result.success;
    summary = result.success;
    yield* Effect.yieldNow;
  }
});
