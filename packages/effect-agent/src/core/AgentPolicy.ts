/**
 * Schema-validated Run limits and context-compaction policy for Agent definitions. Count ceilings may
 * be unbounded; wall-clock duration remains finite.
 *
 * @since 0.1.0
 */

import * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

import { ToolResultBounds } from "./ToolResult.ts";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

const CountLimit = Schema.NullOr(PositiveInt).pipe(
  Schema.decodeTo(
    Schema.Number.check(
      Schema.makeFilter((limit) => limit === Infinity || (Number.isInteger(limit) && limit > 0)),
    ),
    SchemaTransformation.transform({
      decode: (limit) => limit ?? Infinity,
      encode: (limit) => (limit === Infinity ? null : limit),
    }),
  ),
);

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const CompactionMode = Schema.Literals(["prune", "summarize", "prune-then-summarize"]);

/**
 * Inputs normalized and validated by `CompactionPolicy.make`.
 *
 * @category models
 * @since 0.1.0
 */
export type CompactionPolicyInput = Readonly<{
  /** Estimated tokens of recent history preserved verbatim across a compaction; defaults to `20_000`. */
  readonly keepRecentTokens?: number;
  /** Compaction strategy order; defaults to `"prune-then-summarize"`. */
  readonly mode?: typeof CompactionMode.Type;
}>;

/**
 * How the engine reduces model context when `contextTokenLimit` is crossed.
 *
 * @category models
 * @since 0.1.0
 */
export class CompactionPolicy extends Schema.Class<CompactionPolicy>("CompactionPolicy")({
  keepRecentTokens: PositiveInt,
  mode: CompactionMode,
}) {
  /** Normalize and validate compaction bounds, filling documented defaults. */
  static override make(input: CompactionPolicyInput = {}): CompactionPolicy {
    return super.make({
      keepRecentTokens: input.keepRecentTokens ?? 20_000,
      mode: input.mode ?? "prune-then-summarize",
    });
  }
}

const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);

/**
 * Resolution when `maxTurns`, `maxToolCalls`, or `tokenBudget` is exhausted:
 * `"final-answer"` gives the model one constrained opportunity to settle with
 * output (the Run completes with `finishReason: "budget-exhausted"` and the
 * exhausted-dimension marker), while `"fail"` fails the Run typed. Turn and
 * Tool Call bounds resolve per RUN-018/RUN-019; the token dimension
 * participates per RUN-025 with a one-shot bound (at most one grace call).
 * Duration, cost, and repeated-failure bounds are hard rails regardless of
 * this setting.
 */
const OnExhaustion = Schema.Literals(["final-answer", "fail"]);

const AgentPolicyFields = Schema.Struct({
  /** Null in declarations/JSON disables this ceiling; resolved policies use Infinity. */
  maxTurns: CountLimit,
  /** Null in declarations/JSON disables this ceiling; resolved policies use Infinity. */
  maxToolCalls: CountLimit,
  maxDuration: FinitePositiveDuration,
  toolConcurrency: PositiveInt,
  repeatedFailureLimit: NonNegativeInt,
  onExhaustion: OnExhaustion,
  /** Observed cumulative usage can cross this threshold before exhaustion is handled. */
  tokenBudget: Schema.optionalKey(PositiveInt),
  /** Tokens withheld from research calls so one final delivery call remains admissible. */
  completionReserveTokens: NonNegativeInt,
  /**
   * Estimated observed cost, not prepaid spending admission. Usage can overshoot; a strict
   * monetary ceiling requires host reservation of possible charges before provider I/O.
   */
  costBudgetMicrousd: Schema.optionalKey(NonNegativeInt),
  /** Per-model-call live-context bound; crossing it triggers compaction rather than failure. */
  contextTokenLimit: Schema.optionalKey(PositiveInt),
  toolResultBounds: ToolResultBounds,
  runStatus: Schema.Literals(["appended", "off"]),
  /** Restart a disposable model call on joined input, at most twice per Run. Default off. */
  restartOnJoinedInput: Schema.optionalKey(Schema.Boolean),
  /**
   * Retries per model call after a retryable `AiError` or a 429/5xx stream error part,
   * before any content. Provider-defined Tools must be `Tool.Readonly`. Exponential
   * backoff honors longer `retryAfter` delays and counts toward `maxDuration`. Default 0.
   */
  modelRetries: Schema.optionalKey(NonNegativeInt),
  compaction: CompactionPolicy,
}).check(
  Schema.makeFilter((policy) =>
    policy.tokenBudget !== undefined && policy.completionReserveTokens > policy.tokenBudget
      ? {
          path: ["completionReserveTokens"],
          issue: "completionReserveTokens cannot exceed tokenBudget",
        }
      : undefined,
  ),
);

type AgentPolicyFields = typeof AgentPolicyFields.Type;

/**
 * Inputs normalized and validated by `AgentPolicy.make`.
 *
 * @category models
 * @since 0.1.0
 */
export type AgentPolicyInput = Readonly<
  Omit<
    AgentPolicyFields,
    | "maxTurns"
    | "maxToolCalls"
    | "maxDuration"
    | "repeatedFailureLimit"
    | "onExhaustion"
    | "toolResultBounds"
    | "runStatus"
    | "compaction"
    | "completionReserveTokens"
  > & {
    /** Maximum model turns, or null to leave turn count unbounded. */
    readonly maxTurns: number | null;
    /** Maximum declared and programmatic Tool Calls, or null to leave count unbounded. */
    readonly maxToolCalls: number | null;
    /** Finite, positive wall-clock duration accepted in any Effect Duration input form. */
    readonly maxDuration: Duration.Input;
    /** Non-negative repeated-failure bound; defaults to `3`. */
    readonly repeatedFailureLimit?: number;
    /** Turn/Tool-Call/token exhaustion resolution; defaults to `"final-answer"`. */
    readonly onExhaustion?: typeof OnExhaustion.Type;
    /** Byte bound for each encoded Tool result; defaults to `50 KiB`. */
    readonly toolResultBounds?: ToolResultBounds;
    /** Whether a derived run-status message is appended to each model call; defaults to `"off"` to preserve retained prompt cache boundaries. */
    readonly runStatus?: AgentPolicyFields["runStatus"];
    /** Tokens reserved for final delivery; defaults to 20% of tokenBudget, capped at 4,096. */
    readonly completionReserveTokens?: number;
    /** Compaction strategy applied when `contextTokenLimit` is crossed; defaults documented on `CompactionPolicy`. */
    readonly compaction?: CompactionPolicy;
  }
>;

/**
 * Schema class for agent run limits; wall-clock duration remains finite.
 *
 * @category models
 * @since 0.1.0
 */
export class AgentPolicy extends Schema.Class<AgentPolicy>("AgentPolicy")(AgentPolicyFields) {
  /** Resolve an optional declaration against standalone or inherited defaults. */
  static resolve(input: Partial<AgentPolicyInput> = {}, parent?: AgentPolicy): AgentPolicy {
    return AgentPolicy.make({
      maxTurns: 12,
      maxToolCalls: 24,
      maxDuration: "5 minutes",
      toolConcurrency: 4,
      ...parent,
      ...input,
      // A smaller token budget must not inherit a reserve larger than itself.
      ...(input.completionReserveTokens !== undefined || parent === undefined
        ? {}
        : {
            completionReserveTokens: Math.min(
              parent.completionReserveTokens,
              input.tokenBudget ?? parent.tokenBudget ?? parent.completionReserveTokens,
            ),
          }),
    });
  }

  /** Normalize and validate policy bounds, throwing on invalid input. */
  static override make(input: AgentPolicyInput): AgentPolicy {
    const completionReserveTokens =
      input.completionReserveTokens ??
      (input.tokenBudget === undefined
        ? 4_096
        : Math.min(4_096, Math.floor(input.tokenBudget / 5)));

    return super.make({
      ...input,
      maxTurns: input.maxTurns === null ? Infinity : input.maxTurns,
      maxToolCalls: input.maxToolCalls === null ? Infinity : input.maxToolCalls,
      maxDuration: Duration.fromInputUnsafe(input.maxDuration),
      repeatedFailureLimit: input.repeatedFailureLimit ?? 3,
      onExhaustion: input.onExhaustion ?? "final-answer",
      toolResultBounds: input.toolResultBounds ?? ToolResultBounds.make({ maxBytes: 50 * 1024 }),
      runStatus: input.runStatus ?? "off",
      completionReserveTokens,
      compaction: input.compaction ?? CompactionPolicy.make(),
    });
  }
}
