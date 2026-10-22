/**
 * Cumulative execution-policy accounting retained across Attempts of one logical Run.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

/**
 * Cumulative policy accounting for one logical Run across replacement Attempts.
 * Turns and model-declared calls include a pending batch. The failure streak excludes that
 * entire batch until its declaration-ordered outcomes are folded once during continuation.
 * Programmatic calls are reserved before execution; finalization is reserved before its model call.
 *
 * @category schemas
 * @since 0.1.0
 */
export const RunPolicyUsage = Schema.Struct({
  committedTurns: Schema.Natural,
  toolCalls: Schema.Natural,
  programmaticToolCalls: Schema.Natural,
  consecutiveToolFailures: Schema.Natural,
  finalizationUsed: Schema.Boolean,
  /** Joined-input replacements already reserved; absent before this feature. */
  modelRestarts: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 2 }))),
});

/**
 * Decoded value of {@link RunPolicyUsage}.
 *
 * @category models
 * @since 0.1.0
 */
export type RunPolicyUsage = typeof RunPolicyUsage.Type;
