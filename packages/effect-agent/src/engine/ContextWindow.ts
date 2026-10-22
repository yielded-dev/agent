/**
 * Live model-context capacity, rollover requests, and per-call context bounds, separate from
 * cumulative Run budgets.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Schema from "effect/Schema";

import { RunId, ThreadId } from "../core/Identifiers.ts";

/**
 * Model-authored continuation state; fuller notes belong in an application-owned memory store.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ContextHandoff = Schema.NonEmptyString.check(
  Schema.isMaxLength(20_000),
  Schema.isPattern(/\S/),
  Schema.makeFilter((text) => Hex.encode(JSON.stringify(text)).length / 2 <= 32_768, {
    expected: "at most 32768 JSON-encoded UTF-8 bytes",
  }),
);

/**
 * Successful output of an explicitly designated context-window Tool.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ContextRolloverRequest = Schema.Struct({
  handoff: Schema.optionalKey(ContextHandoff),
});

/**
 * Decoded value of {@link ContextRolloverRequest}.
 *
 * @category models
 * @since 0.1.0
 */
export type ContextRolloverRequest = typeof ContextRolloverRequest.Type;

/**
 * Host-selected exclusive source-message boundary, applied at the next safe model seam.
 * Omit through to reset the prefix before the protected current instructions/input. The
 * engine resolves that prefix after preparation; an empty prior prefix needs no new window.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ContextRolloverSelection = Schema.Struct({
  ...ContextRolloverRequest.fields,
  through: Schema.optionalKey(Schema.Natural),
});

/**
 * Decoded value of {@link ContextRolloverSelection}.
 *
 * @category models
 * @since 0.1.0
 */
export type ContextRolloverSelection = typeof ContextRolloverSelection.Type;

/**
 * Bounds resolved alongside the native model for one Turn. Output reserve must match the
 * selected provider configuration. Uncounted overhead includes only provider framing or image
 * token costs absent from the prompt estimate; prompt text, output contracts, and native Tool
 * schemas are counted by the engine. These are live-context bounds, not cumulative Run budgets.
 *
 * @category models
 * @since 0.1.0
 */
export class ModelCallContext extends Schema.Class<ModelCallContext>("ModelCallContext")({
  contextCapacity: Schema.Int.check(Schema.isGreaterThan(0)),
  maxInputTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  outputReserveTokens: Schema.Natural,
  uncountedOverheadTokens: Schema.Natural,
}) {
  /** Supply bounds with a native Model Layer; a prepared modelCall takes precedence. */
  static readonly current = Context.Reference<ModelCallContext | undefined>(
    "@effect-agent/engine/ModelCallContext",
    { defaultValue: () => undefined },
  );
}

/**
 * Definition-owned control annotation. A successful singleton application Tool carrying this
 * annotation returns ContextRolloverRequest. The engine applies it at the next Turn seam,
 * including after durable Tool-result replay. A Tool name alone never grants this authority.
 *
 * @category services
 * @since 0.1.0
 */
export const ContextRolloverTool = Context.Reference<boolean>(
  "@effect-agent/engine/ContextRolloverTool",
  { defaultValue: () => false },
);

/**
 * Best available live-context estimate; unbounded calls compute it only when requested.
 *
 * @category models
 * @since 0.1.0
 */
export class ContextWindowStatus extends Schema.Class<ContextWindowStatus>("ContextWindowStatus")({
  threadId: ThreadId,
  runId: RunId,
  windowId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  estimatedTokens: Schema.Natural,
  contextTokenLimit: Schema.NullOr(Schema.Natural),
  remainingTokens: Schema.NullOr(Schema.Natural),
}) {}

/**
 * Engine-owned identity and context accounting for the current Run.
 *
 * @category services
 * @since 0.1.0
 */
export class ContextWindow extends Context.Service<
  ContextWindow,
  {
    readonly status: Effect.Effect<ContextWindowStatus>;
  }
>()("@effect-agent/engine/ContextWindow") {}
