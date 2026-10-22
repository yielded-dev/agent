/**
 * Host-owned storage and bounded retrieval of full encoded Tool results, isolated by authorized
 * Thread identity.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { ThreadId } from "./Identifiers.ts";

/**
 * Content identity within a host-authorized Thread, never a storage URL or key.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ToolResultId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
/**
 * Decoded value of {@link ToolResultId}.
 *
 * @category models
 * @since 0.1.0
 */
export type ToolResultId = typeof ToolResultId.Type;

/**
 * Full Tool output is unavailable, missing, too large, or invalid for storage.
 *
 * @category errors
 * @since 0.1.0
 */
export class ToolResultStorageError extends Schema.TaggedError<ToolResultStorageError>()(
  "ToolResultStorageError",
  {
    reason: Schema.Literals(["unavailable", "not-found", "too-large", "invalid-output"]),
    message: Schema.String.check(Schema.isMaxLength(1024)),
  },
) {}

/**
 * Offsets count UTF-16 code units, as in String.slice. Follow nextOffset verbatim.
 *
 * @category models
 * @since 0.1.0
 */
export class ToolResultRead extends Schema.Class<ToolResultRead>("ToolResultRead")({
  threadId: ThreadId,
  id: ToolResultId,
  offset: Schema.Natural,
  maxChars: Schema.Int.check(Schema.isBetween({ minimum: 2, maximum: 4_000 })),
}) {}

/**
 * Schema for a bounded stored-result text page. Offsets count UTF-16 code units; continue with
 * nextOffset verbatim.
 *
 * @category models
 * @since 0.1.0
 */
export class ToolResultPage extends Schema.Class<ToolResultPage>("ToolResultPage")({
  id: ToolResultId,
  offset: Schema.Natural,
  text: Schema.String.check(Schema.isMaxLength(4_000)),
  nextOffset: Schema.NullOr(Schema.Natural),
}) {}

/**
 * Optional host-owned storage for full encoded application Tool results. Save must
 * finish before returning an immutable ID; read must enforce Thread isolation.
 * Hosts own retention, quotas, encryption and cleanup, including unreferenced writes
 * after interruption. Results are untrusted data and may contain private information.
 * The engine saves only oversized results, with an 8 MiB ceiling and a five-second
 * deadline. An expected storage failure leaves a bounded preview with no reference.
 *
 * @category models
 * @since 0.1.0
 */
export interface ToolResultStorage {
  readonly save: (request: {
    readonly threadId: ThreadId;
    readonly text: string;
  }) => Effect.Effect<ToolResultId, ToolResultStorageError>;
  readonly read: (request: ToolResultRead) => Effect.Effect<ToolResultPage, ToolResultStorageError>;
}

/**
 * Captured by durable runtimes; callers cannot replace a host's store on resume.
 *
 * @category services
 * @since 0.1.0
 */
export const ToolResultStorage = Context.Reference<ToolResultStorage | undefined>(
  "@effect-agent/core/ToolResultStorage",
  { defaultValue: () => undefined },
);

/**
 * Maximum full encoded Tool-result size supported by the storage contract: 8 MiB.
 *
 * @category constants
 * @since 0.1.0
 */
export const MAX_STORED_TOOL_RESULT_BYTES = 8 * 1024 * 1024;
