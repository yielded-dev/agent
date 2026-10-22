/**
 * Transaction hooks for enrolling newly committed work in host maintenance.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/**
 * Failure to enroll newly committed execution work. The source transaction must roll back.
 *
 * @category errors
 * @since 0.1.0
 */
export class SqlStorageProgressError extends Schema.TaggedError<SqlStorageProgressError>()(
  "SqlStorageProgressError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Committed source changes that can enroll host maintenance work.
 *
 * @category models
 * @since 0.1.0
 */
export type SqlStorageProgressKind =
  | "canonical"
  | "submission"
  | "control"
  | "lifecycle"
  | "lifecycle-ack"
  | "delivery";

/**
 * Source-transaction enrollment for durable maintenance. Call only after a new fact was
 * written, before its transaction commits. Replays, claims, leases and retry clocks are not
 * progress. Platforms may atomically replenish their finite maintenance budget and alarm;
 * ordinary SQL consumers do not need an alarm implementation.
 *
 * @category services
 * @since 0.1.0
 */
export const SqlStorageProgress = Context.Reference<{
  readonly committed: (
    kind: SqlStorageProgressKind,
  ) => Effect.Effect<void, SqlStorageProgressError>;
}>("@effect-agent/storage-sql/SqlStorageProgress", {
  defaultValue: () => ({ committed: () => Effect.void }),
});
