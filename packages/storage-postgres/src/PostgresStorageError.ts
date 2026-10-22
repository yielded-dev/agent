/**
 * Typed PostgreSQL storage, compatibility, and contention failures.
 *
 * @since 0.1.0
 */
import { SqlStorageFailpointLocation } from "@yielded/agent-storage-sql/sql-storage-failpoint";
import * as Schema from "effect/Schema";

/**
 * The database uses an unsupported or inconsistent storage layout or record format.
 *
 * @category errors
 * @since 0.1.0
 */
export class PostgresStorageCompatibilityError extends Schema.TaggedError<PostgresStorageCompatibilityError>()(
  "PostgresStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/**
 * Stored bytes failed the current Schema and cannot be used as recovery truth.
 *
 * @category errors
 * @since 0.1.0
 */
export class PostgresStorageCorruptionError extends Schema.TaggedError<PostgresStorageCorruptionError>()(
  "PostgresStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
  },
) {}

/**
 * Postgres infrastructure failed while opening or operating the store.
 *
 * @category errors
 * @since 0.1.0
 */
export class PostgresStorageError extends Schema.TaggedError<PostgresStorageError>()(
  "PostgresStorageError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * A write transaction lost a concurrency race and was rolled back by Postgres: a
 * serialization failure (40001), a deadlock (40P01), or a lock timeout (55P03). The
 * transaction mutated no canonical state and is safe to retry.
 *
 * @category errors
 * @since 0.1.0
 */
export class PostgresWriteContention extends Schema.TaggedError<PostgresWriteContention>()(
  "PostgresWriteContention",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * Schema for PostgreSQL storage fault-injection boundaries.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PostgresStorageFailpointLocation = SqlStorageFailpointLocation;

/**
 * A PostgreSQL storage fault-injection boundary.
 *
 * @category models
 * @since 0.1.0
 */
export type PostgresStorageFailpointLocation = typeof PostgresStorageFailpointLocation.Type;

/**
 * Deterministic test-only fault or pause injected at a Postgres operation boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class PostgresStorageFailpointError extends Schema.TaggedError<PostgresStorageFailpointError>()(
  "PostgresStorageFailpointError",
  {
    location: PostgresStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected Postgres storage failure at ${this.location}.`;
  }
}

/**
 * Every way opening Postgres storage can fail before it serves a request.
 *
 * @category errors
 * @since 0.1.0
 */
export type PostgresStorageInitializationError =
  | PostgresStorageCompatibilityError
  | PostgresStorageCorruptionError
  | PostgresStorageError
  | PostgresWriteContention;
