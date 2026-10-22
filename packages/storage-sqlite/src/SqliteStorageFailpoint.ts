/**
 * Explicit fault injection for SQLite storage operation boundaries.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type {
  SqliteStorageFailpointError,
  SqliteStorageFailpointLocation,
} from "./SqliteStorageError.ts";

/**
 * Handler invoked at an explicit SQLite storage fault-injection boundary.
 *
 * @category models
 * @since 0.1.0
 */
export type SqliteStorageFailpointHandler = (
  location: SqliteStorageFailpointLocation,
) => Effect.Effect<void, SqliteStorageFailpointError>;

const noFailpoint: SqliteStorageFailpointHandler = () => Effect.void;

/**
 * Explicit fault-injection authority used at SQLite operation boundaries.
 *
 * @category services
 * @since 0.1.0
 */
export class SqliteStorageFailpoint extends Context.Service<
  SqliteStorageFailpoint,
  {
    readonly hit: SqliteStorageFailpointHandler;
  }
>()("@effect-agent/storage-sqlite/SqliteStorageFailpoint") {
  /**
   * Production default: no fault injection.
   *
   * @category layers
   * @since 0.1.0
   */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });
}
