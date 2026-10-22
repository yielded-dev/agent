/**
 * SQLite activity progress with scoped process-local ownership.
 *
 * @since 0.1.0
 */
import { makeSqlActivityStore } from "@yielded/agent-storage-sql/sql-activity-store";
import {
  ActivityMutationFailpoint,
  type ActivityMutationFailure,
  ActivityProcessorStore,
  type ActivityStoreError,
} from "@yielded/agent/activity-store";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClientService from "effect/sql/SqlClient";

/**
 * Failures while initializing SQLite activity progress storage.
 *
 * @category errors
 * @since 0.1.0
 */
export type SqliteActivityInitializationError = ActivityStoreError | ActivityMutationFailure;

const makeActivityStore = Effect.fnUntraced(function* () {
  const sql = yield* SqlClientService.SqlClient;

  return yield* makeSqlActivityStore(sql.withTransaction);
});

/**
 * SQLite activity progress with mutation failpoints kept injectable for recovery tests.
 *
 * @category layers
 * @since 0.1.0
 */
export const activityProcessorStoreLayerWithFailpoints: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient | ActivityMutationFailpoint
> = Layer.effect(ActivityProcessorStore, makeActivityStore());

/**
 * Optional activity progress with scoped local sessions. Share the same SqlClient across
 * live Layers for one physical database; separate processes cannot share an activity key.
 *
 * @category layers
 * @since 0.1.0
 */
export const activityProcessorStoreLayer: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient
> = activityProcessorStoreLayerWithFailpoints.pipe(Layer.provide(ActivityMutationFailpoint.layer));
