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

export type SqliteActivityInitializationError = ActivityStoreError | ActivityMutationFailure;

const makeActivityStore = Effect.fnUntraced(function* () {
  const sql = yield* SqlClientService.SqlClient;

  return yield* makeSqlActivityStore(sql.withTransaction);
});

/** SQLite activity progress with mutation failpoints kept injectable for recovery tests. */
export const activityProcessorStoreLayerWithFailpoints: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient | ActivityMutationFailpoint
> = Layer.effect(ActivityProcessorStore, makeActivityStore());

/**
 * Optional activity progress with scoped local sessions. Share the same SqlClient across
 * live Layers for one physical database; separate processes cannot share an activity key.
 */
export const activityProcessorStoreLayer: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient
> = activityProcessorStoreLayerWithFailpoints.pipe(Layer.provide(ActivityMutationFailpoint.layer));
