/**
 * SQLite persistence for an explicitly owned subscription partition.
 *
 * @since 0.1.0
 */
import { makeSqlSubscriptionStore } from "@yielded/agent-storage-sql/sql-subscription-store";
import { SourcePartition, SubscriptionError, SubscriptionStore } from "@yielded/agent/subscription";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as SqlClientService from "effect/sql/SqlClient";

import { initializeSqliteStorage } from "./internal/sqlite-journal.ts";
import type { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import type { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import type { SqliteStorageInitializationError } from "./SqliteThreadStore.ts";

const makeSubscriptionStore = Effect.fnUntraced(function* (owned: SourcePartition) {
  const partition = yield* Schema.decodeEffect(SourcePartition)(owned).pipe(
    Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
  );

  yield* initializeSqliteStorage();

  return yield* makeSqlSubscriptionStore(partition, {
    maxStoredJsonLength: 16 * 1024 * 1024,
  });
});

/**
 * Subscription storage for one explicit source partition.
 *
 * @category layers
 * @since 0.1.0
 */
export const subscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore,
  SqliteStorageInitializationError | SubscriptionError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient
> => Layer.effect(SubscriptionStore, makeSubscriptionStore(partition));
