import { makeSqlSubscriptionStore } from "@yielded/agent-storage-sql/sql-subscription-store";
import { SourcePartition, SubscriptionError, SubscriptionStore } from "@yielded/agent/subscription";
import { Effect, Layer, Schema } from "effect";
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

export const subscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore,
  SqliteStorageInitializationError | SubscriptionError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient
> => Layer.effect(SubscriptionStore, makeSubscriptionStore(partition));
