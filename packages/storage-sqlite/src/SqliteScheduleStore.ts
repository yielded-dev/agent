/**
 * SQLite persistence for scheduled agent input.
 *
 * @since 0.1.0
 */
import {
  makeSqlScheduleStore,
  SqlScheduleWriteTransaction,
} from "@yielded/agent-storage-sql/sql-schedule-store";
import { ScheduleStore } from "@yielded/agent/schedule";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { initializeSqliteStorage } from "./internal/sqlite-journal.ts";
import type { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import type { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import type { SqliteStorageInitializationError } from "./SqliteThreadStore.ts";

/**
 * SQLite implementation of the atomic ScheduleStore port.
 *
 * @category layers
 * @since 0.1.0
 */
export const scheduleStoreLayer: Layer.Layer<
  ScheduleStore,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClient.SqlClient
> = Layer.effect(
  ScheduleStore,
  Effect.gen(function* () {
    yield* initializeSqliteStorage();

    return yield* makeSqlScheduleStore();
  }),
).pipe(
  Layer.provide(
    Layer.effect(
      SqlScheduleWriteTransaction,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        return { run: sql.withTransaction };
      }),
    ),
  ),
);
