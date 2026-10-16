import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { describe, it } from "@effect/vitest";
import { scheduleStoreLayer } from "@yielded/agent-storage-sqlite/sqlite-schedule-store";
import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
} from "@yielded/agent-storage-sqlite/sqlite-storage-config";
import { SqliteStorageFailpoint } from "@yielded/agent-storage-sqlite/sqlite-storage-failpoint";
import { scheduleStoreConformanceCases } from "@yielded/agent/testing/schedule-store-conformance";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import type * as PlatformError from "effect/PlatformError";

const testLayer = (filename: string) =>
  scheduleStoreLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqliteStorageConfig)(
          SqliteStorageConfigValue.make({
            observationPollInterval: 1,
            busyTimeout: 5_000,
            synchronous: "FULL",
            verifyOnOpen: false,
          }),
        ),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-schedule-sqlite-",
      });

      return yield* use(`${directory}/schedules.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((filename) =>
        conformanceCase.run.pipe(Effect.provide(testLayer(filename))),
      ),
    );
  }
});
