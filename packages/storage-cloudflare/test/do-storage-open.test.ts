import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import {
  ScheduleFailpoint,
  ScheduleFailpointError,
  ScheduleStorageError,
} from "@yielded/agent/schedule";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import {
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
} from "@yielded/agent/subscription";
import { Effect, Layer } from "effect";
import * as SqlClientService from "effect/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import { DoScheduleTransaction } from "../src/DoScheduleStore.ts";
import { DoStorageFailpointError } from "../src/DoStorageError.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/DoSubmissionLedger.ts";
import { DoSubscriptionTransaction } from "../src/DoSubscriptionStore.ts";
import { storageConfigLayer } from "../src/DoThreadStore.ts";
import { admission, withScheduleStorage } from "./harness.ts";

let counter = 0;

const services = (
  storage: DurableObjectStorage,
  hit: (point: string) => "failure" | "interrupt" | undefined,
) => {
  const fault = <E>(point: string, error: E): Effect.Effect<void, E> => {
    switch (hit(point)) {
      case "failure":
        return Effect.fail(error);
      case "interrupt":
        return Effect.interrupt;
      case undefined:
        return Effect.void;
    }
  };

  return Layer.mergeAll(
    storageConfigLayer({ storage }),
    BrowserCrypto.layer,
    Layer.succeed(DoStorageFailpoint)({
      hit: (location) => fault(location, DoStorageFailpointError.make({ location })),
    }),
    Layer.succeed(ScheduleFailpoint)({
      hit: (point) => fault(point, ScheduleFailpointError.make({ point })),
    }),
    Layer.succeed(SubscriptionFailpoint)({
      hit: (point) => fault(point, SubscriptionFailpointError.make({ point })),
    }),
    Layer.effect(DoScheduleTransaction)(
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        return DoScheduleTransaction.of({
          run: (body) =>
            sql.withTransaction(body(() => Effect.void)).pipe(
              Effect.catchTag("SqlError", () =>
                ScheduleStorageError.make({
                  operation: "fixture transaction",
                  reason: "unavailable",
                }),
              ),
            ),
        });
      }),
    ),
    Layer.effect(DoSubscriptionTransaction)(
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        return DoSubscriptionTransaction.of({
          run: (body) =>
            sql
              .withTransaction(body(() => Effect.void))
              .pipe(
                Effect.catchTag("SqlError", () =>
                  SubscriptionError.make({ reason: "storage", code: "fixture-transaction" }),
                ),
              ),
        });
      }),
    ),
  );
};

describe("fresh thread storage with application tables", () => {
  // Regression: https://github.com/yielded-dev/agent/commit/78d05490ac4f3512b57ec37be37cab4a454a03a3
  it("initializes beside an application's migration history and preserves admission receipts", () =>
    withScheduleStorage(`shared-sql-migrations-${counter++}`, (storage) =>
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        yield* sql`CREATE TABLE effect_sql_migrations (
          migration_id INTEGER PRIMARY KEY,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          name VARCHAR(255) NOT NULL
        )`;
        yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (100, 'application')`;
        yield* sql`CREATE TABLE application_messages (id TEXT PRIMARY KEY, body TEXT NOT NULL)`;
        yield* sql`INSERT INTO application_messages VALUES ('human', 'hey')`;

        const submit = Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const request = yield* admission("shared-sql-thread", "human", { text: "hey" });

          return yield* ledger.admit(request);
        }).pipe(
          Effect.provide(
            submissionLedgerLayer.pipe(Layer.provideMerge(services(storage, () => undefined))),
          ),
        );

        const first = yield* submit;
        const replay = yield* submit;

        expect(first.replayed).toBe(false);
        expect(replay).toEqual({ ...first, replayed: true });
        expect(yield* sql`SELECT migration_id, name FROM effect_sql_migrations`).toEqual([
          { migration_id: 100, name: "application" },
        ]);
        expect(yield* sql`SELECT id, body FROM application_messages`).toEqual([
          { id: "human", body: "hey" },
        ]);
      }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
    ));
});
