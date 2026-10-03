import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import {
  ScheduleFailpoint,
  ScheduleFailpointError,
  ScheduleStorageError,
  ScheduleStore,
} from "@yielded/agent/schedule";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import {
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
  SubscriptionStore,
} from "@yielded/agent/subscription";
import { Effect, Exit, Layer } from "effect";
import * as SqlClientService from "effect/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  restoreV2,
  assertPreserved,
  assertSubscriptionReplay,
  snapshotStore,
  fixturePartition,
} from "../../../test/fixtures/storage-upgrade.ts";
import { DoScheduleTransaction, scheduleStoreLayer } from "../src/DoScheduleStore.ts";
import { DoStorageFailpointError } from "../src/DoStorageError.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/DoSubmissionLedger.ts";
import { DoSubscriptionTransaction, doSubscriptionStoreLayer } from "../src/DoSubscriptionStore.ts";
import { storageConfigLayer, type DoStorageInitializationError } from "../src/DoThreadStore.ts";
import { admission, withScheduleStorage } from "./harness.ts";

let counter = 0;

const fixture = <A, E>(
  store: "schedule" | "subscription",
  body: (
    open: Effect.Effect<
      void,
      | DoStorageFailpointError
      | ScheduleStorageError
      | SubscriptionError
      | DoStorageInitializationError,
      SqlClientService.SqlClient
    >,
    dependencies: ReturnType<typeof services>,
  ) => Effect.Effect<A, E, SqlClientService.SqlClient>,
  hit: (point: string) => "failure" | "interrupt" | undefined = () => undefined,
) =>
  withScheduleStorage(`v2-upgrade-${counter++}`, (storage) =>
    Effect.gen(function* () {
      yield* restoreV2(store);
      yield* Effect.promise(() => storage.setAlarm(4_000_000_000_000));
      const deps = services(storage, hit);

      const open =
        store === "schedule"
          ? ScheduleStore.pipe(
              Effect.asVoid,
              Effect.provide(scheduleStoreLayer.pipe(Layer.provide(deps))),
            )
          : SubscriptionStore.pipe(
              Effect.asVoid,
              Effect.provide(doSubscriptionStoreLayer(fixturePartition).pipe(Layer.provide(deps))),
            );

      const value = yield* body(open, deps);

      expect(yield* Effect.promise(() => storage.getAlarm())).toBe(4_000_000_000_000);

      return value;
    }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
  );

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

describe("current thread storage and independent v2 store upgrades", () => {
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

  for (const store of ["schedule", "subscription"] as const) {
    it(`preserves ${store} state, native alarm and repeated acquisition`, () =>
      fixture(store, (open, deps) =>
        Effect.gen(function* () {
          yield* open;
          yield* assertPreserved(store);
          const after = yield* snapshotStore;

          yield* open;
          expect(yield* snapshotStore).toEqual(after);
          const sql = yield* SqlClientService.SqlClient;

          expect(
            yield* sql`SELECT storage_version,alarm_generation FROM ${sql(store === "schedule" ? "effect_agent_schedule_store_state" : "effect_agent_subscription_store_state")}`,
          ).toEqual([{ storage_version: 3, alarm_generation: store === "schedule" ? 17 : 23 }]);
          if (store === "subscription")
            yield* assertSubscriptionReplay.pipe(
              Effect.provide(doSubscriptionStoreLayer(fixturePartition).pipe(Layer.provide(deps))),
            );
        }),
      ));
    for (const mode of ["interrupt"] as const) {
      it(`rolls back ${store} on ${mode} after mutation and reopens`, () => {
        let armed = true;

        return fixture(
          store,
          (open) =>
            Effect.gen(function* () {
              const before = yield* snapshotStore;

              expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
              expect(yield* snapshotStore).toEqual(before);
              armed = false;
              yield* open;
              yield* assertPreserved(store);
            }),
          (point) => (armed && point === "upgrade:after-mutation" ? mode : undefined),
        );
      });
    }
    it(`rolls back ${store} when version write fails`, () => {
      let armed = true;

      return fixture(
        store,
        (open) =>
          Effect.gen(function* () {
            const before = yield* snapshotStore;

            expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
            expect(yield* snapshotStore).toEqual(before);
            armed = false;
            yield* open;
            yield* assertPreserved(store);
          }),
        (point) => (armed && point === "upgrade:after-version" ? "failure" : undefined),
      );
    });
  }
  it("retains ambiguous prepared delivery bytes and the v2 version", () =>
    fixture("subscription", (open) =>
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        yield* sql`UPDATE effect_agent_subscription_deliveries SET record_json=json_set(record_json,'$.subscriptionFingerprint',${"f".repeat(64)}) WHERE event_id='prepared'`;
        const before = yield* snapshotStore;
        const failure = yield* open.pipe(Effect.result);

        expect(failure).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "SubscriptionError", reason: "corrupt" },
        });
        expect(yield* snapshotStore).toEqual(before);
      }),
    ));
});
