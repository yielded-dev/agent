import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { expect, it } from "@effect/vitest";
import * as PostgresStorage from "@yielded/agent-storage-postgres/postgres-storage";
import {
  PostgresStorageCompatibilityError,
  PostgresStorageError,
} from "@yielded/agent-storage-postgres/postgres-storage-error";
import { ThreadStore } from "@yielded/agent/thread-store";
import { Cause, Effect, Exit, Redacted, Schema, String } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { TestClock } from "effect/testing";

import { withTemporaryDatabase } from "./harness.ts";

const Session = Schema.Array(Schema.Struct({ name: Schema.String, pid: Schema.Int }));

// 87e5dad4: rendered definitions reject physical tuning and quote_all_identifiers.
// Reopen must inspect current indexes without rewriting equivalent definitions.
it.effect(
  "opens equivalent current layout definitions but rejects a changed predicate without mutation",
  () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        yield* ThreadStore.pipe(Effect.asVoid, Effect.provide(PostgresStorage.threadStoreLayer()));

        yield* sql`INSERT INTO effect_agent_threads VALUES ('retained', '2026-10-05', 0, 'digest', 0)`;
        const retained = yield* sql`SELECT * FROM effect_agent_threads`;

        yield* sql`ALTER INDEX effect_agent_submissions_nonterminal SET (fillfactor = 80)`;

        const open = ThreadStore.pipe(
          Effect.asVoid,
          Effect.provide(PostgresStorage.threadStoreLayer()),
        );

        yield* open;
        expect(yield* sql`SELECT version FROM effect_agent_storage_version`).toEqual([
          { version: 22n },
        ]);
        expect(yield* sql`SELECT * FROM effect_agent_threads`).toEqual(retained);
        yield* open;

        yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
        yield* sql`CREATE INDEX effect_agent_submissions_nonterminal
        ON effect_agent_submissions (thread_id, queue_sequence) WHERE state = 'settled'`;
        const rejected = yield* Effect.flip(open);

        expect(rejected).toBeInstanceOf(PostgresStorageCompatibilityError);
        expect(yield* sql`SELECT version FROM effect_agent_storage_version`).toEqual([
          { version: 22n },
        ]);
        expect(
          yield* sql`SELECT to_regclass('public.effect_agent_schema') IS NOT NULL AS header`,
        ).toEqual([{ header: true }]);
        expect(yield* sql`SELECT * FROM effect_agent_threads`).toEqual(retained);
      }).pipe(
        Effect.provide([
          PgClient.layer({
            url: Redacted.make(url),
            startupParameters: { quote_all_identifiers: "on" },
          }),
          NodeCrypto.layer,
        ]),
      ),
    ),
);

it.effect(
  "qualifies storage tables without changing the native client across concurrent and replaced connections",
  () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const configuredUrl = new URL(url);

        configuredUrl.searchParams.set("options", "-c search_path=pg_catalog");

        const client = {
          url: Redacted.make(configuredUrl.toString()),
          startupParameters: { search_path: "pg_catalog" },
          transformQueryNames: String.snakeToCamel,
          maxConnections: 2,
          connectionTTL: 1000,
        };

        // A keyword schema also exercises identifier quoting during the adapter's own DDL.
        yield* Effect.gen(function* () {
          yield* Effect.asVoid(ThreadStore).pipe(
            Effect.provide(PostgresStorage.threadStoreLayer({ schema: "select" })),
          );
          const sql = yield* SqlClient.SqlClient;

          const initial = yield* Effect.scoped(
            Effect.gen(function* () {
              const first = yield* sql.reserve;
              const second = yield* sql.reserve;

              return [
                yield* Schema.decodeEffect(Session)(
                  yield* first.executeUnprepared(
                    "SELECT current_schema() AS name, pg_backend_pid() AS pid",
                    [],
                    undefined,
                  ),
                ),
                yield* Schema.decodeEffect(Session)(
                  yield* second.executeUnprepared(
                    "SELECT current_schema() AS name, pg_backend_pid() AS pid",
                    [],
                    undefined,
                  ),
                ),
              ].flat();
            }),
          );

          expect(initial.map((session) => session.name)).toEqual(["pg_catalog", "pg_catalog"]);
          expect(new Set(initial.map((session) => session.pid)).size).toBe(2);

          yield* TestClock.adjust(1001);

          const replacement = yield* Schema.decodeUnknownEffect(Session)(
            yield* sql`SELECT current_schema() AS name, pg_backend_pid() AS pid`,
          );

          expect(replacement[0]?.name).toBe("pg_catalog");
          expect(initial.map((session) => session.pid)).not.toContain(replacement[0]?.pid);

          const tables =
            yield* sql`SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='effect_agent_storage_version'`;

          expect(tables).toEqual([{ nspname: "select" }]);
        }).pipe(Effect.provide([PgClient.layer(client), NodeCrypto.layer]));
      }),
    ),
);

it.effect("rejects a zero writer timeout before opening storage", () =>
  Effect.gen(function* () {
    const opened = yield* ThreadStore.pipe(
      Effect.provide(PostgresStorage.threadStoreLayer({ lockTimeout: 0 })),
      Effect.provide([PgClient.layer({ port: 1 }), NodeCrypto.layer]),
      Effect.exit,
    );

    expect(Exit.isFailure(opened)).toBe(true);
    if (Exit.isFailure(opened)) {
      const failure = Cause.squash(opened.cause);

      expect(failure).toBeInstanceOf(PostgresStorageError);
      expect(failure).toMatchObject({
        operation: "configure Postgres storage",
        cause: { _tag: "SchemaError" },
      });
    }
  }),
);
