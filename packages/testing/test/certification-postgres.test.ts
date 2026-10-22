import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as PgClient from "@effect/sql-pg/PgClient";
import { it } from "@effect/vitest";
import * as PostgresStorage from "@yielded/agent-storage-postgres/postgres-storage";
import { durableCoordinatorConformance } from "@yielded/agent-testing/durable-coordinator-conformance";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";

/** Keep coordinator checks outside the adapter package to avoid a storage-postgres -> testing cycle. */
const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

let databaseCounter = 0;

const admin = (statement: string) =>
  Effect.flatMap(PgClient.PgClient, (sql) => sql.unsafe(statement)).pipe(
    Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
    Effect.orDie,
  );

/** A database per run; `WITH (FORCE)` ends any pooled connection the run left open. */
const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      databaseCounter = databaseCounter + 1;

      return `effect_agent_certification_${process.pid}_${databaseCounter}`;
    }).pipe(Effect.tap((database) => admin(`CREATE DATABASE ${database}`))),
    (database) => {
      const url = new URL(adminUrl);

      url.pathname = `/${database}`;

      return use(url.toString());
    },
    (database) => admin(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`).pipe(Effect.ignore),
  );

const combinedAdapters = (url: string) =>
  PostgresStorage.layerWith({ observationPollInterval: 1 }).pipe(
    Layer.provideMerge(NodeCrypto.layer),
    Layer.provideMerge(PgClient.layer({ url: Redacted.make(url) })),
  );

it.effect(
  "converges PostgreSQL coordinator recovery after injected faults",
  () =>
    withTemporaryDatabase((url) =>
      durableCoordinatorConformance.pipe(Effect.provide(combinedAdapters(url))),
    ),
  300_000,
);
