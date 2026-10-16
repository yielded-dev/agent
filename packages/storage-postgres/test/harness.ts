import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as PostgresStorage from "@yielded/agent-storage-postgres/postgres-storage";
import type { MessageDeliveryStoreLimits } from "@yielded/agent/message-delivery";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";

/**
 * A live Postgres is required for transaction rollback and connection cleanup;
 * an in-process double cannot establish those properties. `EFFECT_AGENT_TEST_POSTGRES_URL` points CI at its own service.
 */
const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

let databaseCounter = 0;

const admin = <A, E>(effect: Effect.Effect<A, E, PgClient.PgClient>) =>
  effect.pipe(
    Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
    Effect.catchTag("SqlError", (error) =>
      Effect.die(
        new Error(
          "The storage-postgres tests need a reachable configured Postgres server " +
            "(set EFFECT_AGENT_TEST_POSTGRES_URL, or run one with " +
            "`docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=effect_agent postgres:17-alpine`).",
          { cause: error },
        ),
      ),
    ),
  );

/**
 * Each case owns a database. `WITH (FORCE)` ends
 * any pooled connection the case left open.
 */
export const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      databaseCounter = databaseCounter + 1;

      return `effect_agent_test_${process.pid}_${databaseCounter}`;
    }).pipe(
      Effect.tap((database) =>
        admin(
          Effect.flatMap(PgClient.PgClient, (sql) => sql.unsafe(`CREATE DATABASE ${database}`)),
        ),
      ),
    ),
    (database) => {
      const url = new URL(adminUrl);

      url.pathname = `/${database}`;

      return use(url.toString());
    },
    (database) =>
      admin(
        Effect.flatMap(PgClient.PgClient, (sql) =>
          sql.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`),
        ),
      ).pipe(Effect.ignore),
  );

export const clientLayer = (url: string) => PgClient.layer({ url: Redacted.make(url) });

/** Supply the real native client at the test composition root. */
export const storage = (
  url: string,
  options: PostgresStorage.PostgresStorageOptions = {},
  client: Omit<PgClient.PgPoolConfig, "url"> = {},
) => {
  const settings = { observationPollInterval: 1, ...options };
  const clientLayer = PgClient.layer({ ...client, url: Redacted.make(url) });
  const dependencies = Layer.merge(clientLayer, NodeCrypto.layer);

  return {
    clientLayer,
    threadStore: PostgresStorage.threadStoreLayer(settings).pipe(Layer.provide(dependencies)),
    submissionLedger: PostgresStorage.submissionLedgerLayer(settings).pipe(
      Layer.provide(dependencies),
    ),
    scheduleStore: PostgresStorage.scheduleStoreLayer(settings).pipe(Layer.provide(dependencies)),
    activityStore: PostgresStorage.activityStoreLayer(settings).pipe(Layer.provide(dependencies)),
    messageDeliveryStore: (limits?: MessageDeliveryStoreLimits) =>
      PostgresStorage.messageDeliveryStoreLayer({ ...settings, limits }).pipe(
        Layer.provide(dependencies),
      ),
    subscriptionStore: (partition: Parameters<typeof PostgresStorage.subscriptionStoreLayer>[0]) =>
      PostgresStorage.subscriptionStoreLayer(partition, settings).pipe(Layer.provide(dependencies)),
  };
};

/** One pool connection makes subsequent operations verify failed-transaction cleanup. */
export const singleConnectionStorage = (url: string, lockTimeout: number) =>
  storage(url, { lockTimeout }, { maxConnections: 1 });

/**
 * Holds a native table lock like an administrative operation, then rolls it back.
 */
export const whileHoldingTableLock = <A, E>(
  url: string,
  use: Effect.Effect<A, E>,
  table: "effect_agent_threads" | "effect_agent_submissions" = "effect_agent_threads",
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* PgClient.PgClient;
      const connection = yield* sql.reserve;

      yield* connection.executeUnprepared("BEGIN", [], undefined);
      yield* connection.executeUnprepared(`LOCK TABLE ${table} IN SHARE MODE`, [], undefined);

      const result = yield* use;

      yield* connection.executeUnprepared("ROLLBACK", [], undefined);

      return result;
    }),
  ).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(url), maxConnections: 1 })));
