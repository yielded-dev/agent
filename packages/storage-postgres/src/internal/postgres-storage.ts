import { makeSqlJournal, sqlStorageOwner } from "@yielded/agent-storage-sql/sql-journal";
import {
  makeSqlQuery,
  makeSqlTransaction,
  type StorageErrorFields,
  type CorruptionErrorFields,
} from "@yielded/agent-storage-sql/sql-storage";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/sql/SqlError";

import {
  PostgresStorageCorruptionError,
  PostgresStorageError,
  PostgresWriteContention,
  type PostgresStorageFailpointLocation,
  type PostgresStorageFailpointError,
} from "../PostgresStorageError.ts";
import { applyPostgresLayout, inspectPostgresStorage } from "./storage-layout.ts";

export { CurrentPostgresStorageVersion, readPostgresStorageHeader } from "./storage-layout.ts";

const storageError = (operation: string) => (cause: SqlError) =>
  PostgresStorageError.make({ operation, cause, message: cause.message });

export const classifyWriteFailure =
  (operation: string) =>
  (cause: SqlError): PostgresStorageError | PostgresWriteContention =>
    cause.reason._tag === "SerializationError" ||
    cause.reason._tag === "DeadlockError" ||
    cause.reason._tag === "LockTimeoutError"
      ? PostgresWriteContention.make({
          operation,
          cause,
          message: `Postgres reported a transient transaction conflict; ${operation} is safe to retry.`,
        })
      : storageError(operation)(cause);

/**
 * Applications route each Thread to one scoped owner. This local gate is shared by all ports
 * using the same client/namespace; it is not a distributed lock or failover protocol.
 */
export const makeWriteTransaction = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  lockTimeout: number,
  namespace: string,
) {
  const owner = yield* sqlStorageOwner(sql, namespace);

  const transaction = makeSqlTransaction(sql, {
    begin: "BEGIN ISOLATION LEVEL READ COMMITTED",
    prelude: sql`SELECT set_config('lock_timeout', ${`${lockTimeout}ms`}, true)`.withoutTransform,
  });

  return <A, E, R>(body: Effect.Effect<A, E, R>) => owner.withMutation(transaction(body));
});

/** Every page of a multi-query export observes the same snapshot without creating write authority. */
const withReadTransaction = (sql: SqlClient.SqlClient) =>
  makeSqlTransaction(sql, { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" });

/** Run while holding the owner transaction, including when this schema does not yet exist. */
export const ensurePostgresSchema = Effect.fnUntraced(function* (schema: string) {
  const sql = yield* SqlClient.SqlClient;
  const { execute } = yield* makeSqlQuery();

  const existing = yield* execute(sql`SELECT 1 FROM pg_namespace WHERE nspname = ${schema}`);

  // Even IF NOT EXISTS requires database-wide CREATE permission.
  if (existing.length === 0) {
    yield* execute(sql`CREATE SCHEMA IF NOT EXISTS ${sql(schema)}`);
  }
});

export const postgresStorageErrors = {
  storage: (fields: StorageErrorFields) => PostgresStorageError.make(fields),
  corruption: (fields: CorruptionErrorFields) => PostgresStorageCorruptionError.make(fields),
  isCorruption: Schema.is(PostgresStorageCorruptionError),
};

const isTransactionFailure = Schema.is(
  Schema.Union([PostgresStorageError, PostgresWriteContention]),
);

/** Inspect under the owner gate before any DDL; current headers commit together. */
export const initializePostgresStorage = Effect.fn("PostgresStorage.initializeLayout")(function* ({
  lockTimeout,
  schema,
}: {
  readonly lockTimeout: number;
  readonly schema: string;
}) {
  const sql = yield* SqlClient.SqlClient;

  const write = yield* makeWriteTransaction(sql, lockTimeout, schema);

  yield* write(
    Effect.gen(function* () {
      const header = yield* inspectPostgresStorage(schema, true);

      if (header === undefined) yield* ensurePostgresSchema(schema);
      yield* applyPostgresLayout(schema, header);
    }),
  ).pipe(
    Effect.catchTag("SqlError", (error) =>
      Effect.fail(classifyWriteFailure("initialize storage")(error)),
    ),
  );
});

/** Bind shared journal operations without repeating format initialization. */
export const makePostgresJournal = Effect.fnUntraced(function* (
  lockTimeout: number,
  hitFailpoint: (
    location: PostgresStorageFailpointLocation,
  ) => Effect.Effect<void, PostgresStorageFailpointError>,
  namespace: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const write = yield* makeWriteTransaction(sql, lockTimeout, namespace);
  const read = withReadTransaction(sql);

  return yield* makeSqlJournal({
    namespace,
    errors: postgresStorageErrors,
    hitFailpoint,
    transactions: {
      withWriteTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          write(body).pipe(
            Effect.mapError((error) => {
              if (isSqlError(error)) return classifyWriteFailure(operation)(error);
              if (Schema.is(PostgresStorageError)(error) && isSqlError(error.cause)) {
                return classifyWriteFailure(error.operation)(error.cause);
              }

              return error;
            }),
          ),
      withReadTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          read(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? storageError(operation)(error) : error,
            ),
          ),
      isTransactionFailure,
    },
  });
});
