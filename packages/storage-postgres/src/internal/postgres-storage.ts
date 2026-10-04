import { makeSqlJournal } from "@yielded/agent-storage-sql/sql-journal";
import {
  makeSqlQuery,
  makeSqlTransaction,
  type StorageErrorFields,
  type CorruptionErrorFields,
} from "@yielded/agent-storage-sql/sql-storage";
import { CURRENT_RECORD_FORMAT } from "@yielded/agent/records";
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

/**
 * This exact FNV-1a hash, including its tag and UTF-8 encoding, is a persistent advisory-lock
 * wire format and must never change: a different key would let an old and a new deployment write
 * concurrently. The shape follows `SqlRunnerStorage`'s lock namespace in Effect's cluster module.
 */
const advisoryLockKey = (tag: string): number => {
  const bytes = new TextEncoder().encode(`effect-agent:${tag}`);
  let hash = 0x811c9dc5;

  for (const byte of bytes) hash = Math.imul(hash ^ byte, 0x01000193);

  return hash | 0;
};

/** One key serialises every writer, which is the scope SQLite's write lock had. */
export const WRITER_LOCK_KEY = advisoryLockKey("storage/writer");

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
          message: `Another producer won the Postgres write race; ${operation} is safe to retry.`,
        })
      : storageError(operation)(cause);

/**
 * READ COMMITTED observes the preceding writer's changes after the interruptible lock wait.
 * The shared transaction rolls back before releasing its connection on failure.
 */
export const withWriterLockTransaction = (sql: SqlClient.SqlClient, lockTimeout: number) =>
  makeSqlTransaction(sql, {
    begin: "BEGIN ISOLATION LEVEL READ COMMITTED",
    prelude: Effect.gen(function* () {
      yield* sql`SELECT set_config('lock_timeout', ${`${lockTimeout}ms`}, true)`.withoutTransform;
      yield* sql`SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`.withoutTransform;
    }),
  });

/** Every page of a multi-query export observes the same snapshot without taking the writer lock. */
const withReadTransaction = (sql: SqlClient.SqlClient) =>
  makeSqlTransaction(sql, { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" });

/** Run while holding the writer transaction, including when this schema does not yet exist. */
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

/** Inspect under the writer lock before any DDL; header and legacy marker commit together. */
export const initializePostgresStorage = Effect.fn("PostgresStorage.upgradeLayout")(function* ({
  lockTimeout,
  schema,
}: {
  readonly lockTimeout: number;
  readonly schema: string;
}) {
  const sql = yield* SqlClient.SqlClient;

  yield* withWriterLockTransaction(
    sql,
    lockTimeout,
  )(
    Effect.gen(function* () {
      const header = yield* inspectPostgresStorage(schema, [CURRENT_RECORD_FORMAT]);

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
  const write = withWriterLockTransaction(sql, lockTimeout);
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
