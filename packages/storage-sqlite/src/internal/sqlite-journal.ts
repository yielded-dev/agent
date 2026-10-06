import { makeSqlJournalKernel } from "@yielded/agent-storage-sql/sql-journal";
import {
  makeRowDecoder,
  makeSqlTransaction,
  type CorruptionErrorFields,
  type StorageErrorFields,
} from "@yielded/agent-storage-sql/sql-storage";
import { Effect, Option, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { Connection } from "effect/sql/SqlConnection";
import { isSqlError, type SqlError } from "effect/sql/SqlError";

import { SqliteStorageConfig } from "../SqliteStorageConfig.ts";
import {
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
  SqliteWriteContention,
} from "../SqliteStorageError.ts";
import { SqliteStorageFailpoint } from "../SqliteStorageFailpoint.ts";
import { CurrentSqliteStorageVersion, ensureSqliteStorageLayout } from "./migrations.ts";

class SqliteJournalModeRow extends Schema.Class<SqliteJournalModeRow>("SqliteJournalModeRow")({
  journal_mode: Schema.NonEmptyString.check(Schema.isMaxLength(32)),
}) {}

const storageError =
  (operation: string) =>
  (error: SqlError): SqliteStorageError =>
    SqliteStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

export const sqliteErrors = {
  storage: (fields: StorageErrorFields) => SqliteStorageError.make(fields),
  corruption: (fields: CorruptionErrorFields) => SqliteStorageCorruptionError.make(fields),
  isCorruption: Schema.is(SqliteStorageCorruptionError),
};

const { decodeSingleRow } = makeRowDecoder((fields) => SqliteStorageCorruptionError.make(fields));
const decodeJournalModeRow = decodeSingleRow(Schema.Array(SqliteJournalModeRow));

const SynchronousRow = Schema.Tuple([Schema.Struct({ synchronous: Schema.Int })]);

const DatabaseHeader = Schema.Tuple([
  Schema.Struct({ user_version: Schema.Int, schema_object_count: Schema.Natural }),
]);

const connectionModes = new WeakMap<Connection, "FULL" | "NORMAL">();

/** Select once per actual connection, including aliases with different SQL transformations. */
export const configureSqliteSynchronous = Effect.fnUntraced(function* () {
  const sql = yield* SqlClient.SqlClient;
  const { synchronous } = yield* SqliteStorageConfig;
  const operation = "configure SQLite synchronization";

  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* SqliteStorageError.make({
      operation,
      message: "SQLite storage must be constructed outside an existing SQL transaction.",
    });
  yield* Effect.scoped(
    Effect.gen(function* () {
      const connection = yield* sql.reserve;
      const selected = connectionModes.get(connection);

      if (selected !== undefined && selected !== synchronous)
        return yield* SqliteStorageError.make({
          operation,
          message: `This SQLite connection already uses ${selected}; stores sharing a client must use the same synchronous option.`,
        });
      if (selected === undefined)
        yield* connection.executeUnprepared(
          synchronous === "FULL" ? "PRAGMA synchronous = FULL" : "PRAGMA synchronous = NORMAL",
          [],
          undefined,
        );

      const [actual] = yield* Schema.decodeUnknownEffect(SynchronousRow)(
        yield* connection.executeUnprepared("PRAGMA synchronous", [], undefined),
      ).pipe(
        Effect.mapError((cause) =>
          SqliteStorageError.make({ operation, message: cause.message, cause }),
        ),
      );

      if (actual.synchronous !== (synchronous === "FULL" ? 2 : 1))
        return yield* SqliteStorageError.make({
          operation,
          message: `SQLite synchronization changed after construction or could not be configured as ${synchronous}.`,
        });
      connectionModes.set(connection, synchronous);
    }),
  ).pipe(Effect.catchTag("SqlError", (cause) => storageError(operation)(cause)));
});

export const initializeSqliteJournalKernel = Effect.fnUntraced(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const { hit: failpoint } = yield* SqliteStorageFailpoint;
  const { busyTimeout } = yield* SqliteStorageConfig;

  yield* configureSqliteSynchronous();
  yield* sql`PRAGMA foreign_keys = ON`.pipe(Effect.mapError(storageError("enable foreign keys")));
  // PRAGMA statements do not accept bound parameters; the value is a schema-validated
  // non-negative integer, never caller-controlled text.
  yield* sql
    .unsafe(`PRAGMA busy_timeout = ${busyTimeout}`)
    .pipe(Effect.mapError(storageError("configure busy timeout")));

  const journalModeRows = yield* sql<Record<string, unknown>>`PRAGMA journal_mode`.pipe(
    Effect.mapError(storageError("read journal mode")),
  );

  let journalMode = yield* decodeJournalModeRow(
    "pragma_journal_mode",
    "singleton",
    journalModeRows,
  );

  if (journalMode.journal_mode.toLowerCase() !== "wal") {
    const [header] = yield* Schema.decodeUnknownEffect(DatabaseHeader)(
      yield* sql`SELECT (SELECT user_version FROM pragma_user_version) AS user_version,
        (SELECT COUNT(*) FROM sqlite_master) AS schema_object_count`.pipe(
        Effect.mapError(storageError("inspect fresh SQLite file")),
      ),
    ).pipe(
      Effect.mapError((cause) =>
        SqliteStorageError.make({
          operation: "inspect fresh SQLite file",
          message: "Cannot determine whether the SQLite file is empty",
          cause,
        }),
      ),
    );

    if (header.user_version !== 0 || header.schema_object_count !== 0)
      return yield* SqliteStorageCompatibilityError.make({
        actualVersion: header.user_version,
        supportedVersion: CurrentSqliteStorageVersion,
        message: `Existing SQLite storage must use WAL; the file was not changed (${journalMode.journal_mode}).`,
      });

    journalMode = yield* decodeJournalModeRow(
      "pragma_journal_mode",
      "singleton",
      yield* sql`PRAGMA journal_mode = WAL`.pipe(
        Effect.mapError(storageError("enable WAL for fresh SQLite file")),
      ),
    );

    if (journalMode.journal_mode.toLowerCase() !== "wal")
      return yield* SqliteStorageCompatibilityError.make({
        actualVersion: 0,
        supportedVersion: CurrentSqliteStorageVersion,
        message: `SQLite WAL mode is required; the database reported ${journalMode.journal_mode}.`,
      });
  }

  yield* ensureSqliteStorageLayout();

  const classifyWriteFailure =
    (operation: string) =>
    (error: SqlError): SqliteStorageError | SqliteWriteContention =>
      error.reason._tag === "LockTimeoutError"
        ? SqliteWriteContention.make({
            cause: error,
            operation,
            message: `Another producer holds the SQLite write lock; ${operation} is safe to retry.`,
          })
        : storageError(operation)(error);

  return yield* makeSqlJournalKernel({
    errors: sqliteErrors,
    hitFailpoint: failpoint,
    transactions: {
      withWriteTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? classifyWriteFailure(operation)(error) : error,
            ),
          ),
      withReadTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          makeSqlTransaction(sql, { begin: "BEGIN" })(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? storageError(operation)(error) : error,
            ),
          ),
      isTransactionFailure: Schema.is(Schema.Union([SqliteStorageError, SqliteWriteContention])),
    },
  });
});

export const initializeSqliteJournal = () =>
  Effect.map(initializeSqliteJournalKernel(), (kernel) => kernel.journal);

export type SqliteJournal = Effect.Success<ReturnType<typeof initializeSqliteJournal>>;
