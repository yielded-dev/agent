import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { sqlStorageOwner } from "@yielded/agent-storage-sql/sql-journal";
import { makeSqlRunStorage } from "@yielded/agent-storage-sql/sql-run-storage";
import { makeSqlTransaction, SqlInteger } from "@yielded/agent-storage-sql/sql-storage";
import { makeSqlThreadImport } from "@yielded/agent-storage-sql/sql-thread-import";
import { makeSqlThreadStoreKernel } from "@yielded/agent-storage-sql/sql-thread-store";
import { RunStorage, StorageOwner } from "@yielded/agent/run-storage";
import { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import { ThreadImport } from "@yielded/agent/thread-import";
import type { ThreadExportRequest } from "@yielded/agent/thread-store";
import {
  ThreadExportSource,
  ThreadReader,
  ThreadStore,
  ThreadStoreError,
} from "@yielded/agent/thread-store";
import { checkExportSource } from "@yielded/agent/thread-transfer";
import { Context, Crypto, Effect, Layer, Schema, Scope } from "effect";
import * as SqlClientService from "effect/sql/SqlClient";
import { CurrentTransformer } from "effect/sql/Statement";

import {
  CurrentSqliteStorageVersion,
  readSqliteStorageHeader,
  inspectManagedSqliteStorage,
} from "./internal/migrations.ts";
import {
  configureSqliteSynchronous,
  initializeSqliteJournal,
  initializeSqliteJournalKernel,
  sqliteErrors,
} from "./internal/sqlite-journal.ts";
import { sqlFailure } from "./internal/sqlite-ledger-errors.ts";
import { SqliteStorageConfig, SqliteStorageConfigValue } from "./SqliteStorageConfig.ts";
import type { SqliteStorageCorruptionError } from "./SqliteStorageError.ts";
import { SqliteStorageCompatibilityError, SqliteStorageError } from "./SqliteStorageError.ts";
import {
  SqliteStorageFailpoint,
  type SqliteStorageFailpointHandler,
} from "./SqliteStorageFailpoint.ts";

export interface SqliteStorageOptions {
  readonly filename: string;
  readonly observationPollInterval?: number | undefined;
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition, in milliseconds. */
  readonly busyTimeout?: number | undefined;
  /**
   * SQLite WAL synchronization for this store's connection. Defaults to FULL. NORMAL can lose
   * acknowledged commits after power loss or an OS crash, potentially repeating external effects.
   * All stores sharing a client must select the same mode when constructed.
   */
  readonly synchronous?: "FULL" | "NORMAL" | undefined;
  /**
   * Audit stored payloads, identities, and batch coverage on open. Defaults to false. Full reads validate
   * canonical records; readPrompt trusts write-time validation of the omitted fields.
   */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: SqliteStorageFailpointHandler | undefined;
}

export type SqliteStorageInitializationError =
  | SqliteStorageCompatibilityError
  | SqliteStorageCorruptionError
  | SqliteStorageError;

/** Only the exclusive client acquisition can construct managed mutation authority. */
class ExclusiveSqliteHost extends Context.Service<
  ExclusiveSqliteHost,
  { readonly sql: SqlClientService.SqlClient }
>()("@effect-agent/storage-sqlite/internal/ExclusiveSqliteHost") {}

const ExclusiveMode = Schema.Tuple([Schema.Struct({ locking_mode: Schema.Literal("exclusive") })]);
const JournalMode = Schema.Tuple([Schema.Struct({ journal_mode: Schema.String })]);

const DatabaseHeader = Schema.Tuple([
  Schema.Struct({ user_version: SqlInteger, schema_object_count: SqlInteger }),
]);

/**
 * Acquire process-lifetime authority over a dedicated SQLite client before exposing it to a
 * managed host. Open the client with `disableWAL: true`: WAL setup and lock contention must
 * fail through this Layer's typed initialization error. The client Scope owns the lock;
 * never change its locking mode or close it while host work is alive.
 *
 * This excludes ALL other database connections, including readers. After compatibility checks,
 * initialize only the fresh supported layout, then recover from durable facts. Scope closure
 * retires local capabilities; no ownership is restored from the database. Only the exact
 * declared triggers on Thread storage tables are permitted before any initialization;
 * application and optional memory tables retain their own schema contracts.
 */
export const exclusiveHostClientLayer: Layer.Layer<
  SqlClientService.SqlClient | ExclusiveSqliteHost,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(
  Effect.gen(function* () {
    const sql = (yield* SqlClientService.SqlClient).withoutTransforms();

    const acquireError = (cause: unknown) =>
      SqliteStorageError.make({
        operation: "acquire exclusive host",
        message:
          "Cannot acquire exclusive SQLite host authority; close other database connections.",
        cause,
      });

    yield* Effect.gen(function* () {
      yield* sql`PRAGMA busy_timeout = 0`;
      const mode = yield* sql`PRAGMA main.locking_mode = EXCLUSIVE`;

      yield* Schema.decodeUnknownEffect(ExclusiveMode)(mode);

      const [journal] = yield* Schema.decodeUnknownEffect(JournalMode)(
        yield* sql`PRAGMA journal_mode`,
      );

      if (journal.journal_mode !== "wal") {
        const [header] = yield* Schema.decodeUnknownEffect(DatabaseHeader)(
          yield* sql`
          SELECT (SELECT user_version FROM pragma_user_version) AS user_version,
                 (SELECT COUNT(*) FROM sqlite_master) AS schema_object_count
        `,
        );

        // Only a version-zero file with no schema objects may change journal mode. Existing
        // stores must already satisfy the adapter's WAL contract, including predecessors.
        if (header.user_version !== 0 || header.schema_object_count !== 0) {
          return yield* SqliteStorageCompatibilityError.make({
            actualVersion: header.user_version,
            supportedVersion: CurrentSqliteStorageVersion,
            message:
              "An existing managed-host database must use SQLite WAL mode; no data was changed.",
          });
        }
        yield* sql`PRAGMA journal_mode = WAL`;
      }
      yield* configureSqliteSynchronous();
      // A mode setting alone is not authority. Acquire the write lock now; EXCLUSIVE mode
      // retains it after commit/rollback until this client's enclosing Scope closes.
      yield* makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(Effect.void);

      yield* inspectManagedSqliteStorage();
    }).pipe(
      Effect.mapError((error) =>
        Schema.is(SqliteStorageCompatibilityError)(error) ? error : acquireError(error),
      ),
    );

    yield* initializeSqliteJournal().pipe(Effect.provideService(SqlClientService.SqlClient, sql));

    return Context.make(SqlClientService.SqlClient, sql).pipe(
      Context.add(ExclusiveSqliteHost, { sql }),
    );
  }),
);

const makeServices = Effect.fnUntraced(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal();

  const services = yield* makeSqlThreadStoreKernel(journal, {
    ...config,
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    offsetPrefix: "effect-agent-sqlite@1:",
  });

  return { ...services, owner: journal.owner };
});

/**
 * SQLite Thread Store implementation with configuration, failpoint, SQL, and Crypto
 * authority kept visible in its input channel.
 * Inject a client opened with `disableWAL: true`; this adapter enables WAL only for a
 * fresh file so unsupported files reject before persistent driver configuration changes.
 */
export const threadStoreLayer: Layer.Layer<
  ThreadStore | ThreadReader | ThreadImport | StorageOwner,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effect(
  ThreadReader,
  Effect.map(ThreadStore, (store) => ThreadReader.fromStore(store)),
).pipe(
  Layer.provideMerge(
    Layer.effectContext(
      Effect.map(makeServices(), ({ store, importer, owner }) =>
        Context.make(ThreadStore, store).pipe(
          Context.add(ThreadImport, importer),
          Context.add(StorageOwner, owner),
        ),
      ),
    ),
  ),
  Layer.provide(exclusiveHostClientLayer),
);

/**
 * Managed-host storage for an already acquired exclusiveHostClientLayer. The composition
 * root consumes the mutation ports privately and exposes only ThreadReader to application
 * bindings. Its private construction witness binds the exact exclusive client; supplying a
 * generic SqlClient cannot bypass the required exclusive acquisition.
 */
export const exclusiveRunStorageLayer = Layer.effectContext(
  Effect.gen(function* () {
    const exclusive = yield* ExclusiveSqliteHost;

    const live = yield* Effect.context<
      Crypto.Crypto | SqliteStorageConfig | SqliteStorageFailpoint | Scope.Scope
    >();

    const context = Context.add(
      Context.pick(Crypto.Crypto, SqliteStorageConfig, SqliteStorageFailpoint, Scope.Scope)(live),
      SqlClientService.SqlClient,
      exclusive.sql,
    );

    return yield* Effect.setContext(
      Effect.gen(function* () {
        const config = yield* SqliteStorageConfig;
        const failpoint = yield* SqliteStorageFailpoint;
        const journal = yield* initializeSqliteJournalKernel();

        const services = yield* makeSqlRunStorage(
          journal,
          {
            ...config,
            errors: sqliteErrors,
            hitFailpoint: failpoint.hit,
            offsetPrefix: "effect-agent-sqlite@1:",
          },
          {
            errors: sqliteErrors,
            hitFailpoint: failpoint.hit,
            offsetPrefix: "effect-agent-sqlite@1:",
            sqlFailure,
          },
        );

        return Context.make(ThreadStore, services.store).pipe(
          Context.add(ThreadReader, services.reader),
          Context.add(ThreadImport, services.importer),
          Context.add(SubmissionLedger, services.ledger),
          Context.add(SettlementPublisher, services.publisher),
          Context.add(RunStorage, services.runStorage),
          Context.add(StorageOwner, services.owner),
        );
      }),
      Context.merge(
        Context.omit(CurrentTransformer, exclusive.sql.transactionService)(live),
        context,
      ),
    );
  }),
);

/**
 * Validated SQLite storage configuration Layer with the documented defaults applied. Shared
 * by the ThreadStore and SubmissionLedger convenience layers so their defaults cannot
 * drift.
 */
export const storageConfigLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageConfig, SqliteStorageError> =>
  Layer.effect(SqliteStorageConfig)(
    Schema.decodeEffect(SqliteStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      busyTimeout: options.busyTimeout ?? 5_000,
      synchronous: options.synchronous ?? "FULL",
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        SqliteStorageError.make({
          cause: error,
          operation: "configure SQLite storage",
          message: error.message,
        }),
      ),
    ),
  );

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const storageFailpointLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageFailpoint> =>
  options.failpoint === undefined
    ? SqliteStorageFailpoint.layer
    : Layer.succeed(SqliteStorageFailpoint)({ hit: options.failpoint });

/**
 * A composition-root convenience Layer for canonical Threads. Durable accepted work is
 * served by the separate SubmissionLedger port.
 */
export const layer = (
  options: SqliteStorageOptions,
): Layer.Layer<
  ThreadStore | ThreadReader | ThreadImport | StorageOwner,
  SqliteStorageInitializationError
> =>
  Layer.unwrap(
    Effect.map(SqliteStorageConfig, (config) =>
      threadStoreLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteStorageConfig)(config),
            storageFailpointLayer(options),
            // The journal enables WAL only after proving the file is fresh.
            SqliteClient.layer({ filename: options.filename, disableWAL: true }).pipe(
              Layer.catch((cause) =>
                Layer.effectContext<
                  Layer.Success<ReturnType<typeof SqliteClient.layer>>,
                  SqliteStorageError,
                  never
                >(
                  Effect.fail(
                    SqliteStorageError.make({
                      operation: "open SQLite",
                      message: cause.message,
                      cause,
                    }),
                  ),
                ),
              ),
            ),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));

/**
 * Export a quiesced current-layout database without initializing layout or retiring
 * ownership. Its read-only connection closes before returning; the source remains untouched.
 */
export const exportThread = Effect.fn("SqliteThreadStore.exportThread")(function* (
  options: Pick<SqliteStorageOptions, "filename" | "busyTimeout">,
  request: ThreadExportRequest,
) {
  yield* checkExportSource(request.threadId);

  return yield* Effect.gen(function* () {
    const sql = yield* SqlClientService.SqlClient;

    const owner = yield* sqlStorageOwner(sql);

    return yield* owner.withMutation(
      makeSqlTransaction(sql, { begin: "BEGIN" })(
        Effect.gen(function* () {
          yield* readSqliteStorageHeader();

          const transfer = yield* makeSqlThreadImport({
            owner,
            offsetPrefix: "effect-agent-sqlite@1:",
            read: (body) => body,
            write: () => Effect.die("A read-only exporter cannot import"),
          });

          return yield* transfer.export(request);
        }),
      ).pipe(
        Effect.catchTag("SqlError", (cause) =>
          SqliteStorageError.make({
            operation: "export Thread snapshot",
            message: cause.message,
            cause,
          }),
        ),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.merge(
        SqliteClient.layer({
          filename: options.filename,
          readonly: true,
          disableWAL: true,
          busyTimeout: options.busyTimeout ?? 5_000,
        }).pipe(
          Layer.catch((cause) =>
            Layer.effectContext<
              Layer.Success<ReturnType<typeof SqliteClient.layer>>,
              SqliteStorageError,
              never
            >(
              Effect.fail(
                SqliteStorageError.make({
                  operation: "open SQLite for export",
                  message: cause.message,
                  cause,
                }),
              ),
            ),
          ),
        ),
        NodeCrypto.layer,
      ),
    ),
  );
});

/** Read-only page exports retain native diagnostics in the transfer port's typed storage failure. */
export const exportSourceLayer = (
  options: Pick<SqliteStorageOptions, "filename" | "busyTimeout">,
): Layer.Layer<ThreadExportSource> => {
  const failure = (cause: SqliteStorageInitializationError) =>
    ThreadStoreError.make({ operation: "export Thread snapshot", message: cause.message, cause });

  return Layer.succeed(ThreadExportSource, {
    export: (request) =>
      exportThread(options, request).pipe(
        Effect.catchTags({
          SqliteStorageCompatibilityError: failure,
          SqliteStorageCorruptionError: failure,
          SqliteStorageError: failure,
        }),
      ),
  });
};
