/**
 * PostgreSQL storage Layers for Thread history and durable work.
 *
 * @since 0.1.0
 */
import { makeSqlActivityStore } from "@yielded/agent-storage-sql/sql-activity-store";
import { sqlStorageOwner } from "@yielded/agent-storage-sql/sql-journal";
import {
  makeSqlMessageDeliveryStore,
  registerCanonicalMessageWriter,
} from "@yielded/agent-storage-sql/sql-message-delivery-store";
import {
  makeSqlScheduleStore,
  SqlScheduleWriteTransaction,
} from "@yielded/agent-storage-sql/sql-schedule-store";
import { makeSqlTransaction } from "@yielded/agent-storage-sql/sql-storage";
import { makeSqlSubmissionLedgerKernel } from "@yielded/agent-storage-sql/sql-submission-ledger";
import { makeSqlSubscriptionStore } from "@yielded/agent-storage-sql/sql-subscription-store";
import { makeSqlThreadImport } from "@yielded/agent-storage-sql/sql-thread-import";
import { makeSqlThreadStoreKernel } from "@yielded/agent-storage-sql/sql-thread-store";
import {
  ActivityMutationFailpoint,
  ActivityProcessorStore,
  ActivityStoreError,
} from "@yielded/agent/activity-store";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import { StorageOwner } from "@yielded/agent/run-storage";
import { ScheduleStore } from "@yielded/agent/schedule";
import { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import { LedgerError, SubmissionLedger } from "@yielded/agent/submission-ledger";
import { SubscriptionError, SubscriptionStore, SourcePartition } from "@yielded/agent/subscription";
import { ThreadImport } from "@yielded/agent/thread-import";
import type { ThreadExportRequest } from "@yielded/agent/thread-store";
import { ThreadReader, ThreadStore } from "@yielded/agent/thread-store";
import { checkExportSource } from "@yielded/agent/thread-transfer";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import {
  classifyWriteFailure,
  ensurePostgresSchema,
  initializePostgresStorage,
  makePostgresJournal,
  postgresStorageErrors,
  readPostgresStorageHeader,
  makeWriteTransaction,
} from "./internal/postgres-storage.ts";
import {
  PostgresStorageError,
  type PostgresStorageFailpointError,
  type PostgresStorageFailpointLocation,
} from "./PostgresStorageError.ts";

/**
 * Current PostgreSQL adapter layout version; record encoding is versioned independently.
 *
 * @category constants
 * @since 0.1.0
 */
export { CurrentPostgresStorageVersion } from "./internal/postgres-storage.ts";

/**
 * Handler invoked at an explicit PostgreSQL storage fault-injection boundary.
 *
 * @category models
 * @since 0.1.0
 */
export type PostgresStorageFailpointHandler = (
  location: PostgresStorageFailpointLocation,
) => Effect.Effect<void, PostgresStorageFailpointError>;

const Settings = Schema.Struct({
  observationPollInterval: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  lockTimeout: Schema.Int.check(Schema.isGreaterThan(0)),
  verifyOnOpen: Schema.Boolean,
  schema: Schema.NonEmptyString.check(
    Schema.isMaxLength(63),
    Schema.isPattern(/^[a-z_][a-z0-9_]*$/),
  ),
});

/**
 * Namespace, polling, and transaction options for PostgreSQL storage.
 *
 * @category models
 * @since 0.1.0
 */
export interface PostgresStorageOptions {
  /** Namespace for qualified storage tables, created under the owner gate when absent. Defaults to public. */
  readonly schema?: string | undefined;
  /** Journal observation polling interval in milliseconds. Defaults to 25. */
  readonly observationPollInterval?: number | undefined;
  /** Positive database lock timeout in milliseconds. Defaults to 5,000. */
  readonly lockTimeout?: number | undefined;
  /** Audit stored payloads, identities, and batch coverage on open. Defaults to false. */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: PostgresStorageFailpointHandler | undefined;
  readonly activityFailpoint?: ActivityMutationFailpoint["Service"]["hit"] | undefined;
}

const settings = (options: PostgresStorageOptions) =>
  Schema.decodeEffect(Settings)({
    observationPollInterval: options.observationPollInterval ?? 25,
    lockTimeout: options.lockTimeout ?? 5_000,
    verifyOnOpen: options.verifyOnOpen ?? false,
    schema: options.schema ?? "public",
  }).pipe(
    Effect.mapError((cause) =>
      PostgresStorageError.make({
        cause,
        operation: "configure Postgres storage",
        message: cause.message,
      }),
    ),
  );

const initialize = Effect.fnUntraced(function* (options: PostgresStorageOptions) {
  const config = yield* settings(options);

  yield* initializePostgresStorage(config);

  return config;
});

const openJournal = Effect.fnUntraced(function* (options: PostgresStorageOptions) {
  const config = yield* initialize(options);
  const hitFailpoint: PostgresStorageFailpointHandler = options.failpoint ?? (() => Effect.void);
  const journal = yield* makePostgresJournal(config.lockTimeout, hitFailpoint, config.schema);

  return { config, journal, hitFailpoint };
});

type Journal = Effect.Success<ReturnType<typeof openJournal>>;

const makeThreadStore = ({ config, journal, hitFailpoint }: Journal) =>
  makeSqlThreadStoreKernel(journal, {
    ...config,
    namespace: config.schema,
    errors: postgresStorageErrors,
    hitFailpoint,
    offsetPrefix: "effect-agent-postgres@1:",
  }).pipe(Effect.map((services) => ({ ...services, owner: journal.owner })));

const makeSubmissionLedger = ({ config, journal, hitFailpoint }: Journal) =>
  makeSqlSubmissionLedgerKernel(journal, {
    namespace: config.schema,
    errors: postgresStorageErrors,
    hitFailpoint,
    offsetPrefix: "effect-agent-postgres@1:",
    sqlFailure: (operation) => (cause) => {
      const internal = classifyWriteFailure(operation)(cause);

      return LedgerError.make({ operation, message: internal.message, cause: internal });
    },
  }).pipe(Effect.map((services) => ({ ...services, owner: journal.owner })));

/**
 * Thread history and submissions over one scoped owner. The application must route each Thread
 * exclusively to this owner; Postgres does not provide distributed ownership or failover here.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerWith = (options: PostgresStorageOptions) =>
  ThreadReader.layer().pipe(
    Layer.provideMerge(
      Layer.effectContext(
        Effect.gen(function* () {
          const journal = yield* openJournal(options);
          const threadStore = yield* makeThreadStore(journal);
          const submissionLedger = yield* makeSubmissionLedger(journal);

          return Context.make(ThreadStore, threadStore.store).pipe(
            Context.add(ThreadImport, threadStore.importer),
            Context.add(SubmissionLedger, submissionLedger.ledger),
            Context.add(SettlementPublisher, submissionLedger.publisher),
            Context.add(StorageOwner, journal.journal.owner),
          );
        }),
      ),
    ),
  );

/**
 * Thread history and submissions with default settings. Requires SqlClient and Crypto.
 * SQL text parameters, including identities, must exclude NUL and unpaired UTF-16
 * surrogates. Canonical JSON payload strings remain intact through escaped encoding.
 *
 * @category layers
 * @since 0.1.0
 */
export const layer = layerWith({});

/**
 * Standalone thread history over the application's SqlClient and Crypto.
 *
 * @category layers
 * @since 0.1.0
 */
export const threadStoreLayer = (options: PostgresStorageOptions = {}) =>
  ThreadReader.layer().pipe(
    Layer.provideMerge(
      Layer.effectContext(
        Effect.flatMap(openJournal(options), makeThreadStore).pipe(
          Effect.map(({ store, importer, owner }) =>
            Context.make(ThreadStore, store).pipe(
              Context.add(ThreadImport, importer),
              Context.add(StorageOwner, owner),
            ),
          ),
        ),
      ),
    ),
  );

/**
 * Export a quiesced current-layout store through a read-only snapshot; never run DDL.
 *
 * @category operations
 * @since 0.1.0
 */
export const exportThread = Effect.fn("PostgresStorage.exportThread")(function* (
  request: ThreadExportRequest,
  options: PostgresStorageOptions = {},
) {
  yield* checkExportSource(request.threadId);
  const config = yield* settings(options);
  const sql = yield* SqlClient.SqlClient;

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const owner = yield* sqlStorageOwner(sql, config.schema);

      return yield* owner.withMutation(
        makeSqlTransaction(sql, {
          begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        })(
          Effect.gen(function* () {
            yield* readPostgresStorageHeader(config.schema);

            const transfer = yield* makeSqlThreadImport({
              owner,
              namespace: config.schema,
              offsetPrefix: "effect-agent-postgres@1:",
              read: (body) => body,
              write: () => Effect.die("A read-only exporter cannot import"),
            });

            return yield* transfer.export(request);
          }),
        ).pipe(
          Effect.catchTag("SqlError", (cause) =>
            PostgresStorageError.make({
              operation: "export Thread snapshot",
              message: cause.message,
              cause,
            }),
          ),
        ),
      );
    }),
  );
});

/**
 * Submissions and canonical settlement publication over the application's SqlClient and Crypto.
 *
 * @category layers
 * @since 0.1.0
 */
export const submissionLedgerLayer = (options: PostgresStorageOptions = {}) =>
  Layer.effectContext(
    Effect.flatMap(openJournal(options), makeSubmissionLedger).pipe(
      Effect.map((services) =>
        Context.make(SubmissionLedger, services.ledger).pipe(
          Context.add(SettlementPublisher, services.publisher),
          Context.add(StorageOwner, services.owner),
        ),
      ),
    ),
  );

/**
 * Schedules over the application's SqlClient.
 *
 * @category layers
 * @since 0.1.0
 */
export const scheduleStoreLayer = (options: PostgresStorageOptions = {}) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* initialize(options);
      const run = yield* makeWriteTransaction(sql, config.lockTimeout, config.schema);

      return Layer.effect(ScheduleStore, makeSqlScheduleStore(config.schema)).pipe(
        Layer.provide(Layer.succeed(SqlScheduleWriteTransaction, { run })),
      );
    }),
  );

/**
 * Message delivery over the application's SqlClient and Crypto, with optional live capacity and payload limits.
 *
 * @category layers
 * @since 0.1.0
 */
export const messageDeliveryStoreLayer = (
  options: PostgresStorageOptions & {
    readonly limits?: MessageDeliveryStoreLimits | undefined;
  } = {},
) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* initialize(options);

      const owner = yield* sqlStorageOwner(sql, config.schema);

      const store = yield* makeSqlMessageDeliveryStore(owner, options.limits, {
        namespace: config.schema,
        transaction: yield* makeWriteTransaction(sql, config.lockTimeout, config.schema),
      });

      yield* registerCanonicalMessageWriter(owner, store);

      return store;
    }),
  );

/**
 * Subscriptions owned by an explicit partition. Invalid partitions fail before accessing SQL.
 *
 * @category layers
 * @since 0.1.0
 */
export const subscriptionStoreLayer = (
  owned: SourcePartition,
  options: PostgresStorageOptions = {},
) =>
  Layer.effect(
    SubscriptionStore,
    Effect.gen(function* () {
      const partition = yield* Schema.decodeEffect(SourcePartition)(owned).pipe(
        Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
      );

      const config = yield* initialize(options);
      const sql = yield* SqlClient.SqlClient;
      const transaction = yield* makeWriteTransaction(sql, config.lockTimeout, config.schema);

      // Retention DDL must share the owner gate with format initialization.
      return yield* transaction(
        makeSqlSubscriptionStore(partition, {
          namespace: config.schema,
          transaction,
          maxStoredJsonLength: 16 * 1024 * 1024,
        }),
      ).pipe(
        Effect.catchTag("SqlError", () =>
          SubscriptionError.make({ reason: "storage", code: "initialize" }),
        ),
      );
    }),
  );

/**
 * Independent activity progress over one shared SqlClient; does not initialize the Thread journal.
 * Route each activity key to one process. Sessions are local and close with their owning Scope.
 *
 * @category layers
 * @since 0.1.0
 */
export const activityStoreLayer = (options: PostgresStorageOptions = {}) =>
  Layer.effect(
    ActivityProcessorStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* settings(options);

      return yield* makeSqlActivityStore(
        yield* makeWriteTransaction(sql, config.lockTimeout, config.schema),
        ensurePostgresSchema(config.schema).pipe(
          Effect.mapError(() =>
            ActivityStoreError.make({
              operation: "initialize activity schema",
              reason: "unavailable",
            }),
          ),
        ),
        config.schema,
      );
    }),
  ).pipe(
    Layer.provide(
      options.activityFailpoint === undefined
        ? ActivityMutationFailpoint.layer
        : Layer.succeed(ActivityMutationFailpoint)({ hit: options.activityFailpoint }),
    ),
  );
