/**
 * Validated configuration and service authority for SQLite storage.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const BusyTimeoutMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * Validated construction configuration consumed by the SQLite storage Layer. The database
 * identity itself belongs to the SqlClient Layer; duplicating it here could silently diverge
 * from the connection actually in use.
 *
 * @category schemas
 * @since 0.1.0
 */
export class SqliteStorageConfigValue extends Schema.Class<SqliteStorageConfigValue>(
  "@effect-agent/storage-sqlite/SqliteStorageConfigValue",
)({
  observationPollInterval: ObservationPollInterval,
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition, in milliseconds. */
  busyTimeout: BusyTimeoutMillis,
  /** Connection synchronization selected at construction; FULL is the convenience default. */
  synchronous: Schema.Literals(["FULL", "NORMAL"]),
  /**
   * Audit stored payload schemas, identities, and batch coverage while opening the store.
   * Per-operation Schema decoding remains enabled; this complete scan is an explicit opt-in.
   */
  verifyOnOpen: Schema.Boolean,
}) {}

/**
 * Explicit SQLite storage configuration authority.
 *
 * @category services
 * @since 0.1.0
 */
export class SqliteStorageConfig extends Context.Service<
  SqliteStorageConfig,
  SqliteStorageConfigValue
>()("@effect-agent/storage-sqlite/SqliteStorageConfig") {}
