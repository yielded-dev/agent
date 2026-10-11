import { Context, Schema } from "effect";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const BusyTimeoutMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const OwnershipLeaseMillis = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Validated construction configuration consumed by the SQLite storage Layer. The database
 * identity itself belongs to the SqlClient Layer; duplicating it here could silently diverge
 * from the connection actually in use.
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
   * Submission ownership lease duration in milliseconds (D5). The lease is a liveness hint
   * that makes an abandoned claim reclaimable; correctness never depends on it because every
   * canonical append is fenced by producer epoch. Convenience layers default this to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `@yielded/agent/submission-ledger`.
   */
  ownershipLeaseDuration: OwnershipLeaseMillis,
  /**
   * Audit stored payload schemas, identities, and batch coverage while opening the store.
   * Per-operation Schema decoding remains enabled; this complete scan is an explicit opt-in.
   */
  verifyOnOpen: Schema.Boolean,
}) {}

/** Explicit SQLite storage configuration authority. */
export class SqliteStorageConfig extends Context.Service<
  SqliteStorageConfig,
  SqliteStorageConfigValue
>()("@effect-agent/storage-sqlite/SqliteStorageConfig") {}
