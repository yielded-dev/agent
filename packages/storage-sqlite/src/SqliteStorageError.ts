import { SqlStorageFailpointLocation as SqliteStorageFailpointLocation } from "@yielded/agent-storage-sql/sql-storage-failpoint";
import { CanonicalSequence } from "@yielded/agent/records";
import * as Schema from "effect/Schema";

/** The SQLite file uses a private-development format this adapter cannot read. */
export class SqliteStorageCompatibilityError extends Schema.TaggedError<SqliteStorageCompatibilityError>()(
  "SqliteStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/** Stored bytes failed the current Schema and cannot be used as recovery truth. */
export class SqliteStorageCorruptionError extends Schema.TaggedError<SqliteStorageCorruptionError>()(
  "SqliteStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
  },
) {}

/** SQLite infrastructure failed while opening or operating the store. */
export class SqliteStorageError extends Schema.TaggedError<SqliteStorageError>()(
  "SqliteStorageError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * SQLite infrastructure failed while operating the Submission Ledger. Surfaces at the
 * SubmissionLedger port as the typed `LedgerError` with this error preserved as its cause,
 * so the adapter-level tag is never erased.
 */
export class SqliteLedgerError extends Schema.TaggedError<SqliteLedgerError>()(
  "SqliteLedgerError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * A canonical batch retry conflicts with existing append state. Tail conflicts carry the
 * actual committed tail as a diagnostic resume hint.
 */
export class SqliteAppendConflict extends Schema.TaggedError<SqliteAppendConflict>()(
  "SqliteAppendConflict",
  {
    message: Schema.String,
    reason: Schema.Literals(["batch-content", "record-identity", "tail", "stale-command"]),
    actualTailSequence: Schema.optionalKey(CanonicalSequence),
  },
) {}

/**
 * A write transaction could not acquire the SQLite write lock within the configured busy
 * timeout (SQLITE_BUSY / SQLITE_LOCKED). Another transiently coexisting owner is writing;
 * the operation did not mutate canonical state and is safe to retry.
 */
export class SqliteWriteContention extends Schema.TaggedError<SqliteWriteContention>()(
  "SqliteWriteContention",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/** A checkpoint conflicts with a previously stored checkpoint at the same offset. */
export class SqliteCheckpointConflict extends Schema.TaggedError<SqliteCheckpointConflict>()(
  "SqliteCheckpointConflict",
  {
    message: Schema.String,
  },
) {}

export { SqlStorageFailpointLocation as SqliteStorageFailpointLocation } from "@yielded/agent-storage-sql/sql-storage-failpoint";

/** Deterministic test-only fault or pause injected at a SQLite operation boundary. */
export class SqliteStorageFailpointError extends Schema.TaggedError<SqliteStorageFailpointError>()(
  "SqliteStorageFailpointError",
  {
    location: SqliteStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected SQLite storage failure at ${this.location}.`;
  }
}
