import { LedgerError } from "@yielded/agent/submission-ledger";
import type { SqlError } from "effect/sql/SqlError";

import { SqliteLedgerError, SqliteWriteContention } from "../SqliteStorageError.ts";

/** Wrap an adapter-internal failure into the port's LedgerError without erasing its tag. */
const internalFailure =
  (operation: string) =>
  (error: { readonly message: string }): LedgerError =>
    LedgerError.make({ operation, message: error.message, cause: error });

/** Classify raw SQL failures: write-lock timeouts stay retryable typed contention. */
export const sqlFailure =
  (operation: string) =>
  (error: SqlError): LedgerError => {
    const internal =
      error.reason._tag === "LockTimeoutError"
        ? SqliteWriteContention.make({
            cause: error,
            operation,
            message: `Another producer holds the SQLite write lock; ${operation} is safe to retry.`,
          })
        : SqliteLedgerError.make({
            cause: error,
            operation,
            message: error.message,
          });

    return internalFailure(operation)(internal);
  };
