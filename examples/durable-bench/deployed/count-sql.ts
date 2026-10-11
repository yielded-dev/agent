import { Schema } from "effect";

const Calls = Schema.Struct({
  calls: Schema.Natural,
  failures: Schema.Natural,
});

const QueryCounts = Schema.Struct({
  query: Schema.String,
  calls: Schema.Natural,
  failures: Schema.Natural,
  rowsRead: Schema.Natural,
  rowsWritten: Schema.Natural,
  logicalBoundStringBytes: Schema.Natural,
  logicalBoundBlobBytes: Schema.Natural,
});

const LeadingSqlTransactionControl = Schema.Struct({
  begin: Schema.Natural,
  commit: Schema.Natural,
  end: Schema.Natural,
  rollback: Schema.Natural,
  savepoint: Schema.Natural,
  release: Schema.Natural,
});

/** Native API work observed during one diagnostic window, not physical SQLite write volume. */
export const SqlCounts = Schema.Struct({
  execCalls: Schema.Natural,
  execFailures: Schema.Natural,
  queries: Schema.Array(QueryCounts),
  rowsRead: Schema.Natural,
  rowsWritten: Schema.Natural,
  logicalBoundStringBytes: Schema.Natural,
  logicalBoundBlobBytes: Schema.Natural,
  transactionSync: Calls,
  transaction: Schema.Struct({ ...Calls.fields, pending: Schema.Natural }),
  leadingSqlTransactionControl: LeadingSqlTransactionControl,
  databaseSizeStart: Schema.Natural,
  databaseSizeEnd: Schema.Natural,
  databaseSizeDelta: Schema.Int,
  bytesWritten: Schema.Null,
  limitations: Schema.Array(Schema.String),
});

export type SqlCounts = typeof SqlCounts.Type;

type Mutable<A> = { -readonly [Key in keyof A]: A[Key] };
type QueryCount = Mutable<typeof QueryCounts.Type>;

interface Window {
  readonly databaseSizeStart: number;
  readonly queries: Map<string, QueryCount>;
  readonly cursors: Array<{
    readonly cursor: SqlStorageCursor<Record<string, SqlStorageValue>>;
    readonly query: QueryCount;
  }>;
  readonly transactionSync: Mutable<SqlCounts["transactionSync"]>;
  readonly transaction: Mutable<SqlCounts["transaction"]>;
  readonly leadingSqlTransactionControl: Mutable<SqlCounts["leadingSqlTransactionControl"]>;
}

const limitations = [
  "Counts are sql.exec API calls, not prepares or internal SQLite statements; one exec may contain multiple statements. Only its leading transaction-control keyword is classified.",
  "Cursor row counters are sampled at stop without draining; they can include rolled-back work. An exec that throws returns no observable cursor; iteration errors are not exec failures.",
  "Logical bound-string/blob bytes count attempted UTF-8/string and buffer/view inputs, including repeated values and read predicates. They exclude numbers, nulls, SQL literals, generated values and index representation; parameters are not retained.",
  "databaseSizeDelta is net allocation, not bytes written. Physical WAL/page/VFS write volume is unavailable, so bytesWritten is null.",
  "Transaction calls/failures describe API entry and thrown/rejected results, not physical commits. A resolved explicit rollback is not an API failure; pending calls have not settled at stop.",
  "Only calls through these installed methods are visible, not hidden KV SQL or previously captured methods. Cursors are retained until stop; this diagnostic changes retention and supplies no timing evidence.",
];

/**
 * Install once on each native storage object before adapters capture its methods.
 * Counting starts disabled. start resets the window; stop disables before reading
 * native counters and returns the same snapshot on repeated calls.
 */
export const installSqlCount = (storage: DurableObjectStorage) => {
  const sql = storage.sql;
  // Preserve the original receiver, including native errors for invalid receivers.
  // oxlint-disable typescript/unbound-method
  const nativeExec = sql.exec;
  const nativeSync = storage.transactionSync;
  const nativeTransaction = storage.transaction;
  // oxlint-enable typescript/unbound-method
  const encoder = new TextEncoder();
  let active: Window | undefined;
  let stopped: SqlCounts | undefined;

  const exec: SqlStorage["exec"] = function <Row extends Record<string, SqlStorageValue>>(
    this: SqlStorage,
    query: string,
    ...bindings: Array<unknown>
  ): SqlStorageCursor<Row> {
    const window = active;

    if (window === undefined) return nativeExec.bind(this)<Row>(query, ...bindings);

    let counts = window.queries.get(query);

    if (counts === undefined) {
      counts = {
        query,
        calls: 0,
        failures: 0,
        rowsRead: 0,
        rowsWritten: 0,
        logicalBoundStringBytes: 0,
        logicalBoundBlobBytes: 0,
      };
      window.queries.set(query, counts);
    }
    counts.calls++;
    for (const binding of bindings) {
      if (typeof binding === "string")
        counts.logicalBoundStringBytes += encoder.encode(binding).byteLength;
      else if (binding instanceof ArrayBuffer || ArrayBuffer.isView(binding))
        counts.logicalBoundBlobBytes += binding.byteLength;
    }

    const leading =
      /^(?:\s|;|--[^\r\n]*(?:\r?\n|$)|\/\*[\s\S]*?\*\/)*(BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b/i
        .exec(query)?.[1]
        ?.toLowerCase();

    if (
      leading === "begin" ||
      leading === "commit" ||
      leading === "end" ||
      leading === "rollback" ||
      leading === "savepoint" ||
      leading === "release"
    )
      window.leadingSqlTransactionControl[leading]++;

    try {
      const cursor = nativeExec.bind(this)<Row>(query, ...bindings);

      window.cursors.push({ cursor, query: counts });

      return cursor;
    } catch (error) {
      counts.failures++;
      throw error;
    }
  };

  const transactionSync: DurableObjectStorage["transactionSync"] = function <Value>(
    this: DurableObjectStorage,
    callback: () => Value,
  ): Value {
    const counts = active?.transactionSync;

    if (counts === undefined) return nativeSync.bind(this)(callback);
    counts.calls++;
    try {
      return nativeSync.bind(this)(callback);
    } catch (error) {
      counts.failures++;
      throw error;
    }
  };

  const transaction: DurableObjectStorage["transaction"] = function <Value>(
    this: DurableObjectStorage,
    callback: (transaction: DurableObjectTransaction) => Promise<Value>,
  ): Promise<Value> {
    const counts = active?.transaction;

    if (counts === undefined) return nativeTransaction.bind(this)(callback);
    counts.calls++;
    counts.pending++;
    try {
      const pending = nativeTransaction.bind(this)(callback);

      // Observe settlement without replacing the native Promise or callback.
      void pending.then(
        () => {
          counts.pending--;
        },
        () => {
          counts.pending--;
          counts.failures++;
        },
      );

      return pending;
    } catch (error) {
      counts.pending--;
      counts.failures++;
      throw error;
    }
  };

  Object.defineProperty(sql, "exec", { configurable: true, writable: true, value: exec });
  Object.defineProperties(storage, {
    transactionSync: { configurable: true, writable: true, value: transactionSync },
    transaction: { configurable: true, writable: true, value: transaction },
  });

  return {
    start(): void {
      active = undefined;
      stopped = undefined;
      active = {
        databaseSizeStart: sql.databaseSize,
        queries: new Map(),
        cursors: [],
        transactionSync: { calls: 0, failures: 0 },
        transaction: { calls: 0, failures: 0, pending: 0 },
        leadingSqlTransactionControl: {
          begin: 0,
          commit: 0,
          end: 0,
          rollback: 0,
          savepoint: 0,
          release: 0,
        },
      };
    },
    stop(): SqlCounts {
      const window = active;

      active = undefined;
      if (window === undefined) {
        if (stopped === undefined) throw new Error("SQL count window has not started.");

        return stopped;
      }
      try {
        for (const { cursor, query } of window.cursors) {
          query.rowsRead += cursor.rowsRead;
          query.rowsWritten += cursor.rowsWritten;
        }
      } finally {
        window.cursors.length = 0;
      }
      const databaseSizeEnd = sql.databaseSize;
      const queries = [...window.queries.values()].map((query) => ({ ...query }));

      const sum = (field: Exclude<keyof QueryCount, "query">) =>
        queries.reduce((total, query) => total + query[field], 0);

      stopped = {
        execCalls: sum("calls"),
        execFailures: sum("failures"),
        queries,
        rowsRead: sum("rowsRead"),
        rowsWritten: sum("rowsWritten"),
        logicalBoundStringBytes: sum("logicalBoundStringBytes"),
        logicalBoundBlobBytes: sum("logicalBoundBlobBytes"),
        transactionSync: { ...window.transactionSync },
        transaction: { ...window.transaction },
        leadingSqlTransactionControl: { ...window.leadingSqlTransactionControl },
        databaseSizeStart: window.databaseSizeStart,
        databaseSizeEnd,
        databaseSizeDelta: databaseSizeEnd - window.databaseSizeStart,
        bytesWritten: null,
        limitations: [...limitations],
      };

      return stopped;
    },
  };
};
