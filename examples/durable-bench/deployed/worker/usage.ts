/* oxlint-disable effect/no-untyped-throw -- Native storage wrappers preserve synchronous throws; snapshots expose a typed Schema error outside Effect. */
import { Schema } from "effect";

const Natural = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Explicit native SQL writes, successful KV key mutations, and setAlarm calls. */
export const Usage = Schema.Struct({
  sqlRowsWritten: Natural,
  kvRowsWritten: Natural,
});

export type Usage = typeof Usage.Type;

export class UsageUnavailable extends Schema.TaggedError<UsageUnavailable>()("UsageUnavailable", {
  reason: Schema.String,
}) {}

export const UsageSnapshot = Schema.Union([Usage, Schema.Struct({ unavailable: Schema.String })]);
export type UsageSnapshot = typeof UsageSnapshot.Type;

type Meter = {
  sqlRowsWritten: number;
  kvRowsWritten: number;
  pending?: () => void;
  asynchronous: number;
  unavailable?: string;
  decorated: WeakSet<object>;
};

const meters = new WeakMap<DurableObjectStorage, Meter>();

// The native cursor covers only the final statement of a batch. This recognizes
// statement boundaries, including CREATE TRIGGER's semicolon-delimited body.
function singleStatement(query: string): boolean {
  if (!query.includes(";")) return true;

  const tokens = query.matchAll(
    /'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|--[^\r\n]*|\/\*[\s\S]*?\*\/|[a-z_][a-z_0-9]*|[^\s]/gi,
  );

  let prefix = "";
  let trigger = false;
  let previous = "";
  let ended = false;

  for (const match of tokens) {
    const token = match[0].toUpperCase();

    if (token.startsWith("--") || token.startsWith("/*")) continue;
    if (ended) {
      if (token !== ";") return false;
      continue;
    }
    if (prefix === "" && token === "CREATE") prefix = "CREATE";
    else if (prefix === "CREATE" && (token === "TEMP" || token === "TEMPORARY"))
      prefix = "CREATE TEMP";
    else if ((prefix === "CREATE" || prefix === "CREATE TEMP") && token === "TRIGGER") {
      trigger = true;
      prefix = "other";
    } else prefix = "other";

    if (token === ";" && (!trigger || previous === ";END")) ended = true;
    previous = token === "END" && previous === ";" ? ";END" : token;
  }

  return true;
}

function unavailable(meter: Meter, reason: string): void {
  meter.unavailable ??= reason;
}

function trackCursor<T extends Record<string, SqlStorageValue>>(
  cursor: SqlStorageCursor<T>,
  meter: Meter,
): SqlStorageCursor<T> {
  let counted = 0;

  const flush = () => {
    const current = cursor.rowsWritten;

    if (!Number.isSafeInteger(current) || current < counted) {
      unavailable(meter, "Native SQL cursor returned an invalid row-write count");

      return;
    }
    meter.sqlRowsWritten += current - counted;
    counted = current;
  };

  const consumed = <A>(run: () => A): A => {
    try {
      return run();
    } finally {
      flush();
    }
  };

  const next = cursor.next.bind(cursor);
  const toArray = cursor.toArray.bind(cursor);
  const one = cursor.one.bind(cursor);
  const raw = cursor.raw.bind(cursor);
  const iterator = cursor[Symbol.iterator].bind(cursor);

  const trackIterator = <A>(value: IterableIterator<A>): IterableIterator<A> => {
    const next = value.next.bind(value);

    value.next = (...args) => consumed(() => next(...args));

    return value;
  };

  cursor.next = () => consumed(next);
  cursor.toArray = () => consumed(toArray);
  cursor.one = () => consumed(one);
  cursor.raw = <A extends SqlStorageValue[]>() => trackIterator(raw<A>());
  cursor[Symbol.iterator] = () => trackIterator(iterator());
  // Only the newest cursor is retained. Older cursors still account for their
  // own consumption through their wrappers, even after another exec/snapshot.
  meter.pending = flush;
  flush();

  return cursor;
}

function completed<A>(
  meter: Meter,
  run: () => Promise<A>,
  count: (value: A) => number,
): Promise<A> {
  let result: Promise<A>;

  try {
    result = run();
  } catch (cause) {
    unavailable(meter, "Native KV/alarm operation failed; its write count is unavailable");
    throw cause;
  }
  meter.asynchronous++;
  // Return the original promise: accounting must not add an await to storage.
  void result.then(
    (value) => {
      meter.asynchronous--;
      meter.kvRowsWritten += count(value);
    },
    () => {
      meter.asynchronous--;
      unavailable(meter, "Native KV/alarm operation failed; its write count is unavailable");
    },
  );

  return result;
}

function decorateKv(storage: DurableObjectStorage | DurableObjectTransaction, meter: Meter): void {
  if (meter.decorated.has(storage)) return;
  meter.decorated.add(storage);
  const put = storage.put.bind(storage);
  const remove = storage.delete.bind(storage);
  const setAlarm = storage.setAlarm.bind(storage);

  storage.put = <T>(
    key: string | Record<string, T>,
    value?: T | DurableObjectPutOptions,
    options?: DurableObjectPutOptions,
  ): Promise<void> => {
    if (typeof key === "string")
      return completed(
        meter,
        () => put(key, value, options),
        () => 1,
      );
    let count = 0;

    for (const name of Object.keys(key)) {
      const property = Object.getOwnPropertyDescriptor(key, name);

      if (!property || !("value" in property))
        unavailable(meter, "KV put entries with getters cannot be counted without rereading them");
      else if (property.value !== undefined) count++;
    }

    // The overloaded native signature uses its second argument as options.
    return completed(
      meter,
      () => put(key, value as DurableObjectPutOptions | undefined),
      () => count,
    );
  };
  function deleteKeys(key: string, options?: DurableObjectPutOptions): Promise<boolean>;
  function deleteKeys(key: string[], options?: DurableObjectPutOptions): Promise<number>;
  function deleteKeys(
    key: string | string[],
    options?: DurableObjectPutOptions,
  ): Promise<boolean | number> {
    return typeof key === "string"
      ? completed(meter, () => remove(key, options), Number)
      : completed(meter, () => remove(key, options), Number);
  }
  storage.delete = deleteKeys;
  storage.setAlarm = (time, options) =>
    completed(
      meter,
      () => setAlarm(time, options),
      () => 1,
    );
  // deleteAlarm has no billed write-unit increment in workerd, unlike setAlarm.
}

/**
 * Install before the native superclass can capture storage methods or write.
 * Counters belong to this storage incarnation and perform no storage I/O.
 * SQL counts retain attempted writes, including rolled-back transactions.
 * KV counts are explicit key mutations plus setAlarm calls, not an invoice or
 * hidden storage/replication total. deleteAll/PITR and failed native writes make
 * subsequent snapshots unavailable; their write counts are not exposed.
 */
export function installUsage(storage: DurableObjectStorage): void {
  if (meters.has(storage)) return;

  const meter: Meter = {
    sqlRowsWritten: 0,
    kvRowsWritten: 0,
    asynchronous: 0,
    decorated: new WeakSet(),
  };

  meters.set(storage, meter);
  try {
    const sql = storage.sql;
    const exec = sql.exec.bind(sql);

    sql.exec = <T extends Record<string, SqlStorageValue>>(
      query: string,
      ...bindings: unknown[]
    ) => {
      meter.pending?.();
      if (!singleStatement(query))
        unavailable(meter, "Multi-statement SQL exposes only its final statement's write count");
      try {
        return trackCursor(exec<T>(query, ...bindings), meter);
      } catch (cause) {
        unavailable(meter, "Native SQL failed before returning a complete write count");
        throw cause;
      }
    };
    decorateKv(storage, meter);
    const kv = storage.kv;
    const put = kv.put.bind(kv);
    const remove = kv.delete.bind(kv);

    kv.put = (key, value) => {
      try {
        put(key, value);
        meter.kvRowsWritten++;
      } catch (cause) {
        unavailable(meter, "Native synchronous KV write failed");
        throw cause;
      }
    };
    kv.delete = (key) => {
      try {
        const removed = remove(key);

        meter.kvRowsWritten += Number(removed);

        return removed;
      } catch (cause) {
        unavailable(meter, "Native synchronous KV delete failed");
        throw cause;
      }
    };
    const transaction = storage.transaction.bind(storage);

    storage.transaction = (run) =>
      transaction((txn) => {
        decorateKv(txn, meter);

        return run(txn);
      });
    const deleteAll = storage.deleteAll.bind(storage);
    const restore = storage.onNextSessionRestoreBookmark.bind(storage);

    storage.deleteAll = (options) => {
      unavailable(meter, "deleteAll does not expose its native write count");

      return deleteAll(options);
    };
    storage.onNextSessionRestoreBookmark = (bookmark) => {
      unavailable(meter, "PITR does not expose its native write count");

      return restore(bookmark);
    };
  } catch {
    unavailable(meter, "This native storage implementation cannot install write accounting");
  }
}

/** Snapshot after outstanding storage promises settle; never substitutes zero for unknown usage. */
export function usage(storage: DurableObjectStorage): Usage {
  const meter = meters.get(storage);

  if (!meter)
    throw UsageUnavailable.make({
      reason: "Write accounting was not installed for this incarnation",
    });
  try {
    meter.pending?.();
  } catch {
    unavailable(meter, "Native SQL cursor no longer exposes its write count");
  }
  if (meter.unavailable) throw UsageUnavailable.make({ reason: meter.unavailable });
  if (meter.asynchronous !== 0)
    throw UsageUnavailable.make({
      reason: "Native storage writes are still pending",
    });

  return Usage.make({ sqlRowsWritten: meter.sqlRowsWritten, kvRowsWritten: meter.kvRowsWritten });
}

const isUsageUnavailable = Schema.is(UsageUnavailable);

/** Diagnostic failures remain data so accounting cannot invalidate a timing sample. */
export function snapshotUsage(storage: DurableObjectStorage): UsageSnapshot {
  try {
    return usage(storage);
  } catch (cause) {
    if (isUsageUnavailable(cause)) return { unavailable: cause.reason };
    throw cause;
  }
}
