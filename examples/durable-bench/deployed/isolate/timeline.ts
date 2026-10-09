import type { Phase } from "./protocol.ts";

const traces = new WeakMap<DurableObjectStorage, Timeline>();

/** Frozen I/O-clock events, not a wall/CPU stopwatch. No extra storage reads. */
export class Timeline {
  readonly events: Phase[] = [];
  private serial = 0;
  private entered = false;
  point(phase: string, id?: number) {
    if (this.events.length < 2000)
      this.events.push({ phase, atMs: Date.now(), ...(id === undefined ? {} : { id }) });
  }
  begin() {
    if (this.entered) this.events.length = 0;
    this.entered = true;
    this.point("endpoint.entry");
  }
  operation(name: string) {
    const id = this.serial++;

    this.point(name + ".dispatch", id);

    return id;
  }
}

export const timeline = (storage: DurableObjectStorage) => traces.get(storage);

export const instrumentTimeline = (ctx: DurableObjectState): void => {
  const trace = new Timeline();

  traces.set(ctx.storage, trace);
  trace.point("constructor.entry");
  const gate = ctx.blockConcurrencyWhile.bind(ctx);

  Object.defineProperty(ctx, "blockConcurrencyWhile", {
    value: <T>(callback: () => Promise<T>): Promise<T> => {
      const id = trace.operation("gate");

      return gate(async () => {
        trace.point("gate.callback", id);
        try {
          return await callback();
        } finally {
          trace.point("gate.callback.end", id);
        }
      }).then(
        (value) => {
          trace.point("gate.resolved", id);

          return value;
        },
        (cause: unknown) => {
          trace.point("gate.rejected", id);
          throw cause;
        },
      );
    },
  });
  const transaction = ctx.storage.transaction.bind(ctx.storage);

  Object.defineProperty(ctx.storage, "transaction", {
    value: <T>(callback: (txn: DurableObjectTransaction) => Promise<T>): Promise<T> => {
      const id = trace.operation("transaction");

      return transaction(async (txn) => {
        trace.point("transaction.callback", id);
        try {
          return await callback(txn);
        } finally {
          trace.point("transaction.callback.end", id);
        }
      }).then(
        (value) => {
          trace.point("transaction.resolved", id);

          return value;
        },
        (cause: unknown) => {
          trace.point("transaction.rejected", id);
          throw cause;
        },
      );
    },
  });
  for (const name of [
    "get",
    "put",
    "delete",
    "getAlarm",
    "setAlarm",
    "deleteAlarm",
    "sync",
  ] as const) {
    const original = ctx.storage[name];

    Object.defineProperty(ctx.storage, name, {
      value: (...args: unknown[]) => {
        const id = trace.operation(name);
        const result: Promise<unknown> = Reflect.apply(original, ctx.storage, args);

        return result.then(
          (value) => {
            trace.point(name + ".resolved", id);

            return value;
          },
          (cause: unknown) => {
            trace.point(name + ".rejected", id);
            throw cause;
          },
        );
      },
    });
  }
  const exec = ctx.storage.sql.exec.bind(ctx.storage.sql);
  let first = true;

  Object.defineProperty(ctx.storage.sql, "exec", {
    configurable: true,
    value: (sql: string, ...bindings: SqlStorageValue[]) => {
      if (first) trace.point("sql.first.before");
      const cursor = exec(sql, ...bindings);

      if (first) trace.point("sql.first.after");
      first = false;

      return cursor;
    },
  });
};

// These are deliberately recognizable sampled frames in profiling-only requests.
export function coldBisectBeforeInit(seed: number): number {
  for (let index = 0; index < 50_000_000; index++) seed = Math.imul(seed ^ index, 0x45d9f3b);

  return seed;
}

export function coldBisectAfterInit(seed: number): number {
  for (let index = 0; index < 50_000_000; index++) seed = Math.imul(seed + index, 0x45d9f3b);

  return seed;
}
