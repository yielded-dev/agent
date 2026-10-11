import { Schema } from "effect";

import { beginCodecs, CodecCount, endCodecs } from "./count-codec.ts";
import { installSqlCount, SqlCounts } from "./count-sql.ts";

export const CountReport = Schema.Struct({
  sample: Schema.NonEmptyString,
  scope: Schema.Literal("target bundle between admission and quiescent metrics"),
  sql: SqlCounts,
  codecs: Schema.Array(CodecCount),
});

export type CountReport = typeof CountReport.Type;

const storageCounters = new WeakMap<DurableObjectStorage, ReturnType<typeof installSqlCount>>();
let active: { sample: string; sql: ReturnType<typeof installSqlCount> } | undefined;

export const installCountStorage = (storage: DurableObjectStorage) => {
  let counter = storageCounters.get(storage);

  if (!counter) {
    counter = installSqlCount(storage);
    storageCounters.set(storage, counter);
  }

  return counter;
};

export const attachCount = <Query extends { readonly sample: string }>(
  storage: DurableObjectStorage,
  meter: { begin(query: Query): void },
) => {
  const sql = installCountStorage(storage);
  const begin = meter.begin.bind(meter);

  meter.begin = (query) => {
    if (query.sample.startsWith("count-")) {
      if (active) throw new Error("A benchmark counting window is already active");
      sql.start();
      beginCodecs();
      active = { sample: query.sample, sql };
    }
    begin(query);
  };
};

export const stopCount = (): CountReport | undefined => {
  const window = active;

  active = undefined;
  if (!window) return undefined;
  const codecs = endCodecs();
  const sql = window.sql.stop();

  return CountReport.make({
    sample: window.sample,
    scope: "target bundle between admission and quiescent metrics",
    sql,
    codecs,
  });
};
