/** Restore idle fixture bytes before acquiring the production runtime. */
import { Effect, Schema } from "effect";

import { tables } from "../../src/serve.ts";
import { BulkFixture, errorText, RESTORE_BYTES, RestoreChunk, type Query } from "./protocol.ts";
import { FixtureError, importRows, RestoreStorage, verifyFixture } from "./storage.ts";

const key = "durable-bench/restore";
const Session = Schema.Struct({ fixture: BulkFixture, next: Schema.Natural });
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const fail = (message: string) => FixtureError.make({ message });

const foreignError = (cause: unknown) =>
  FixtureError.make({ message: `Snapshot restore failed: ${errorText(cause)}`, cause });

const read = <A>(body: () => A) => Effect.try({ try: body, catch: foreignError });

const attempt = <A>(body: () => Promise<A>) =>
  Effect.tryPromise({ try: body, catch: foreignError });

export const restore = Effect.fnUntraced(function* (request: Request, query: Query) {
  const storage = yield* RestoreStorage;
  const path = new URL(request.url).pathname;

  if (request.method !== "POST") return yield* fail("Restore requires POST");
  const text = yield* attempt(() => request.text());

  if (new TextEncoder().encode(text).byteLength > RESTORE_BYTES)
    return yield* fail("Restore request exceeds the byte bound");

  if (path === "/restore/start") {
    const fixture = yield* Schema.decodeEffect(Schema.fromJsonString(BulkFixture))(text).pipe(
      Effect.mapError(foreignError),
    );

    if (
      fixture.target !== query.target ||
      fixture.history !== query.history ||
      query.storageObject !== fixture.threadId ||
      fixture.mode !== "import" ||
      !fixture.thread ||
      fixture.thread.tables.some((table) => table.rows.length !== 0) ||
      new Set(fixture.thread.tables.map((table) => table.name)).size !==
        fixture.thread.tables.length
    )
      return yield* fail("Restore requires an unchanged source identity and empty schema rows");
    const initialized = yield* read(() => tables(storage.sql));
    const bootstrap = new Set(["effect_agent_schema", "effect_agent_meta"]);

    if (
      (yield* attempt(() => storage.get(key))) ||
      Object.entries(initialized).some(
        ([table, count]) =>
          !(table in fixture.tables) || (count !== 0 && !(bootstrap.has(table) && count === 1)),
      )
    )
      return yield* fail(
        `Restore requires an empty initialized Object: ${JSON.stringify(initialized)}`,
      );
    const dump = fixture.thread;

    yield* attempt(() =>
      storage.transaction(async (txn) => {
        // Only the constructor's empty layout is replaced; historical rows are never rewritten.
        // Cold setup aborts this runtime before any measured admission.
        for (const table of Object.keys(initialized).reverse())
          storage.sql.exec(`DROP TABLE ${quote(table)}`);
        importRows(storage, { tables: dump.tables, indexes: [] });
        await txn.put(key, { fixture, next: 0 });
      }),
    );

    return { ok: true };
  }

  const session = yield* Schema.decodeUnknownEffect(Session)(
    yield* attempt(() => storage.get(key)),
  ).pipe(Effect.mapError(foreignError));

  const { fixture } = session;

  if (fixture.target !== query.target || fixture.history !== query.history || !fixture.thread)
    return yield* fail("Restore session mismatch");
  const dump = fixture.thread;

  if (path === "/restore/rows") {
    const chunk = yield* Schema.decodeEffect(Schema.fromJsonString(RestoreChunk))(text).pipe(
      Effect.mapError(foreignError),
    );

    if (chunk.sequence !== session.next)
      return yield* fail("Restore chunks cannot be retried or reordered");
    yield* attempt(() =>
      storage.transaction(async (txn) => {
        for (const part of chunk.parts) {
          const table = dump.tables.find((table) => table.name === part.table);

          if (
            !table ||
            part.rows.some((row) => row.length !== table.columns.length) ||
            part.offset + part.rows.length > (fixture.tables[part.table] ?? -1)
          )
            // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Reject the native transaction to roll back rows from preceding parts of this restore chunk.
            throw new Error("Restore chunk differs from source schema/counts");

          const count = storage.sql
            .exec<{ n: number }>(`SELECT COUNT(*) n FROM ${quote(table.name)}`)
            .one().n;

          // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Counts depend on prior writes in this transaction; rejection rolls back the complete chunk.
          if (count !== part.offset) throw new Error("Restore offset mismatch");
          const batchSize = Math.max(1, Math.floor(100 / table.columns.length));

          for (let offset = 0; offset < part.rows.length; offset += batchSize) {
            const rows = part.rows.slice(offset, offset + batchSize);

            const values = rows.flatMap((row) =>
              row.map((value) =>
                typeof value === "object" && value !== null
                  ? Uint8Array.from(atob(value.blob), (char) => char.charCodeAt(0)).buffer
                  : value,
              ),
            );

            storage.sql.exec(
              `INSERT INTO ${quote(table.name)} (${table.columns.map(quote).join(",")}) VALUES ${rows.map(() => `(${table.columns.map(() => "?").join(",")})`).join(",")}`,
              ...values,
            );
          }
        }
        await txn.put(key, { fixture, next: session.next + 1 });
      }),
    );

    return { ok: true };
  }
  if (path !== "/restore/finalize") return yield* fail("Unknown restore operation");
  // Large snapshots have many indexes. Bound each setup transaction independently;
  // admission stays disabled until all indexes and the complete verification succeed.
  for (const sql of [...dump.indexes, ...(dump.triggers ?? [])]) {
    yield* read(() => storage.transactionSync(() => storage.sql.exec(sql)));
    yield* attempt(() => storage.sync());
  }
  const verified = yield* verifyFixture(fixture);

  yield* Effect.tryPromise({
    try: async () => {
      await storage.delete(key);
      await storage.sync();
    },
    catch: (cause) => FixtureError.make({ message: "Finish snapshot restore", cause }),
  });

  return {
    ok: true,
    target: fixture.target,
    history: fixture.history,
    mode: fixture.mode,
    ...verified,
  };
});
