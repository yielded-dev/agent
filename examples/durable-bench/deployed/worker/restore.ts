/** Restore idle fixture bytes before acquiring the production runtime. */
import { Context, Effect, Schema } from "effect";

import { tables } from "../../src/serve.ts";
import { BulkFixture, RESTORE_BYTES, RestoreChunk, type Query } from "./protocol.ts";
import { FixtureError, importRows, verifyFixture } from "./storage.ts";

const key = "durable-bench/restore";
const Session = Schema.Struct({ fixture: BulkFixture, next: Schema.Natural });
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

/** Supplied only at the Durable Object fetch boundary. */
export class RestoreStorage extends Context.Service<RestoreStorage, DurableObjectStorage>()(
  "durable-bench/RestoreStorage",
) {}

export const restore = Effect.fnUntraced(function* (request: Request, query: Query) {
  const storage = yield* RestoreStorage;

  return yield* Effect.tryPromise({
    try: async () => {
      const path = new URL(request.url).pathname;

      if (request.method !== "POST") throw new Error("Restore requires POST");
      const text = await request.text();

      if (new TextEncoder().encode(text).byteLength > RESTORE_BYTES)
        throw new Error("Restore request exceeds the byte bound");

      if (path === "/restore/start") {
        const fixture = Schema.decodeUnknownSync(BulkFixture)(JSON.parse(text));

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
          throw new Error("Restore requires an unchanged source identity and empty schema rows");
        const initialized = tables(storage.sql);
        const bootstrap = new Set(["effect_agent_schema", "effect_agent_meta"]);

        if (
          (await storage.get(key)) ||
          Object.entries(initialized).some(
            ([table, count]) =>
              !(table in fixture.tables) || (count !== 0 && !(bootstrap.has(table) && count === 1)),
          )
        )
          throw new Error(
            `Restore requires an empty initialized Object: ${JSON.stringify(initialized)}`,
          );
        const dump = fixture.thread;

        await storage.transaction(async (txn) => {
          // Only the constructor's empty layout is replaced; historical rows are never rewritten.
          // Cold setup aborts this runtime before any measured admission.
          for (const table of Object.keys(initialized).reverse())
            storage.sql.exec(`DROP TABLE ${quote(table)}`);
          importRows(storage, { tables: dump.tables, indexes: [] });
          await txn.put(key, { fixture, next: 0 });
        });

        return { ok: true };
      }

      const session = Schema.decodeUnknownSync(Session)(await storage.get(key));
      const { fixture } = session;

      if (fixture.target !== query.target || fixture.history !== query.history || !fixture.thread)
        throw new Error("Restore session mismatch");

      const dump = fixture.thread;

      if (path === "/restore/rows") {
        const chunk = Schema.decodeUnknownSync(RestoreChunk)(JSON.parse(text));

        if (chunk.sequence !== session.next)
          throw new Error("Restore chunks cannot be retried or reordered");
        await storage.transaction(async (txn) => {
          for (const part of chunk.parts) {
            const table = dump.tables.find((table) => table.name === part.table);

            if (
              !table ||
              part.rows.some((row) => row.length !== table.columns.length) ||
              part.offset + part.rows.length > (fixture.tables[part.table] ?? -1)
            )
              throw new Error("Restore chunk differs from source schema/counts");

            const count = storage.sql
              .exec<{ n: number }>(`SELECT COUNT(*) n FROM ${quote(table.name)}`)
              .one().n;

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
        });

        return { ok: true };
      }
      if (path !== "/restore/finalize") throw new Error("Unknown restore operation");
      // Large snapshots have many indexes. Bound each setup transaction independently;
      // admission stays disabled until all indexes and the complete verification succeed.
      for (const sql of [...dump.indexes, ...(dump.triggers ?? [])]) {
        storage.transactionSync(() => storage.sql.exec(sql));
        await storage.sync();
      }
      const verified = await Effect.runPromise(verifyFixture(storage, fixture));

      await storage.delete(key);
      await storage.sync();

      return {
        ok: true,
        target: fixture.target,
        history: fixture.history,
        mode: fixture.mode,
        ...verified,
      };
    },
    catch: (cause) => new FixtureError({ message: "Snapshot restore failed", cause }),
  });
});
