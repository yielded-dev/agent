import { fingerprint, type Message } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { expectedSeed, argument, array, object, string } from "./native-protocol.ts";
import type { BulkFixture, SqlDump } from "./protocol.ts";

const fail = (message: string) => new Error(message);
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

export const importRows = (storage: DurableObjectStorage, dump: SqlDump): void => {
  storage.transactionSync(() => {
    for (const table of dump.tables) {
      if (table.name.startsWith("_cf_") || table.name.startsWith("sqlite_"))
        throw fail("Platform tables cannot be imported");
      storage.sql.exec(table.sql.replace(/^CREATE TABLE /i, "CREATE TABLE IF NOT EXISTS "));
      if (
        storage.sql.exec<{ n: number }>(`SELECT COUNT(*) n FROM ${quote(table.name)}`).one().n !== 0
      )
        throw fail(`Import destination table ${table.name} is not empty`);
      for (const row of table.rows) {
        const values = row.map((value) =>
          typeof value === "object" && value !== null
            ? Uint8Array.from(atob(value.blob), (char) => char.charCodeAt(0)).buffer
            : value,
        );

        storage.sql.exec(
          `INSERT INTO ${quote(table.name)} (${table.columns.map(quote).join(",")}) VALUES (${values.map(() => "?").join(",")})`,
          ...values,
        );
      }
    }
    for (const index of dump.indexes)
      storage.sql.exec(index.replace(/^CREATE (UNIQUE )?INDEX /i, "CREATE $1INDEX IF NOT EXISTS "));
  });
};

/** Same pi fixture projection as worker/storage.ts, without another framework's decoders. */
export const transcript = (sql: SqlStorage): readonly Message[] => {
  const result: Message[] = [];

  for (const row of sql
    .exec<{ record: string }>("SELECT record FROM entries ORDER BY commit_seq")
    .toArray()) {
    for (const input of array(object(JSON.parse(row.record)).model)) {
      const message = object(input);

      if (message.role !== "user" && message.role !== "assistant" && message.role !== "toolResult")
        continue;

      const parts = typeof message.content === "string" ? [] : array(message.content).map(object);

      const calls = parts.flatMap((part) =>
        part.type === "toolCall" ? [argument(part.arguments)] : [],
      );

      const text =
        typeof message.content === "string"
          ? message.content
          : parts
              .flatMap((part) =>
                part.type === "text" ? [part.text === undefined ? "" : string(part.text)] : [],
              )
              .join("");

      result.push({
        role: message.role === "toolResult" ? "tool" : message.role,
        text,
        ...(calls.length ? { calls } : {}),
      });
    }
  }
  if (result.at(-1)?.role !== "assistant" || result.at(-1)?.calls?.length)
    throw fail("Fixture is not settled");

  return result.slice(0, -1);
};

export const verifyFixture = async (storage: DurableObjectStorage, fixture: BulkFixture) => {
  const digest = await fingerprint(transcript(storage.sql));

  if (
    digest !== fixture.fingerprint ||
    (expectedSeed[fixture.history] !== undefined && digest !== expectedSeed[fixture.history])
  )
    throw fail("Imported transcript fingerprint differs from local fixture");
  const counts = tables(storage.sql);

  if (Object.keys(counts).sort().join("\n") !== Object.keys(fixture.tables).sort().join("\n"))
    throw fail("Initialized table inventory differs from local source fixture");

  for (const [table, expected] of Object.entries(fixture.tables)) {
    if (counts[table] !== expected)
      throw fail(`Initialized ${table} count ${counts[table]} differs from source ${expected}`);
  }

  return { fingerprint: digest, tables: counts };
};
