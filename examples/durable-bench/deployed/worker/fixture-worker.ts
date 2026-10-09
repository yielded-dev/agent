/** Local-only transfer boundary for the existing Miniflare seed databases. */
import { DurableObject } from "cloudflare:workers";
import { Effect, Schema } from "effect";

import { fingerprint } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { BulkFixture, SqlDump } from "./protocol.ts";
import { exportCanonical, importCanonical, transcript } from "./storage.ts";
import { YieldedDO as ProductionYieldedDO } from "./yielded.ts";

const quote = (value: string) => `"${value.replaceAll('"', '""')}"`;

async function transfer(storage: DurableObjectStorage, request: Request) {
  const url = new URL(request.url);

  if (url.pathname === "/import") {
    const fixture = Schema.decodeUnknownSync(BulkFixture)(await request.json());

    if (!fixture.archive) throw new Error("Missing canonical archive");
    await Effect.runPromise(importCanonical(storage, fixture.archive, "main"));
  }

  const target = Schema.decodeUnknownSync(BulkFixture.fields.target)(
    url.searchParams.get("target"),
  );

  if (url.pathname === "/export" && target === "yielded")
    return Response.json({
      archive: await Effect.runPromise(exportCanonical(storage)),
      tables: tables(storage.sql),
      fingerprint: await fingerprint(transcript(storage.sql, target)),
    });
  if (url.pathname === "/import")
    return Response.json({
      tables: tables(storage.sql),
      fingerprint: await fingerprint(transcript(storage.sql, target)),
    });
  const sql = storage.sql;

  const schema = sql
    .exec<{ name: string; sql: string }>(
      "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY rowid",
    )
    .toArray();

  const dump = Schema.decodeUnknownSync(SqlDump)({
    tables: schema.map((table) => {
      const columns = sql
        .exec<{ name: string }>(`PRAGMA table_info(${quote(table.name)})`)
        .toArray()
        .map((column) => column.name);

      return {
        ...table,
        columns,
        rows: sql
          .exec(`SELECT * FROM ${quote(table.name)}`)
          .toArray()
          .map((row) =>
            columns.map((column) => {
              const value = row[column];

              return value instanceof ArrayBuffer
                ? {
                    blob: btoa(
                      Array.from(new Uint8Array(value), (byte) => String.fromCharCode(byte)).join(
                        "",
                      ),
                    ),
                  }
                : value;
            }),
          ),
      };
    }),
    indexes: sql
      .exec<{ sql: string }>(
        "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'",
      )
      .toArray()
      .map((row) => row.sql),
  });

  return Response.json({
    dump,
    tables: tables(sql),
    ...(url.searchParams.get("actor") === "true"
      ? {}
      : { fingerprint: await fingerprint(transcript(sql, target)) }),
  });
}
class FixtureDO extends DurableObject {
  fetch(request: Request) {
    return transfer(this.ctx.storage, request);
  }
  // Quiesced snapshot copies never run the old host's retained maintenance.
  alarm() {}
}

export class NormalizedYieldedDO extends ProductionYieldedDO {
  override fetch(request: Request) {
    return transfer(this.ctx.storage, request);
  }
}

export class YieldedDO extends FixtureDO {}
export class PiDO extends FixtureDO {}
export class ThreadDO extends FixtureDO {}
export class ActorDO extends FixtureDO {}

export default {
  fetch(request: Request, env: { OBJECT: DurableObjectNamespace<FixtureDO> }) {
    const url = new URL(request.url);
    const target = url.searchParams.get("target");
    const actor = url.searchParams.get("actor") === "true";

    return env.OBJECT.getByName(
      target === "tardie" && !actor ? JSON.stringify(["bench-agent", "main", "bench"]) : "main",
    ).fetch(request);
  },
};
