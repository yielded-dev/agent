import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { MEASURED_TOOLS, turn } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { storageProbe } from "../isolate/cold-storage.ts";
import { timeline } from "../isolate/timeline.ts";
import { attach, type Observation } from "./observe.ts";
import {
  BulkFixture,
  errorText,
  readQuery,
  type Env,
  type Query,
  type Target,
  type SqlDump,
} from "./protocol.ts";
import { addressTardie, importRows, verifyFixture } from "./storage.ts";

export const COLD_ABORT = "durable-bench explicit cold";

export class Host {
  static async importTardie(storage: DurableObjectStorage, dump: SqlDump, object: string) {
    importRows(storage, await Effect.runPromise(addressTardie(dump, object)));
    const actual = tables(storage.sql);

    for (const table of dump.tables)
      if (actual[table.name] !== table.rows.length)
        throw new Error(`Tardie ${table.name} import count mismatch`);
    if (Object.keys(actual).length !== dump.tables.length)
      throw new Error("Tardie table inventory mismatch");

    return actual;
  }
  readonly meter: Observation;
  constructor(
    readonly ctx: DurableObjectState,
    readonly env: Env,
    readonly target: Target,
  ) {
    this.meter = attach(ctx, env);
  }
  async fetch(
    request: Request,
    handlers: {
      import: (fixture: BulkFixture, query: Query) => Promise<void>;
      empty?: () => Promise<void>;
      run?: (input: { id: string; text: string }) => Promise<void>;
    },
  ): Promise<Response> {
    const url = new URL(request.url);
    const query = readQuery(url);

    if (query.target !== this.target) throw new Error("Target mismatch");
    if (url.pathname === "/identity") return Response.json(this.meter.identity());
    if (url.pathname === "/cold") {
      await this.ctx.storage.sync();
      this.meter.marker("cold-abort", query);
      this.ctx.abort(COLD_ABORT);
    }
    try {
      if (url.pathname === "/empty") {
        if (query.history !== 0) throw new Error("Empty preparation requires history0");
        await handlers.empty?.();
        await this.ctx.storage.sync();

        return Response.json({
          ok: true,
          identity: this.meter.identity(),
          tables: tables(this.ctx.storage.sql),
          bytes: this.ctx.storage.sql.databaseSize,
        });
      }
      if (url.pathname === "/storage") {
        const result = storageProbe(this.ctx.storage)?.padding(await request.json());

        if (!result) throw new Error("Storage probe not installed");
        await this.ctx.storage.sync();

        return Response.json(result);
      }
      if (url.pathname === "/import") {
        const fixture = Schema.decodeUnknownSync(BulkFixture)(await request.json());

        if (fixture.target !== query.target || fixture.history !== query.history)
          throw new Error("Fixture/query mismatch");
        if (fixture.mode === "replay" && fixture.target !== "yielded")
          throw new Error("Only Yielded supports the fixture replay fallback");
        await handlers.import(fixture, query);
        const verified = await Effect.runPromise(verifyFixture(this.ctx.storage, fixture));

        return Response.json({
          ok: true,
          target: query.target,
          history: query.history,
          ...verified,
          bytes: this.ctx.storage.sql.databaseSize,
          mode: fixture.mode,
          ...(fixture.fallbackReason === undefined
            ? {}
            : { fallbackReason: fixture.fallbackReason }),
          identity: this.meter.identity(),
        });
      }
      if (url.pathname === "/run") {
        if (!handlers.run) throw new Error("Yielded uses submit and await");
        this.meter.begin(query);
        this.meter.marker("run");
        await handlers.run(turn(query.sample, MEASURED_TOOLS));

        return Response.json({ ok: true, outcome: "completed", identity: this.meter.entry });
      }
      if (url.pathname === "/metrics") {
        if (!this.meter.query || this.meter.query.sample !== query.sample)
          throw new Error("No metrics for this sample/incarnation");

        return Response.json({
          ok: true,
          query: this.meter.query,
          identity: this.meter.entry,
          calls: this.meter.calls,
          storage: storageProbe(this.ctx.storage)?.report(),
          timeline: timeline(this.ctx.storage)?.events,
          tables: tables(this.ctx.storage.sql),
          bytes: this.ctx.storage.sql.databaseSize,
        });
      }

      return new Response("not found", { status: 404 });
    } catch (cause) {
      return Response.json(
        { ok: false, error: errorText(cause), sample: query.sample },
        { status: 500 },
      );
    }
  }
}
