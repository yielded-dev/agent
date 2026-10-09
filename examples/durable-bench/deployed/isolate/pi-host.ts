import { MEASURED_TOOLS, turn } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { attach, type Observation } from "../worker/observe.ts";
import { storageProbe } from "./cold-storage.ts";
import { parseFixture, parseIsolate, errorText, readQuery } from "./native-protocol.ts";
import { INGRESS_HEADER } from "./observation.ts";
import { verifyFixture } from "./pi-storage.ts";
import type { BulkFixture, Env, Query } from "./protocol.ts";

export const COLD_ABORT = "durable-bench explicit cold";

/** The existing deployed Host contract, with only pi's native fixture reader reachable. */
export class Host {
  readonly meter: Observation;
  constructor(
    readonly ctx: DurableObjectState,
    readonly env: Env,
    readonly target: "pi",
  ) {
    this.meter = attach(ctx, env);
  }
  async fetch(
    request: Request,
    handlers: {
      import: (fixture: BulkFixture, query: Query) => Promise<void>;
      run: (input: { id: string; text: string }) => Promise<void>;
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
      if (url.pathname === "/storage") {
        const result = storageProbe(this.ctx.storage)?.padding(await request.json());

        if (!result) throw new Error("Storage probe not installed");
        await this.ctx.storage.sync();

        return Response.json(result);
      }
      if (url.pathname === "/import") {
        const fixture = parseFixture(await request.json());

        if (fixture.target !== query.target || fixture.history !== query.history)
          throw new Error("Fixture/query mismatch");
        if (fixture.mode === "replay")
          throw new Error("Only Yielded supports the fixture replay fallback");
        await handlers.import(fixture, query);
        const verified = await verifyFixture(this.ctx.storage, fixture);

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
        const ingress = request.headers.get(INGRESS_HEADER);

        this.meter.begin(query, ingress === null ? undefined : parseIsolate(JSON.parse(ingress)));
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
