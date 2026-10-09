import { DurableObject } from "cloudflare:workers";

import { turn } from "../../src/plan.ts";
import { attach } from "../worker/observe.ts";
import { errorText, parseIdentity, parseIsolate, readQuery } from "./native-protocol.ts";
import { buildMismatch, INGRESS_HEADER, observeFetch } from "./observation.ts";
import type { Env } from "./protocol.ts";
import {
  coldBisectAfterInit,
  coldBisectBeforeInit,
  instrumentTimeline,
  timeline,
} from "./timeline.ts";

const COLD_ABORT = "cold-bisect explicit cold";

/** Platform floor: no framework, no constructor storage, one timed SQLite INSERT. */
export class BareDO extends DurableObject<Env> {
  private readonly meter;
  private pending?: Promise<void>;
  constructor(ctx: DurableObjectState, env: Env) {
    instrumentTimeline(ctx);
    super(ctx, env);
    this.meter = attach(ctx, env);
    timeline(ctx.storage)?.point("constructor.return");
  }
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const query = readQuery(url);

    if (query.target !== "bare" || query.history !== 0)
      throw new Error("Bare history must be zero");
    if (url.pathname === "/profile-id")
      return Response.json({
        actorId: this.ctx.id.toString(),
        version: this.env.VERSION?.id,
        identity: this.meter.identity(),
      });
    if (url.pathname === "/sentinel") {
      const result =
        url.searchParams.get("phase") === "before"
          ? coldBisectBeforeInit(7)
          : coldBisectAfterInit(7);

      return Response.json({ ok: true, result, identity: this.meter.identity() });
    }
    if (url.pathname === "/identity") return Response.json(this.meter.identity());
    if (url.pathname === "/empty") {
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS bare_receipts (id TEXT PRIMARY KEY)");
      await this.ctx.storage.sync();

      return Response.json({
        ok: true,
        identity: this.meter.identity(),
        tables: { bare_receipts: 0 },
        bytes: this.ctx.storage.sql.databaseSize,
      });
    }
    if (url.pathname === "/cold") {
      await this.ctx.storage.sync();
      this.ctx.abort(COLD_ABORT);
    }
    if (url.pathname === "/submit") {
      const ingress = request.headers.get(INGRESS_HEADER);

      this.meter.begin(query, ingress === null ? undefined : parseIsolate(JSON.parse(ingress)));
      this.meter.marker("submit");
      this.ctx.storage.sql.exec("INSERT INTO bare_receipts VALUES (?)", query.sample);
      timeline(this.ctx.storage)?.point("bare.write.return");
      await this.ctx.storage.sync();
      timeline(this.ctx.storage)?.point("submit.complete");
      // A single post-durability provider probe supplies a comparable first-request boundary.
      // Its response is awaited separately; it is never part of the receipt's write floor.
      this.pending = (async () => {
        const response = await this.meter.fetch(this.env.PROVIDER_URL + "/chat/completions", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            model: "scripted-1",
            stream: true,
            messages: [{ role: "user", content: turn(query.sample, 0).text }],
          }),
        });

        await response.text();
        if (!response.ok) throw new Error("Bare provider probe failed");
      })();
      this.ctx.waitUntil(this.pending);

      return Response.json({ ok: true, receipt: { id: query.sample } });
    }
    if (url.pathname === "/await") {
      if (!this.pending) throw new Error("No submitted probe in this incarnation");
      await this.pending;

      return Response.json({ ok: true, outcome: "completed", identity: this.meter.entry });
    }
    if (url.pathname === "/metrics") {
      if (this.meter.query?.sample !== query.sample) throw new Error("Wrong sample");

      return Response.json({
        ok: true,
        query,
        identity: this.meter.entry,
        calls: this.meter.calls,
        tables: {},
        bytes: this.ctx.storage.sql.databaseSize,
        timeline: timeline(this.ctx.storage)?.events,
      });
    }

    return new Response("not found", { status: 404 });
  }
}

type Bindings = Env & { BARE: DurableObjectNamespace<BareDO> };

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    const ingress = observeFetch(env);

    if (!env.BENCH_TOKEN || request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    try {
      if (url.pathname === "/health")
        return Response.json({ ok: true, build: env.BUILD, workerIsolate: ingress });
      const mismatch = buildMismatch(request, env);

      if (mismatch) return mismatch;
      const query = readQuery(url);
      const stub = env.BARE.getByName(query.object, { locationHint: "wnam" });

      if (url.pathname === "/cold") {
        const identityUrl = new URL(url);

        identityUrl.pathname = "/identity";

        const before = parseIdentity(
          await (await stub.fetch(new Request(identityUrl, { headers: request.headers }))).json(),
        );

        let threadAborted = false;

        try {
          const response = await stub.fetch(request);

          threadAborted = response.status === 500 && (await response.text()).includes(COLD_ABORT);
        } catch (cause) {
          threadAborted = errorText(cause).includes(COLD_ABORT);
        }

        return Response.json(
          { ok: threadAborted, before, threadAborted },
          { status: threadAborted ? 200 : 502 },
        );
      }
      const headers = new Headers(request.headers);

      headers.set(INGRESS_HEADER, JSON.stringify(ingress));

      return await stub.fetch(new Request(request, { headers }));
    } catch (cause) {
      return Response.json(
        { ok: false, error: errorText(cause), sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
