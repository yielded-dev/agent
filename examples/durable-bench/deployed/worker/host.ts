import { Effect, Schema } from "effect";

import { CYCLE, history, MEASURED_TOOLS, turn } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { attach, type Observation } from "./observe.ts";
import {
  BulkFixture,
  errorText,
  readQuery,
  SeedBatch,
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
    constructedMs = Date.now(),
  ) {
    this.meter = attach(ctx, env, constructedMs);
  }
  async fetch(
    request: Request,
    handlers: {
      import: (fixture: BulkFixture, query: Query) => Promise<void>;
      run?: (input: { id: string; text: string }, query: Query) => Promise<void>;
      close?: () => Promise<void>;
      directoryUsed?: boolean;
    },
  ): Promise<Response> {
    const url = new URL(request.url);
    const query = readQuery(url);

    if (query.target !== this.target) throw new Error("Target mismatch");
    if (url.pathname === "/identity") return Response.json(this.meter.identity());
    if (url.pathname === "/cold") {
      await handlers.close?.();
      await this.ctx.storage.sync();
      this.ctx.abort(COLD_ABORT);
    }
    try {
      if (url.pathname === "/prime") {
        this.meter.assertBuild(query);

        // Real network I/O advances the clock; timer callbacks can remain clamped.
        // Keep the probe's latency instead of estimating/subtracting invisible CPU.
        const probe = await fetch(new URL("/health", this.env.PROVIDER_URL), {
          headers: { authorization: `Bearer ${this.env.BENCH_TOKEN}`, "cache-control": "no-store" },
          signal: AbortSignal.timeout(30_000),
        });

        if (!probe.ok) throw new Error(`Constructor clock probe returned HTTP ${probe.status}`);
        await probe.arrayBuffer();

        return Response.json({
          ok: true,
          identity: this.meter.identity(),
          constructorAndProbeMs: Date.now() - this.meter.constructedMs,
        });
      }
      if (url.pathname === "/seed") {
        this.meter.assertBuild(query);
        if (!handlers.run) throw new Error("Target has no native history builder");
        const { from, to } = Schema.decodeUnknownSync(SeedBatch)(await request.json());

        if (to <= from || to - from > 50 || to > query.history)
          throw new Error("History batches must contain 1–50 turns within the requested history");
        const key = "durable-bench/history";

        await this.ctx.blockConcurrencyWhile(async () => {
          const stored = await this.ctx.storage.get(key);

          const progress =
            stored === undefined
              ? { completed: 0, inFlight: false }
              : Schema.decodeUnknownSync(
                  Schema.Struct({ completed: Schema.Natural, inFlight: Schema.Boolean }),
                )(stored);

          if (progress.inFlight || progress.completed !== from)
            throw new Error(
              "History batch is repeated, out of order, or has an uncertain predecessor",
            );
          await this.ctx.storage.put(key, { completed: from, inFlight: true });
        });
        const providerColos = new Set<string>();

        for (const [offset, input] of history(from, to).entries()) {
          const seedQuery = {
            ...query,
            sample: input.id,
            ttftMs: 0 as const,
            chunkDelayMs: 0,
            textStreaming: false,
          };

          this.meter.begin(seedQuery);
          await handlers.run(input, seedQuery);
          if (
            this.meter.calls.length !== (CYCLE[(from + offset) % CYCLE.length] ?? 0) + 1 ||
            this.meter.calls.some(
              (call) =>
                call.status !== 200 ||
                call.error !== undefined ||
                call.endMs === undefined ||
                call.receipt?.fingerprint !== call.fingerprint ||
                call.receipt.expectedBuild !== query.expectedBuild ||
                call.receipt.objectBuild !== this.env.BUILD,
            )
          )
            throw new Error(`History ${input.id} did not complete its scripted model calls`);
          for (const call of this.meter.calls)
            if (call.receipt?.colo) providerColos.add(call.receipt.colo);
        }
        const fingerprint = this.meter.calls.at(-1)?.fingerprint;

        if (!fingerprint) throw new Error("History batch has no final model fingerprint");
        await this.ctx.storage.put(key, { completed: to, inFlight: false });
        await this.ctx.storage.sync();

        return Response.json({
          ok: true,
          from,
          to,
          fingerprint,
          identity: this.meter.identity(),
          providerColos: [...providerColos],
        });
      }
      if (url.pathname === "/import") {
        this.meter.assertBuild(query);
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
        await handlers.run(turn(query.sample, MEASURED_TOOLS), query);

        return Response.json({ ok: true, outcome: "completed", identity: this.meter.entry });
      }
      if (url.pathname === "/metrics") {
        if (!this.meter.query || this.meter.query.sample !== query.sample)
          throw new Error("No metrics for this sample/incarnation");

        return Response.json({
          ok: true,
          query: this.meter.query,
          identity: this.meter.entry,
          ...(handlers.directoryUsed === undefined
            ? {}
            : { directoryUsed: handlers.directoryUsed }),
          calls: this.meter.calls,
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
