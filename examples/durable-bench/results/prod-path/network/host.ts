import { fingerprint, history, MEASURED_TOOLS, turn, type Turn } from "../../../src/plan.ts";
import { observation } from "./observe.ts";
import {
  decodeSeed,
  decodeSeedState,
  errorText,
  expectedSeed,
  readQuery,
  type Env,
  type Query,
} from "./protocol.ts";

export interface Operations {
  wake(): Promise<void>;
  turn(input: Turn): Promise<void>;
}

const seedKey = "prod-path:seed";

export class Host {
  private busy = false;
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
    private readonly target: Query["target"],
  ) {}
  async fetch(request: Request, operations: Operations): Promise<Response> {
    const url = new URL(request.url);
    const meter = observation(this.state.storage);

    if (!this.env.TOKEN || request.headers.get("authorization") !== `Bearer ${this.env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    let query: Query;

    try {
      query = readQuery(url);
    } catch (cause) {
      return Response.json({ ok: false, error: errorText(cause) }, { status: 400 });
    }
    if (url.pathname === "/metrics") {
      if (meter.query?.sample !== query.sample || !meter.measurement)
        return Response.json(
          { ok: false, error: "No observation for this sample" },
          { status: 409 },
        );
      const seeded = decodeSeedState(await this.state.storage.get(seedKey));

      await meter.drainMarkers();

      const result = {
        ok: true,
        ...meter.measurement,
        ...meter.finish(),
        seedFingerprint: seeded.fingerprint,
        databaseBytes: this.state.storage.sql.databaseSize,
        metricsObservedMs: Date.now(),
      };

      return Response.json(result);
    }
    const entry = meter.entry();
    const constructorSql = meter.initialSql();

    const identity = () => ({
      constructorSql,
      entry,
      ...query,
      ...meter.identity(this.state, this.env),
    });

    if (query.target !== this.target)
      return Response.json(
        { ok: false, error: "target/binding mismatch", ...identity() },
        { status: 400 },
      );
    if (url.pathname === "/identity" && request.method === "GET")
      return Response.json({ ok: true, ...identity() });
    if (this.busy)
      return Response.json(
        { ok: false, error: "Object already has an active harness request", ...identity() },
        { status: 409 },
      );
    if (request.method !== "POST") return new Response("POST required", { status: 405 });
    this.busy = true;
    const startedMs = Date.now();

    meter.reset(query);
    console.log({ prodPath: "target-start", path: url.pathname, ...identity(), startedMs });
    try {
      if (url.pathname === "/cold") {
        // abort() deliberately rejects this invocation. The outer Worker obtains
        // identity before this call; m0 must prove a changed incarnation afterward.
        await this.state.storage.sync();
        console.log({ prodPath: "cold-requested", ...identity() });
        this.state.abort("prod-path explicit cold incarnation");
      }
      if (url.pathname === "/seed") {
        if (this.env.PHASE !== "seed") throw new Error("/seed requires PHASE=seed");
        const { from, to } = decodeSeed(await request.json());

        if (from < 0 || to <= from || to > query.history || to - from > 10)
          throw new Error("Seed chunks must be consecutive, at most ten turns, and within history");
        const prior = await this.state.storage.get(seedKey);

        const progress =
          prior === undefined ? { through: 0, history: query.history } : decodeSeedState(prior);

        if (progress.history !== query.history || progress.through !== from)
          throw new Error(`Seed cursor mismatch: expected ${progress.through}`);
        for (const input of history(from, to)) {
          await operations.turn(input);
          const hash = await fingerprint(meter.seen);

          await this.state.storage.put(seedKey, {
            history: query.history,
            through: Number(input.id.slice(1)) + 1,
            fingerprint: hash,
          });
        }
        const hash = await fingerprint(meter.seen);

        if (to === query.history && hash !== expectedSeed[query.history])
          throw new Error(`Seed transcript mismatch: ${hash} != ${expectedSeed[query.history]}`);

        return Response.json({
          ok: true,
          ...identity(),
          from,
          to,
          fingerprint: hash,
          databaseBytes: this.state.storage.sql.databaseSize,
        });
      }
      if (this.env.PHASE !== "measure") throw new Error("Measurement requires PHASE=measure");
      const seeded = decodeSeedState(await this.state.storage.get(seedKey));

      if (
        seeded.through !== query.history ||
        seeded.history !== query.history ||
        seeded.fingerprint !== expectedSeed[query.history]
      )
        throw new Error("Complete verified seed required");
      if (url.pathname === "/reset") {
        // Warm/recover through the target's original open path, then clear only
        // observations. Canonical history is never reset or rewritten.
        await operations.wake();
        meter.reset(query);

        return Response.json({
          ok: true,
          ...identity(),
          seedFingerprint: seeded.fingerprint,
          warm: true,
          databaseBytes: this.state.storage.sql.databaseSize,
        });
      }
      if (url.pathname !== "/run") return new Response("not found", { status: 404 });
      meter.reset(query);
      meter.active = true;
      meter.measurement = {
        ...identity(),
        targetEntryMs: Date.now(),
        eventAcquisitionsAtStart: meter.eventAcquisitions,
      };
      const runStartedMs = Date.now();

      await operations.turn(turn(query.sample, MEASURED_TOOLS));
      const runEndedMs = Date.now();

      meter.active = false;
      const metrics = meter.finish();

      if (
        metrics.calls.length !== 9 ||
        metrics.calls.some(
          (call) =>
            call.status !== 200 ||
            call.endMs === undefined ||
            call.error !== undefined ||
            !call.providerReceipt,
        )
      )
        throw new Error("Expected nine completed, successful native provider streams");

      const result = {
        ok: true,
        ...identity(),
        runStartedMs,
        runEndedMs,
        doWallMs: runEndedMs - runStartedMs,
        seedFingerprint: seeded.fingerprint,
        databaseBytes: this.state.storage.sql.databaseSize,
        ...metrics,
      };

      meter.measurement = result;

      // The complete receipt is returned to the controller. Avoid duplicating
      // its large per-call counters in sampled Cloudflare console logs.
      console.log({
        prodPath: "target-end",
        ...identity(),
        runStartedMs,
        runEndedMs,
        providerRequests: metrics.calls.map((call) => call.providerRequest),
        sql: metrics.sql,
      });

      return Response.json({ ok: true, runStartedMs, runEndedMs });
    } catch (cause) {
      meter.active = false;
      const error = errorText(cause);
      const metrics = meter.finish();

      const result = {
        ok: false,
        ...identity(),
        path: url.pathname,
        startedMs,
        endedMs: Date.now(),
        error,
        ...metrics,
      };

      console.error({ prodPath: "target-failure", ...result });

      return Response.json(result, { status: 500 });
    } finally {
      meter.active = false;
      this.busy = false;
    }
  }
}
