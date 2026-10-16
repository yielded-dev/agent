import { exportThread } from "@yielded/agent-storage-cloudflare/do-thread-store";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { CYCLE, history, MEASURED_TOOLS, turn } from "../../src/plan.ts";
import { tables } from "../../src/serve.ts";
import { attach, type Observation } from "./observe.ts";
import {
  type MaintenanceStatus,
  errorText,
  Failure,
  readQuery,
  ProfileTarget,
  SeedBatch,
  SeedProgress,
  SeedResult,
  TransientObjectTransport,
  transientObjectTransport,
  type Env,
  type Query,
  type Target,
  type SqlDump,
} from "./protocol.ts";
import { restore } from "./restore.ts";
import { FixtureError, FixtureExport, importRows, RestoreStorage } from "./storage.ts";

export const COLD_ABORT = "durable-bench explicit cold";

export class Host {
  static async importTardie(storage: DurableObjectStorage, dump: SqlDump, object: string) {
    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Tardie import RPC rejects an identity change before any storage mutation.
    if (object !== "main") throw new Error("Snapshot identity must remain main");
    importRows(storage, dump);
    const actual = tables(storage.sql);

    for (const table of dump.tables)
      if (actual[table.name] !== table.rows.length)
        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Tardie import RPC rejects mismatched counts instead of acknowledging an invalid fixture.
        throw new Error(`Tardie ${table.name} import count mismatch`);
    if (Object.keys(actual).length !== dump.tables.length)
      // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Tardie import RPC rejects an unexpected table inventory before acknowledging the fixture.
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
      run?: (input: { id: string; text: string }, query: Query) => Promise<void>;
      close?: () => Promise<void>;
      maintenance?: () => Promise<typeof MaintenanceStatus.Type>;
      restored?: () => Promise<void>;
      directoryUsed?: boolean;
    },
  ): Promise<Response> {
    const url = new URL(request.url);
    const query = readQuery(url);

    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Object fetch entrypoint rejects routing to the wrong benchmark target.
    if (query.target !== this.target) throw new Error("Target mismatch");
    if (url.pathname === "/identity") return Response.json(this.meter.identity());
    if (url.pathname === "/maintenance-status") {
      await handlers.close?.();

      return Response.json(
        handlers.maintenance
          ? await handlers.maintenance()
          : { logical: null, native: await this.ctx.storage.getAlarm() },
      );
    }
    if (url.pathname === "/profile-target") {
      this.meter.assertBuild(query);

      return Response.json(
        Schema.decodeUnknownSync(ProfileTarget)({
          actorId: this.ctx.id.toString(),
          binding:
            this.target === "yielded"
              ? "YIELDED"
              : this.target === "pi"
                ? "PI"
                : this.target === "effect-ai-floor"
                  ? "FLOOR"
                  : "THREADS",
          versionId: this.env.VERSION?.id,
          build: this.env.BUILD,
        }),
      );
    }
    if (url.pathname === "/cold") {
      await handlers.close?.();
      await this.ctx.storage.sync();
      this.ctx.abort(COLD_ABORT);
    }
    let classifyRunFailure = false;

    try {
      if (url.pathname.startsWith("/restore/")) {
        this.meter.assertBuild(query);

        if (url.pathname === "/restore/clear") {
          // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Object fetch handler maps this rejection to its existing 500 response before destructive reset.
          if (request.method !== "POST") throw new Error("Restore reset requires POST");
          await handlers.close?.();
          await this.ctx.storage.deleteAll();
          await this.ctx.storage.sync();

          return Response.json({ ok: true, identity: this.meter.identity() });
        }

        const restored = await Effect.runPromise(
          restore(request, query).pipe(
            Effect.provideService(RestoreStorage, this.ctx.storage),
            Effect.provideService(FixtureExport, {
              open: Effect.fail(
                FixtureError.make({
                  message: "Yielded restore requires its native restore entrypoint",
                }),
              ),
              page: (input) => exportThread({ storage: this.ctx.storage }, input),
            }),
            Effect.scoped,
          ),
        );

        if (url.pathname === "/restore/finalize") await handlers.restored?.();

        return Response.json({ ...restored, identity: this.meter.identity() });
      }
      if (url.pathname === "/seed-progress") {
        this.meter.assertBuild(query);
        const stored = await this.ctx.storage.get("durable-bench/history");

        const progress =
          stored === undefined
            ? { completed: 0, inFlight: false }
            : Schema.decodeUnknownSync(SeedProgress)(stored);

        return Response.json({ ...progress, identity: this.meter.identity() });
      }
      if (url.pathname === "/prime") {
        this.meter.assertBuild(query);

        // Real network I/O advances the clock; timer callbacks can remain clamped.
        // Keep the probe's latency instead of estimating/subtracting invisible CPU.
        const probe = await fetch(new URL("/health", this.env.PROVIDER_URL), {
          headers: { authorization: `Bearer ${this.env.BENCH_TOKEN}`, "cache-control": "no-store" },
          signal: AbortSignal.timeout(30_000),
        });

        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Object fetch handler rejects an unsuccessful constructor clock probe instead of returning timing evidence.
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
        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Object fetch handler rejects unsupported history building before admitting a batch.
        if (!handlers.run) throw new Error("Target has no native history builder");
        const { from, to, startedMs } = Schema.decodeUnknownSync(SeedBatch)(await request.json());

        if (to <= from || to - from > 50 || to > query.history)
          // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Object fetch handler rejects invalid batch bounds before entering the admission gate.
          throw new Error("History batches must contain 1–50 turns within the requested history");
        const key = "durable-bench/history";

        await this.ctx.blockConcurrencyWhile(async () => {
          const stored = await this.ctx.storage.get(key);

          const progress =
            stored === undefined
              ? { completed: 0, inFlight: false }
              : Schema.decodeUnknownSync(SeedProgress)(stored);

          if (progress.inFlight || progress.completed !== from)
            // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Cloudflare blockConcurrencyWhile must reject to close the admission gate on an uncertain or repeated batch.
            throw new Error(
              "History batch is repeated, out of order, or has an uncertain predecessor",
            );
          if (progress.batch && progress.batch.startedMs !== startedMs)
            // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Reject this native blockConcurrencyWhile callback before it mutates the persisted batch clock.
            throw new Error("History build clock changed");
          await this.ctx.storage.put(key, { ...progress, inFlight: true });
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
          this.meter.validateCalls();
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
            // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native history HTTP handler rejects incomplete provider evidence and leaves the batch marked in flight.
            throw new Error(`History ${input.id} did not complete its scripted model calls`);
          for (const call of this.meter.calls)
            if (call.receipt?.colo) providerColos.add(call.receipt.colo);
        }
        const fingerprint = this.meter.calls.at(-1)?.fingerprint;

        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native history HTTP handler rejects missing evidence before persisting a completed batch.
        if (!fingerprint) throw new Error("History batch has no final model fingerprint");

        const batch = SeedResult.make({
          ok: true,
          from,
          to,
          startedMs,
          fingerprint,
          identity: this.meter.identity(),
          providerColos: [...providerColos],
        });

        await this.ctx.storage.put(key, { completed: to, inFlight: false, batch });
        await this.ctx.storage.sync();

        return Response.json(batch);
      }
      if (url.pathname === "/run") {
        // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native Object fetch handler rejects this unsupported RPC path before invoking a target.
        if (!handlers.run) throw new Error("Yielded uses submit and await");
        this.meter.begin(query);
        this.meter.marker("run");
        classifyRunFailure = true;
        await handlers.run(turn(query.sample, MEASURED_TOOLS), query);
        classifyRunFailure = false;
        this.meter.settled();

        return Response.json({ ok: true, outcome: "completed", identity: this.meter.entry });
      }
      if (url.pathname === "/metrics") {
        if (!this.meter.query || this.meter.query.sample !== query.sample)
          return Response.json({
            ok: false,
            reason: this.meter.query ? "sample-mismatch" : "incarnation-lost",
            identity: this.meter.identity(),
            requestedSample: query.sample,
            ...(this.meter.query ? { observedSample: this.meter.query.sample } : {}),
          });

        if (url.searchParams.get("quiescent") === "true") {
          await handlers.close?.();
          await this.ctx.storage.sync();
        }

        this.meter.validateCalls();

        return Response.json({
          ok: true,
          query: this.meter.query,
          identity: this.meter.entry,
          ...(handlers.directoryUsed === undefined
            ? {}
            : { directoryUsed: handlers.directoryUsed }),
          calls: this.meter.calls,
          phases: this.meter.phases(),
          cpuInvocationIds: this.meter.cpuInvocations?.get(query.sample),
          tables: tables(this.ctx.storage.sql),
          bytes: this.ctx.storage.sql.databaseSize,
        });
      }

      return new Response("not found", { status: 404 });
    } catch (cause) {
      const transport = Schema.is(TransientObjectTransport)(cause)
        ? cause
        : classifyRunFailure
          ? transientObjectTransport(cause)
          : undefined;

      return Response.json(
        Schema.encodeSync(Failure)({
          ok: false,
          error: errorText(transport ?? cause),
          sample: query.sample,
          ...(transport === undefined ? {} : { transport }),
        }),
        { status: 500 },
      );
    }
  }
}
