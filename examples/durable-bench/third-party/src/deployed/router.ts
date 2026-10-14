import { Schema } from "../../../node_modules/effect/dist/index.js";
import { COLD_ABORT } from "../../../deployed/worker/host.ts";
import { BulkFixture, Failure, Identity, ObjectIdentity, MetricsResult, measurementPaths, SeedBatch, TransientObjectTransport, errorText, readQuery, transientObjectTransport, type Env, type Query, type Target } from "../../../deployed/worker/protocol.ts";
import type { ActorDO } from "./tardie.ts";

const expectedAbort = async (abort: () => Promise<unknown>) => {
  try {
    const result = await abort();

    return (
      result instanceof Response &&
      result.status === 500 &&
      (await result.text()).includes(COLD_ABORT)
    );
  } catch (cause) {
    return errorText(cause).includes(COLD_ABORT);
  }
};

export const router = <E extends Env>(target: Target, routes: {
  stub: (env: E, query: Query) => Pick<DurableObjectStub, "fetch">;
  ready?: (env: E) => Promise<void>;
  handle?: (request: Request, env: E, query: Query) => Promise<Response | undefined>;
  actor?: (env: E, query: Query) => DurableObjectStub<ActorDO>;
}) => ({
  async fetch(request: Request, env: E): Promise<Response> {
    if (!env.BENCH_TOKEN || request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    try {
      if (url.pathname === "/health") {
        await routes.ready?.(env);

        return Response.json({ ok: true, build: env.BUILD });
      }
      const query = readQuery(url);
      if (query.target !== target) return new Response("target mismatch", { status: 400 });
      if (request.method === "GET" && url.searchParams.get("readiness") === "1" && measurementPaths(query.target).includes(url.pathname))
        return Response.json({ ok: true, build: env.BUILD });
      if (["/prime", "/seed", "/seed-progress", "/submit", "/await", "/result", "/run", "/text", "/metrics", "/profile-target"].includes(url.pathname) && query.expectedBuild !== env.BUILD)
        return Response.json(
          {
            ok: false,
            error: "Benchmark Worker build mismatch; request was not dispatched",
            sample: query.sample,
          },
          { status: 503 },
        );
      const mutating = url.pathname.startsWith("/restore/") || ["/prime", "/seed", "/cold", "/submit", "/await", "/result", "/run"].includes(url.pathname);

      if (url.pathname === "/profile-target") return routes.stub(env, query).fetch(request);

      if (mutating && request.method !== "POST")
        return new Response("POST required", { status: 405 });
      if (!url.pathname.startsWith("/restore/") && !["/identity", "/prime", "/seed", "/seed-progress", "/cold", "/submit", "/await", "/result", "/run", "/text", "/metrics"].includes(url.pathname))
        return new Response("not found", { status: 404 });

      const actor = routes.actor?.(env, query);
      const stub = routes.stub(env, query);

      const fetchObject = async (request: Request): Promise<Response> => {
        try {
          return await stub.fetch(request);
        } catch (cause) {
          throw transientObjectTransport(cause) ?? cause;
        }
      };

      const handled = await routes.handle?.(request, env, query);
      if (handled) return handled;

      if (url.pathname === "/identity") {
        const response = await fetchObject(new Request(url, { headers: request.headers }));

        if (!response.ok) throw new Error("Object identity probe failed");

        return Response.json(Schema.decodeUnknownSync(ObjectIdentity)({
          identity: await response.json(),
          ...(actor === undefined ? {} : { directory: await actor.identity() }),
        }));
      }

      if (url.pathname === "/cold") {
        const identityUrl = new URL(url);

        identityUrl.pathname = "/identity";
        const response = await fetchObject(new Request(identityUrl, { headers: request.headers }));

        if (!response.ok) throw new Error("Pre-cold identity failed");
        const before = Schema.decodeUnknownSync(Identity)(await response.json());
        const directoryBefore = actor === undefined ? undefined : await actor.identity();

        const threadAborted = await expectedAbort(() => fetchObject(request));
        const directoryAborted =
          actor === undefined ? undefined : await expectedAbort(() => actor.abortCold());

        // The driver next attaches its observer, then times submission on this new incarnation.
        return Response.json(
          {
            ok: threadAborted && directoryAborted !== false,
            before,
            directoryBefore,
            threadAborted,
            directoryAborted,
          },
          { status: threadAborted && directoryAborted !== false ? 200 : 502 },
        );
      }
      let directoryTables: Readonly<Record<string, number>> | undefined;

      if (url.pathname === "/restore/clear" && actor) await actor.clearFixture(query);
      if (url.pathname === "/restore/start" && actor) {
        const fixture = Schema.decodeUnknownSync(BulkFixture)(await request.clone().json());

        if (!fixture.actor || fixture.target !== "tardie")
          throw new Error("Missing Tardie Actor fixture");
        directoryTables = await actor.importFixture(fixture.actor, query);
      }
      if (url.pathname === "/seed" && actor) {
        const batch = Schema.decodeUnknownSync(SeedBatch)(await request.clone().json());

        if (batch.to <= batch.from || batch.to - batch.from > 50 || batch.to > query.history)
          throw new Error("Invalid history batch");
        if (batch.from === 0) await actor.prepareSeed(query);
      }
      const response = await fetchObject(request);

      if (url.pathname === "/restore/start" && directoryTables && response.ok)
        return Response.json({
          ...Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(
            await response.json(),
          ),
          directoryTables,
        });

      if (url.pathname === "/metrics" && actor && response.ok) {
        const metrics = Schema.decodeUnknownSync(MetricsResult)(
          await response.json(),
        );

        if (!metrics.ok) return Response.json(metrics);
        const directoryUsed = Schema.decodeUnknownSync(Schema.Boolean)(metrics.directoryUsed);

        const directoryBytes = await actor.bytes().catch((cause: unknown) => {
          throw transientObjectTransport(cause) ?? cause;
        });

        return Response.json({
          ...metrics,
          bytes: metrics.bytes + directoryBytes,
          directoryBytes,
          directory: await actor.end(directoryUsed).catch((cause: unknown) => {
            throw transientObjectTransport(cause) ?? cause;
          }),
        });
      }

      return response;
    } catch (cause) {
      const transport = Schema.is(TransientObjectTransport)(cause) ? cause : undefined;

      return Response.json(
        Schema.encodeSync(Failure)({
          ok: false,
          error: errorText(cause),
          sample: url.searchParams.get("sample"),
          ...(transport === undefined ? {} : { transport }),
        }),
        { status: 500 },
      );
    }
  },
});
