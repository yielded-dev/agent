import { BrowserCrypto } from "../../../node_modules/@effect/platform-browser/dist/index.js";
import { ThreadObjectNamespace } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { Settlement } from "@yielded/agent/submission-ledger";
// The router uses the checkout's Effect; Tardie's adapter uses its isolated SDK version.
import { Effect, Layer, ManagedRuntime, Schema } from "../../../node_modules/effect/dist/index.js";

import { history, MEASURED_TOOLS, turn } from "../../../src/plan.ts";
import { COLD_ABORT } from "../../../deployed/worker/host.ts";
import { PiDO } from "./pi.ts";
import { BulkFixture, Identity, errorText, readQuery, type Env, type Query } from "../../../deployed/worker/protocol.ts";
import { FixtureError } from "../../../deployed/worker/storage.ts";
import { ActorDO, ThreadDO, cloudflareThreadName, coordinate } from "./tardie.ts";
import { agent, definitions, YieldedDO } from "../../../deployed/worker/yielded.ts";

export { YieldedDO, PiDO, ActorDO, ThreadDO };
type Bindings = Env & {
  YIELDED: DurableObjectNamespace<YieldedDO>;
  PI: DurableObjectNamespace<PiDO>;
  ACTORS: DurableObjectNamespace<ActorDO>;
  THREADS: DurableObjectNamespace<ThreadDO>;
};

const clients = new WeakMap<Bindings, ReturnType<typeof makeClient>>();

const makeClient = (env: Bindings) => {
  const queries = new Map<string, Query>();

  const namespace = Layer.succeed(ThreadObjectNamespace, {
    get: (threadId) => {
      const stub = env.YIELDED.getByName(threadId, { locationHint: "wnam" });

      return new Proxy(stub, {
        get(target, method) {
          if (method === "submitEncoded")
            return (encoded: unknown) => {
              const query = queries.get(threadId);

              if (!query) throw new Error("Missing submission query");

              return target.benchSubmit(encoded, query);
            };
          const value = Reflect.get(target, method, target);

          return typeof value === "function"
            ? (...args: unknown[]) => Reflect.apply(value, target, args)
            : value;
        },
      });
    },
  });

  const runtime = ManagedRuntime.make(
    CloudflareThreadClient.layer.pipe(
      Layer.provide(namespace),
      Layer.provideMerge(BrowserCrypto.layer),
    ),
  );

  // Worker-only acquisition: obtaining the service and hashing definitions invoke no Object.
  const ready = runtime.runPromise(
    Effect.gen(function* () {
      return {
        client: yield* CloudflareThreadClient,
        digests: yield* digestDefinitions(definitions),
      };
    }),
  );

  return { runtime, queries, ready };
};

const getClient = (env: Bindings) => {
  const cached = clients.get(env);

  if (cached) return cached;
  const client = makeClient(env);

  clients.set(env, client);

  return client;
};

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

export default {
  async fetch(request: Request, env: Bindings): Promise<Response> {
    if (!env.BENCH_TOKEN || request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    try {
      if (url.pathname === "/health") {
        await getClient(env).ready;

        return Response.json({ ok: true, build: env.BUILD });
      }
      const query = readQuery(url);
      if (["/import", "/submit", "/await", "/run"].includes(url.pathname) && query.expectedBuild !== env.BUILD)
        return Response.json(
          {
            ok: false,
            error: "Benchmark Worker build mismatch; request was not dispatched",
            sample: query.sample,
          },
          { status: 503 },
        );
      const mutating = ["/import", "/cold", "/submit", "/await", "/run"].includes(url.pathname);

      if (mutating && request.method !== "POST")
        return new Response("POST required", { status: 405 });
      if (!["/import", "/cold", "/submit", "/await", "/run", "/metrics"].includes(url.pathname))
        return new Response("not found", { status: 404 });

      const actor =
        query.target === "tardie"
          ? env.ACTORS.getByName(query.object, { locationHint: "wnam" })
          : undefined;

      const stub =
        query.target === "yielded"
          ? env.YIELDED.getByName(query.object, { locationHint: "wnam" })
          : query.target === "pi"
            ? env.PI.getByName(query.object, { locationHint: "wnam" })
            : env.THREADS.getByName(cloudflareThreadName(coordinate(query.object)), {
                locationHint: "wnam",
              });

      if (url.pathname === "/cold") {
        const identityUrl = new URL(url);

        identityUrl.pathname = "/identity";
        const response = await stub.fetch(new Request(identityUrl, { headers: request.headers }));

        if (!response.ok) throw new Error("Pre-cold identity failed");
        const before = Schema.decodeUnknownSync(Identity)(await response.json());
        const directoryBefore = actor === undefined ? undefined : await actor.identity();

        const directoryAborted =
          actor === undefined ? undefined : await expectedAbort(() => actor.abortCold());

        const threadAborted = await expectedAbort(() => stub.fetch(request));

        // No Object invocation follows these aborts. The driver starts the timer next.
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
      if (url.pathname === "/submit" || url.pathname === "/await") {
        if (query.target !== "yielded") throw new Error("Only Yielded uses native receipts");
        const receiptWire = url.pathname === "/await" ? await request.json() : undefined;

        const cached = getClient(env);
        const { client, digests } = await cached.ready;
        const submitting = url.pathname === "/submit";

        if (submitting && cached.queries.has(query.object))
          throw new Error("Concurrent submission to one benchmark Object");
        if (submitting) cached.queries.set(query.object, query);
        try {
          const result = await cached.runtime.runPromise(
            Effect.gen(function* () {
              if (submitting) {
                const input = turn(query.sample, MEASURED_TOOLS);

                const receipt = yield* client.submit(agent, input.text, {
                  threadId: ThreadId.make(query.object),
                  principal: Principal.make("bench"),
                  idempotencyKey: IdempotencyKey.make(input.id),
                  definitions: digests,
                });

                return { ok: true, receipt: yield* Schema.encodeEffect(Receipt)(receipt) };
              }
              const receipt = yield* Schema.decodeUnknownEffect(Receipt)(receiptWire);

              if (receipt.threadId !== query.object)
                return yield* Effect.die("Receipt belongs to another Object");
              const settlement = yield* client.awaitSettlement(receipt);

              return { ok: true, settlement: yield* Schema.encodeEffect(Settlement)(settlement) };
            }),
          );

          return Response.json(result);
        } finally {
          if (submitting) cached.queries.delete(query.object);
        }
      }

      if (url.pathname === "/run" && query.target === "yielded")
        throw new Error("Yielded requires submit followed by await");
      let directoryTables: Readonly<Record<string, number>> | undefined;

      if (url.pathname === "/import" && query.target === "yielded") {
        const fixture = Schema.decodeUnknownSync(BulkFixture)(await request.clone().json());

        if (fixture.target !== query.target || fixture.history !== query.history)
          throw new Error("Fixture/query mismatch");
        if (fixture.mode === "replay") {
          if (!fixture.fallbackReason) throw new Error("Replay requires a fallback reason");
          const cached = getClient(env);
          const { client, digests } = await cached.ready;

          if (cached.queries.has(query.object))
            throw new Error("Concurrent submission to one benchmark Object");
          cached.queries.set(query.object, query);
          try {
            await env.YIELDED.getByName(query.object, { locationHint: "wnam" }).beginReplay(query);
            await cached.runtime.runPromise(
              Effect.gen(function* () {
                for (const input of history(0, fixture.history)) {
                  cached.queries.set(query.object, {
                    ...query,
                    sample: input.id,
                    ttftMs: 0,
                    chunkDelayMs: 0,
                  });

                  const receipt = yield* client.submit(agent, input.text, {
                    threadId: ThreadId.make(query.object),
                    principal: Principal.make("bench"),
                    idempotencyKey: IdempotencyKey.make(input.id),
                    definitions: digests,
                  });

                  const settlement = yield* client.awaitSettlement(receipt);

                  if (settlement.outcome !== "completed")
                    return yield* new FixtureError({
                      message: `Seed ${input.id} did not complete; initialization will not be retried`,
                    });
                }
              }),
            );
          } finally {
            cached.queries.delete(query.object);
          }
        }
      }
      if (url.pathname === "/import" && actor) {
        const fixture = Schema.decodeUnknownSync(BulkFixture)(await request.clone().json());

        if (!fixture.actor || fixture.target !== "tardie")
          throw new Error("Missing Tardie Actor fixture");
        directoryTables = await actor.importFixture(fixture.actor, query);
      }
      const response = await stub.fetch(request);

      if (url.pathname === "/import" && directoryTables && response.ok)
        return Response.json({
          ...Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(
            await response.json(),
          ),
          directoryTables,
        });

      if (url.pathname === "/metrics" && actor && response.ok)
        return Response.json({
          ...Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(
            await response.json(),
          ),
          directory: await actor.end(),
        });

      return response;
    } catch (cause) {
      return Response.json(
        { ok: false, error: errorText(cause), sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
