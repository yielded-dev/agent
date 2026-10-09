import { BrowserCrypto } from "@effect/platform-browser";
import { ThreadObjectNamespace } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { Settlement } from "@yielded/agent/submission-ledger";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Schema from "effect/Schema";

import { history, MEASURED_TOOLS, turn } from "../../src/plan.ts";
import { COLD_ABORT } from "../worker/host.ts";
import { FixtureError } from "../worker/storage.ts";
import { agent, definitions, YieldedDO } from "../worker/yielded.ts";
import { buildMismatch, observeFetch } from "./observation.ts";
import {
  ReplayChunk,
  type Phase,
  Identity,
  errorText,
  readQuery,
  type Env,
  type Query,
  type IsolateState,
} from "./protocol.ts";

export { YieldedDO };

type Bindings = Env & { YIELDED: DurableObjectNamespace<YieldedDO> };

const clients = new WeakMap<Bindings, ReturnType<typeof makeClient>>();

const makeClient = (env: Bindings) => {
  const queries = new Map<string, { query: Query; workerIsolate: IsolateState }>();

  const namespace = Layer.succeed(ThreadObjectNamespace, {
    get: (threadId) => {
      const stub = env.YIELDED.getByName(threadId, { locationHint: "wnam" });

      return new Proxy(stub, {
        get(target, method) {
          if (method === "submitEncoded")
            return (encoded: unknown) => {
              const entry = queries.get(threadId);

              if (!entry) throw new Error("Missing submission query");

              return target.benchSubmit(encoded, entry.query, entry.workerIsolate);
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

  // Acquisition and definition hashing happen only on the routing Worker's request path.
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
    const routing: Phase[] = [{ phase: "router.entry", atMs: Date.now() }];
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

      if (query.target !== "yielded") throw new Error("Target mismatch");

      const mutating = [
        "/empty",
        "/seed",
        "/import",
        "/cold",
        "/submit",
        "/await",
        "/run",
        "/storage",
      ].includes(url.pathname);

      if (mutating && request.method !== "POST")
        return new Response("POST required", { status: 405 });
      if (
        ![
          "/empty",
          "/seed",
          "/import",
          "/cold",
          "/submit",
          "/await",
          "/run",
          "/metrics",
          "/identity",
          "/profile-id",
          "/sentinel",
          "/storage",
        ].includes(url.pathname)
      )
        return new Response("not found", { status: 404 });

      const stub = env.YIELDED.getByName(query.object, { locationHint: "wnam" });

      if (url.pathname === "/cold") {
        const identityUrl = new URL(url);

        identityUrl.pathname = "/identity";
        const response = await stub.fetch(new Request(identityUrl, { headers: request.headers }));

        if (!response.ok) throw new Error("Pre-cold identity failed");
        const before = Schema.decodeUnknownSync(Identity)(await response.json());
        const threadAborted = await expectedAbort(() => stub.fetch(request));

        return Response.json(
          { ok: threadAborted, before, threadAborted },
          { status: threadAborted ? 200 : 502 },
        );
      }
      if (url.pathname === "/submit" || url.pathname === "/await") {
        const receiptWire = url.pathname === "/await" ? await request.json() : undefined;

        routing.push({ phase: "router.client.start", atMs: Date.now() });
        const cached = getClient(env);
        const { client, digests } = await cached.ready;

        routing.push({ phase: "router.client.ready", atMs: Date.now() });
        const submitting = url.pathname === "/submit";

        if (submitting && cached.queries.has(query.object))
          throw new Error("Concurrent submission to one benchmark Object");
        if (submitting) cached.queries.set(query.object, { query, workerIsolate: ingress });
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

          routing.push({ phase: "router.return", atMs: Date.now() });

          return Response.json({ ...result, routing });
        } finally {
          if (submitting) cached.queries.delete(query.object);
        }
      }
      if (url.pathname === "/run") throw new Error("Yielded requires submit followed by await");

      if (url.pathname === "/seed") {
        const { from, to } = Schema.decodeUnknownSync(ReplayChunk)(await request.json());

        if (to <= from || to - from > 25 || to > query.history)
          throw new Error("Invalid seed range");
        const cached = getClient(env);
        const { client, digests } = await cached.ready;

        if (cached.queries.has(query.object)) throw new Error("Concurrent seed");
        cached.queries.set(query.object, { query, workerIsolate: ingress });
        try {
          if (from === 0) await stub.beginReplay();
          await cached.runtime.runPromise(
            Effect.gen(function* () {
              for (const input of history(from, to)) {
                cached.queries.set(query.object, {
                  query: { ...query, sample: input.id, ttftMs: 0, chunkDelayMs: 0 },
                  workerIsolate: ingress,
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

          return Response.json({ ok: true, next: to });
        } finally {
          cached.queries.delete(query.object);
        }
      }

      return await stub.fetch(request);
    } catch (cause) {
      return Response.json(
        { ok: false, error: errorText(cause), sample: url.searchParams.get("sample") },
        { status: 500 },
      );
    }
  },
};
