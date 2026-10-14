import { BrowserCrypto } from "../../../node_modules/@effect/platform-browser/dist/index.js";
import { ThreadObjectNamespace } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient, ThreadClientError } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { Settlement } from "@yielded/agent/submission-ledger";
// The build resolver selects the ref's Effect; Tardie's adapter stays isolated.
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "../../../node_modules/effect/dist/index.js";

import { MEASURED_TOOLS, turn } from "../../../src/plan.ts";
import { agent, definitions, YieldedDO } from "../../../deployed/worker/yielded.ts";
import { TextObservation, transientObjectTransport, type Env, type Query } from "../../../deployed/worker/protocol.ts";
import { router } from "./router.ts";
export { YieldedDO };
type Bindings = Env & { YIELDED: DurableObjectNamespace<YieldedDO> };
const clients = new WeakMap<Bindings, ReturnType<typeof makeClient>>();

const makeClient = (env: Bindings) => {
  const queries = new Map<string, Query>();
  const observations = env.CPU === true || env.CPU === "true" ? new Map<string, Query>() : undefined;

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
          if (method === "watchTextEncoded" && observations !== undefined)
            return (encoded: unknown) => {
              const query = observations.get(threadId);

              if (!query) throw new Error("Missing observation query");

              return target.benchWatchText(encoded, query);
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

  return { runtime, queries, observations, ready };
};

const getClient = (env: Bindings) => {
  const cached = clients.get(env);

  if (cached) return cached;
  const client = makeClient(env);

  clients.set(env, client);

  return client;
};

export default router<Bindings>("yielded", {
  stub: (env, query) => env.YIELDED.getByName(query.storageObject ?? query.object, { locationHint: "wnam" }),
  ready: async (env) => { await getClient(env).ready; },
  handle: async (request, env, query) => {
    const url = new URL(request.url);
      if (url.pathname === "/text" && query.target === "yielded") {
        const cached = getClient(env);
        cached.observations?.set(query.storageObject ?? query.object, query);
        const { client } = await cached.ready;
        const encoder = new TextEncoder();
        const body = await cached.runtime.runPromise(client.watchText(ThreadId.make((query.storageObject ?? query.object))).pipe(
          Stream.map((frame): readonly TextObservation[] => {
            if (frame._tag === "Reset") return [{ _tag: "Ready" }];
            const event = frame.event;

            return event._tag === "Text" && event.part.type === "text-delta" && event.part.delta
              ? [{ _tag: "Text", input: event.submissionId, text: event.part.delta }]
              : [];
          }),
          Stream.flatMap(Stream.fromArray),
          Stream.map((frame) => encoder.encode(JSON.stringify(frame) + "\n")),
          Stream.toReadableStreamEffect,
        ));

        return new Response(body, {
          headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
        });
      }

      if (url.pathname === "/submit" || url.pathname === "/await" || url.pathname === "/result") {
        if (query.target !== "yielded") throw new Error("Only Yielded uses native receipts");
        const receiptWire = url.pathname === "/submit" ? undefined : await request.json();

        const cached = getClient(env);
        const { client, digests } = await cached.ready;
        const submitting = url.pathname === "/submit";

        if (submitting && cached.queries.has((query.storageObject ?? query.object)))
          throw new Error("Concurrent submission to one benchmark Object");
        if (submitting) cached.queries.set((query.storageObject ?? query.object), query);
        try {
          const result = await cached.runtime.runPromise(
            Effect.gen(function* () {
              if (submitting) {
                const input = turn(query.sample, MEASURED_TOOLS);

                const receipt = yield* client.submit(agent, input.text, {
                  threadId: ThreadId.make((query.storageObject ?? query.object)),
                  principal: Principal.make("bench"),
                  idempotencyKey: IdempotencyKey.make(input.id),
                  definitions: digests,
                });

                return { ok: true, receipt: yield* Schema.encodeEffect(Receipt)(receipt) };
              }
              const receipt = yield* Schema.decodeUnknownEffect(Receipt)(receiptWire);

              if (receipt.threadId !== (query.storageObject ?? query.object))
                return yield* Effect.die("Receipt belongs to another Object");
              if (url.pathname === "/result") {
                const record = yield* client.awaitSettlementRecord(receipt);

                if (record.outcome !== "completed")
                  return yield* Effect.die("Expected a completed assistant reply");

                return { ok: true, text: yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(record.result) };
              }
              const settlement = yield* client.awaitSettlement(receipt);

              return { ok: true, settlement: yield* Schema.encodeEffect(Settlement)(settlement) };
            }).pipe(
              Effect.mapError((error) =>
                Schema.is(ThreadClientError)(error)
                  ? transientObjectTransport(error.cause) ?? error
                  : error,
              ),
            ),
          );

          return Response.json(result);
        } finally {
          if (submitting) cached.queries.delete((query.storageObject ?? query.object));
        }
      }

    return undefined;
  },
});
