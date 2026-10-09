import { Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { RpcClient, RpcSerialization } from "effect/rpc";

import { MicroDO } from "./micro.ts";
import {
  Batch,
  Call,
  Fault,
  Payload,
  PushOptions,
  Rpcs,
  mark,
  replies,
  type Meta,
  type Result,
} from "./model.ts";
import {
  BenchThreadDO,
  threadBatch,
  threadCpuDrain,
  threadCpuFinish,
  threadCpuPrepare,
  threadCpuSample,
} from "./thread.ts";
import { pushExperiment, unaryBatch } from "./websocket.ts";

export { BenchThreadDO, MicroDO };
interface Env {
  BENCH_TOKEN: string;
  BUILD: string;
  MICRO: DurableObjectNamespace<MicroDO>;
  THREADS: DurableObjectNamespace<BenchThreadDO>;
}

const encode = Schema.encodeEffect(Call);
const decode = Schema.decodeUnknownEffect(Payload);
const encodeSync = Schema.encodeSync(Call);
const decodeSync = Schema.decodeUnknownSync(Payload);

const io = <A>(work: () => Promise<A>) =>
  Effect.tryPromise({
    try: work,
    catch: () => new Fault({ message: "Invocation failed; no automatic retry" }),
  });

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (new URL(request.url).pathname === "/health")
      return Response.json({ ok: true, build: env.BUILD });
    try {
      const path = new URL(request.url).pathname;

      if (path === "/thread-cpu/prepare") {
        const batch = Schema.decodeUnknownSync(Batch)(await request.json());

        await guard(
          () =>
            env.THREADS.getByName(`rpc-overhead-${batch.object}`, {
              locationHint: "wnam",
            }),
          batch.build,
        );

        return Response.json(await threadCpuPrepare(env, batch));
      }
      if (path.startsWith("/thread-cpu/")) {
        const input = Schema.decodeUnknownSync(
          Schema.Struct({
            control: Schema.Unknown,
            index: Schema.optionalKey(Schema.Natural),
            receipt: Schema.optionalKey(Schema.Unknown),
          }),
        )(await request.json());

        if (path === "/thread-cpu/sample") {
          if (input.index === undefined) throw new Error("Missing sample index");

          return Response.json(await threadCpuSample(env, input.control, input.index));
        }
        if (path === "/thread-cpu/drain") {
          await threadCpuDrain(env, input.control, input.receipt);

          return Response.json({ ok: true });
        }
        if (path === "/thread-cpu/finish")
          return Response.json(await threadCpuFinish(env, input.control));
        throw new Error("Unknown CPU operation");
      }
      if (new URL(request.url).pathname === "/push") {
        const options = Schema.decodeUnknownSync(PushOptions)(await request.json());

        if (options.build !== env.BUILD) throw new Error("Driver build mismatch");

        const getStub = () =>
          env.MICRO.getByName(`rpc-overhead-push-${options.object}`, {
            locationHint: "wnam",
          });

        const { stub, before } = await guard(getStub, options.build);

        const restartForCleanup = async () => {
          try {
            await stub.reset();
          } catch {
            /* reset rejects by design; verify recreation on a fresh target */
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
          const replacement = await guard(getStub, options.build);

          if (replacement.before.instance === before.instance)
            throw new Error("Native cleanup did not restart the Object");

          return replacement.stub;
        };

        return Response.json(
          await Effect.runPromise(pushExperiment(stub, options, restartForCleanup)),
        );
      }
      const batch = Schema.decodeUnknownSync(Batch)(await request.json());

      if (batch.build !== env.BUILD) throw new Error("Driver build mismatch");
      if (batch.variant.startsWith("thread-")) {
        await guard(
          () =>
            env.THREADS.getByName(`rpc-overhead-${batch.object}`, {
              locationHint: "wnam",
            }),
          batch.build,
        );
        const result = await threadBatch(env, batch);

        driverMarker(batch);

        return Response.json({
          ok: true,
          batch,
          ...result,
          colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
        } satisfies Result);
      }

      const { stub, before } = await guard(
        () => env.MICRO.getByName(`rpc-overhead-${batch.object}`, { locationHint: "wnam" }),
        batch.build,
      );

      const setupStart = Date.now();

      const result =
        batch.variant === "effect-ws"
          ? await Effect.runPromise(unaryBatch(stub, batch))
          : await Effect.runPromise(
              Effect.gen(function* () {
                let currentMeta: Meta = { ...batch, phase: "warmup", index: -1 };

                const serialization =
                  batch.variant === "effect-ndjson"
                    ? RpcSerialization.layerNdjson
                    : RpcSerialization.layerJson;

                const protocol = RpcClient.layerProtocolHttp({
                  url: `https://rpc-overhead/${batch.variant === "effect-ndjson" ? "ndjson" : "json"}`,
                }).pipe(
                  Layer.provide(serialization),
                  Layer.provide(FetchHttpClient.layer),
                  Layer.provide(
                    Layer.succeed(FetchHttpClient.Fetch, (input, init) => {
                      const outgoing = new Request(input, init);

                      outgoing.headers.set("x-bench-build", batch.build);
                      outgoing.headers.set("x-bench-meta", JSON.stringify(currentMeta));

                      return stub.fetch(outgoing);
                    }),
                  ),
                );

                const rpc = batch.variant.startsWith("effect-")
                  ? yield* Effect.gen(function* () {
                      const context = yield* Layer.build(protocol);

                      return yield* RpcClient.make(Rpcs).pipe(Effect.provideContext(context));
                    })
                  : undefined;

                const setupMs = Date.now() - setupStart;
                const latencyMs: number[] = [];

                for (let i = -batch.warmup; i < batch.calls; i++) {
                  currentMeta = {
                    round: batch.round,
                    object: batch.object,
                    variant: batch.variant,
                    size: batch.size,
                    phase: i < 0 ? "warmup" : "measure",
                    index: i,
                  };

                  const call: Call = {
                    meta: currentMeta,
                    build: batch.build,
                    payload: replies[batch.size],
                  };

                  const start = Date.now();
                  let value: Payload;

                  switch (batch.variant) {
                    case "native":
                      value = yield* io(() => stub.native(call));
                      break;
                    case "fetch":
                      value = yield* io(async () => {
                        const response = await stub.fetch(
                          new Request("https://rpc-overhead/noop", {
                            method: "POST",
                            body: JSON.stringify(call),
                          }),
                        );

                        return response.json<Payload>();
                      });
                      break;
                    case "schema-sync":
                      value = yield* io(async () =>
                        decodeSync(await stub.schemaSync(encodeSync(call))),
                      );
                      break;
                    case "schema-runtime": {
                      const encoded = yield* encode(call);

                      value = yield* decode(yield* io(() => stub.schemaRuntime(encoded)));
                      break;
                    }
                    case "effect-json":
                    case "effect-ndjson": {
                      if (!rpc) return yield* new Fault({ message: "Missing RPC client" });
                      value = yield* rpc.noop(call);
                      break;
                    }
                    default:
                      return yield* new Fault({ message: "Variant not implemented" });
                  }
                  const elapsed = Date.now() - start;

                  if (value.version !== 1 || value.text !== replies[batch.size].text)
                    return yield* new Fault({ message: "Response mismatch" });
                  if (i >= 0) latencyMs.push(elapsed);
                }

                return {
                  latencyMs,
                  setupMs,
                  clientSetup: "request-scoped; excluded from per-call RTT",
                };
              }).pipe(Effect.scoped),
            );

      const after = await stub.identity();

      if (after.build !== batch.build || after.instance !== before.instance)
        throw new Error("Warm residency/build guard failed");
      driverMarker(batch);

      const response: Result = {
        ok: true,
        batch,
        ...result,
        sameInstance: true,
        colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
        clientSetup: result.clientSetup,
      };

      return Response.json(response);
    } catch (cause) {
      return Response.json(
        { ok: false, error: cause instanceof Error ? cause.message : "Benchmark failed" },
        { status: 500 },
      );
    }
  },
};

const guard = async <
  Stub extends {
    identity(): Promise<{ build: string; instance: string }>;
    reset(identity: { build: string; instance: string }): Promise<void>;
  },
>(
  getStub: () => Stub,
  build: string,
) => {
  let stub = getStub();
  let before = await stub.identity();

  // Worker and Object code propagate separately. Never time an unverified build.
  for (let attempt = 0; before.build !== build && attempt < 36; attempt++) {
    try {
      await stub.reset(before);
    } catch {
      /* reset rejects by design */
    }
    await new Promise((resolve) => setTimeout(resolve, 5000));
    // An aborted RPC session stays broken; setup probes must acquire a fresh target.
    stub = getStub();
    before = await stub.identity();
  }
  if (before.build !== build) throw new Error("Object build did not propagate");

  return { stub, before };
};

const driverMarker = (batch: Batch) =>
  mark(
    "driver",
    {
      round: batch.round,
      object: batch.object,
      variant: batch.variant,
      size: batch.size,
      phase: "measure",
      index: 0,
    },
    batch.calls,
    batch.warmup,
  );
