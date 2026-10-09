import { Config, Context, Effect, Layer, Schema } from "effect";
import { DurableObject } from "effect-cf";
import { HttpEffect } from "effect/http";
import { RpcSerialization, RpcServer } from "effect/rpc";

import { Call, Fault, Payload, Rpcs, mark, replies, type Meta } from "./model.ts";
import { wsConstructed, wsEvents, wsLayer, wsRpc, wsUpgrade } from "./websocket.ts";

const decode = Schema.decodeUnknownEffect(Call);
const encode = Schema.encodeEffect(Payload);
const decodeSync = Schema.decodeUnknownSync(Call);
const encodeSync = Schema.encodeSync(Payload);

class Fixture extends Context.Service<Fixture, { readonly build: string }>()(
  "rpc-overhead/Fixture",
) {
  static readonly layer = Layer.effect(
    Fixture,
    Effect.map(Config.String("BUILD"), (build) => ({ build })),
  );
}

const respond = Effect.fnUntraced(function* (call: Call) {
  const fixture = yield* Fixture;

  if (call.build !== fixture.build) return yield* new Fault({ message: "Object build mismatch" });

  return replies[call.meta.size];
});

const handlers = Rpcs.toLayer({ noop: respond });

class HttpEndpoints extends Context.Service<
  HttpEndpoints,
  {
    readonly json: (request: Request) => Promise<Response>;
    readonly ndjson: (request: Request) => Promise<Response>;
  }
>()("rpc-overhead/HttpEndpoints") {
  static readonly layer = Layer.effect(
    HttpEndpoints,
    Effect.gen(function* () {
      const json = yield* RpcServer.toHttpEffect(Rpcs).pipe(
        Effect.provideService(RpcSerialization.RpcSerialization, RpcSerialization.json),
      );

      const ndjson = yield* RpcServer.toHttpEffect(Rpcs).pipe(
        Effect.provideService(RpcSerialization.RpcSerialization, RpcSerialization.ndjson),
      );

      return { json: HttpEffect.toWebHandler(json), ndjson: HttpEffect.toWebHandler(ndjson) };
    }),
  ).pipe(Layer.provide(handlers));
}

const application = Layer.mergeAll(
  HttpEndpoints.layer.pipe(Layer.provideMerge(Fixture.layer)),
  wsLayer,
);

const Base = DurableObject.make(application, {
  ...wsEvents,
  rpc: {
    ...wsRpc,
    schemaRuntime: (raw: unknown) =>
      Effect.gen(function* () {
        const call = yield* decode(raw);

        mark("object", call.meta);

        return yield* encode(yield* respond(call));
      }),
  },
});

export class MicroDO extends Base {
  private readonly instance = crypto.randomUUID();
  private http: Promise<HttpEndpoints["Service"]> | undefined;
  private readonly build: string;

  constructor(ctx: globalThis.DurableObjectState, env: { BUILD: string }) {
    super(ctx, env);
    this.build = env.BUILD;
    wsConstructed(ctx, this.instance);
  }

  identity() {
    return { build: this.build, instance: this.instance };
  }

  private assert(call: Call) {
    if (call.build !== this.build) throw new Error("Object build mismatch");
    mark("object", call.meta);
  }

  native(call: Call) {
    this.assert(call);

    return replies[call.meta.size];
  }

  schemaSync(raw: unknown) {
    const call = decodeSync(raw);

    this.assert(call);

    return encodeSync(replies[call.meta.size]);
  }

  async reset(): Promise<void> {
    await this.ctx.storage.sync();
    this.ctx.abort("rpc-overhead controlled restart");
  }

  override async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname.replace(/\/$/, "");

    if (path === "/rpc")
      return this[DurableObject.RunSymbol](wsUpgrade(request), { event: "fetch" });
    if (path === "/noop") {
      const call: Call = await request.json();

      this.assert(call);

      return Response.json(replies[call.meta.size]);
    }
    if (path === "/json" || path === "/ndjson") {
      if (request.headers.get("x-bench-build") !== this.build)
        throw new Error("Object build mismatch");
      // Attribution is attached to the platform fetch invocation, outside the RPC server fiber.
      const meta: Meta = JSON.parse(request.headers.get("x-bench-meta") ?? "null");

      mark("object", meta);
      this.http ??= this[DurableObject.RunSymbol](HttpEndpoints);
      const http = await this.http;

      return (path === "/json" ? http.json : http.ndjson)(request);
    }

    return new Response("not found", { status: 404 });
  }
}
