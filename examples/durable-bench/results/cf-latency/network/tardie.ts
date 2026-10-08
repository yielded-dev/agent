import { Effect, Layer, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { Rpc } from "effect/unstable/rpc";
import {
  actorContext,
  agentMethods,
  compact,
  infer,
  messages,
  tools,
  type ContextView,
} from "tardie/agent";
import {
  Model,
  ModelLock,
  liveModelServices,
  modelActs,
  modelInfo,
  toolActs,
} from "tardie/agent/services";
import { atom, defineActor, type Atom, type ThreadCoordinate } from "tardie/core";
import { defineLibrary, MethodDescription, MethodHints } from "tardie/libraries";
import { modelLockService } from "tardie/model/lock";
import { providerLayer } from "tardie/model/providers/openai-compat";
import { cloudflareThreadName, createActorWorker } from "tardie/platform/cloudflare";

import { next, payload, type Turn } from "../../../src/plan.ts";
import { Host } from "./host.ts";
import { instrument, observation } from "./observe.ts";
import { type Env, type Query } from "./protocol.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";
const MODEL = { provider: "cf-latency", model_id: "cf-latency-1" };

export const coordinate = (object: string) => ({
  actor: "cf-latency-agent",
  instance: object,
  thread: "bench",
});

const kv = defineLibrary({
  name: "kv",
  description: "Record lookup",
  toolNames: { lookup: "lookup" },
  methods: [
    Rpc.make("lookup", { payload: Schema.Struct({ n: Schema.Finite }), success: Schema.String })
      .annotate(MethodDescription, "Look up record number n")
      .annotate(MethodHints, { readOnlyHint: true, openWorldHint: false }),
  ],
});

const actor = defineActor(
  "cf-latency-agent",
  Effect.gen(function* () {
    const toolView = yield* tools([kv]);
    // Tardie 0.44.0 loses compact() generics through NativeAtom.withLabel.
    // Its implementation returns this ContextView; no stored value is cast.
    const context = (yield* compact(messages)) as Atom<ContextView>;

    const agent = yield* infer(
      atom((get) => ({ system: SYSTEM, tools: get(toolView), context: get(context) })),
    );

    return { atom: agent, methods: agentMethods };
  }),
);

function services(env: Env, storage: DurableObjectStorage) {
  const meter = observation(storage);
  const args = Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Int }));

  const seed = Layer.succeed(Model, {
    call: (input) =>
      Effect.sync(() => {
        meter.seen = input.context.map((m) => ({
          role: m.role,
          text:
            m.role === "tool"
              ? Schema.decodeUnknownSync(Schema.String)(JSON.parse(m.text))
              : "text" in m
                ? m.text
                : m.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join(""),
          ...(m.role === "assistant" && m.toolCalls.length
            ? { calls: m.toolCalls.map((c) => args(c.input).n) }
            : {}),
        }));
        const step = next(meter.seen);
        const usage = { input: 0, output: 0, usd: null };

        return "call" in step
          ? {
              text: "",
              toolCalls: [{ callId: `call-${step.call}`, name: "lookup", input: { n: step.call } }],
              usage,
            }
          : { text: step.answer, toolCalls: [], usage };
      }),
  });

  const lock = Layer.succeed(
    ModelLock,
    modelLockService(
      {
        schema: 2,
        providers: {
          "cf-latency": {
            protocol: "openai-chat-completions",
            baseUrl: env.PROVIDER_URL,
            env: ["CF_LATENCY_TOKEN"],
          },
        },
        models: [{ ...MODEL, contextWindowTokens: 1e9 }],
      },
      { default: MODEL, allow: "*" },
    ),
  );

  const native = liveModelServices({
    credentials: { CF_LATENCY_TOKEN: env.TOKEN },
    // Tardie builds a fresh native model and default HttpClient for each call.
    // Scope Fetch on the executed HTTP effect via the adapter's client hook;
    // binding it only while constructing a nested Layer does not reach dispatch.
    providerLayer: (options) => {
      if (options.provider !== "openai-compat") throw new Error("Expected native compat provider");
      return providerLayer({
        ...options,
        client: {
          ...options.client,
          transformClient: (http) =>
            HttpClient.transformResponse(options.client.transformClient?.(http) ?? http, (response) =>
              Effect.provideService(response, FetchHttpClient.Fetch, meter.fetch(env)),
            ),
        },
      });
    },
    configure: () => ({ retry: { backoffMs: [], maxRetryAfterMs: 0, retryAfterJitterMs: 0 } }),
  }).pipe(Layer.provide(lock));

  return Layer.mergeAll(
    modelInfo,
    modelActs,
    toolActs([kv.implement({ lookup: ({ n }) => Effect.succeed(payload(n)) })]),
  ).pipe(Layer.provide(Layer.merge(env.PHASE === "seed" ? seed : native, lock)));
}

export const actorWorker = createActorWorker({
  actor,
  actorContext,
  services: (env: Env, _coordinate, _runtime, storage) => services(env, storage),
});

type Reference = {
  wait: Effect.Effect<void, Error>;
  methods: {
    message: (input: { text: string }, request: { id: string }) => Effect.Effect<unknown, Error>;
  };
};
type Internals = {
  identityReady: Promise<void>;
  address(): ThreadCoordinate;
  actors(): { reference(c: ThreadCoordinate): Effect.Effect<Reference, Error> };
};

// Tardie's provision/lookup paths call getByName internally as well as at ingress.
function withLocation<Bindings extends Env & { ACTORS: object; THREADS: object }>(
  env: Bindings,
): Bindings {
  return new Proxy(env, {
    get(target, key) {
      if (key !== "ACTORS" && key !== "THREADS") return Reflect.get(target, key, target);
      const binding = key === "ACTORS" ? target.ACTORS : target.THREADS;

      return new Proxy(binding, {
        get(namespace, method) {
          const member = Reflect.get(namespace, method, namespace);

          if (method === "getByName")
            return (name: string) => member.call(namespace, name, { locationHint: "wnam" });

          return typeof member === "function" ? member.bind(namespace) : member;
        },
      });
    },
  });
}

export class NetworkActorDO extends actorWorker.ActorObject {
  constructor(
    ctx: DurableObjectState,
    env: ConstructorParameters<typeof actorWorker.ActorObject>[1],
  ) {
    super(instrument(ctx, env), withLocation(env));
  }
  override async alarm(
    ...args: Parameters<InstanceType<typeof actorWorker.ActorObject>["alarm"]>
  ): Promise<void> {
    const meter = observation(this.ctx.storage);
    const alarmId = crypto.randomUUID();
    const log = (edge: "start" | "end") => meter.alarmEvent(alarmId, edge, this.ctx);

    log("start");
    try {
      await super.alarm(...args);
    } finally {
      log("end");
    }
  }
  identity() {
    return observation(this.ctx.storage).identity(this.ctx, this.env);
  }
  async abortCold(query: Query): Promise<void> {
    await this.ctx.storage.sync();
    console.log({
      cfLatency: "cold-requested",
      scope: "actor",
      ...query,
      ...observation(this.ctx.storage).identity(this.ctx, this.env),
    });
    this.ctx.abort("cf-latency explicit cold Actor incarnation");
  }
  begin(query: Query) {
    const meter = observation(this.ctx.storage);
    const entry = meter.entry();
    const constructorSql = meter.initialSql();

    if (meter.active) throw new Error("Actor meter already active");
    meter.reset(query);
    meter.active = true;

    return { ...meter.identity(this.ctx, this.env), constructorSql, entry };
  }
  end() {
    const meter = observation(this.ctx.storage);

    meter.active = false;

    return {
      ...meter.identity(this.ctx, this.env),
      ...meter.finish(),
      databaseBytes: this.ctx.storage.sql.databaseSize,
    };
  }
}

export class NetworkThreadDO extends actorWorker.ThreadObject {
  private reference?: Reference;
  private readonly host: Host;
  constructor(
    ctx: DurableObjectState,
    env: ConstructorParameters<typeof actorWorker.ThreadObject>[1],
  ) {
    const wrapped = instrument(ctx, env);

    super(wrapped, withLocation(env));
    this.host = new Host(wrapped, env, "tardie");
  }
  override async alarm(
    ...args: Parameters<InstanceType<typeof actorWorker.ThreadObject>["alarm"]>
  ): Promise<void> {
    const meter = observation(this.ctx.storage);
    const alarmId = crypto.randomUUID();
    const log = (edge: "start" | "end") => meter.alarmEvent(alarmId, edge, this.ctx);

    log("start");
    try {
      await super.alarm(...args);
    } finally {
      log("end");
    }
  }
  private async open() {
    if (this.reference) return this.reference;
    // Same pinned Tardie 0.44.0 access bridge as the original durable-bench.
    const self = this as unknown as Internals;

    await self.identityReady;
    const reference = await Effect.runPromise(self.actors().reference(self.address()));

    await Effect.runPromise(reference.wait);

    return (this.reference = reference);
  }
  private async turn({ id, text }: Turn) {
    const reference = await this.open();

    await Effect.runPromise(reference.methods.message({ text }, { id }));
    await Effect.runPromise(reference.wait);
  }
  override fetch(request: Request) {
    if (!["/identity", "/seed", "/reset", "/run", "/cold"].includes(new URL(request.url).pathname))
      return super.fetch(request);

    return this.host.fetch(request, {
      wake: async () => {
        await this.open();
      },
      turn: (input) => this.turn(input),
    });
  }
}

export { cloudflareThreadName };
