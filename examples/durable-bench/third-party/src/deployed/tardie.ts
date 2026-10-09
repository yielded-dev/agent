import { payload, type Turn } from "../../../src/plan.ts";
import { Effect, Layer, Schema } from "../../node_modules/effect/dist/index.js";
import {
  FetchHttpClient,
  HttpClient,
} from "../../node_modules/effect/dist/unstable/http/index.js";
import { Rpc } from "../../node_modules/effect/dist/unstable/rpc/index.js";
import {
  actorContext,
  agentMethods,
  compact,
  infer,
  messages,
  tools,
  type ContextView,
} from "../../node_modules/tardie/src/agent/index.ts";
import {
  ModelLock,
  liveModelServices,
  modelActs,
  modelInfo,
  toolActs,
} from "../../node_modules/tardie/src/agent/services/index.ts";
import {
  atom,
  defineActor,
  type Atom,
  type ThreadCoordinate,
} from "../../node_modules/tardie/src/core/index.ts";
import {
  defineLibrary,
  MethodDescription,
  MethodHints,
} from "../../node_modules/tardie/src/libraries/index.ts";
import { modelLockService } from "../../node_modules/tardie/src/model/lock.ts";
import { providerLayer } from "../../node_modules/tardie/src/model/providers/openai-compat.ts";
import {
  cloudflareThreadName,
  createActorWorker,
} from "../../node_modules/tardie/src/platform/cloudflare/index.ts";
import { Host, COLD_ABORT } from "../../../deployed/worker/host.ts";
import { attach, observation } from "../../../deployed/worker/observe.ts";
import { type SqlDump, type Env } from "../../../deployed/worker/protocol.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";
const MODEL = { provider: "scripted", model_id: "scripted-1" };

export const coordinate = (object: string) => ({
  actor: "bench-agent",
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
  "bench-agent",
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

  const lock = Layer.succeed(
    ModelLock,
    modelLockService(
      {
        schema: 2,
        providers: {
          scripted: {
            protocol: "openai-chat-completions",
            baseUrl: env.PROVIDER_URL,
            env: ["REBENCH_TOKEN"],
          },
        },
        models: [{ ...MODEL, contextWindowTokens: 1e9 }],
      },
      { default: MODEL, allow: "*" },
    ),
  );

  const native = liveModelServices({
    credentials: { REBENCH_TOKEN: env.BENCH_TOKEN },
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
            HttpClient.transformResponse(
              options.client.transformClient?.(http) ?? http,
              (response) => Effect.provideService(response, FetchHttpClient.Fetch, meter.fetch),
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
  ).pipe(Layer.provide(Layer.merge(native, lock)));
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

export class ActorDO extends actorWorker.ActorObject {
  private readonly meter;
  private nativeEntry?: ReturnType<ReturnType<typeof observation>["identity"]>;
  constructor(
    ctx: DurableObjectState,
    env: ConstructorParameters<typeof actorWorker.ActorObject>[1],
  ) {
    super(ctx, withLocation(env));
    this.meter = attach(ctx, env);
  }
  override lookup(...args: Parameters<InstanceType<typeof actorWorker.ActorObject>["lookup"]>) {
    this.nativeEntry ??= this.meter.identity();
    this.meter.marker("actor-lookup");

    return super.lookup(...args);
  }
  override allocate(...args: Parameters<InstanceType<typeof actorWorker.ActorObject>["allocate"]>) {
    this.nativeEntry ??= this.meter.identity();

    return super.allocate(...args);
  }
  identity() {
    return this.meter.identity();
  }
  end() {
    return this.nativeEntry ?? this.meter.identity();
  }
  async abortCold(): Promise<void> {
    await this.ctx.storage.sync();
    this.ctx.abort(COLD_ABORT);
  }
  async importFixture(dump: SqlDump, object: string) {
    const counts = await Host.importTardie(this.ctx.storage, dump, object);

    await this.ctx.storage.put("tardie:actor:instance", object);

    return counts;
  }
}

export class ThreadDO extends actorWorker.ThreadObject {
  private reference?: Reference;
  private readonly host: Host;
  private importedHere = false;
  constructor(
    ctx: DurableObjectState,
    env: ConstructorParameters<typeof actorWorker.ThreadObject>[1],
  ) {
    super(ctx, withLocation(env));
    this.host = new Host(ctx, env, "tardie");
  }
  override async alarm(
    ...args: Parameters<InstanceType<typeof actorWorker.ThreadObject>["alarm"]>
  ): Promise<void> {
    this.host.meter.alarm();
    this.host.meter.marker("alarm");
    await super.alarm(...args);
  }
  private async open() {
    if (this.importedHere) throw new Error("Tardie requires /cold after bulk import");
    if (this.reference) return this.reference;
    // Pinned Tardie 0.44.0 native reference bridge, shared with the existing seed.
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
    if (
      !["/identity", "/import", "/run", "/metrics", "/cold"].includes(new URL(request.url).pathname)
    )
      return super.fetch(request);

    return this.host.fetch(request, {
      import: async (fixture, query) => {
        if (!fixture.thread) throw new Error("Missing Tardie Thread fixture");
        await Host.importTardie(this.ctx.storage, fixture.thread, query.object);
        await this.ctx.storage.put("tardie:thread:coordinate", coordinate(query.object));
        this.importedHere = true;
      },
      run: (input) => this.turn(input),
    });
  }
}

export { cloudflareThreadName };
