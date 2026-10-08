// Generated routing and observability only; bench.mjs is the unchanged prepare() output.
import original, { PiDO as MainDO } from "./bench.mjs";

const keys = new Set(["50_0","50_1","50_2","250_0","250_1","250_2","1000_0","1000_1","1000_2"]);
const bindings = ["PI"];
let moduleId;

function pinned(namespace) {
  return new Proxy(namespace, {
    get(ns, property) {
      if (property === "getByName") return (name, options) => ns.getByName(name, { ...options, locationHint: "wnam" });
      if (property === "get") return (id, options) => ns.get(id, { ...options, locationHint: "wnam" });
      // Native namespace methods (including idFromName) require their original receiver.
      const value = Reflect.get(ns, property, ns);
      return typeof value === "function" ? value.bind(ns) : value;
    },
  });
}

function remap(env, key) {
  const mapped = { ...env };
  for (const binding of bindings) mapped[binding] = pinned(env[binding + "_" + key]);
  return mapped;
}

class Observed extends MainDO {
  #identity;

  constructor(ctx, env) {
    super(ctx, env);
    // Worker global evaluation forbids random I/O. Mint once, lazily in a DO constructor.
    moduleId ??= crypto.randomUUID();
    this.#identity = {
      objectId: ctx.id.toString(),
      moduleId,
      runtimeId: crypto.randomUUID(),
      version: env.BENCH_VERSION.id,
      generation: env.BENCH_GENERATION,
    };
  }

  identity() { return this.#identity; }

  // The original fixture's seen transcript is module-local and is lost on eviction.
  // Call immediately after the final turn, outside timing and without other key traffic.
  fingerprint() { return super.seed([]); }

  async cold(input) {
    const started = Date.now();
    await super.wake();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "cold" };
  }

  async warm1(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm1" };
  }

  async warm2(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm2" };
  }

  async warm3(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm3" };
  }

  async warm4(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm4" };
  }

  async warm5(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm5" };
  }

  async warm6(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm6" };
  }

  async warm7(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm7" };
  }

  async warm8(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm8" };
  }

  async warm9(input) {
    const started = Date.now();
    await super.turn(input);
    const insideWallMs = Date.now() - started;
    return { ...this.#identity, insideWallMs, phase: "warm9" };
  }
}

export class C50_0 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "50_0")); }
}

export class C50_1 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "50_1")); }
}

export class C50_2 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "50_2")); }
}

export class C250_0 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "250_0")); }
}

export class C250_1 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "250_1")); }
}

export class C250_2 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "250_2")); }
}

export class C1000_0 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "1000_0")); }
}

export class C1000_1 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "1000_1")); }
}

export class C1000_2 extends Observed {
  constructor(ctx, env) { super(ctx, remap(env, "1000_2")); }
}

export default {
  async fetch(request, env) {
    if (typeof env.BENCH_TOKEN !== "string" || env.BENCH_TOKEN.length === 0 ||
        request.headers.get("authorization") !== "Bearer " + env.BENCH_TOKEN) {
      return new Response("unauthorized", { status: 401 });
    }
    const generation = request.headers.get("x-bench-generation");
    if (generation !== null && generation !== env.BENCH_GENERATION) {
      return new Response("generation mismatch", { status: 412 });
    }
    if (typeof env.BENCH_GENERATION !== "string" || typeof env.BENCH_VERSION?.id !== "string") {
      return new Response("missing benchmark identity bindings", { status: 503 });
    }
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health") {
      return Response.json({ generation: env.BENCH_GENERATION, version: env.BENCH_VERSION.id });
    }
    const phase = url.pathname.slice(1);
    const measured = request.method === "POST" && /^(cold|warm[1-9])$/.test(phase);
    const inspect = request.method === "GET" && (phase === "identity" || phase === "fingerprint");
    const delegate = (request.method === "POST" && (phase === "setup" || phase === "seed")) ||
      (request.method === "GET" && phase === "stats");
    if (!measured && !inspect && !delegate) return new Response("not found", { status: 404 });
    const key = url.searchParams.get("key");
    if (!keys.has(key)) return new Response("invalid key", { status: 400 });
    try {
      const mapped = remap(env, key);
      if (delegate) {
        // serve() parses JSON even for setup. A bodyless setup still means the original setup.
        const delegated = phase === "setup" && request.body === null
          ? new Request(request.url, { method: "POST", headers: request.headers, body: "null" })
          : request;
        return await original.fetch(delegated, mapped);
      }
      const object = mapped.PI.getByName("main");
      return Response.json(measured ? await object[phase](await request.json()) : await object[phase]());
    } catch (error) {
      console.error(error);
      return new Response(error instanceof Error ? error.message : String(error), { status: 500 });
    }
  },
};
