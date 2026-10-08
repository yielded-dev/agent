import { DurableObject } from "cloudflare:workers";

// Deliberately no framework/library code: the experiment probes the platform.
type Mode = "return" | "fetch" | "sync-end" | "sync-each";
type Probe = { sample: string; bytes: number; transactions: number; mode: Mode };
type Env = {
  PROBES: DurableObjectNamespace<ProbeDO>;
  TOKEN: string;
  PROVIDER_URL: string;
  VERSION: { id: string };
  PHASE: string;
};

const parseProbe = (value: unknown): Probe => {
  if (typeof value !== "object" || value === null) throw new Error("Expected probe object");
  const { sample, bytes, transactions, mode } = value as Record<string, unknown>;
  if (typeof sample !== "string" || sample.length > 160 ||
    typeof bytes !== "number" || ![0, 1024, 16384, 65536, 131072, 524288].includes(bytes) ||
    typeof transactions !== "number" || ![1, 4, 12].includes(transactions) ||
    (mode !== "return" && mode !== "fetch" && mode !== "sync-end" && mode !== "sync-each")) {
    throw new Error("Invalid bounded probe");
  }
  return { sample, bytes, transactions, mode };
};

export class ProbeDO extends DurableObject<Env> {
  readonly instance = crypto.randomUUID();
  private readonly material = new Uint8Array(524288);
  private prepared = false;
  private counter = 0;
  private recentAlarms: Array<{ sample: string; startMs: number; endMs: number; iterations: number }> = [];

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // An alarm can wake a fresh incarnation between requests. Preparation is
    // persisted schema, not an in-memory flag; this constructor read writes nothing.
    this.prepared = ctx.storage.sql.exec(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cf_latency_payload'",
    ).toArray().length === 1;
    // Generate incompressible deterministic material outside measured requests.
    let v = 0x12345678;
    for (let i = 0; i < this.material.length; i++) {
      v ^= v << 13;
      v ^= v >>> 17;
      v ^= v << 5;
      this.material[i] = v & 255;
    }
  }

  identity() {
    return { instance: this.instance, objectId: this.ctx.id.toString(), version: this.env.VERSION.id };
  }

  async prepare() {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS cf_latency_payload (slot INTEGER PRIMARY KEY, payload BLOB NOT NULL)");
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS cf_latency_alarm (slot INTEGER PRIMARY KEY, sample TEXT NOT NULL, iterations INTEGER NOT NULL)");
    await this.ctx.storage.sync();
    this.prepared = true;
    return this.identity();
  }

  async probe(raw: unknown) {
    if (!this.prepared) throw new Error("Prepare before measuring");
    const input = parseProbe(raw);
    console.log({ cfLatency: "probe", sample: input.sample, ...input });
    const startMs = Date.now();
    const waits: number[] = [];
    let written = 0;
    for (let index = 0; index < input.transactions; index++) {
      const size = Math.floor(input.bytes / input.transactions) + (index < input.bytes % input.transactions ? 1 : 0);
      if (size > 0) {
        const value = this.material.slice(written, written + size);
        // Each UPDATE changes its content even when the case repeats.
        value[0] ^= ++this.counter & 255;
        this.ctx.storage.transactionSync(() => {
          this.ctx.storage.sql.exec(
            "INSERT INTO cf_latency_payload VALUES (?, ?) ON CONFLICT(slot) DO UPDATE SET payload=excluded.payload",
            index, value.buffer,
          );
        });
        written += size;
      }
      if (input.mode === "sync-each") {
        const before = Date.now();
        await this.ctx.storage.sync();
        waits.push(Date.now() - before);
      }
    }
    if (input.mode === "sync-end") {
      const before = Date.now();
      await this.ctx.storage.sync();
      waits.push(Date.now() - before);
    }
    let outbound;
    if (input.mode === "fetch") {
      const issuedMs = Date.now();
      const response = await fetch(`${this.env.PROVIDER_URL}/echo?sample=${encodeURIComponent(input.sample)}`, {
        headers: { authorization: `Bearer ${this.env.TOKEN}` },
      });
      if (!response.ok) throw new Error(`Echo failed: ${response.status}`);
      const echo = await response.json();
      outbound = { issuedMs, completedMs: Date.now(), echo };
    }
    return {
      ...input, ...this.identity(), startMs, returnMs: Date.now(), waits, written,
      outbound, recentAlarms: this.recentAlarms,
    };
  }

  async fetch(request: Request) {
    if (request.headers.get("authorization") !== `Bearer ${this.env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    return Response.json(await this.probe(await request.json()));
  }

  async arm(sample: string, leadMs: number, iterations: number) {
    if (!this.prepared || !Number.isInteger(leadMs) || leadMs < 0 || leadMs > 1000 ||
      ![0, 20_000_000, 80_000_000].includes(iterations)) throw new Error("Invalid alarm experiment");
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO cf_latency_alarm VALUES (0, ?, ?)", sample, iterations);
    const scheduledMs = Date.now() + leadMs;
    await this.ctx.storage.setAlarm(scheduledMs);
    return { sample, scheduledMs, returnedMs: Date.now(), ...this.identity() };
  }

  async alarm() {
    const row = this.ctx.storage.sql.exec<{ sample: string; iterations: number }>(
      "SELECT sample, iterations FROM cf_latency_alarm WHERE slot=0",
    ).one();
    const startMs = Date.now();
    console.log({ cfLatency: "alarm-start", sample: row.sample, startMs, iterations: row.iterations });
    let value = 0x12345678;
    for (let i = 0; i < row.iterations; i++) value = Math.imul(value ^ i, 1664525) + 1013904223 | 0;
    // A timer gives the frozen clock an I/O boundary after the synchronous work.
    await new Promise((resolve) => setTimeout(resolve, 1));
    const event = { ...row, startMs, endMs: Date.now() };
    this.recentAlarms = [...this.recentAlarms.slice(-3), event];
    console.log({ cfLatency: "alarm-end", ...event, value });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`) return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    const object = url.searchParams.get("object");
    if (!object) return new Response("missing object", { status: 400 });
    const stub = env.PROBES.get(env.PROBES.idFromName(object), { locationHint: "wnam" });
    const ingressStartMs = Date.now();
    let result;
    if (url.pathname === "/prepare") result = await stub.prepare();
    else if (url.pathname === "/identity") result = await stub.identity();
    else if (url.pathname === "/probe") result = await stub.probe(await request.json());
    else if (url.pathname === "/fetch-probe") {
      const response = await stub.fetch(new Request("https://cf-latency/probe", {
        method: "POST", headers: { authorization: `Bearer ${env.TOKEN}`, "content-type": "application/json" },
        body: JSON.stringify(await request.json()),
      }));
      if (!response.ok) throw new Error(`Fetch probe ${response.status}`);
      result = await response.json();
    }
    else if (url.pathname === "/arm") result = await stub.arm(
      url.searchParams.get("sample") ?? "alarm", Number(url.searchParams.get("leadMs") ?? 150), Number(url.searchParams.get("iterations") ?? 80000000),
    );
    else return new Response("not found", { status: 404 });
    return Response.json({ result, ingressStartMs, stubReturnedMs: Date.now(), colo: request.cf?.colo ?? null });
  },
};
