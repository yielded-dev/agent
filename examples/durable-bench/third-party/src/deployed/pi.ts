import { instrumentTimeline, timeline } from "../../../deployed/isolate/timeline.ts";
import { DurableObject } from "cloudflare:workers";

import { payload, type Turn } from "../../../src/plan.ts";
import { BACKGROUND_CONTEXT as context } from "../../node_modules/@earendil-works/chord/dist/context/index.js";
import {
  stream,
  streamSimple,
} from "../../node_modules/@earendil-works/pi-ai/dist/api/openai-completions.js";
import { Type } from "../../node_modules/@earendil-works/pi-ai/dist/index.js";
import {
  createModels,
  createProvider,
  hasApi,
} from "../../node_modules/@earendil-works/pi-ai/dist/models.js";
import {
  createRegistry,
  defineExtension,
  defineTool,
  Harness,
  section,
  type Conversation,
} from "../../node_modules/@earendil-works/pi-durable/dist/index.js";
import {
  SqliteStorage,
  type SqliteDatabase,
  type SqliteExecutor,
  type SqliteValue,
} from "../../node_modules/@earendil-works/pi-durable/dist/storage/sqlite/index.js";
import { Host } from "../../../deployed/isolate/pi-host.ts";
import { instrumentStorage } from "../../../deployed/isolate/cold-storage.ts";
import type { Observation } from "../../../deployed/worker/observe.ts";
import type { Env } from "../../../deployed/worker/protocol.ts";
import { importRows } from "../../../deployed/isolate/pi-storage.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";

function modelCollection(env: Env, meter: Observation) {
  const models = createModels();

  models.setProvider(
    createProvider({
      id: "faux",
      baseUrl: env.PROVIDER_URL,
      auth: {
        apiKey: { name: "faux", resolve: async () => ({ auth: { apiKey: env.BENCH_TOKEN } }) },
      },
      models: [
        {
          id: "scripted-1",
          name: "faux",
          provider: "faux",
          api: "openai-completions",
          baseUrl: env.PROVIDER_URL,
          input: ["text"],
          reasoning: false,
          contextWindow: 1e9,
          maxTokens: 1024,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          compat: {
            supportsDeveloperRole: false,
            supportsStore: false,
            supportsUsageInStreaming: true,
          },
        },
      ],
      api: {
        stream: (model, transcript, options) => {
          if (!hasApi(model, "openai-completions"))
            throw new Error("Expected native Chat Completions model");

          return stream(model, transcript, {
            ...options,
            apiKey: env.BENCH_TOKEN,
            fetch: meter.fetch,
          });
        },
        streamSimple: (model, transcript, options) => {
          if (!hasApi(model, "openai-completions"))
            throw new Error("Expected native Chat Completions model");

          return streamSimple(model, transcript, {
            ...options,
            apiKey: env.BENCH_TOKEN,
            fetch: meter.fetch,
          });
        },
      },
    }),
  );

  return models;
}

const lookup = defineTool({
  name: "lookup",
  description: "Look up record number n",
  parameters: Type.Object({ n: Type.Number() }),
  execute: async ({ n }) => ({ content: [{ type: "text", text: payload(n) }] }),
});

const registry = createRegistry();

registry.install(
  defineExtension({
    name: "bench",
    tools: [lookup],
    sections: [section("preamble", () => SYSTEM, { tag: false })],
  }),
);

const bind = (params: SqliteValue[]) =>
  params.map((p) =>
    p instanceof Uint8Array ? p.slice().buffer : typeof p === "bigint" ? Number(p) : p,
  );

const read = <T>(row: Record<string, SqlStorageValue>) =>
  Object.fromEntries(
    Object.entries(row).map(([k, v]) => [k, v instanceof ArrayBuffer ? new Uint8Array(v) : v]),
  ) as T;

function database(storage: DurableObjectStorage): SqliteDatabase {
  const sql = storage.sql;

  const rows = <T>(query: string, params: SqliteValue[]) =>
    sql
      .exec(query, ...bind(params))
      .toArray()
      .map((row) => read<T>(row));

  const executor: SqliteExecutor = {
    exec: async (query) => {
      sql.exec(query);
    },
    run: async (query, ...params) => {
      sql.exec(query, ...bind(params));
    },
    async get<T extends object>(query: string, ...params: SqliteValue[]) {
      return rows<T>(query, params)[0];
    },
    async all<T extends object>(query: string, ...params: SqliteValue[]) {
      return rows<T>(query, params);
    },
  };

  let queue: Promise<unknown> = Promise.resolve();

  const serial = <T>(work: () => Promise<T>) => {
    const result = queue.then(work);

    queue = result.catch(() => {});

    return result;
  };

  return {
    exec: (query) => serial(() => executor.exec(query)),
    run: (query, ...params) => serial(() => executor.run(query, ...params)),
    get: (query, ...params) => serial(() => executor.get(query, ...params)),
    all: (query, ...params) => serial(() => executor.all(query, ...params)),
    transaction: (callback) => serial(() => storage.transaction(() => callback(executor))),
    close: async () => {},
  };
}

export class PiDO extends DurableObject<Env> {
  private root?: Conversation;
  private pending?: Promise<void>;
  private readonly host: Host;
  constructor(ctx: DurableObjectState, env: Env) {
    instrumentTimeline(ctx);
    ctx = instrumentStorage(ctx);
    super(ctx, env);
    this.host = new Host(ctx, env, "pi");
    timeline(ctx.storage)?.point("constructor.return");
  }
  private async open() {
    if (this.root) return this.root;
    timeline(this.ctx.storage)?.point("pi.open.start");
    const models = modelCollection(this.env, this.host.meter);

    const harness = await Harness.open(
      await SqliteStorage.open(database(this.ctx.storage)),
      {
        models,
        registry,
        settings: { compaction: { enabled: false } },
      },
      context,
    );

    const root = await harness.root(context, {
      agent: { model: { provider: "faux", modelId: "scripted-1" } },
    });
    timeline(this.ctx.storage)?.point("pi.open.end");
    return (this.root = root);
  }
  private async admit({id, text}: Turn) {
    const root = await this.open();
    timeline(this.ctx.storage)?.point("pi.submit.start");
    const submission = await root.submit({type: "input", content: text, requestId: id}, context);
    timeline(this.ctx.storage)?.point("pi.submit.return");
    this.pending = (async () => {
      const settled = await submission.wait(context);
      if (settled.status !== "done") throw new Error(JSON.stringify(settled));
      await root.waitForIdle(context);
    })();
    this.ctx.waitUntil(this.pending);
    return {id: submission.id};
  }
  private async wait() {
    if (!this.pending) throw new Error("No pending pi submission");
    await this.pending;
  }
  private async turn(input: Turn) {
    await this.admit(input);
    await this.wait();
  }
  override fetch(request: Request) {
    return this.host.fetch(request, {
      empty: async () => { await this.open(); },
      import: async (fixture) => {
        if (!fixture.thread) throw new Error("Missing pi SQLite fixture");
        importRows(this.ctx.storage, fixture.thread);
      },
      run: (input) => this.turn(input),
      submit: (input) => this.admit(input),
      wait: () => this.wait(),
    });
  }
}
