import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { Type } from "@earendil-works/pi-ai";
import { stream, streamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import { createModels, createProvider, hasApi } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
  type FauxResponseFactory,
} from "@earendil-works/pi-ai/providers/faux";
import {
  createRegistry,
  defineExtension,
  defineTool,
  Harness,
  section,
  type Conversation,
} from "@earendil-works/pi-durable";
import {
  SqliteStorage,
  type SqliteDatabase,
  type SqliteExecutor,
  type SqliteValue,
} from "@earendil-works/pi-durable/storage/sqlite";
import { DurableObject } from "cloudflare:workers";

import { next, payload, type Turn } from "../../../src/plan.ts";
import { Host } from "./host.ts";
import { instrument, observation, type Observation } from "./observe.ts";
import type { Env } from "./protocol.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";

function modelCollection(env: Env, meter: Observation) {
  const models = createModels();

  if (env.PHASE === "measure") {
    models.setProvider(
      createProvider({
        id: "prod-admit",
        baseUrl: env.PROVIDER_URL,
        auth: {
          apiKey: { name: "prod-admit", resolve: async () => ({ auth: { apiKey: env.TOKEN } }) },
        },
        models: [
          {
            id: "prod-admit-1",
            name: "prod-admit",
            provider: "prod-admit",
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
              apiKey: env.TOKEN,
              fetch: meter.fetch(env),
            });
          },
          streamSimple: (model, transcript, options) => {
            if (!hasApi(model, "openai-completions"))
              throw new Error("Expected native Chat Completions model");

            return streamSimple(model, transcript, {
              ...options,
              apiKey: env.TOKEN,
              fetch: meter.fetch(env),
            });
          },
        },
      }),
    );

    return models;
  }

  const faux = fauxProvider({
    provider: "prod-admit",
    api: "openai-completions",
    models: [{ id: "prod-admit-1", contextWindow: 1e9 }],
    tokenSize: { min: 1e9, max: 1e9 },
  });

  const respond: FauxResponseFactory = (transcript) => {
    faux.appendResponses([respond]);
    meter.seen = transcript.messages
      .filter((m) => m.role === "user" || m.role === "assistant" || m.role === "toolResult")
      .map((m) => {
        const blocks =
          typeof m.content === "string" ? [{ type: "text" as const, text: m.content }] : m.content;

        const text = blocks.flatMap((b) => (b.type === "text" ? [b.text] : [])).join("");

        const calls = blocks.flatMap((b) =>
          b.type === "toolCall" && typeof b.arguments.n === "number" ? [b.arguments.n] : [],
        );

        return {
          role: m.role === "toolResult" ? "tool" : m.role === "user" ? "user" : "assistant",
          text,
          ...(calls.length ? { calls } : {}),
        };
      });
    const step = next(meter.seen);

    return "call" in step
      ? fauxAssistantMessage(
          fauxToolCall("lookup", { n: step.call }, { id: `call-${step.call}` }),
          { stopReason: "toolUse" },
        )
      : fauxAssistantMessage(step.answer);
  };

  faux.setResponses([respond]);
  models.setProvider(faux.provider);

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

export class NetworkPiDO extends DurableObject<Env> {
  private root?: Conversation;
  private readonly host: Host;
  constructor(ctx: DurableObjectState, env: Env) {
    const wrapped = instrument(ctx, env);

    super(wrapped, env);
    this.host = new Host(wrapped, env, "pi");
  }
  private async open() {
    if (this.root) return this.root;
    const models = modelCollection(this.env, observation(this.ctx.storage));

    const harness = await Harness.open(
      await SqliteStorage.open(database(this.ctx.storage)),
      {
        models,
        registry,
        settings: { compaction: { enabled: false } },
      },
      context,
    );

    return (this.root = await harness.root(context, {
      agent: { model: { provider: "prod-admit", modelId: "prod-admit-1" } },
    }));
  }
  private async turn({ id, text }: Turn) {
    const root = await this.open();
    const submission = await root.submit({ type: "input", content: text, requestId: id }, context);
    const settled = await submission.wait(context);

    if (settled.status !== "done") throw new Error(JSON.stringify(settled));
    await root.waitForIdle(context);
  }
  override fetch(request: Request) {
    return this.host.fetch(request, {
      wake: async () => {
        await this.open();
      },
      turn: (input) => this.turn(input),
    });
  }
}
