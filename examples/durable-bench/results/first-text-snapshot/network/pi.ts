import { BACKGROUND_CONTEXT as context, withCancel } from "@earendil-works/chord/context";
import { Type, type Message } from "@earendil-works/pi-ai";
import { stream, streamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import { createModels, createProvider, hasApi } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
  type FauxResponseFactory,
} from "@earendil-works/pi-ai/providers/faux";
import {
  getCurrentSystemPrompt,
  getCurrentTools,
  normalizeContext,
  type TranscriptContext,
} from "@earendil-works/pi-ai/utils/transcript";
import {
  createRegistry,
  defineExtension,
  defineTool,
  Harness,
  section,
  watchEvents,
  type AgentEventStream,
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
import { decodeHostResult, type Env } from "./protocol.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";
const OUTPUT =
  "Final output contract: write the final reply as ordinary assistant text, without JSON wrapping. " +
  "An empty reply is valid only when allowed by the output Schema and the task instructions.";

// Match Effect's native message boundaries without changing Pi's durable entries.
const providerTranscript = (transcript: TranscriptContext): TranscriptContext =>
  normalizeContext({
    systemPrompt: getCurrentSystemPrompt(transcript.messages),
    tools: getCurrentTools(transcript.messages),
    messages: [
      { role: "system", content: OUTPUT, timestamp: 0 },
      ...transcript.messages.flatMap<Message>((message) => {
        if (message.role === "system") return [];
        if (
          message.role !== "assistant" ||
          !message.content.some((block) => block.type === "text") ||
          !message.content.some((block) => block.type === "toolCall")
        )
          return [message];

        // Benchmark text precedes lookup calls; Effect sends them as two messages.
        return [
          { ...message, content: message.content.filter((block) => block.type !== "toolCall") },
          { ...message, content: message.content.filter((block) => block.type === "toolCall") },
        ];
      }),
    ],
  });

function modelCollection(env: Env, meter: Observation) {
  const models = createModels();

  if (env.PHASE === "measure") {
    models.setProvider(
      createProvider({
        id: "first-text",
        baseUrl: env.PROVIDER_URL,
        auth: {
          apiKey: { name: "first-text", resolve: async () => ({ auth: { apiKey: env.TOKEN } }) },
        },
        models: [
          {
            id: "first-text-1",
            name: "first-text",
            provider: "first-text",
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
              supportsStrictMode: true,
              supportsMidConvoSystemMessages: true,
              maxTokensField: "max_tokens",
            },
          },
        ],
        api: {
          stream: (model, transcript, options) => {
            if (!hasApi(model, "openai-completions"))
              throw new Error("Expected native Chat Completions model");

            return stream(model, providerTranscript(transcript), {
              ...options,
              toolChoice: "auto",
              maxTokens: 1024,
              apiKey: env.TOKEN,
              fetch: meter.fetch(env),
            });
          },
          streamSimple: (model, transcript, options) => {
            if (!hasApi(model, "openai-completions"))
              throw new Error("Expected native Chat Completions model");

            return streamSimple(model, providerTranscript(transcript), {
              ...options,
              toolChoice: "auto",
              maxTokens: 1024,
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
    provider: "first-text",
    api: "openai-completions",
    models: [{ id: "first-text-1", contextWindow: 1e9 }],
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
  parameters: Type.Object({ n: Type.Integer() }, { additionalProperties: false }),
  constrainedSampling: { type: "json_schema", strict: "require" },
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
  private opened?: { harness: Harness; root: Conversation };
  private readonly host: Host;
  constructor(ctx: DurableObjectState, env: Env) {
    const wrapped = instrument(ctx, env);

    super(wrapped, env);
    this.host = new Host(wrapped, env, "pi");
  }
  private async open() {
    if (this.opened) return this.opened;
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

    const root = await harness.root(context, {
      agent: { model: { provider: "first-text", modelId: "first-text-1" } },
    });

    return (this.opened = { harness, root });
  }
  private async turn({ id, text }: Turn) {
    const { root } = await this.open();
    const submission = await root.submit({ type: "input", content: text, requestId: id }, context);
    const settled = await submission.wait(context);

    if (settled.status !== "done") throw new Error(JSON.stringify(settled));
    await root.waitForIdle(context);
  }
  private streamRun(request: Request): Response {
    const channel = new TransformStream<Uint8Array, Uint8Array>();
    const writer = channel.writable.getWriter();
    const encoder = new TextEncoder();
    const observer = withCancel(context);
    let watch: AgentEventStream | undefined;
    let delivery = Promise.resolve();
    let disconnected = false;
    let finishDelivery!: () => void;
    const runDelivered = new Promise<void>((resolve) => {
      finishDelivery = resolve;
    });
    const write = (value: unknown) => writer.write(encoder.encode(`${JSON.stringify(value)}\n`));

    this.ctx.waitUntil(
      writer.closed.catch((reason: unknown) => {
        disconnected = true;
        observer.cancel(reason);
        finishDelivery();
      }),
    );

    const work = async () => {
      try {
        const response = await this.host.fetch(request, {
          wake: async () => {
            await this.open();
          },
          turn: async (input) => {
            const { harness, root } = await this.open();

            watch = await watchEvents(harness, root.id, observer.context);
            // The first line is the attachment snapshot; later lines are event arrays.
            await write(watch.snapshot);
            watch.start((events) => {
              delivery = (async () => {
                // Pi bounds pending batches and replaces overflow with a snapshot.
                await write(events);
                if (
                  events.some(
                    (event) =>
                      event.type === "run_end" ||
                      (event.type === "snapshot" && event.run === undefined),
                  )
                )
                  finishDelivery();
              })();
              return delivery;
            });

            // Consumer cancellation owns only the watch, never the accepted turn.
            await this.turn(input);
            if (!disconnected) {
              const ended = await Promise.race([runDelivered, watch.closed]);

              if (ended?.reason === "listener_error") throw ended.error;
              if (ended && !disconnected)
                throw new Error(`Pi watch closed before run delivery: ${ended.reason}`);
            }
          },
        });

        // stop() drops pending batches and does not join a running callback.
        await watch?.stop();
        await delivery;
        if (disconnected) return;
        const result = decodeHostResult(await response.json());

        await write({ ...result, kind: "settled", status: response.status });
        await writer.close();
      } catch (cause) {
        if (!disconnected) await writer.abort(cause);
      } finally {
        observer.cancel();
        await watch?.stop();
        writer.releaseLock();
      }
    };

    this.ctx.waitUntil(work());
    return new Response(channel.readable, {
      headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
    });
  }
  override fetch(request: Request) {
    if (
      new URL(request.url).pathname === "/run" &&
      this.env.PHASE === "measure" &&
      request.method === "POST" &&
      this.env.TOKEN &&
      request.headers.get("authorization") === `Bearer ${this.env.TOKEN}`
    )
      return this.streamRun(request);

    return this.host.fetch(request, {
      wake: async () => {
        await this.open();
      },
      turn: (input) => this.turn(input),
    });
  }
}
