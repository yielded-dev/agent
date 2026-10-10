import { DurableObject } from "cloudflare:workers";

import { payload, type Turn } from "../../../src/plan.ts";
import { BACKGROUND_CONTEXT as context, withCancel } from "../../node_modules/@earendil-works/chord/dist/context/index.js";
import {
  stream,
  streamSimple,
} from "../../node_modules/@earendil-works/pi-ai/dist/api/openai-completions.js";
import { Type, type Message } from "../../node_modules/@earendil-works/pi-ai/dist/index.js";
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
  watchEvents,
  type AgentEvent,
  type Conversation,
} from "../../node_modules/@earendil-works/pi-durable/dist/index.js";
import { openDurableObjectSqliteStorage } from "../../node_modules/@earendil-works/pi-durable/dist/storage/sqlite/cloudflare.js";
import { Host } from "../../../deployed/worker/host.ts";
import { type Observation } from "../../../deployed/worker/observe.ts";
import { readQuery, type Env, type Query, type TextObservation } from "../../../deployed/worker/protocol.ts";
import { importRows } from "../../../deployed/worker/storage.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";

const messageText = (message: Message | undefined): string =>
  message?.role === "assistant"
    ? message.content.flatMap((part) => part.type === "text" ? [part.text] : []).join("")
    : "";

const eventText = (event: AgentEvent): string => {
  if (event.type === "message_start") return messageText(event.message);
  if (event.type === "message_end") return event.entry.model?.map(messageText).join("") ?? "";
  if (event.type !== "message_update") return "";

  return event.changes.map((change) => {
    if (change.type === "text_delta") return change.delta;
    if (change.type === "message") return messageText(change.message);
    if ((change.type === "text_start" || change.type === "block") && change.block.type === "text")
      return change.block.text;

    return "";
  }).join("");
};

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

export class PiDO extends DurableObject<Env> {
  private opened?: { harness: Harness; root: Conversation };
  private readonly host: Host;
  constructor(ctx: DurableObjectState, env: Env) {
    const constructedMs = Date.now();

    super(ctx, env);
    this.host = new Host(ctx, env, "pi", constructedMs);
  }
  private async open() {
    if (this.opened) return this.opened;
    const models = modelCollection(this.env, this.host.meter);
    // Registry subscriptions retain their Harness; an Object abort does not clear isolate globals.
    const registry = createRegistry();

    registry.install(
      defineExtension({
        name: "bench",
        tools: [lookup],
        sections: [section("preamble", () => SYSTEM, { tag: false })],
      }),
    );

    const harness = await Harness.open(
      await openDurableObjectSqliteStorage(this.ctx.storage),
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

    return (this.opened = { harness, root });
  }
  private async turn({ id, text }: Turn) {
    const { root } = await this.open();
    const submission = await root.submit({ type: "input", content: text, requestId: id }, context);
    const settled = await submission.wait(context);

    if (settled.status !== "done") throw new Error(JSON.stringify(settled));
    await root.waitForIdle(context);
  }
  private async textResponse(request: Request): Promise<Response> {
    const query = readQuery(new URL(request.url));

    this.host.meter.assertBuild(query);
    const { harness, root } = await this.open();
    const observer = withCancel(context);
    const watch = await watchEvents(harness, root.id, observer.context);

    if (watch.snapshot.run || watch.snapshot.generation) {
      observer.cancel();
      await watch.stop();
      throw new Error("Text observation requires an idle pi conversation");
    }
    const priorEntries = new Set(watch.snapshot.entries.map((entry) => entry.id));
    const channel = new TransformStream<Uint8Array, Uint8Array>();
    const writer = channel.writable.getWriter();
    const encoder = new TextEncoder();
    const write = (frame: TextObservation) => writer.write(encoder.encode(JSON.stringify(frame) + "\n"));
    let delivery = Promise.resolve();

    // Client cancellation owns only the native watch, never the independently submitted turn.
    this.ctx.waitUntil(writer.closed.catch((cause) => { observer.cancel(cause); }));
    this.ctx.waitUntil((async () => {
      try {
        await write({ _tag: "Ready" });
        watch.start((events) => {
          delivery = (async () => {
            for (const event of events) {
              const text = event.type === "snapshot"
                ? messageText(event.generation?.message) || event.entries
                    .filter((entry) => !priorEntries.has(entry.id))
                    .flatMap((entry) => entry.model ?? []).map(messageText).join("")
                : eventText(event);

              if (text) await write({ _tag: "Text", input: query.sample, text });
            }
          })();
          return delivery;
        });
        const ended = await watch.closed;

        if (ended.reason === "listener_error") throw ended.error;
        await writer.close();
      } catch (cause) {
        await writer.abort(cause);
      } finally {
        observer.cancel();
        await watch.stop();
        await delivery.catch(() => {});
        writer.releaseLock();
      }
    })());

    return new Response(channel.readable, {
      headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
    });
  }
  override fetch(request: Request) {
    if (new URL(request.url).pathname === "/text") return this.textResponse(request);

    return this.host.fetch(request, {
      import: async (fixture) => {
        if (!fixture.thread) throw new Error("Missing pi SQLite fixture");
        importRows(this.ctx.storage, fixture.thread);
      },
      run: (input, _query?: Query) => this.turn(input),
      close: async () => {
        const opened = this.opened;

        this.opened = undefined;
        if (opened) await opened.harness.close(context);
      },
    });
  }
}
