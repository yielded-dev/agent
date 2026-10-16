import { BACKGROUND_CONTEXT as context, withCancel } from "@earendil-works/chord/context";
import { stream, streamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import { createModels, createProvider, hasApi } from "@earendil-works/pi-ai/models";
import {
  createRegistry,
  Harness,
  watchEvents,
  type Conversation,
} from "@earendil-works/pi-durable";
import { openDurableObjectSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/cloudflare";
import { DurableObject } from "cloudflare:workers";

import type { Env } from "../../src/protocol.ts";

function modelCollection(env: Env, fetch: typeof globalThis.fetch) {
  const models = createModels();
  models.setProvider(
    createProvider({
      id: "scripted",
      baseUrl: env.PROVIDER_URL,
      auth: {
        apiKey: { name: "scripted", resolve: async () => ({ auth: { apiKey: env.TOKEN } }) },
      },
      models: [
        {
          id: "scripted-1",
          name: "scripted-1",
          provider: "scripted",
          api: "openai-completions",
          baseUrl: env.PROVIDER_URL,
          input: ["text"],
          reasoning: false,
          contextWindow: 4096,
          maxTokens: 32,
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
          if (!hasApi(model, "openai-completions")) throw new Error("Expected Chat Completions");
          return stream(model, transcript, { ...options, apiKey: env.TOKEN, fetch });
        },
        streamSimple: (model, transcript, options) => {
          if (!hasApi(model, "openai-completions")) throw new Error("Expected Chat Completions");
          return streamSimple(model, transcript, { ...options, apiKey: env.TOKEN, fetch });
        },
      },
    }),
  );
  return models;
}

export class PiObject extends DurableObject<Env> {
  private opened?: { harness: Harness; root: Conversation };
  private calls = 0;
  private running = false;

  private async open() {
    if (this.opened) return this.opened;
    const models = modelCollection(this.env, (input, init) => {
      this.calls++;
      return globalThis.fetch(input, init);
    });
    const harness = await Harness.open(
      await openDurableObjectSqliteStorage(this.ctx.storage),
      {
        models,
        registry: createRegistry(),
        settings: { compaction: { enabled: false } },
      },
      context,
    );
    try {
      const root = await harness.root(context, {
        agent: { model: { provider: "scripted", modelId: "scripted-1" } },
      });
      return (this.opened = { harness, root });
    } catch (cause) {
      await harness.close(context);
      throw cause;
    }
  }

  async run(sample: string): Promise<ReadableStream<Uint8Array>> {
    if (this.running) throw new Error("A turn is already running");
    this.running = true;
    const observer = withCancel(context);
    let watch: Awaited<ReturnType<typeof watchEvents>> | undefined;
    let cancelled = false;
    const encoder = new TextEncoder();
    const initialCalls = this.calls;
    return new ReadableStream<Uint8Array>({
      start: (controller) => {
        const emit = (frame: object) => {
          if (!cancelled) controller.enqueue(encoder.encode(JSON.stringify(frame) + "\n"));
        };
        this.ctx.waitUntil(
          (async () => {
            try {
              const { harness, root } = await this.open();
              if (cancelled) return;
              watch = await watchEvents(harness, root.id, observer.context);
              if (watch.snapshot.run || watch.snapshot.generation || watch.snapshot.inbox.length)
                throw new Error("Conversation is not idle");
              let observed = () => {};
              const delivered = new Promise<void>((resolve) => {
                observed = resolve;
              });
              let text = "";
              const publish = (value: string) => {
                text += value;
                if (value) emit({ type: "text", value: { text: value } });
              };
              watch.start(async (events) => {
                for (const event of events) {
                  if (event.type === "message_start" && event.message.role === "assistant") {
                    publish(
                      event.message.content
                        .flatMap((part) => (part.type === "text" ? [part.text] : []))
                        .join(""),
                    );
                  }
                  if (event.type === "message_update") {
                    for (const change of event.changes) {
                      if (change.type === "text_delta") publish(change.delta);
                      else if (
                        !text &&
                        (change.type === "text_start" || change.type === "block") &&
                        change.block.type === "text"
                      )
                        publish(change.block.text);
                    }
                  }
                  if (event.type === "message_end" && !text) {
                    publish(
                      (event.entry.model ?? [])
                        .flatMap((message) =>
                          message.role === "assistant"
                            ? message.content.flatMap((part) =>
                                part.type === "text" ? [part.text] : [],
                              )
                            : [],
                        )
                        .join(""),
                    );
                  }
                  if (
                    event.type === "submission" &&
                    event.record.requestId === sample &&
                    (event.record.status === "done" || event.record.status === "unanswered")
                  )
                    observed();
                }
              });
              const submission = await root.submit(
                { type: "input", content: "Say hello.", requestId: sample },
                context,
              );
              const settled = await submission.wait(context);
              await root.waitForIdle(context);
              if (settled.status !== "done" || settled.type !== "input")
                throw new Error("Turn did not complete");
              // Replayed input has no new delivery or model call and cannot qualify.
              if (this.calls - initialCalls !== 1)
                throw new Error("Expected exactly one model call");
              const ended = await Promise.race([delivered, watch.closed]);
              if (cancelled) return;
              if (ended) throw new Error("Observer closed before delivery");
              if (text !== "hello") throw new Error("Expected hello");
              emit({ type: "done", value: { outcome: "completed", calls: 1 } });
              controller.close();
            } catch (cause) {
              if (!cancelled) controller.error(cause);
            } finally {
              observer.cancel();
              await watch?.stop();
              this.running = false;
            }
          })(),
        );
      },
      cancel: async () => {
        cancelled = true;
        observer.cancel();
        await watch?.stop();
      },
    });
  }
}
