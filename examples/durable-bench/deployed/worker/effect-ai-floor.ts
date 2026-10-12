import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "effect";
import { LanguageModel, Prompt, type Response as AiResponse } from "effect/ai";
import { FetchHttpClient } from "effect/http";

import { history, payload, fingerprint, type Turn, type Message } from "../../src/plan.ts";
import { TEXT_OUTPUT_INSTRUCTIONS } from "../text.ts";
import { Host } from "./host.ts";
import { readQuery, TextObservation, type Env } from "./protocol.ts";
import { tools } from "./yielded.ts";

class FloorError extends Schema.TaggedError<FloorError>()("FloorError", {
  message: Schema.String,
}) {}

const initialHistory = (size: number) => {
  const content: Prompt.Message[] = [];
  const normalized: Message[] = [];
  let n = 0;

  for (const input of history(0, size)) {
    content.push(
      Prompt.makeMessage("user", { content: [Prompt.makePart("text", { text: input.text })] }),
    );
    normalized.push({ role: "user", text: input.text });
    const count = Number(/tools=(\d+)/.exec(input.text)?.[1] ?? 0);

    for (let i = 0; i < count; i++) {
      n++;
      content.push(
        Prompt.makeMessage("assistant", {
          content: [
            Prompt.makePart("tool-call", {
              id: `call-${n}`,
              name: "lookup",
              params: { n },
              providerExecuted: false,
            }),
          ],
        }),
      );
      content.push(
        Prompt.makeMessage("tool", {
          content: [
            Prompt.makePart("tool-result", {
              id: `call-${n}`,
              name: "lookup",
              result: payload(n),
              isFailure: false,
              providerExecuted: false,
            }),
          ],
        }),
      );
      normalized.push(
        { role: "assistant", text: "", calls: [n] },
        { role: "tool", text: payload(n) },
      );
    }
    const text = `done after ${count} lookups`;

    content.push(Prompt.makeMessage("assistant", { content: [Prompt.makePart("text", { text })] }));
    normalized.push({ role: "assistant", text });
  }

  return {
    prompt: Prompt.setSystem(Prompt.fromMessages(content), TEXT_OUTPUT_INSTRUCTIONS),
    normalized,
  };
};

/** Native provider/tool loop over an ephemeral Prompt. Only warm turns are comparable. */
export class EffectAiFloorDO extends DurableObject<Env> {
  private readonly host: Host;
  private readonly runtime;
  private prompt = Prompt.empty;
  private initialized: number | undefined;
  private hasRun = false;
  private active = false;
  private listener:
    | { sample: string; controller: ReadableStreamDefaultController<Uint8Array> }
    | undefined;
  private readonly encoder = new TextEncoder();
  private readonly encodeText = Schema.encodeSync(Schema.fromJsonString(TextObservation));

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.host = new Host(ctx, env, "effect-ai-floor");

    const provider = OpenAiLanguageModel.model("scripted-1").pipe(
      Layer.provide(OpenAiClient.layer({ apiUrl: env.PROVIDER_URL })),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(Layer.succeed(FetchHttpClient.Fetch, this.host.meter.fetch)),
    );

    this.runtime = ManagedRuntime.make(
      Layer.mergeAll(provider, tools.toLayer({ lookup: ({ n }) => Effect.succeed(payload(n)) })),
    );
  }

  private initialize(size: number) {
    if (this.initialized === size) return;
    if (this.initialized !== undefined) throw new Error("Reset the floor before changing history");
    this.prompt = initialHistory(size).prompt;
    this.initialized = size;
  }

  private detach() {
    this.listener?.controller.close();
    this.listener = undefined;
  }

  private forward(sample: string, text: string) {
    if (!text || this.listener?.sample !== sample) return;
    this.host.meter.forwardText(sample);
    this.listener.controller.enqueue(
      this.encoder.encode(this.encodeText({ _tag: "Text", input: sample, text }) + "\n"),
    );
  }

  private run = Effect.fnUntraced(function* (this: EffectAiFloorDO, input: Turn) {
    this.prompt = Prompt.concat(this.prompt, Prompt.make(input.text));
    for (let turn = 0; turn < 40; turn++) {
      const parts: AiResponse.AnyPart[] = [];
      let toolCalls = 0;

      yield* LanguageModel.streamText({ prompt: this.prompt, toolkit: tools }).pipe(
        Stream.runForEach((part) =>
          Effect.sync(() => {
            parts.push(part);
            if (part.type === "tool-call") toolCalls++;
            if (part.type === "text-delta") this.forward(input.id, part.delta);
          }),
        ),
      );
      this.prompt = Prompt.concat(this.prompt, Prompt.fromResponseParts(parts));
      if (toolCalls === 0) return;
    }

    return yield* new FloorError({ message: "Native AI tool loop exceeded 40 model calls" });
  });

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const query = readQuery(url);

    this.host.meter.assertBuild(query);
    if (query.target !== "effect-ai-floor") throw new Error("Target mismatch");
    if (["/seed", "/seed-progress"].includes(url.pathname))
      return new Response("The floor does not persist history", { status: 400 });
    if (url.pathname.startsWith("/restore/")) {
      if (request.method !== "POST") return new Response("POST required", { status: 405 });
      if (this.active) return new Response("Turn in progress", { status: 409 });
      if (url.pathname === "/restore/clear") {
        this.detach();
        this.prompt = Prompt.empty;
        this.initialized = undefined;
        this.hasRun = false;

        return Response.json({ ok: true, identity: this.host.meter.identity() });
      }
      if (url.pathname === "/restore/start") return Response.json({ ok: true });
      if (url.pathname !== "/restore/finalize")
        return new Response("No storage chunks", { status: 400 });
      if (this.hasRun) return new Response("Floor history is not pristine", { status: 409 });
      this.initialize(query.history);

      return Response.json({
        ok: true,
        target: query.target,
        history: query.history,
        mode: "import",
        fingerprint: await fingerprint(initialHistory(query.history).normalized.slice(0, -1)),
        tables: {},
        identity: this.host.meter.identity(),
      });
    }
    if (["/prime", "/text", "/run"].includes(url.pathname)) this.initialize(query.history);
    if (url.pathname === "/text") {
      if (this.active) return new Response("Observation requires an idle Object", { status: 409 });
      this.host.meter.marker("text", query);
      this.detach();
      let attached: ReadableStreamDefaultController<Uint8Array> | undefined;

      const stream = new ReadableStream<Uint8Array>({
        start: (controller) => {
          attached = controller;
          this.listener = { sample: query.sample, controller };
          controller.enqueue(this.encoder.encode(this.encodeText({ _tag: "Ready" }) + "\n"));
        },
        cancel: () => {
          if (this.listener?.controller === attached) this.listener = undefined;
        },
      });

      return new Response(stream, {
        headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" },
      });
    }
    if (url.pathname === "/run") {
      if (this.active) return new Response("Turn in progress", { status: 409 });
      this.active = true;
      this.hasRun = true;
    }
    try {
      return await this.host.fetch(request, {
        run: (input) => this.runtime.runPromise(this.run(input)),
        close: async () => {
          if (url.pathname === "/cold") {
            this.detach();
            await this.runtime.dispose();
          }
        },
      });
    } finally {
      if (url.pathname === "/run") this.active = false;
    }
  }
}
