import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import { BrowserCrypto } from "@effect/platform-browser";
import { ThreadObject } from "@yielded/agent-platform-cloudflare";
import {
  HostResponse,
  LiveTextFrame,
  SubmitRequest,
} from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import * as Agent from "@yielded/agent/agent";
import { digestDefinitions } from "@yielded/agent/digest";
import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { text } from "@yielded/agent/output";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { DefinitionDigestInput, PersistedJson } from "@yielded/agent/records";
import { Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";
import { Model, Tool, Toolkit } from "effect/ai";
import { FetchHttpClient } from "effect/http";

import { payload, type Turn } from "../../src/plan.ts";
import { Host } from "./host.ts";
import { observation } from "./observe.ts";
import { type Env, type Query } from "./protocol.ts";

const tools = Toolkit.make(
  Tool.make("lookup", {
    description: "Look up record number n",
    parameters: Schema.Struct({ n: Schema.Int }),
    success: Schema.String,
  }).annotate(ToolExecutionClass, "readonly"),
);

const definition = Agent.make("bench", {
  input: Schema.String,
  inputPrompt: (input) => input,
  output: text(Schema.String),
  instructions: "You are a benchmark agent. Call lookup as instructed, then answer briefly.",
  toolkit: tools,
  policy: { maxTurns: 40, maxToolCalls: 32, maxDuration: "2 minutes", toolConcurrency: 1 },
});

const selected = Model.make(
  "scripted",
  "scripted-1",
  Layer.unwrap(
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;
      const meter = observation(state.raw.storage);

      return OpenAiLanguageModel.layer({ model: "scripted-1" }).pipe(
        Layer.provide(OpenAiClient.layer({ apiUrl: meter.env.PROVIDER_URL })),
        Layer.provide(FetchHttpClient.layer),
        Layer.provide(Layer.succeed(FetchHttpClient.Fetch, meter.fetch)),
      );
    }),
  ),
);

export const agent = Agent.withModel(definition, selected);

export const definitions = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "scripted", name: "scripted-1" },
  tools: { lookup: { revision: 1, executionClass: "readonly" } },
});

const definitionDigests = Effect.runSync(
  Effect.cached(digestDefinitions(definitions).pipe(Effect.provide(BrowserCrypto.layer))),
);

const application = ThreadObject.layer([{ agent, definitions }]).pipe(
  Layer.provide(tools.toLayer({ lookup: ({ n }) => Effect.succeed(payload(n)) })),
);

export class YieldedDO extends ThreadObject.make(application, {
  namespaceBinding: "YIELDED",
  deploymentId: "durable-bench",
  producerPrefix: "durable-bench",
}) {
  private readonly host: Host;
  private readonly activeAlarms = new Set<Promise<void>>();
  constructor(ctx: globalThis.DurableObjectState, env: Env) {
    const constructedMs = Date.now();

    super(ctx, env);
    this.host = new Host(ctx, env, "yielded", constructedMs);
  }
  private async historyTurn(input: Turn): Promise<void> {
    const encoded = Schema.encodeSync(SubmitRequest)(
      SubmitRequest.make({
        agentId: definition.id,
        principal: Principal.make("bench"),
        idempotencyKey: IdempotencyKey.make(input.id),
        definitions: await Effect.runPromise(definitionDigests),
        inputPayload: Schema.decodeUnknownSync(PersistedJson)(input.text),
      }),
    );

    // Use the production admission and settlement endpoints inside the owning Object.
    const submitted = Schema.decodeUnknownSync(HostResponse)(await super.submitEncoded(encoded));

    if (submitted._tag !== "SubmitSucceeded")
      throw new Error(`History admission failed: ${JSON.stringify(submitted)}`);

    const settled = Schema.decodeUnknownSync(HostResponse)(
      await super.awaitSettlementEncoded(Schema.encodeSync(Receipt)(submitted.receipt)),
    );

    if (settled._tag !== "SettlementReached" || settled.settlement.outcome !== "completed")
      throw new Error(`History settlement failed: ${JSON.stringify(settled)}`);
  }
  async benchSubmit(encoded: unknown, query: Query): Promise<unknown> {
    this.host.meter.begin(query);
    this.host.meter.marker("submit");

    const response = await super.submitEncoded(encoded);
    const decoded = Schema.decodeUnknownSync(HostResponse)(response);

    if (decoded._tag === "SubmitSucceeded") this.host.meter.admitted(decoded.receipt.submissionId);

    return response;
  }
  override async awaitSettlementEncoded(
    encoded: unknown,
    ...trace: [] | [unknown]
  ): Promise<unknown> {
    this.host.meter.marker("await");

    const response = await super.awaitSettlementEncoded(encoded, ...trace);

    if (Schema.decodeUnknownSync(HostResponse)(response)._tag === "SettlementReached")
      this.host.meter.settled();

    return response;
  }
  override async watchTextEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    const stream = await super.watchTextEncoded(encoded, ...trace);

    if (!(stream instanceof ReadableStream)) return stream;
    const meter = this.host.meter;
    const decoder = new TextDecoder();
    let pending = "";

    return stream.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(bytes, controller) {
          pending += decoder.decode(bytes, { stream: true });
          const lines = pending.split("\n");

          pending = lines.pop() ?? "";
          for (const line of lines) {
            if (!line) continue;
            const frame = Schema.decodeUnknownSync(Schema.fromJsonString(LiveTextFrame))(line);

            if (
              frame._tag === "Event" &&
              frame.event._tag === "Text" &&
              frame.event.part.type === "text-delta" &&
              frame.event.part.delta.trim()
            )
              meter.forwardText(frame.event.submissionId);
          }
          controller.enqueue(bytes);
        },
      }),
    );
  }
  override async alarm(...args: Parameters<ThreadObject.Instance["alarm"]>): Promise<void> {
    this.host.meter.alarm();
    this.host.meter.marker("alarm");
    const running = Promise.resolve(super.alarm(...args));

    this.activeAlarms.add(running);
    try {
      await running;
    } finally {
      this.activeAlarms.delete(running);
    }
  }
  override fetch(request: Request): Promise<Response> {
    return this.host.fetch(request, {
      close: async () => {
        // Native post-settlement checkpoint writers must finish before cold eviction.
        while (this.activeAlarms.size) await Promise.all(this.activeAlarms);
      },
      run: (input) => this.historyTurn(input),
    });
  }
}
