import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import { BrowserCrypto } from "@effect/platform-browser";
import { ThreadObject } from "@yielded/agent-platform-cloudflare";
import {
  HostResponse,
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
import { importCanonical } from "./storage.ts";

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

const replayStarted = "durable-bench/replay-started";

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
  async beginReplay(query: Query): Promise<void> {
    this.host.meter.assertBuild(query);
    await this.ctx.blockConcurrencyWhile(async () => {
      if (
        (await this.ctx.storage.get(replayStarted)) !== undefined ||
        this.ctx.storage.sql
          .exec<{ n: number }>(
            "SELECT (SELECT COUNT(*) FROM effect_agent_submissions) + (SELECT COUNT(*) FROM effect_agent_canonical_records) AS n",
          )
          .one().n !== 0
      )
        throw new Error("Seed replay requires a fresh Object and is never retried");
      await this.ctx.storage.put(replayStarted, true);
    });
  }
  async benchSubmit(encoded: unknown, query: Query): Promise<unknown> {
    this.host.meter.begin(query);
    this.host.meter.marker("submit");

    return super.submitEncoded(encoded);
  }
  override async awaitSettlementEncoded(
    encoded: unknown,
    ...trace: [] | [unknown]
  ): Promise<unknown> {
    this.host.meter.marker("await");

    return super.awaitSettlementEncoded(encoded, ...trace);
  }
  override async alarm(...args: Parameters<ThreadObject.Instance["alarm"]>): Promise<void> {
    this.host.meter.alarm();
    this.host.meter.marker("alarm");
    await super.alarm(...args);
  }
  override fetch(request: Request): Promise<Response> {
    return this.host.fetch(request, {
      run: (input) => this.historyTurn(input),
      import: async (fixture, query) => {
        if (fixture.mode === "replay") {
          if ((await this.ctx.storage.get(replayStarted)) !== true)
            throw new Error("Seed replay was not started");

          return;
        }
        if (!fixture.archive) throw new Error("Missing canonical fixture");
        await Effect.runPromise(importCanonical(this.ctx.storage, fixture.archive, query.object));
      },
    });
  }
}
