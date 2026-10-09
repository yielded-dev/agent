import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import { ThreadObject } from "@yielded/agent-platform-cloudflare";
import * as Agent from "@yielded/agent/agent";
import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { text } from "@yielded/agent/output";
import { DefinitionDigestInput } from "@yielded/agent/records";
import { Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";
import { Model, Tool, Toolkit } from "effect/ai";
import { FetchHttpClient } from "effect/http";

import { payload } from "../../src/plan.ts";
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
    super(ctx, env);
    this.host = new Host(ctx, env, "yielded");
  }
  async beginReplay(): Promise<void> {
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
