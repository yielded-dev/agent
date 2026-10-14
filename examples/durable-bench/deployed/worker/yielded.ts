import { AsyncLocalStorage } from "node:async_hooks";

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
import { DurableObject, DurableObjectAlarm, DurableObjectState } from "effect-cf";
import { Model, Tool, Toolkit } from "effect/ai";
import { FetchHttpClient } from "effect/http";

import { payload, type Turn } from "../../src/plan.ts";
import { INSTRUCTIONS } from "../text.ts";
import { Host } from "./host.ts";
import { observation } from "./observe.ts";
import { type Env, type Query } from "./protocol.ts";

export const tools = Toolkit.make(
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
  instructions: INSTRUCTIONS,
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

const YieldedDOBase = ThreadObject.make(application, {
  namespaceBinding: "YIELDED",
  deploymentId: "durable-bench",
  producerPrefix: "durable-bench",
});

type InvocationServices = Effect.Services<
  Parameters<InstanceType<typeof YieldedDOBase>[typeof DurableObject.RunSymbol]>[0]
>;

export class YieldedDO extends YieldedDOBase {
  private readonly host: Host;
  private readonly activeDispatches = new Set<Promise<unknown>>();
  private readonly invocationQuery = new AsyncLocalStorage<{
    readonly query: Query | undefined;
    readonly source?: string;
  }>();
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
        inputPayload: Schema.decodeSync(PersistedJson)(input.text),
      }),
    );

    // Use the production admission and settlement endpoints inside the owning Object.
    const submitted = Schema.decodeUnknownSync(HostResponse)(await super.submitEncoded(encoded));

    if (submitted._tag !== "SubmitSucceeded")
      // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native history callback must reject a failed admission so Host keeps this batch in flight.
      throw new Error(`History admission failed: ${JSON.stringify(submitted)}`);

    const settled = Schema.decodeUnknownSync(HostResponse)(
      await super.awaitSettlementEncoded(Schema.encodeSync(Receipt)(submitted.receipt)),
    );

    if (settled._tag !== "SettlementReached" || settled.settlement.outcome !== "completed")
      // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- The native history callback must reject a failed settlement so Host cannot acknowledge the batch.
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
  override awaitSettlementRecordEncoded(
    encoded: unknown,
    ...trace: [] | [unknown]
  ): Promise<unknown> {
    this.host.meter.marker("result");

    return super.awaitSettlementRecordEncoded(encoded, ...trace);
  }
  async benchWatchText(encoded: unknown, query: Query): Promise<unknown> {
    this.host.meter.marker("text", query);

    // Observation opens before begin(), while the Object still holds the previous sample.
    return this.invocationQuery.run({ query }, () => this.watchTextEncoded(encoded));
  }
  override [DurableObject.RunSymbol]<A, E>(
    effect: Effect.Effect<A, E, InvocationServices>,
    options: DurableObject.RunOptions = {},
  ): Promise<A> {
    if (options.event === "rpc" || options.event === "fetch") {
      if (!this.host.meter.cpuEnabled) return super[DurableObject.RunSymbol](effect, options);

      return this.invocationQuery.run(
        {
          ...(this.invocationQuery.getStore() ?? { query: this.host.meter.query }),
          source: options.rpc?.method ?? options.event,
        },
        () => super[DurableObject.RunSymbol](effect, options),
      );
    }
    if (options.event !== "alarm") return super[DurableObject.RunSymbol](effect, options);

    // effect-cf's response.finally/waitUntil dispatch bypasses public alarm(), but
    // enters this hook inside the originating RPC/fetch invocation. Tail counts it once.
    const invocation = this.invocationQuery.getStore();
    const query = invocation === undefined ? this.host.meter.query : invocation.query;

    const source = invocation?.source ?? "alarm";
    const started = query ? this.host.meter.marker("dispatch", query, source) : undefined;
    const running = super[DurableObject.RunSymbol](effect, options);

    const completed =
      started && query
        ? running.then((value) => {
            this.host.meter.marker("dispatch", query, source, started);

            return value;
          })
        : running;

    this.activeDispatches.add(completed);

    return completed.finally(() => {
      this.activeDispatches.delete(completed);
    });
  }
  override alarm(...args: Parameters<ThreadObject.Instance["alarm"]>): Promise<void> | void {
    this.host.meter.alarm();
    if (!this.host.meter.cpuEnabled) return super.alarm(...args);

    return this.invocationQuery.run({ query: this.host.meter.query, source: "alarm" }, () =>
      super.alarm(...args),
    );
  }
  private async drainDispatches() {
    while (this.activeDispatches.size) await Promise.all(this.activeDispatches);
  }
  private maintenanceStatus() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;
        const state = yield* DurableObjectState.DurableObjectState;

        const logical = yield* alarms.getAlarmStatus({
          tag: "@yielded/agent/thread-maintenance",
          id: "queue",
        });

        const native = yield* state.storage.getAlarm();

        return {
          logical:
            logical === undefined ? null : { attempts: logical.attempts, parked: logical.parked },
          native,
        };
      }),
      { event: "rpc" },
    );
  }
  override async fetch(request: Request): Promise<Response> {
    // CPU evidence is captured after both timer endpoints, before the observer closes.
    if (this.host.meter.cpuEnabled && new URL(request.url).pathname === "/metrics")
      await this.drainDispatches();

    return this.host.fetch(request, {
      close: () => this.drainDispatches(),
      maintenance: () => this.maintenanceStatus(),
      run: (input) => this.historyTurn(input),
    });
  }
}
