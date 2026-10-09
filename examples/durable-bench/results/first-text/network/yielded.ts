import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import { ThreadObject } from "@yielded/agent-platform-cloudflare";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import * as Agent from "@yielded/agent/agent";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { OperationDenied } from "@yielded/agent/operation-authorizer";
import { text as textOutput } from "@yielded/agent/output";
import { IdempotencyKey, Principal } from "@yielded/agent/receipt";
import { DefinitionDigestInput } from "@yielded/agent/records";
import { Effect, Layer, Schema, Stream } from "effect";
import { DurableObject, DurableObjectState } from "effect-cf";
import { LanguageModel, Model, Tool, Toolkit, type Prompt, type Response } from "effect/ai";
import { FetchHttpClient } from "effect/http";

import { next, payload, type Message, type Turn } from "../../../src/plan.ts";
import { Host } from "./host.ts";
import { instrument, invocation, observation, type Observation } from "./observe.ts";
import { readQuery, type Env } from "./protocol.ts";

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly.";

const usage = { inputTokens: {}, outputTokens: {} };

const textOf = (content: string | ReadonlyArray<{ type: string; text?: string }>): string =>
  typeof content === "string"
    ? content
    : content
        .flatMap((part) => (part.type === "text" && part.text !== undefined ? [part.text] : []))
        .join("");

/** Model-visible transcript, excluding system and output-contract messages. */
const transcript = (prompt: Prompt.Prompt): readonly Message[] => {
  const messages: Message[] = [];

  for (const message of prompt.content) {
    if (message.role === "system") continue;
    if (message.role === "user") {
      messages.push({ role: "user", text: textOf(message.content) });
      continue;
    }
    if (message.role === "assistant") {
      const text = textOf(message.content);

      const calls = message.content.flatMap((part) => {
        if (part.type !== "tool-call" || typeof part.params !== "object" || part.params === null) {
          return [];
        }
        if (!("n" in part.params) || typeof part.params.n !== "number") return [];

        return [part.params.n];
      });

      messages.push(
        calls.length === 0 ? { role: "assistant", text } : { role: "assistant", text, calls },
      );
      continue;
    }
    for (const part of message.content) {
      if (part.type !== "tool-result") continue;
      messages.push({
        role: "tool",
        text: typeof part.result === "string" ? part.result : JSON.stringify(part.result),
      });
    }
  }

  return messages;
};

const respond = (
  prompt: Prompt.Prompt,
  meter: Observation,
): ReadonlyArray<Response.StreamPartEncoded> => {
  const seen = transcript(prompt);

  meter.seen = seen;
  const user = seen.findLast((message) => message.role === "user");

  if (user === undefined || !/tools=\d+/.test(user.text)) {
    throw new Error(`scripted model lost the turn text: ${JSON.stringify(seen.slice(-4))}`);
  }
  const step = next(seen);

  if ("call" in step) {
    return [
      {
        type: "tool-call",
        id: `call-${step.call}`,
        name: "lookup",
        params: { n: step.call },
        providerExecuted: false,
      },
      { type: "finish", reason: "tool-calls", usage },
    ];
  }

  return [
    { type: "text-start", id: "answer" },
    { type: "text-delta", id: "answer", delta: step.answer },
    { type: "text-end", id: "answer" },
    { type: "finish", reason: "stop", usage },
  ];
};

const lookup = Tool.make("lookup", {
  description: "Look up record number n",
  parameters: Schema.Struct({ n: Schema.Int }),
  success: Schema.String,
}).annotate(ToolExecutionClass, "readonly");

const tools = Toolkit.make(lookup);

const definition = Agent.make("bench", {
  input: Schema.String,
  // The other targets put the raw turn text in the user message. The default
  // renderer JSON-encodes Schema.String, which would add quotes around it.
  inputPrompt: (text) => text,
  output: textOutput(Schema.String),
  instructions: SYSTEM,
  toolkit: tools,
  // No contextTokenLimit: compaction stays off, matching the published harness.
  policy: {
    maxTurns: 40,
    maxToolCalls: 32,
    maxDuration: "2 minutes",
    toolConcurrency: 1,
  },
});

const selected = Model.make(
  "first-text",
  "first-text-1",
  Layer.unwrap(
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;
      const meter = observation(state.raw.storage);

      if (meter.env.PHASE === "seed")
        return Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.die("Seed requires streaming"),
            streamText: (request) => Stream.fromIterable(respond(request.prompt, meter)),
          }),
        );

      return OpenAiLanguageModel.layer({
        model: "first-text-1",
        config: { max_output_tokens: 1024 },
      }).pipe(
        Layer.provide(OpenAiClient.layer({ apiUrl: meter.env.PROVIDER_URL })),
        Layer.provide(FetchHttpClient.layer),
        Layer.provide(Layer.succeed(FetchHttpClient.Fetch, meter.fetch(meter.env))),
      );
    }),
  ),
);

export const agent = Agent.withModel(definition, selected);

export const definitions = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "first-text", name: "first-text-1" },
  tools: { lookup: { revision: 1, executionClass: "readonly" } },
});

const runtime = ThreadObject.layer([{ agent, definitions }]).pipe(
  Layer.provide(tools.toLayer({ lookup: ({ n }) => Effect.succeed(payload(n)) })),
);

const principal = Principal.make("bench");

const recover = Effect.gen(function* () {
  const agentRuntime = yield* DurableAgentRuntime;
  const { threadId } = yield* ThreadObjectIdentity;
  let cursor: string | undefined;

  for (let pass = 0; pass < 10_000; pass++) {
    const result = yield* agentRuntime.runRecovery({
      threadId,
      ...(cursor === undefined ? {} : { cursor }),
    });

    if (result.blocked.length > 0) {
      return yield* Effect.die(`recovery blocked: ${JSON.stringify(result.blocked)}`);
    }
    if (result.cursor === undefined) return;
    cursor = result.cursor;
  }

  return yield* Effect.die("recovery did not finish");
});

const execute = Effect.fnUntraced(function* (input: Turn) {
  const agentRuntime = yield* DurableAgentRuntime;
  const { threadId } = yield* ThreadObjectIdentity;

  const receipt = yield* agentRuntime.submitRegistered(agent, input.text, {
    threadId,
    principal,
    idempotencyKey: IdempotencyKey.make(input.id),
  });

  const settled = yield* agentRuntime.processThreadResolved(threadId);
  const own = settled.find((item) => item.submissionId === receipt.submissionId);

  if (own?.outcome !== "completed") {
    return yield* Effect.die(
      `turn ${input.id} did not complete: ${JSON.stringify(own ?? settled)}`,
    );
  }
});

export class NetworkYieldedDO extends ThreadObject.make(runtime, {
  namespaceBinding: "YIELDED",
  deploymentId: "first-text",
  producerPrefix: "first-text",
  operationAuthorizer: {
    authorize: (request) =>
      request.operation === "observe" && request.threadId?.includes("-proof-denied-")
        ? Effect.fail(
            OperationDenied.make({
              operation: request.operation,
              threadId: request.threadId,
              reason: "first-text deployed denial proof",
            }),
          )
        : Effect.void,
  },
}) {
  private readonly host: Host;
  private watchEntry?: ReturnType<Observation["entry"]>;
  constructor(ctx: globalThis.DurableObjectState, env: Env) {
    const wrapped = instrument(ctx, env);

    super(wrapped, env);
    this.host = new Host(wrapped, env, "yielded");
  }
  override async alarm(...args: Parameters<ThreadObject.Instance["alarm"]>): Promise<void> {
    const meter = observation(this.ctx.storage);
    const alarmId = crypto.randomUUID();
    const log = (edge: "start" | "end") => meter.alarmEvent(alarmId, edge, this.ctx);

    return invocation.run({ kind: "alarm", id: alarmId }, async () => {
      log("start");
      try {
        await super.alarm(...args);
      } finally {
        log("end");
      }
    });
  }
  override async submitEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    const { idempotencyKey } = Schema.decodeUnknownSync(
      Schema.Struct({ idempotencyKey: Schema.String }),
    )(encoded);

    const object = this.ctx.id.name;
    const matched = object?.match(/-h(50|250)-d(0|400)-o\d+/);
    const history = matched?.[1];
    const ttftMs = matched?.[2];

    if (!object || history === undefined || ttftMs === undefined)
      throw new Error("Unrecognized benchmark Object name");
    const url = new URL("https://first-text/submit");

    for (const [key, value] of Object.entries({
      target: "yielded",
      object,
      history,
      ttftMs,
      chunkDelayMs: "25",
      sample: idempotencyKey,
      variant: "production",
    }))
      url.searchParams.set(key, value);
    const meter = observation(this.ctx.storage);

    meter.begin(readQuery(url), this.ctx, this.watchEntry);
    this.watchEntry = undefined;
    return super.submitEncoded(encoded, ...trace);
  }
  override watchTextEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    this.watchEntry ??= observation(this.ctx.storage).entry();
    return super.watchTextEncoded(encoded, ...trace);
  }
  override fetch(request: Request): Promise<globalThis.Response> {
    return invocation.run({ kind: "inline", id: crypto.randomUUID() }, () =>
      this.host.fetch(request, {
        wake: () => this[DurableObject.RunSymbol](recover),
        turn: (input) => this[DurableObject.RunSymbol](execute(input)),
      }),
    );
  }
}
