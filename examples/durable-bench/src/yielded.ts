import { BrowserCrypto } from "@effect/platform-browser";
import { ThreadObject } from "@yielded/agent-platform-cloudflare";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import * as Agent from "@yielded/agent/agent";
import { digestDefinitions } from "@yielded/agent/digest";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { ThreadId } from "@yielded/agent/identifiers";
import { text as textOutput } from "@yielded/agent/output";
import { IdempotencyKey, Principal } from "@yielded/agent/receipt";
import { DefinitionDigestInput } from "@yielded/agent/records";
import { DurableObject } from "effect-cf";
import { type Prompt, type Response } from "effect/ai";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { fingerprint, next, payload, type Message, type Turn } from "./plan.ts";
import { serve, tables, type Bench } from "./serve.ts";

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

let seen: readonly Message[] = [];

const respond = (prompt: Prompt.Prompt): ReadonlyArray<Response.StreamPartEncoded> => {
  seen = transcript(prompt);
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

const scripted = Model.make(
  "scripted",
  "scripted-1",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: (request) => Stream.fromIterable(respond(request.prompt)),
    }),
  ),
);

const agent = Agent.withModel(definition, scripted);

const definitions = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "scripted", name: "scripted-1" },
  tools: { lookup: { revision: 1, executionClass: "readonly" } },
});

const definitionDigests = Effect.runSync(
  Effect.cached(digestDefinitions(definitions).pipe(Effect.provide(BrowserCrypto.layer))),
);

const runtime = ThreadObject.layer([{ agent, definitions }]).pipe(
  Layer.provide(tools.toLayer({ lookup: ({ n }) => Effect.succeed(payload(n)) })),
);

const principal = Principal.make("bench");
const threadId = ThreadId.make("main");

const submitAndWait = Effect.fnUntraced(function* (input: Turn) {
  const client = yield* CloudflareThreadClient;

  const receipt = yield* client.submit(agent, input.text, {
    threadId,
    principal,
    idempotencyKey: IdempotencyKey.make(input.id),
    definitions: yield* definitionDigests,
  });

  const settlement = yield* client.awaitSettlement(receipt);

  if (settlement.outcome !== "completed") {
    return yield* Effect.die(`turn ${input.id} did not complete: ${JSON.stringify(settlement)}`);
  }
});

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

const execute = (input: Turn) =>
  Effect.gen(function* () {
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

export class YieldedDO
  extends ThreadObject.make(runtime, {
    namespaceBinding: "THREADS",
    deploymentId: "durable-bench",
    producerPrefix: "durable-bench",
  })
  implements Bench
{
  private run<A>(effect: Effect.Effect<A, unknown, unknown>): Promise<A> {
    // The isolate provides the agent runtime. The runner's context type does not name it.
    return this[DurableObject.RunSymbol](effect as Effect.Effect<A, unknown, never>).catch(
      (error: unknown) => {
        const message = error instanceof Error ? (error.stack ?? error.message) : String(error);

        console.error(message);
        throw new Error(message);
      },
    );
  }

  /** Inline baseline only: open the runtime and explicitly repair the canonical log. */
  open(): Promise<void> {
    return this.run(recover);
  }

  /** Admit one user turn and run it inline, the same worker an alarm would run. */
  turn(input: Turn): Promise<void> {
    return this.run(execute(input));
  }

  async seed(turns: readonly Turn[]): Promise<string> {
    for (const input of turns) await this.turn(input);

    return fingerprint(seen);
  }

  async stats() {
    const sql = this.ctx.storage.sql;

    return { bytes: sql.databaseSize, tables: tables(sql) };
  }
}

type Env = { THREADS: DurableObjectNamespace<YieldedDO> };

let clientRuntime: ManagedRuntime.ManagedRuntime<CloudflareThreadClient, never> | undefined;

const getClientRuntime = (env: Env) =>
  (clientRuntime ??= ManagedRuntime.make(
    CloudflareThreadClient.layerFromBinding({ namespace: env.THREADS }),
  ));

export const inlineWorker = serve<Env>((env) => {
  const stub = env.THREADS.getByName(threadId);

  return {
    wake: () => stub.open(),
    turn: (input) => stub.turn(input),
    seed: (turns) => stub.seed(turns),
    stats: () => stub.stats(),
  };
});

export default serve<Env>(
  (env) => {
    const stub = env.THREADS.getByName(threadId);
    const client = getClientRuntime(env);

    return {
      // The first public submission includes Object construction in the cold measurement.
      wake: () => Promise.resolve(),
      turn: (input) => client.runPromise(submitAndWait(input)),
      seed: (turns) => stub.seed(turns),
      stats: () => stub.stats(),
    };
  },
  (env) => getClientRuntime(env).runPromise(Effect.asVoid(definitionDigests)),
);
