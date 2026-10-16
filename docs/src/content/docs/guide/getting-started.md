---
title: Getting started
description: Run a schema-defined agent with a model and in-memory history.
---

Start with one agent that classifies a bug report. Your application supplies a model and
history Layer; the definition owns the input, output, and execution limits. This example
runs in memory. Choose a [durable host](/platforms/) when accepted work must survive restart.

## Install

In a TypeScript project with Bun:

```sh
bun add @yielded/agent@beta effect @effect/ai-openai @effect/platform-bun
```

The package manifests declare peer compatibility. [Other providers](/reference/packages/)
use native Effect AI model Layers too.

## Define and run

Save as `agent.ts`. Supply `OPENAI_API_KEY` through your environment or secret manager.

```ts twoslash title="agent.ts"
import * as OpenAiClient from "@effect/ai-openai/OpenAiClient";
import * as OpenAiLanguageModel from "@effect/ai-openai/OpenAiLanguageModel";
import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import { Agent, AgentRuntime, InMemory } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Schema from "effect/Schema";

const triage = Agent.make("triage", {
  input: Schema.String,
  output: Schema.Struct({
    severity: Schema.Literals(["low", "medium", "high", "critical"]),
    explanation: Schema.String,
  }),
  instructions: "Classify the bug report and explain the severity in one sentence.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "30 seconds" },
});

const program = AgentRuntime.run(triage, "Users cannot sign in.").pipe(
  Effect.tap((result) => Console.log(result.output)),
  Effect.provide(OpenAiLanguageModel.model("gpt-6-luna")),
  Effect.provide(OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") })),
  Effect.provide(FetchHttpClient.layer),
  Effect.provide(InMemory.layer),
);

BunRuntime.runMain(program);
```

```sh
bun agent.ts
```

The result contains schema-validated output, a Thread ID, and execution metadata. A possible
output is:

```json
{ "severity": "high", "explanation": "Users are blocked from accessing the application." }
```

## Continue the conversation

Share the same `InMemory.layer` across Runs and pass the returned `threadId` for follow-up
input. Providing a fresh Layer around each call creates a separate store. History lasts
until its owner Scope closes; active execution has no process-loss recovery.

The repository has [complete runnable snippets](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
and a [Cloudflare travel planner](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner).
Next, [add tools](/guide/tools/), [stream progress](/guide/run-agents/), or choose
[what the Thread retains](/guide/threads/).
