---
title: Agent definitions
description: Define behavior with schemas and tools; supply models and resources through Layers.
---

An Agent Definition is reusable behavior: input and output schemas, instructions, tools,
and policy. It owns no conversation, provider connection, or database. Your application
supplies those services when the definition runs.

```ts twoslash
import { Agent, AgentRuntime } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
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

const result = AgentRuntime.run(triage, "Users cannot sign in.");
```

`result` is an Effect. The [getting-started example](/guide/getting-started/) supplies its model
and history services and runs it. One definition can serve many Threads.

## What the definition owns

| Field                 | Purpose                                                                         |
| --------------------- | ------------------------------------------------------------------------------- |
| ID                    | Stable Agent identity; keep it independent of deployments                       |
| `input` / `output`    | Effect Schemas for accepted input and validated final output                    |
| `instructions`        | Static instructions or a function of decoded input; may return an Effect        |
| `toolkit`             | Native Effect AI tools with application-provided handlers                       |
| `policy`              | Execution limits; omitted values use [defaults](/concepts/budgets/)             |
| `inputPrompt`         | Optional projection controlling what input reaches the model                    |
| `completion`          | Optional tool whose successful result becomes final output                      |
| `completionFromTools` | Optional pure projections from successful actions to completed output           |
| `updates`             | Schema for intentional intermediate findings                                    |
| `runDisposition`      | Schema and selector for ordinary completed output; omitted on budget exhaustion |

Final output is schema-validated JSON by default. Use `Output.text(Schema.String)` for an
ordinary text reply. `completion` can require a specific final tool; its projector must be
pure and deterministic because recovery may reevaluate recorded results. Completion calls
must be the sole application call in their batch.

## Model and resource ownership

Effect AI model Layers provide generation and provider/model identity. Supply them through
`Effect.provide`, or use `Agent.withModel` to create an explicit Binding. A Binding pairs
behavior with model selection; it still acquires no resources and leaves client requirements
visible. Tool handlers, storage, and provider clients belong in the application Layer.

Durable hosts register a current Binding under the stable Agent ID. Pending operations retain
their original replay contracts. Declare tool replay versions accurately when handler behavior
changes; missing or incompatible bindings leave accepted work owed. See
[durability](/concepts/durability/) and the [package reference](/reference/packages/).

## Input and disclosure

Typed entry points accept the input Schema's **encoded** type; instruction functions receive
its decoded type. Use `runUnknown`, `streamUnknown`, or `startUnknown` at an untrusted input
boundary. Invalid input fails before model execution.

`inputPrompt` changes model disclosure only. Durable admission still retains complete input,
and authorization receives it. Projection failure never falls back to sending the full value.
Keep credentials in host services and separately control tool results, history, and retrieved context.

For a complete application with tools and a durable host, see the
[travel planner](https://github.com/yielded-dev/agent/tree/main/examples/travel-planner).
