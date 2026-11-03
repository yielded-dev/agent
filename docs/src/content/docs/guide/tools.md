---
title: Tools & layers
description: Give agents typed actions, with application-owned authorization and explicit recovery rules.
---

Tools are the boundary between model suggestions and application effects. Effect AI owns their
schemas and handler Layers; your application owns resource access and the meaning of each action.
Yielded Agent validates calls, applies policy, and runs bounded batches.

## Define tools once

```ts twoslash
import { Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";

const Search = Tool.make("search_activities", {
  description: "Find activities in a city.",
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Array(Schema.String),
});

const TravelTools = Toolkit.make(Search);
const TravelToolsLive = TravelTools.toLayer({
  search_activities: ({ city }) =>
    Effect.succeed(city === "Lisbon" ? ["Riverside walk", "Food market"] : []),
});
```

Set the Agent's `toolkit` to `TravelTools` and provide `TravelToolsLive` alongside the model Layer.
Replace the sample data with an application service; its errors and dependencies remain visible
in Effect. The [travel-planner snippets](https://github.com/yielded-dev/agent/tree/main/docs/snippets/travel-planner)
show complete composition.

## Run batches deterministically

The engine validates the complete response before starting application handlers. Calls execute
with bounded concurrency; progress can arrive in completion order, while committed results retain
the model's declaration order. The next model request sees the complete batch.

Use [execution budgets](/concepts/budgets/) to bound turns, calls, output, and concurrency.
Large results are bounded before they enter history or the next prompt.

<a id="failure-remains-failure"></a>

## Keep tool failures typed

A declared `failure` Schema describes expected errors. By default, `failureMode: "error"`
propagates those errors and fails the Run. Set `failureMode: "return"` when the model should
receive a failed tool result and try another approach. Invalid native tool arguments can then
return a validation failure without starting the handler. Defects, interruption, and invalid
result encoding still fail.

Treat an empty search result as successful data, such as an empty array.

## Authorize tool calls

Install `RunToolAuthorization` for action-time policy. It checks executable application calls
before handlers start, including programmatic calls from [Code Mode](/guide/code-mode/).
Durable recovery checks authorization again for unfinished calls. The host must supply actual
caller authority; model-provided identifiers are untrusted. Handlers still enforce access to
the specific resources they use.

## Approve before execution

Effect AI tools can require approval. A pending approval prevents the handler from starting;
a durable host retains the request and accepts an authorized resolution later. An approval is
a decision about the proposed call, not permission to bypass current resource authorization.
Never infer approval from model prose.

## Discover tools progressively

`ToolExposure` limits declarations sent to a model, and `ToolDiscovery` lets it search an eligible
catalogue. Host visibility and inherited subagent grants constrain both discovery and execution.
Selection changes what the model sees; it grants no additional authority. Use this when a large
toolkit would crowd out task context. `ToolDiscovery.make` provides bounded literal search.
For custom ranking, define a native Tool with `DiscoveryTool` and
`ToolExecutionClass: "readonly"`, read `CurrentToolCatalog`, and use
`Schema.Struct({ toolNames: ToolNames })` as its success schema.

## Handle uncertain external effects

Unannotated tools have execution class `"uncertain"`. If an external effect may have happened but
its result was not recorded, durable recovery records **Unknown** and does not automatically repeat
it. Resolve the outcome from external evidence before continuing.

Declare `"readonly"` or `"idempotent"` only when the handler satisfies that contract. A
`DurableStep` can retain named results across retries, but its body may execute again. Use the
external system's idempotency or reconciliation API; Steps do not guarantee exactly-once effects.
See [durability](/concepts/durability/).

## Connect MCP servers

Use `effect/ai/McpClient` for MCP connections and discovery. Acquire the client in a Scope
and bind selected remote calls through ordinary Toolkit handlers. Its `callTool` operation
can decode structured output with the application's Schema; declare `McpClientError` from
`effect/ai/McpClientError` as the Tool's failure schema when returning remote failures to the model.
Keep the client Scope open while its tool handlers can run, and configure transport and
discovery limits through Effect's APIs.

Remote calls follow the same authorization, approval, budgets, and uncertainty rules as other
tools. Leave their execution class `"uncertain"` unless the application can establish a stronger
contract; server hints alone do not establish replay safety.

Use [subagents](/guide/subagents/) for delegation, [browser tools](/guide/browser/) for web
interaction, or [Code Mode](/guide/code-mode/) for a bounded program over several tools.
