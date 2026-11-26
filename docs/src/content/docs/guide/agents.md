---
title: Agent definitions
description: Define behavior with schemas and tools; supply models and resources through Layers.
---

An Agent Definition is reusable behavior: input and output schemas, instructions, tools,
and policy. It owns no conversation, provider connection, or database. Your application
supplies those services when the definition runs.

```ts twoslash
import { Agent, AgentRuntime } from "@yielded/agent";
import { Schema } from "effect";
import { Toolkit } from "effect/ai";

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

| Field              | Purpose                                                                         |
| ------------------ | ------------------------------------------------------------------------------- |
| ID                 | Stable Agent identity; keep it independent of deployments                       |
| `input` / `output` | Effect Schemas for accepted input and validated final output                    |
| `instructions`     | Static instructions or a function of decoded input; may return an Effect        |
| `toolkit`          | Native Effect AI tools with application-provided handlers                       |
| `skills`           | Native Effect Skill bundles exposed through readonly retrieval tools            |
| `policy`           | Execution limits; omitted values use [defaults](/concepts/budgets/)             |
| `inputPrompt`      | Optional projection controlling what input reaches the model                    |
| `completion`       | Designated tool required for final output when configured                       |
| `updates`          | Schema for intentional intermediate findings                                    |
| `runDisposition`   | Schema and selector for ordinary completed output; omitted on budget exhaustion |

Final output is schema-validated JSON by default. Use `Output.text(Schema.String)` for an
ordinary text reply. To require a final tool, set `completion: { tool, project }`. The Agent must
then complete through that tool; assistant text cannot complete the Run.

`project` receives the tool's `{ parameters, result }` and returns the Agent's output. It must
be pure and deterministic because recovery may reevaluate recorded results. The completion tool
must be the sole application call in its batch.

## Give an Agent skills

Pass native Effect bundles through `skills`. The runtime advertises their names and descriptions,
then supplies `load_skill` and `read_skill_file` with their handlers. Your application supplies
handlers only for its own toolkit.

```ts
import * as Skill from "effect/ai/Skill";

const refunds = Skill.make({
  frontmatter: {
    name: "refunds",
    description: "Check refund eligibility and approval requirements.",
  },
  instructions: "Read references/policy.md before deciding refund eligibility.",
  files: {
    "references/policy.md": "Refunds over $100 require approval.",
  },
});

const billing = Agent.make("billing", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Help customers understand their refund eligibility.",
  toolkit: Toolkit.empty,
  skills: [refunds],
});
```

The definition captures bundle contents and rejects duplicate skill names. Application tools named
`load_skill` or `read_skill_file` are rejected when skills are enabled. A bundle with
`disable-model-invocation: true` is unavailable to the model. Each Subagent declares its own skills.

Loading returns complete instructions, optional compatibility information, and text-file paths as
an ordinary tool result. Skill results follow normal history and compaction rules; reload a skill
if its instructions leave context. Supporting files can be reread; `read_skill_file` accepts a one-based `offset`
and a positive `limit`, and returns `nextOffset` when another page remains. Reads do not require a
previous load. Binary files are not advertised and cannot be read by these tools.

Skill results obey the Agent's tool-result byte limit. Instructions that cannot fit completely
produce a recoverable size failure; file reads return complete lines within that limit. Lookup,
range, and unsupported-file failures also let the model correct its request. A single line that
cannot fit produces a size failure.

Skills grant no permissions and execute no scripts. Existing authorization, visibility, and Run
budgets apply to retrieval calls. `allowed-tools` does not grant authority. Other metadata has no
execution behavior.

Durable execution uses the same readonly-tool recovery and replay-version rules as other tools.
Future calls read the bundles in the current Agent definition. If you declare per-tool replay
versions, include `load_skill` and `read_skill_file` when skills are enabled.

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
