# @yielded/agent-ai-decision

Choose a native Effect language model once per thread with `AutoModel`. The host explicitly
resolves a model before binding its agent; follow-up runs retain that choice. Resolve each child
explicitly when it needs its own choice. The upstream `DecisionModel` service evaluates the
application-approved catalog.

```ts
import { OpenAiLanguageModel } from "@effect/ai-openai";
import { AutoModel } from "@yielded/agent-ai-decision";

const ThreadModels = AutoModel.make({
  version: "profiles-v1",
  models: {
    routine: {
      description: "Low cost; routine tasks",
      model: OpenAiLanguageModel.model("gpt-6-luna"),
    },
    complex: {
      description: "Difficult reasoning and ambiguous requirements",
      model: OpenAiLanguageModel.model("gpt-6-astra"),
    },
  },
});
```

At your host's admission boundary, authorize Thread access, validate input, and choose the state
the decision provider may receive. For an agent with string input, the workflow is:

```ts
import { Agent, AgentRuntime } from "@yielded/agent";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const run = Effect.gen(function* () {
  yield* authorizeThread(threadId);
  const input = yield* Schema.decodeUnknownEffect(agent.input)(rawInput);
  const model = yield* ThreadModels.resolve({ threadId, state: { task: input } });

  return yield* AgentRuntime.run(Agent.withModel(agent, model), input, { threadId });
}).pipe(Effect.timeout("30 seconds"), Effect.scoped);
```

Supply a native `DecisionModel`, such as `TypeSafeDecisionModel.model("jev-latest")` from
`@effect/ai-typesafe`, native provider clients, and one shared `AutoModel.layerMemory()` around
the application's resolutions and runs. Keep provider services alive through execution in the
same caller Scope. The outer timeout covers selection and execution; the agent's own generation
budget starts after selection. At least two approved profiles are required. Neither constructing
the selector nor selecting a profile acquires a candidate model.

`resolve` obtains the committed choice from `AutoModel.SelectionStore` and captures provider
requirements in the returned native Layer. Durable hosts implement that store with atomic
creation and retain the complete version 2 record: thread and catalog identity, profile ID,
decision label, probabilities, optional confidence, and usage. Retain old catalogs while their
threads remain active. Wrong-thread, missing-profile, catalog-version, and record-version
mismatches fail without reselection or mutation.

For manual persistence, `select` returns a model and record; persist the full record before
generation. Each execution of `select` makes a new decision. `restore` validates a retained
record and returns its original model without calling the decision provider. The in-memory
store retains up to 10,000 attempted Thread IDs by default, fails at capacity, and provides no
restart recovery. Selection usage remains in the record, separate from generative usage.

For ordinary assessments, import `Decision` and `DecisionModel` from `effect/ai`.
Use `LanguageModelDecisionModel.layer` to answer those decisions with any native language model
that supports structured output. Provide your chosen model Layer to the adapter; provider clients
and configuration remain application-owned. Its probabilities are LLM estimates, not calibrated
confidence scores. Native validation rejects invalid distributions; retries and failover are explicit.
See the [language-model example](examples/language-model.ts) and
[provider setup](https://yielded.dev/agent/reference/decision-models#language-model-adapter).

See the [reference](https://yielded.dev/agent/reference/decision-models#automodel) for ownership,
configuration, and retention requirements, or the runnable [decision](examples/decision.ts),
[direct client](examples/evaluate.ts), and [tool](examples/tool.ts) examples.
