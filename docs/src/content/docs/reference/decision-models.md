---
title: Decision models
description: Evaluate typed decisions and retain a model choice for each Thread.
---

<a id="decision-models"></a>

Effect's `DecisionModel` evaluates schema-defined input and returns evidence.
Your application owns confidence thresholds, routing, and side effects.
`@yielded/agent-ai-decision` adds `AutoModel` for Thread model selection and a
language-model adapter for native decisions.

## Definitions and answers

Create a definition with `Decision.make({ input, decisions })`, then call
`DecisionModel.decide(definition, { input })`. Only include input the decision
provider is allowed to receive.

| Constructor            | Criteria                                 | Answer                                                      |
| ---------------------- | ---------------------------------------- | ----------------------------------------------------------- |
| `Decision.classify`    | At least two named labels                | `label`, probabilities, optional confidence                 |
| `Decision.rate`        | At least two ordered levels              | Numeric `rating`, label, probabilities, optional confidence |
| `Decision.probability` | Optional descriptions for true and false | Probability from 0 to 1                                     |

Responses contain `answers` and optional input/output token usage. Confidence is
provider-defined evidence, not an authorization decision or correctness guarantee.

## AutoModel

Describe at least two approved native model Layers and give the catalog a version:

```ts twoslash title="auto-model.ts" src="snippets/travel-planner/auto-model.ts#catalog"

```

At host admission, authorize Thread access, validate input, and pass only permitted
task context to `resolve`. Bind its returned native model to the Agent before running:

```ts twoslash
import { Agent, AgentRuntime, Identifiers } from "@yielded/agent";
import { Effect, Schema } from "effect";

import { Assistant, ThreadAuthorization, ThreadModels } from "./auto-model.ts";
declare const threadId: Identifiers.ThreadId;
declare const rawInput: unknown;
// ---cut---
const run = Effect.gen(function* () {
  const authorization = yield* ThreadAuthorization;

  yield* authorization.authorize(threadId);
  const input = yield* Schema.decodeUnknownEffect(Assistant.input)(rawInput);
  const model = yield* ThreadModels.resolve({ threadId, state: { task: input } });

  return yield* AgentRuntime.run(Agent.withModel(Assistant, model), input, { threadId });
}).pipe(Effect.timeout("30 seconds"), Effect.scoped);
```

Supply the host's `ThreadAuthorization`, a `DecisionModel`, native provider clients,
and a shared `SelectionStore` around this workflow. Keep selection and execution in
the caller's Scope and deadline; the Agent's generation budget starts after selection. Resolving the
same Thread reuses its committed choice. Child agents use their configured
native model; a separate per-child choice requires explicit host resolution.
The [complete example](https://github.com/yielded-dev/agent/blob/main/docs/snippets/travel-planner/auto-model.ts)
includes provider and store wiring shared across two requests.

### Configuration and records

| API or option                              | Behavior/default                                                              |
| ------------------------------------------ | ----------------------------------------------------------------------------- |
| `make({ models, version, instructions? })` | Returns an explicit selector; construction performs no provider work          |
| `version`                                  | Required nonempty catalog identity; change with model settings                |
| `instructions`                             | Defaults to choosing the least expensive capable profile from descriptions    |
| `layerMemory({ capacity? })`               | 10,000 distinct attempted Thread IDs; no expiry or eviction                   |
| `resolve({ threadId, state })`             | Commits or restores a choice and returns a native Layer with captured clients |
| `select({ threadId, state })`              | Makes a new decision on each execution; returns the model and record          |
| `restore(threadId, record)`                | Validates a retained record and returns its model without decision I/O        |

### Thread ownership

Provide `AutoModel.layerMemory()` once around the application's resolutions and
Runs. It loses selections when its Scope closes and fails at capacity instead of
evicting a choice. Durable hosts supply an application-owned `SelectionStore` that
atomically commits the winning record before generation and retains it across restarts.

Persist the complete version 2 `SelectionRecord`: Thread ID, catalog version,
profile ID, decision label and probabilities, optional confidence, and token usage.
Manual `select` callers must commit that record before using the returned model.
Acquire the returned native Layer during execution.

Keep catalogs needed by active Threads. Invalid records, wrong Thread or catalog
identity, and missing profiles fail with an `AiError` whose reason is
`InvalidRequestError`; they do not silently trigger reselection. Selector usage is
retained in the record, separate from generative Run usage.
There are no implicit retries, fallback models, thresholds, or mid-Thread switches.

## Language model adapter

`LanguageModelDecisionModel.layer` requires a native `LanguageModel` supporting
structured output and provides `DecisionModel`. All decisions share one
`generateObject` call. Probabilities are model estimates. The
[provider-neutral example](https://github.com/yielded-dev/agent/blob/main/packages/ai-decision/examples/language-model.ts)
shows the composition.

## TypeSafe client

Upstream `@effect/ai-typesafe` supplies `TypeSafeDecisionModel.model("jev-latest")`.
Provide `TypeSafeClient.layerConfig()` and an HTTP client. Configuration reads
`TYPESAFE_API_KEY`; `apiUrl` and `transformClient` are optional overrides. This
supplies a decision service, not a generative language model.

## Probability validation

Native validation checks labels, finite probabilities, and distribution sums within
`1e-6` of one. The language-model adapter keeps this strict behavior; TypeSafe
allows two-decimal rounding drift and rescales accepted distributions.

## Errors and policies

Input encoding failures use `InvalidUserInputError`; invalid answers use
`InvalidOutputError`. Structured-output and provider failures retain their typed
errors. Compose retries and timeouts explicitly. Read the
[decision example](https://github.com/yielded-dev/agent/blob/main/packages/ai-decision/examples/decision.ts)
for ordinary assessments without model selection.
