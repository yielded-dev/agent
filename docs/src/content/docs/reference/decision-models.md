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

Provide this Model Layer to the agent with a `DecisionModel`, provider clients,
and shared `SelectionStore`. The first model call selects; later turns and Runs
in that Thread retain the choice. New subagent Threads select independently.
The [complete example](https://github.com/yielded-dev/agent/blob/main/docs/snippets/travel-planner/auto-model.ts)
includes parent, child, provider, and store wiring.

### Configuration and records

| API or option                              | Behavior/default                                                           |
| ------------------------------------------ | -------------------------------------------------------------------------- |
| `make({ models, version, instructions? })` | Builds a native Model Layer; construction performs no selection            |
| `version`                                  | Required nonempty catalog identity; change with model settings             |
| `instructions`                             | Defaults to choosing the least expensive capable profile from descriptions |
| `layerMemory({ capacity? })`               | 10,000 distinct attempted Thread IDs; no expiry or eviction                |
| `resolve({ threadId, state })`             | Uses the store's retained choice                                           |
| `select` / `restore`                       | Explicit selection / validated restoration without selector I/O            |

### Thread ownership

Provide `AutoModel.layerMemory()` once around all parent Runs and child handlers.
It loses selections when its Scope closes. Durable hosts must supply an
application-owned `SelectionStore` that atomically commits a winning `SelectionRecord`
before generation and retains it across restarts. Persist records, never model Layers.

Keep catalogs needed by active Threads. Invalid records, wrong Thread or catalog
identity, and missing profiles fail with `AiError.InvalidRequestError`; they do not
silently trigger reselection. Selector usage is separate from generative Run usage.
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
