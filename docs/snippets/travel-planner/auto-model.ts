// #region catalog

import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect/ai-typesafe";
import { Agent, AgentRuntime, Identifiers, InMemory } from "@yielded/agent";
import { AutoModel } from "@yielded/agent-ai-decision";
import { Config, Effect, Layer, Schema } from "effect";
import { Toolkit } from "effect/ai";
import { FetchHttpClient } from "effect/http";

// ---cut---
export const ThreadModels = AutoModel.make({
  version: "profiles-v1",
  models: {
    routine: {
      model: OpenAiLanguageModel.model("gpt-6-luna", { reasoning: { effort: "medium" } }),
      description: "Low cost. Extraction, summaries, and well-specified tasks with clear steps.",
    },
    complex: {
      model: OpenAiLanguageModel.model("gpt-6-astra", { reasoning: { effort: "medium" } }),
      description: "Higher cost. Difficult reasoning, subtle bugs, and ambiguous requirements.",
    },
  },
});
// #endregion catalog

export const Assistant = Agent.make("assistant", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Compare the travel options and answer the user.",
  toolkit: Toolkit.empty,
});

const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layerConfig()),
  Layer.provide(FetchHttpClient.layer),
);

const OpenAiLive = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
  Layer.provide(FetchHttpClient.layer),
);

// Share one selection store across the application's requests.
// Durable hosts supply a SelectionStore that commits records across restarts.
const Live = Layer.mergeAll(DecisionLive, OpenAiLive, InMemory.layer, AutoModel.layerMemory());

// #region runs
export const run = Effect.fnUntraced(
  function* <E, R>(
    threadId: Identifiers.ThreadId,
    rawInput: unknown,
    authorizeThread: (threadId: Identifiers.ThreadId) => Effect.Effect<void, E, R>,
  ) {
    yield* authorizeThread(threadId);
    const input = yield* Schema.decodeUnknownEffect(Assistant.input)(rawInput);
    const model = yield* ThreadModels.resolve({ threadId, state: { task: input } });

    return yield* AgentRuntime.run(Agent.withModel(Assistant, model), input, { threadId });
  },
  Effect.timeout("30 seconds"),
  Effect.scoped,
);
// #endregion runs

export const program = Effect.fnUntraced(function* <E, R>(
  authorizeThread: (threadId: Identifiers.ThreadId) => Effect.Effect<void, E, R>,
) {
  const threadId = Identifiers.ThreadId.make("auto-example");

  yield* run(threadId, "Compare taking a train or bus from Lisbon to Porto.", authorizeThread);

  // Resolution reuses the committed choice on follow-ups.
  return yield* run(threadId, "Which would you choose for comfort?", authorizeThread);
}, Effect.provide(Live));
