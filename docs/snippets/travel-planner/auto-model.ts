// #region catalog

import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect/ai-typesafe";
import { Agent, AgentRuntime, Identifiers, InMemory } from "@yielded/agent";
import { AutoModel } from "@yielded/agent-ai-decision";
import { Config, Context, Effect, Layer, Schema } from "effect";
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

export class ThreadAccessDenied extends Schema.TaggedError<ThreadAccessDenied>()(
  "ThreadAccessDenied",
  { threadId: Identifiers.ThreadId },
) {}

export class ThreadAuthorization extends Context.Service<
  ThreadAuthorization,
  {
    readonly authorize: (threadId: Identifiers.ThreadId) => Effect.Effect<void, ThreadAccessDenied>;
  }
>()("travel-planner/ThreadAuthorization") {}

const demoThreadId = Identifiers.ThreadId.make("auto-example");

// This local demo grants access only to its own Thread.
// An application supplies its authenticated Thread-ownership policy here.
const AuthorizationLive = Layer.succeed(ThreadAuthorization, {
  authorize: (threadId) =>
    threadId === demoThreadId ? Effect.void : new ThreadAccessDenied({ threadId }),
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
const Live = Layer.mergeAll(
  AuthorizationLive,
  DecisionLive,
  OpenAiLive,
  InMemory.layer,
  AutoModel.layerMemory(),
);

// #region runs
export const run = Effect.fnUntraced(
  function* (threadId: Identifiers.ThreadId, rawInput: unknown) {
    const authorization = yield* ThreadAuthorization;

    yield* authorization.authorize(threadId);
    const input = yield* Schema.decodeUnknownEffect(Assistant.input)(rawInput);
    const model = yield* ThreadModels.resolve({ threadId, state: { task: input } });

    return yield* AgentRuntime.run(Agent.withModel(Assistant, model), input, { threadId });
  },
  Effect.timeout("30 seconds"),
  Effect.scoped,
);
// #endregion runs

export const program = Effect.gen(function* () {
  yield* run(demoThreadId, "Compare taking a train or bus from Lisbon to Porto.");

  // Resolution reuses the committed choice on follow-ups.
  return yield* run(demoThreadId, "Which would you choose for comfort?");
}).pipe(Effect.provide(Live));
