import * as AnthropicLanguageModel from "@effect/ai-anthropic/AnthropicLanguageModel";
import { Agent, AgentRuntime } from "@yielded/agent";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { AppLive } from "./setup";
import { TravelTools } from "./tools";

export const TravelPlanner = Agent.make("travel-planner", {
  input: Schema.Struct({ city: Schema.String, days: Schema.Int.check(Schema.isGreaterThan(0)) }),
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: ({ city, days }) =>
    `Find activities with search_activities, then plan ${days} days in ${city}.`,
  toolkit: TravelTools,
  policy: {
    maxTurns: 6,
    maxToolCalls: 10,
    maxDuration: "2 minutes",
  },
});

export const plan = AgentRuntime.run(TravelPlanner, { city: "Lisbon", days: 2 }).pipe(
  Effect.provide(AnthropicLanguageModel.model("claude-sonnet-5")),
  Effect.provide(AppLive),
);
