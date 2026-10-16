import * as OpenAiClient from "@effect/ai-openai/OpenAiClient";
import * as OpenAiLanguageModel from "@effect/ai-openai/OpenAiLanguageModel";
import { Agent } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
import * as Config from "effect/Config";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

export const planner = Agent.make("trip-planner", {
  input: Schema.String,
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: "Plan a trip with one itinerary entry per day.",
  toolkit: Toolkit.empty,
});

export const ModelLive = OpenAiLanguageModel.model("gpt-6-luna");

export const OpenAiLive = OpenAiClient.layerConfig({
  apiKey: Config.Redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const definitions = { agent: "v1", model: "gpt-6-luna", tools: "v1" };
