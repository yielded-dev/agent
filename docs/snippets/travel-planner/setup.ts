import * as AnthropicClient from "@effect/ai-anthropic/AnthropicClient";
import { InMemory } from "@yielded/agent";
import * as Config from "effect/Config";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";

import { TravelToolsLive } from "./tools";

const AnthropicLive = AnthropicClient.layerConfig({
  apiKey: Config.Redacted("ANTHROPIC_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const AppLive = Layer.mergeAll(TravelToolsLive, InMemory.layer, AnthropicLive);
