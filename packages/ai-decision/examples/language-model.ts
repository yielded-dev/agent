import { LanguageModelDecisionModel } from "@yielded/agent-ai-decision";
import * as Decision from "effect/ai/Decision";
import * as DecisionModel from "effect/ai/DecisionModel";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const Sentiment = Decision.make({
  input: Schema.String,
  decisions: {
    tone: Decision.classify({
      instructions: "Classify the sentiment.",
      criteria: {
        positive: "Expresses satisfaction",
        negative: "Expresses dissatisfaction",
      },
    }),
  },
});

// Supply any native LanguageModel with structured output to run this Effect.
export const program = DecisionModel.decide(Sentiment, {
  input: "This is excellent!",
}).pipe(
  Effect.map(({ answers }) => answers.tone.label),
  Effect.provide(LanguageModelDecisionModel.layer),
);
