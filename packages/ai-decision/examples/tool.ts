import * as AiError from "effect/ai/AiError";
import * as Decision from "effect/ai/Decision";
import * as DecisionModel from "effect/ai/DecisionModel";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

const Assessment = Decision.make({
  input: Schema.Struct({ message: Schema.String }),
  decisions: {
    department: Decision.classify({
      instructions: "Which team should handle the ticket?",
      criteria: { billing: "Payments and refunds", technical: "Bugs and outages" },
    }),
  },
});

export const ClassifyTicket = Tool.make("classify_ticket", {
  description: "Classify a support ticket by department.",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.Struct({
    department: Schema.Literals(["billing", "technical"]),
    probability: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  }),
  failure: AiError.AiError,
  dependencies: [DecisionModel.DecisionModel],
});

export const TicketTools = Toolkit.make(ClassifyTicket);

export const TicketToolsLive = TicketTools.toLayer({
  classify_ticket: Effect.fnUntraced(function* ({ message }) {
    const { answers } = yield* DecisionModel.decide(Assessment, { input: { message } });

    return {
      department: answers.department.label,
      probability: answers.department.probabilities[answers.department.label],
    };
  }),
});

// A LanguageModel can use the same native toolkit. This call exercises the tool
// directly and still requires DecisionModel at execution time.
export const classifyTicket = Effect.gen(function* () {
  const tools = yield* TicketTools;

  const result = yield* tools.handle("classify_ticket", {
    message: "Please refund my duplicate charge.",
  });

  return yield* Stream.runCollect(result);
}).pipe(Effect.provide(TicketToolsLive));
