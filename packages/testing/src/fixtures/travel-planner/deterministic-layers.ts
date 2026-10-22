import { IdGenerator } from "@yielded/agent/id-generator";
import { ThreadId, RunId, TurnId } from "@yielded/agent/identifiers";
import { RunContextPreparationPassthrough } from "@yielded/agent/run-options";
import { ThreadHistory } from "@yielded/agent/thread-history";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import {
  ActivityCatalog,
  ActivitySearchResult,
  ActivityUnavailable,
  FlightCatalog,
  FlightOption,
  FlightUnavailable,
  LodgingCatalog,
  LodgingOption,
  LodgingUnavailable,
  QuoteId,
  TravelGuidance,
  TravelPlannerToolkitLayer,
} from "./definition.ts";

const flight = FlightOption.make({
  quoteId: Schema.decodeSync(QuoteId)("quote-sfo-lhr-001"),
  flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
  estimatedCents: 180_000,
  currency: "USD",
});

const lodging = LodgingOption.make({
  lodging: "Bloomsbury House · refundable studio · 4 nights",
  estimatedCents: 104_000,
  currency: "USD",
});

const activities = ActivitySearchResult.make({
  activities: ["British Museum timed entry", "Thames evening walk"],
});

export const FlightCatalogLayer = Layer.succeed(
  FlightCatalog,
  FlightCatalog.of({
    search: (query) =>
      query.origin === query.destination
        ? Effect.fail(
            FlightUnavailable.make({
              query: `${query.origin}-${query.destination}`,
              message: "Origin and destination must differ.",
            }),
          )
        : Effect.succeed(flight),
  }),
);

export const LodgingCatalogLayer = Layer.succeed(
  LodgingCatalog,
  LodgingCatalog.of({
    search: (query) =>
      query.nights < 1
        ? Effect.fail(
            LodgingUnavailable.make({
              query: query.destination,
              message: "At least one night is required.",
            }),
          )
        : Effect.succeed(lodging),
  }),
);

export const ActivityCatalogLayer = Layer.succeed(
  ActivityCatalog,
  ActivityCatalog.of({
    search: (query) =>
      query.destination === ""
        ? Effect.fail(
            ActivityUnavailable.make({
              query: query.destination,
              message: "Destination is required.",
            }),
          )
        : Effect.succeed(activities),
  }),
);

export const TravelGuidanceLayer = Layer.succeed(
  TravelGuidance,
  TravelGuidance.of({
    instructions: (input) =>
      Effect.succeed(
        [
          "You are the Effect Agent Travel Planner P1 interpreter fixture.",
          `The user asked: ${input.request}`,
          "Call search_flights, search_lodging, and search_activities exactly once in one Tool batch.",
          "Then return only a JSON object of exactly this shape, no prose:",
          '{"itineraries": [{"title": "<short itinerary name>", "route": "<origin-destination>", "dates": "<date range>", "flight": "<flight description from the Tool result>", "lodging": "<lodging description from the Tool result>", "activities": ["<activity>", "..."], "estimatedTotalCents": <positive integer total in cents>, "currency": "USD", "quoteId": "<quoteId from the flight Tool result>", "assumptions": ["<assumption>", "..."], "unresolvedConstraints": [], "nextAction": "review"}]}',
          "Use the Tool results verbatim; activity results may legitimately be an empty array.",
          "This is read-only planning. Require review before any mutation.",
        ].join("\n"),
      ),
  }),
);

export const DeterministicIdGeneratorLayer = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const thread = yield* Ref.make(0);
    const run = yield* Ref.make(0);
    const turn = yield* Ref.make(0);

    return IdGenerator.of({
      nextThreadId: Ref.updateAndGet(thread, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(ThreadId)(`thread-${n}`)),
      ),
      nextRunId: Ref.updateAndGet(run, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(RunId)(`run-${n}`)),
      ),
      nextTurnId: Ref.updateAndGet(turn, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(TurnId)(`turn-${n}`)),
      ),
    });
  }),
);

export const TravelPlannerRuntimeLayer = Layer.mergeAll(
  RunContextPreparationPassthrough,
  ThreadHistory.layer,
  TravelPlannerToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
  TravelGuidanceLayer,
  DeterministicIdGeneratorLayer,
);
