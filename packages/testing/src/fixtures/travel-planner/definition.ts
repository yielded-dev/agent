/**
 * Travel request schemas, catalog services, and a bounded planner agent for deterministic workflows.
 *
 * @since 0.1.0
 */
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/**
 * A non-empty branded airport identifier used by the travel fixtures.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AirportCode = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/AirportCode"),
);

/**
 * The decoded value of the AirportCode schema.
 *
 * @category models
 * @since 0.1.0
 */
export type AirportCode = typeof AirportCode.Type;

/**
 * A non-empty branded quote identifier linking search results to holds and bookings.
 *
 * @category schemas
 * @since 0.1.0
 */
export const QuoteId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/QuoteId"),
);

/**
 * The decoded value of the QuoteId schema.
 *
 * @category models
 * @since 0.1.0
 */
export type QuoteId = typeof QuoteId.Type;

/**
 * A trip request with route, dates, party size, budget, and USD currency.
 *
 * @category schemas
 * @since 0.1.0
 */
export class TripRequest extends Schema.Class<TripRequest>("TripRequest")({
  request: Schema.NonEmptyString,
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
  budgetCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
}) {}

/**
 * Route, departure date, and party size sent to the flight catalog.
 *
 * @category schemas
 * @since 0.1.0
 */
export class FlightQuery extends Schema.Class<FlightQuery>("FlightQuery")({
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/**
 * Destination, dates, stay length, and party size sent to the lodging catalog.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LodgingQuery extends Schema.Class<LodgingQuery>("LodgingQuery")({
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/**
 * Destination, dates, stay length, and party size sent to the activity catalog.
 *
 * @category schemas
 * @since 0.1.0
 */
export class ActivityQuery extends Schema.Class<ActivityQuery>("ActivityQuery")({
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/**
 * A quoted flight with its estimated price in USD cents.
 *
 * @category schemas
 * @since 0.1.0
 */
export class FlightOption extends Schema.Class<FlightOption>("FlightOption")({
  quoteId: QuoteId,
  flight: Schema.String,
  estimatedCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
}) {}

/**
 * A lodging description with its estimated price in USD cents.
 *
 * @category schemas
 * @since 0.1.0
 */
export class LodgingOption extends Schema.Class<LodgingOption>("LodgingOption")({
  lodging: Schema.String,
  estimatedCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
}) {}

/**
 * A successful empty activity search is distinct from supplier unavailability.
 *
 * @category schemas
 * @since 0.1.0
 */
export class ActivitySearchResult extends Schema.Class<ActivitySearchResult>(
  "ActivitySearchResult",
)({
  activities: Schema.Array(Schema.String),
}) {}

/**
 * A review-only itinerary retaining assumptions, unresolved constraints, and the quoted price.
 *
 * @category schemas
 * @since 0.1.0
 */
export class Itinerary extends Schema.Class<Itinerary>("Itinerary")({
  title: Schema.String,
  route: Schema.String,
  dates: Schema.String,
  flight: Schema.String,
  lodging: Schema.String,
  activities: Schema.Array(Schema.String),
  estimatedTotalCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
  quoteId: QuoteId,
  assumptions: Schema.Array(Schema.String),
  unresolvedConstraints: Schema.Array(Schema.String),
  nextAction: Schema.Literal("review"),
}) {}

/**
 * The collection of itineraries returned by the planner.
 *
 * @category schemas
 * @since 0.1.0
 */
export class TravelPlan extends Schema.Class<TravelPlan>("TravelPlan")({
  itineraries: Schema.Array(Itinerary),
}) {}

const unavailableFields = { query: Schema.String, message: Schema.String };

/**
 * The flight catalog could not satisfy the supplied query.
 *
 * @category errors
 * @since 0.1.0
 */
export class FlightUnavailable extends Schema.TaggedError<FlightUnavailable>()(
  "FlightUnavailable",
  unavailableFields,
) {}

/**
 * The lodging catalog could not satisfy the supplied query.
 *
 * @category errors
 * @since 0.1.0
 */
export class LodgingUnavailable extends Schema.TaggedError<LodgingUnavailable>()(
  "LodgingUnavailable",
  unavailableFields,
) {}

/**
 * The activity catalog could not satisfy the supplied query.
 *
 * @category errors
 * @since 0.1.0
 */
export class ActivityUnavailable extends Schema.TaggedError<ActivityUnavailable>()(
  "ActivityUnavailable",
  unavailableFields,
) {}

/**
 * Travel instructions could not be produced for the trip.
 *
 * @category errors
 * @since 0.1.0
 */
export class GuidanceFailure extends Schema.TaggedError<GuidanceFailure>()("GuidanceFailure", {
  message: Schema.String,
}) {}

/**
 * Supplies a quoted flight or a typed unavailability result for a flight query.
 *
 * @category services
 * @since 0.1.0
 */
export class FlightCatalog extends Context.Service<
  FlightCatalog,
  { readonly search: (query: FlightQuery) => Effect.Effect<FlightOption, FlightUnavailable> }
>()("@effect-agent/testing/travel-planner/FlightCatalog") {}

/**
 * Supplies lodging or a typed unavailability result for a lodging query.
 *
 * @category services
 * @since 0.1.0
 */
export class LodgingCatalog extends Context.Service<
  LodgingCatalog,
  { readonly search: (query: LodgingQuery) => Effect.Effect<LodgingOption, LodgingUnavailable> }
>()("@effect-agent/testing/travel-planner/LodgingCatalog") {}

/**
 * Supplies activity search results, including an empty result, or typed unavailability.
 *
 * @category services
 * @since 0.1.0
 */
export class ActivityCatalog extends Context.Service<
  ActivityCatalog,
  {
    readonly search: (
      query: ActivityQuery,
    ) => Effect.Effect<ActivitySearchResult, ActivityUnavailable>;
  }
>()("@effect-agent/testing/travel-planner/ActivityCatalog") {}

/**
 * Produces planner instructions from the current trip request.
 *
 * @category services
 * @since 0.1.0
 */
export class TravelGuidance extends Context.Service<
  TravelGuidance,
  { readonly instructions: (input: TripRequest) => Effect.Effect<string, GuidanceFailure> }
>()("@effect-agent/testing/travel-planner/TravelGuidance") {}

/**
 * A flight search tool backed by the FlightCatalog service.
 *
 * @category constructors
 * @since 0.1.0
 */
export const SearchFlights = Tool.make("search_flights", {
  parameters: FlightQuery,
  success: FlightOption,
  failure: FlightUnavailable,
  failureMode: "error",
  dependencies: [FlightCatalog],
});

/**
 * A lodging search tool backed by the LodgingCatalog service.
 *
 * @category constructors
 * @since 0.1.0
 */
export const SearchLodging = Tool.make("search_lodging", {
  parameters: LodgingQuery,
  success: LodgingOption,
  failure: LodgingUnavailable,
  failureMode: "error",
  dependencies: [LodgingCatalog],
});

/**
 * An activity search tool backed by the ActivityCatalog service.
 *
 * @category constructors
 * @since 0.1.0
 */
export const SearchActivities = Tool.make("search_activities", {
  parameters: ActivityQuery,
  success: ActivitySearchResult,
  failure: ActivityUnavailable,
  failureMode: "error",
  dependencies: [ActivityCatalog],
});

/**
 * The flight, lodging, and activity search tools available to the planner.
 *
 * @category constructors
 * @since 0.1.0
 */
export const TravelPlannerToolkit = Toolkit.make(SearchFlights, SearchLodging, SearchActivities);

/**
 * Connects planner search handlers to their catalog services.
 *
 * @category layers
 * @since 0.1.0
 */
export const TravelPlannerToolkitLayer = TravelPlannerToolkit.toLayer({
  search_flights: (query) => Effect.flatMap(FlightCatalog, (catalog) => catalog.search(query)),
  search_lodging: (query) => Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(query)),
  search_activities: (query) => Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(query)),
});

/**
 * A planner that searches three catalogs concurrently and returns a review-only itinerary.
 *
 * @category constructors
 * @since 0.1.0
 */
export const TravelPlanner = Agent.make("travel-planner", {
  input: TripRequest,
  output: TravelPlan,
  instructions: (input) =>
    Effect.flatMap(TravelGuidance, (guidance) => guidance.instructions(input)),
  toolkit: TravelPlannerToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 3,
  }),
  description: "Build one review-only itinerary from bounded parallel deterministic searches.",
});
