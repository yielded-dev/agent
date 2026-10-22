/**
 * A history-aware scripted planner and stable identities for persisted-runtime restart checks.
 *
 * @since 0.1.0
 */
import * as Agent from "@yielded/agent/agent";
import { DurableWorkerBinding, type ResolvedBinding } from "@yielded/agent/agent-registration";
import { DeploymentId } from "@yielded/agent/records";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { TravelPlan } from "./definition.ts";
import {
  TravelPlannerPhase4,
  phase4TravelPlannerDefinitionDigests,
  phase4TravelPlannerWorkerLayer,
} from "./phase4.ts";
import { expectedTravelPlan } from "./scenarios.ts";

/**
 * Stable deployment identity for the persisted-runtime restart fixture.
 *
 * @category models
 * @since 0.1.0
 */
export const phase6TravelPlannerDeploymentId = Schema.decodeSync(DeploymentId)(
  "travel-planner-p6-deployment",
);

/**
 * Each Thread Object mints its producer identity from this prefix and its Thread ID.
 *
 * @category models
 * @since 0.1.0
 */
export const phase6TravelPlannerProducerPrefix = "travel-planner-p6-producer";

const scriptedUsage = { inputTokens: { total: 128 }, outputTokens: { total: 96 } };

/**
 * The P1/P4 happy-path Tool Call identities (scenarios.ts, byte-stable since P1).
 *
 * @category models
 * @since 0.1.0
 */
export const phase6FlightCallId = "flight-call-1";

/**
 * Stable lodging tool call identifier used by the restart planner model.
 *
 * @category models
 * @since 0.1.0
 */
export const phase6LodgingCallId = "lodging-call-1";

/**
 * Stable activity tool call identifier used by the restart planner model.
 *
 * @category models
 * @since 0.1.0
 */
export const phase6ActivityCallId = "activity-call-1";

/** Turn 1 of the planner: the SAME three search declarations as `phase1HappyPathTurns`. */
const plannerSearchTurnParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: phase6FlightCallId,
    name: "search_flights",
    params: { origin: "SFO", destination: "LHR", departOn: "2026-09-14", travelers: 2 },
  },
  {
    type: "tool-call",
    id: phase6LodgingCallId,
    name: "search_lodging",
    params: { destination: "LHR", departOn: "2026-09-14", nights: 4, travelers: 2 },
  },
  {
    type: "tool-call",
    id: phase6ActivityCallId,
    name: "search_activities",
    params: { destination: "LHR", departOn: "2026-09-14", nights: 4, travelers: 2 },
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

/** Turn 2 of the planner: the SAME itinerary text as `phase1HappyPathTurns`. */
const plannerPlanTurnParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "itinerary-json" },
  {
    type: "text-delta",
    id: "itinerary-json",
    delta: JSON.stringify(Schema.encodeSync(TravelPlan)(expectedTravelPlan)),
  },
  { type: "text-end", id: "itinerary-json" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

const plannerDecide = (promptJson: string): Stream.Stream<Response.StreamPartEncoded> =>
  promptJson.includes(phase6FlightCallId)
    ? Stream.fromIterable(plannerPlanTurnParts)
    : Stream.fromIterable(plannerSearchTurnParts);

/**
 * Select the next response from committed search history so a fresh runtime incarnation
 * resumes the planner without retaining an in-memory model-turn counter.
 *
 * @category layers
 * @since 0.1.0
 */
export const phase6PlannerModel = Model.make(
  "scripted",
  "travel-planner-phase-4",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: (options) =>
        Stream.unwrap(Effect.sync(() => plannerDecide(JSON.stringify(options.prompt)))),
    }),
  ),
);

/**
 * Capture the planner registration used by the persisted-runtime restart scenarios.
 *
 * @category models
 * @since 0.1.0
 */
export const makePhase6TravelPlannerBindings: Effect.Effect<ReadonlyArray<ResolvedBinding>> =
  DurableWorkerBinding.make(
    Agent.withModel(TravelPlannerPhase4, phase6PlannerModel),
    phase4TravelPlannerDefinitionDigests,
  ).pipe(
    Effect.map((planner) => [planner]),
    Effect.provide(phase4TravelPlannerWorkerLayer),
  );
