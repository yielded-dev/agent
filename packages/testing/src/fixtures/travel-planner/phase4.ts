import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import { type DurableSubmitOptions } from "@yielded/agent/durable-agent-runtime";
import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { type ThreadId } from "@yielded/agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@yielded/agent/records";
import { Principal, type IdempotencyKey } from "@yielded/agent/submission-ledger";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import {
  ActivityCatalog,
  ActivityQuery,
  ActivitySearchResult,
  ActivityUnavailable,
  FlightCatalog,
  FlightOption,
  FlightQuery,
  FlightUnavailable,
  LodgingCatalog,
  LodgingOption,
  LodgingQuery,
  LodgingUnavailable,
  TravelGuidance,
  TravelPlan,
  TripRequest,
} from "./definition.ts";
import {
  ActivityCatalogLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  TravelGuidanceLayer,
} from "./deterministic-layers.ts";

export const phase4TravelPlannerDeploymentId = Schema.decodeSync(DeploymentId)(
  "travel-planner-p4-deployment",
);

export const phase4TravelPlannerProducerId = Schema.decodeSync(ProducerId)(
  "travel-planner-p4-producer",
);

export const phase4TravelPlannerPrincipal = Schema.decodeSync(Principal)(
  "travel-planner-p4-principal",
);

const digest = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/** Redacted, deterministic definition identities for the current fixture version. */
export const phase4TravelPlannerDefinitionDigests = DefinitionDigests.make({
  agent: digest("d"),
  model: digest("e"),
  tools: digest("f"),
});

/** Durable admission options for one Travel Planner Submission on one trip lane. */
export const phase4TravelPlannerSubmitOptions = (
  threadId: ThreadId,
  idempotencyKey: IdempotencyKey,
): DurableSubmitOptions => ({
  threadId,
  principal: phase4TravelPlannerPrincipal,
  idempotencyKey,
  definitions: phase4TravelPlannerDefinitionDigests,
});

/** Read-only catalog searches can repeat safely after an interrupted Attempt. */
export const DurableSearchFlights = Tool.make("search_flights", {
  parameters: Schema.Struct(FlightQuery.fields),
  success: FlightOption,
  failure: FlightUnavailable,
  failureMode: "error",
  dependencies: [FlightCatalog],
}).annotate(ToolExecutionClass, "readonly");

export const DurableSearchLodging = Tool.make("search_lodging", {
  parameters: Schema.Struct(LodgingQuery.fields),
  success: LodgingOption,
  failure: LodgingUnavailable,
  failureMode: "error",
  dependencies: [LodgingCatalog],
}).annotate(ToolExecutionClass, "readonly");

export const DurableSearchActivities = Tool.make("search_activities", {
  parameters: Schema.Struct(ActivityQuery.fields),
  success: ActivitySearchResult,
  failure: ActivityUnavailable,
  failureMode: "error",
  dependencies: [ActivityCatalog],
}).annotate(ToolExecutionClass, "readonly");

export const TravelPlannerPhase4Toolkit = Toolkit.make(
  DurableSearchFlights,
  DurableSearchLodging,
  DurableSearchActivities,
);

export const TravelPlannerPhase4ToolkitLayer = TravelPlannerPhase4Toolkit.toLayer({
  search_flights: (query) =>
    Effect.flatMap(FlightCatalog, (catalog) => catalog.search(FlightQuery.make(query))),
  search_lodging: (query) =>
    Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(LodgingQuery.make(query))),
  search_activities: (query) =>
    Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(ActivityQuery.make(query))),
});

/** A durable review-only planner over deterministic, safe-to-repeat catalog searches. */
export const TravelPlannerPhase4 = Agent.make("travel-planner-phase-4", {
  input: TripRequest,
  output: TravelPlan,
  instructions: (input) =>
    Effect.flatMap(TravelGuidance, (guidance) => guidance.instructions(input)),
  toolkit: TravelPlannerPhase4Toolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 3,
  }),
  description:
    "Durably plan one review-only itinerary from safe-to-repeat deterministic searches; supplier booking is not claimed safely replayable.",
});

/**
 * Everything a durable worker needs beyond the runtime stack, reusing the deterministic P1
 * travel-service Layers. The durable coordinator supplies its own deterministic `IdGenerator`,
 * so this Layer deliberately provides none.
 */
export const phase4TravelPlannerWorkerLayer = Layer.mergeAll(
  TravelPlannerPhase4ToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
  TravelGuidanceLayer,
);
