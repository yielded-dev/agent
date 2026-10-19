import { AgentId, ThreadId, RunId, SubmissionId } from "@yielded/agent/identifiers";
import {
  BatchId,
  CanonicalBatch,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  RecordId,
} from "@yielded/agent/records";
import * as Schema from "effect/Schema";

import { TravelPlan, TripRequest } from "./definition.ts";
import { expectedTravelPlan, phase1Trip } from "./scenarios.ts";

export const phase3TravelPlannerThreadId = Schema.decodeSync(ThreadId)("travel-planner-p3-thread");

export const phase3TravelPlannerProducerId = Schema.decodeSync(ProducerId)(
  "travel-planner-p3-producer",
);

export const phase3TravelPlannerRunId = Schema.decodeSync(RunId)("travel-planner-p3-run");

const deploymentId = Schema.decodeSync(DeploymentId)("travel-planner-p3-scripted");
const agentId = Schema.decodeSync(AgentId)("travel-planner");
const submissionId = Schema.decodeSync(SubmissionId)("travel-planner-p3-submission");

const digest = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/** Redacted, deterministic definition identities for the current fixture version. */
export const phase3TravelPlannerDefinitionDigests = DefinitionDigests.make({
  agent: digest("a"),
  model: digest("b"),
  tools: digest("c"),
});

const tripInput = Schema.encodeSync(TripRequest)(phase1Trip);
const travelPlanOutput = Schema.encodeSync(TravelPlan)(expectedTravelPlan);

const record = (recordId: string, createdAt: string, payload: unknown) =>
  Schema.decodeUnknownSync(RecordEnvelope)({
    recordId: Schema.decodeSync(RecordId)(recordId),
    family: "thread",
    schemaVersion: 1,
    createdAt,
    deploymentId,
    payload,
  });

/**
 * The first atomic append establishes the Thread and records its input.
 * Its encoded value is the redacted current-version persistence fixture.
 */
export const phase3TravelPlannerInitialBatch = CanonicalBatch.make({
  batchId: Schema.decodeSync(BatchId)("travel-planner-p3-initial"),
  producerId: phase3TravelPlannerProducerId,
  records: [
    record("travel-planner-p3-created", "2026-09-01T00:00:00.000Z", {
      _tag: "ThreadCreated",
      agentId,
      definitions: phase3TravelPlannerDefinitionDigests,
    }),
    record("travel-planner-p3-input", "2026-09-01T00:00:01.000Z", {
      _tag: "UserInputRecorded",
      submissionId,
      kind: "user",
      runId: phase3TravelPlannerRunId,
      input: tripInput,
    }),
  ],
});

/** The second append records the Schema-decoded itinerary and terminal Run result. */
export const phase3TravelPlannerCompletionBatch = CanonicalBatch.make({
  batchId: Schema.decodeSync(BatchId)("travel-planner-p3-completion"),
  producerId: phase3TravelPlannerProducerId,
  records: [
    record("travel-planner-p3-model", "2026-09-01T00:00:02.000Z", {
      _tag: "ModelCompleted",
      runId: phase3TravelPlannerRunId,
      output: travelPlanOutput,
    }),
    record("travel-planner-p3-completed", "2026-09-01T00:00:03.000Z", {
      _tag: "RunCompleted",
      runId: phase3TravelPlannerRunId,
      output: travelPlanOutput,
    }),
  ],
});

export const phase3TravelPlannerBatches = [
  phase3TravelPlannerInitialBatch,
  phase3TravelPlannerCompletionBatch,
] as const;

/** Portable current-version fixture; it contains no passenger identity or credentials. */
export const phase3TravelPlannerEncodedFixture: ReadonlyArray<typeof CanonicalBatch.Encoded> =
  Schema.encodeSync(Schema.Array(CanonicalBatch))(phase3TravelPlannerBatches);
