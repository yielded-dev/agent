/**
 * Durable destination research harnesses with deterministic identifiers and invocation counts.
 *
 * @since 0.1.0
 */
import * as Agent from "@yielded/agent/agent";
import { DurableWorkerBinding, type ResolvedBinding } from "@yielded/agent/agent-registration";
import { type RuntimeBinding } from "@yielded/agent/agent-runtime";
import { type DurableSubmitOptions } from "@yielded/agent/durable-agent-runtime";
import { type ThreadId } from "@yielded/agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@yielded/agent/records";
import * as Subagent from "@yielded/agent/subagent";
import { delegationAllocationFromPolicy } from "@yielded/agent/subagent";
import { SubagentReservationsMemoryLive } from "@yielded/agent/subagent-reservations";
import { Principal, type IdempotencyKey } from "@yielded/agent/submission-ledger";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import type * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { DeterministicIdGeneratorLayer } from "./deterministic-layers.ts";
import type {
  DestinationBrief,
  DestinationReport,
  DestinationResearcherToolkit,
} from "./subagents.ts";
import {
  DestinationFacts,
  DestinationGuide,
  DestinationRecommendation,
  DestinationResearcher,
  DestinationResearcherToolkitLayer,
  destinationLookup,
  destinationReportFor,
  destinationResearchDelegation,
  destinationResearchPolicy,
  DestinationShortlist,
  encodedDestinationReport,
  mapResearchChildFailure,
  ResearchDispatchGate,
  TravelCoordinator,
} from "./subagents.ts";

/**
 * Registered coordinator and researcher bindings for durable delegation tests.
 *
 * @category models
 * @since 0.1.0
 */
export const s2TravelPlannerDeploymentId = Schema.decodeSync(DeploymentId)(
  "travel-planner-s2-deployment",
);

/**
 * Producer identity for the durable destination research fixture.
 *
 * @category models
 * @since 0.1.0
 */
export const s2TravelPlannerProducerId = Schema.decodeSync(ProducerId)(
  "travel-planner-s2-producer",
);

/**
 * Principal submitting the durable destination research fixture.
 *
 * @category models
 * @since 0.1.0
 */
export const s2TravelPlannerPrincipal = Schema.decodeSync(Principal)("travel-planner-s2-principal");

const digestOf = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/**
 * Redacted, deterministic parent (coordinator) definition digests for this fixture version.
 *
 * @category models
 * @since 0.1.0
 */
export const s2CoordinatorDigests = DefinitionDigests.make({
  agent: digestOf("a"),
  model: digestOf("b"),
  tools: digestOf("c"),
});

/**
 * Child Binding digests retained from host registration for durable recovery.
 *
 * @category models
 * @since 0.1.0
 */
export const s2ResearcherDigestStrings = {
  agent: "d".repeat(64),
  model: "e".repeat(64),
  tools: "f".repeat(64),
} as const;

/**
 * Definition digests used when restoring the destination researcher child.
 *
 * @category models
 * @since 0.1.0
 */
export const s2ResearcherDigests = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(s2ResearcherDigestStrings.agent),
  model: Schema.decodeSync(Digest)(s2ResearcherDigestStrings.model),
  tools: Schema.decodeSync(Digest)(s2ResearcherDigestStrings.tools),
});

/**
 * Durable admission options for one coordinator Submission on one mission lane.
 *
 * @category utils
 * @since 0.1.0
 */
export const s2TravelPlannerSubmitOptions = (
  threadId: ThreadId,
  idempotencyKey: IdempotencyKey,
): DurableSubmitOptions => ({
  threadId,
  principal: s2TravelPlannerPrincipal,
  idempotencyKey,
  definitions: s2CoordinatorDigests,
});

/**
 * The structural submit slice of the coordinator Binding (`DurableAgentRuntime.submit`).
 *
 * @category models
 * @since 0.1.0
 */
export const s2CoordinatorSubmitAgent = {
  definition: { id: TravelCoordinator.id, input: TravelCoordinator.input },
} as const;

/**
 * The per-invocation reservation the durable handler computes from the S1
 * delegation policy (`delegationAllocationFromPolicy`): the conservation
 * evidence in the S2 tests checks the ledger reservation rows and the
 * canonical `SubagentJoined.finalAccounting` against exactly this value.
 *
 * @category models
 * @since 0.1.0
 */
export const durableResearchAllocation = delegationAllocationFromPolicy(destinationResearchPolicy);

/**
 * The one scripted delegation Tool Call id of the durable coordinator Run.
 *
 * @category models
 * @since 0.1.0
 */
export const durableResearchCallId = "research-lhr-1";

/**
 * The child's own scripted guide-lookup Tool Call id.
 *
 * @category utils
 * @since 0.1.0
 */
export const durableChildLookupCallId = (destination: string): string => `lookup-${destination}`;

/**
 * The projected finding the parent joins (only the advisory crosses, SUB-015).
 *
 * @category utils
 * @since 0.1.0
 */
export const durableResearchFinding = (destination: string) => ({
  destination: destinationReportFor(destination).destination,
  summary: destinationReportFor(destination).advisory,
});

/**
 * The coordinator's expected final shortlist for one researched destination.
 *
 * @category utils
 * @since 0.1.0
 */
export const durableResearchShortlist = (destination: string): DestinationShortlist =>
  DestinationShortlist.make({
    recommendations: [
      DestinationRecommendation.make({
        destination: destinationReportFor(destination).destination,
        summary: destinationReportFor(destination).advisory,
      }),
    ],
    nextAction: "review",
  });

/**
 * The deterministic guide facts in encoded (wire) form: the "supplier truth"
 * an authorized operator records through `resolveUnknown` when a child guide
 * lookup stopped at an Unknown Outcome (DUR-017 — the framework never guesses
 * or replays it).
 *
 * @category utils
 * @since 0.1.0
 */
export const encodedDestinationFacts = (destination: string): unknown => {
  const report = destinationReportFor(destination);

  return Schema.encodeSync(DestinationFacts)(
    DestinationFacts.make({
      destination: report.destination,
      highlights: report.highlights,
      advisory: report.advisory,
    }),
  );
};

// ---------------------------------------------------------------------------
// Invocation-counting scripted models: the call counter and captured prompts live OUTSIDE the Model
// Layer, so they survive Layer rebuilds across Attempts and across separate
// runtime handles over the same SQLite file — "the child was never
// re-executed" is asserted, not assumed.
// ---------------------------------------------------------------------------

const scriptedUsage = { inputTokens: { total: 96 }, outputTokens: { total: 64 } };

/**
 * One scripted model whose behavior is keyed by the global invocation index.
 *
 * @category layers
 * @since 0.1.0
 */
export const makeInvocationCountingModel = Effect.fnUntraced(function* (
  name: string,
  script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>,
) {
  const calls = yield* Ref.make(0);
  const prompts = yield* Ref.make<ReadonlyArray<string>>([]);

  const model = Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const call = yield* Ref.getAndUpdate(calls, (value) => value + 1);

              yield* Ref.update(prompts, (previous) => [
                ...previous,
                JSON.stringify(request.prompt.content),
              ]);

              return Stream.fromIterable(script(call));
            }),
          ),
      }),
    ),
  );

  return { model, calls: Ref.get(calls), prompts: Ref.get(prompts) };
});

const delegationTurnParts = (
  toolCallId: string,
  destination: string,
  focus: string,
): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: toolCallId,
    name: "delegate_destination_research",
    params: { destination, focus },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const shortlistParts = (
  shortlist: DestinationShortlist,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "shortlist" },
  {
    type: "text-delta",
    id: "shortlist",
    delta: JSON.stringify(Schema.encodeSync(DestinationShortlist)(shortlist)),
  },
  { type: "text-end", id: "shortlist" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

const researcherLookupParts = (destination: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: durableChildLookupCallId(destination),
    name: "lookup_destination",
    params: { destination },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const researcherReportParts = (destination: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "destination-report" },
  { type: "text-delta", id: "destination-report", delta: encodedDestinationReport(destination) },
  { type: "text-end", id: "destination-report" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

// ---------------------------------------------------------------------------
// Durable delegation wiring: the immutable declaration uses the exact child
// Definition registered with the host. The coordinator retains that binding's
// digests when it establishes the child.
// ---------------------------------------------------------------------------

/**
 * Runtime wiring for the durable slice: the declaration plus its registered child Binding.
 *
 * @category layers
 * @since 0.1.0
 */
export const durableDestinationResearchHandlersLayer = <ModelProvides, ModelRequires>(
  childBinding: RuntimeBinding<
    typeof DestinationBrief,
    typeof DestinationReport,
    string,
    Toolkit.Tools<typeof DestinationResearcherToolkit>,
    ModelProvides,
    ModelRequires
  >,
) =>
  Subagent.layer(destinationResearchDelegation, childBinding, {
    mapChildFailure: mapResearchChildFailure,
  });

/**
 * Optional overrides for one durable research harness.
 *
 * @category models
 * @since 0.1.0
 */
export interface DurableResearchHarnessOptions {
  /** Researched destination; defaults to "LHR". */
  readonly destination?: string | undefined;
  /** Delegation focus; defaults to "museums". */
  readonly focus?: string | undefined;
  /** Host registration digests for the child Binding; defaults to `s2ResearcherDigests`. */
  readonly childRegistrationDigests?: DefinitionDigests | undefined;
}

/**
 * One durable coordinator/researcher pair with observable invocation counters.
 *
 * @category models
 * @since 0.1.0
 */
export interface DurableResearchHarness {
  /** Host registrations for `NodeDurableAgentRuntimeOptions.bindings` (parent + child). */
  readonly bindings: ReadonlyArray<ResolvedBinding>;
  /** Total coordinator model invocations across every Attempt and runtime handle. */
  readonly parentModelCalls: Effect.Effect<number>;
  /** JSON-encoded coordinator prompts in request order. */
  readonly parentPrompts: Effect.Effect<ReadonlyArray<string>>;
  /** Total researcher model invocations across every Attempt and runtime handle. */
  readonly childModelCalls: Effect.Effect<number>;
  /** JSON-encoded researcher prompts in request order (context-isolation evidence). */
  readonly childPrompts: Effect.Effect<ReadonlyArray<string>>;
  /** Deterministic guide-lookup handler executions (ordinary child Tool side effects). */
  readonly guideInvocations: Effect.Effect<number>;
}

/**
 * Build the S2 Travel Planner harness: an invocation-counting scripted
 * coordinator (Turn 1 declares the one delegation call, Turn 2 writes the
 * shortlist), an invocation-counting scripted researcher (Turn 1 consults the
 * guide, Turn 2 writes the report), and both worker Bindings captured with
 * their requirement Contexts via `DurableWorkerBinding.make` under the exact
 * fixture digests. The returned `bindings` are plain values: they can be
 * registered with several `NodeDurableAgentRuntime` stacks over the same SQLite
 * file while the counters keep counting across all of them.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeDurableResearchHarness = Effect.fnUntraced(function* (
  options?: DurableResearchHarnessOptions,
) {
  const destination = options?.destination ?? "LHR";
  const focus = options?.focus ?? "museums";

  const guideInvocations = yield* Ref.make(0);

  const guideLayer = Layer.succeed(
    DestinationGuide,
    DestinationGuide.of({
      lookup: (query) =>
        Ref.update(guideInvocations, (count) => count + 1).pipe(
          Effect.andThen(destinationLookup(query)),
        ),
    }),
  );

  const childToolkitLayer = DestinationResearcherToolkitLayer.pipe(Layer.provideMerge(guideLayer));

  const childModel = yield* makeInvocationCountingModel("destination-researcher-s2", (call) =>
    call === 0 ? researcherLookupParts(destination) : researcherReportParts(destination),
  );

  const childBinding = Agent.withModel(DestinationResearcher, childModel.model);

  const parentModel = yield* makeInvocationCountingModel("travel-coordinator-s2", (call) =>
    call === 0
      ? delegationTurnParts(durableResearchCallId, destination, focus)
      : shortlistParts(durableResearchShortlist(destination)),
  );

  const parentBinding = Agent.withModel(TravelCoordinator, parentModel.model);

  const delegationLayer = durableDestinationResearchHandlersLayer(childBinding).pipe(
    Layer.provide(
      Layer.mergeAll(
        childToolkitLayer,
        SubagentReservationsMemoryLive,
        DeterministicIdGeneratorLayer,
        ResearchDispatchGate.layerOpen,
      ),
    ),
  );

  const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
    parentBinding,
    s2CoordinatorDigests,
  ).pipe(Effect.provide(delegationLayer));

  const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
    childBinding,
    options?.childRegistrationDigests ?? s2ResearcherDigests,
  ).pipe(Effect.provide(childToolkitLayer));

  const harness: DurableResearchHarness = {
    bindings: [parentResolved, childResolved],
    parentModelCalls: parentModel.calls,
    parentPrompts: parentModel.prompts,
    childModelCalls: childModel.calls,
    childPrompts: childModel.prompts,
    guideInvocations: Ref.get(guideInvocations),
  };

  return harness;
});
