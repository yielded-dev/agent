/**
 * Bounded destination research delegation with scripted child models and explicit result projections.
 *
 * @since 0.1.0
 */
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import { type RuntimeBinding } from "@yielded/agent/agent-runtime";
import * as Subagent from "@yielded/agent/subagent";
import { SubagentPolicy } from "@yielded/agent/subagent";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { ScriptedTurnInput } from "../../ScriptedModel.ts";
import { AirportCode } from "./definition.ts";

// ---------------------------------------------------------------------------
// Destination Researcher: the S1 specialist child Agent.
// The researcher is a normal Agent with one deterministic
// travel-service Tool; it never becomes a second child loop (SUB-001/003).
// ---------------------------------------------------------------------------

/**
 * The destination requested from the research guide.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationQuery extends Schema.Class<DestinationQuery>("DestinationQuery")({
  destination: AirportCode,
}) {}

/**
 * Destination highlights and advisory text returned by the guide.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationFacts extends Schema.Class<DestinationFacts>("DestinationFacts")({
  destination: AirportCode,
  highlights: Schema.Array(Schema.String),
  advisory: Schema.NonEmptyString,
}) {}

/**
 * The guide could not supply facts for the requested destination.
 *
 * @category errors
 * @since 0.1.0
 */
export class DestinationGuideUnavailable extends Schema.TaggedError<DestinationGuideUnavailable>()(
  "DestinationGuideUnavailable",
  {
    destination: AirportCode,
    message: Schema.String,
  },
) {}

/**
 * Looks up destination facts or reports typed unavailability.
 *
 * @category services
 * @since 0.1.0
 */
export class DestinationGuide extends Context.Service<
  DestinationGuide,
  {
    readonly lookup: (
      query: DestinationQuery,
    ) => Effect.Effect<DestinationFacts, DestinationGuideUnavailable>;
  }
>()("@effect-agent/testing/travel-planner/DestinationGuide") {}

/**
 * A destination lookup tool backed by DestinationGuide.
 *
 * @category constructors
 * @since 0.1.0
 */
export const LookupDestination = Tool.make("lookup_destination", {
  parameters: DestinationQuery,
  success: DestinationFacts,
  failure: DestinationGuideUnavailable,
  failureMode: "error",
  dependencies: [DestinationGuide],
});

/**
 * The destination lookup tool available to the researcher child.
 *
 * @category constructors
 * @since 0.1.0
 */
export const DestinationResearcherToolkit = Toolkit.make(LookupDestination);

/**
 * Connects the child lookup handler to DestinationGuide.
 *
 * @category layers
 * @since 0.1.0
 */
export const DestinationResearcherToolkitLayer = DestinationResearcherToolkit.toLayer({
  lookup_destination: (query) => Effect.flatMap(DestinationGuide, (guide) => guide.lookup(query)),
});

/**
 * The destination and focus passed into the researcher child.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationBrief extends Schema.Class<DestinationBrief>("DestinationBrief")({
  destination: AirportCode,
  focus: Schema.NonEmptyString,
}) {}

/**
 * The bounded research report returned by the child.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationReport extends Schema.Class<DestinationReport>("DestinationReport")({
  destination: AirportCode,
  highlights: Schema.Array(Schema.String),
  advisory: Schema.NonEmptyString,
}) {}

/**
 * A child agent that looks up one destination and returns a bounded report.
 *
 * @category constructors
 * @since 0.1.0
 */
export const DestinationResearcher = Agent.make("destination-researcher", {
  input: DestinationBrief,
  output: DestinationReport,
  instructions:
    "Consult lookup_destination exactly once for the briefed airport, then return only a JSON destination report.",
  toolkit: DestinationResearcherToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  description: "Research one candidate destination with the deterministic travel guide.",
});

// ---------------------------------------------------------------------------
// Deterministic guide data and Layer (deterministic-layers.ts conventions).
// ---------------------------------------------------------------------------

const decodeAirportCode = Schema.decodeSync(AirportCode);

const guideFacts = new Map<string, DestinationFacts>([
  [
    "LHR",
    DestinationFacts.make({
      destination: decodeAirportCode("LHR"),
      highlights: ["Barbican brutalism walk", "Kew glasshouse survey"],
      advisory: "London favors museum mornings and riverside evenings.",
    }),
  ],
  [
    "CDG",
    DestinationFacts.make({
      destination: decodeAirportCode("CDG"),
      highlights: ["Marais passage crawl", "Seine bookstall loop"],
      advisory: "Paris rewards early galleries and late cafes.",
    }),
  ],
]);

/**
 * Deterministic guide lookup shared by the default and test-local guide Layers.
 *
 * @category utils
 * @since 0.1.0
 */
export const destinationLookup = (
  query: DestinationQuery,
): Effect.Effect<DestinationFacts, DestinationGuideUnavailable> => {
  const facts = guideFacts.get(query.destination);

  return facts === undefined
    ? Effect.fail(
        DestinationGuideUnavailable.make({
          destination: query.destination,
          message: "No deterministic guide entry exists for this destination.",
        }),
      )
    : Effect.succeed(facts);
};

const requireDestinationFacts = (destination: string): DestinationFacts => {
  const facts = guideFacts.get(destination);

  if (facts === undefined) {
    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Synchronous deterministic fixture lookup rejects programmer-supplied destination configuration.
    throw new Error(`No deterministic guide entry exists for destination ${destination}`);
  }

  return facts;
};

/**
 * The report the scripted researcher writes after consulting the guide.
 *
 * @category utils
 * @since 0.1.0
 */
export const destinationReportFor = (destination: string): DestinationReport => {
  const facts = requireDestinationFacts(destination);

  return DestinationReport.make({
    destination: facts.destination,
    highlights: facts.highlights,
    advisory: facts.advisory,
  });
};

/**
 * Encode the expected destination report as JSON for a scripted model response.
 *
 * @category utils
 * @since 0.1.0
 */
export const encodedDestinationReport = (destination: string): string =>
  JSON.stringify(Schema.encodeSync(DestinationReport)(destinationReportFor(destination)));

/**
 * Provides destination facts from the deterministic research corpus.
 *
 * @category layers
 * @since 0.1.0
 */
export const DestinationGuideLayer = Layer.succeed(
  DestinationGuide,
  DestinationGuide.of({ lookup: destinationLookup }),
);

/**
 * Child-side construction requirements of the delegation handler Layer.
 *
 * @category layers
 * @since 0.1.0
 */
export const DestinationResearchSupportLayer = Layer.mergeAll(
  DestinationResearcherToolkitLayer,
  DestinationGuideLayer,
);

// ---------------------------------------------------------------------------
// Delegation Definition: the coordinator sees exactly
// one Effect AI Tool with explicit input/result projections and finite bounds.
// ---------------------------------------------------------------------------

/**
 * The destination and focus requested through the coordinator delegation tool.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationResearchRequest extends Schema.Class<DestinationResearchRequest>(
  "DestinationResearchRequest",
)({
  destination: AirportCode,
  focus: Schema.NonEmptyString,
}) {}

/**
 * The projected destination findings permitted to cross back to the parent.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationResearchFindings extends Schema.Class<DestinationResearchFindings>(
  "DestinationResearchFindings",
)({
  destination: AirportCode,
  summary: Schema.NonEmptyString,
}) {}

/**
 * A child research failure exposed through its error tag.
 *
 * @category errors
 * @since 0.1.0
 */
export class DestinationResearchFailed extends Schema.TaggedError<DestinationResearchFailed>()(
  "DestinationResearchFailed",
  {
    childErrorTag: Schema.NonEmptyString,
  },
) {}

/**
 * Deterministic delegation-admission choreography seam. `prepareInput` awaits
 * this gate before the handler reserves budget or spawns, so tests can order
 * concurrent delegation preflights without sleeps. It also keeps the
 * projection's construction requirements honestly visible in the handler
 * Layer's `R`. The open Layer never waits.
 *
 * @category services
 * @since 0.1.0
 */
export class ResearchDispatchGate extends Context.Service<
  ResearchDispatchGate,
  { readonly awaitDispatch: (destination: string) => Effect.Effect<void> }
>()("@effect-agent/testing/travel-planner/ResearchDispatchGate") {
  /**
   * Allow every research dispatch immediately.
   *
   * @category layers
   * @since 0.1.0
   */
  static readonly layerOpen = Layer.succeed(
    this,
    ResearchDispatchGate.of({ awaitDispatch: () => Effect.void }),
  );
}

/**
 * Finite per-invocation bounds (SUB-009): each child may use two Turns and
 * one Tool Call; the parent Run may establish at most two children with at
 * most two running concurrently.
 *
 * @category constructors
 * @since 0.1.0
 */
export const destinationResearchPolicy = SubagentPolicy.make({
  maxChildren: 2,
  maxConcurrency: 2,
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "10 seconds",
});

/**
 * Delegates bounded destination research and projects only approved findings to the parent.
 *
 * @category constructors
 * @since 0.1.0
 */
export const destinationResearchDelegation = Subagent.make("delegate_destination_research", {
  description:
    "Research one candidate destination with the deterministic travel guide and return a bounded finding.",
  target: DestinationResearcher,
  parameters: DestinationResearchRequest,
  success: DestinationResearchFindings,
  failure: DestinationResearchFailed,
  prepareInput: Effect.fnUntraced(function* (request) {
    const gate = yield* ResearchDispatchGate;

    yield* gate.awaitDispatch(request.destination);

    return DestinationBrief.make({
      destination: request.destination,
      focus: `research:${request.focus}`,
    });
  }),
  // The explicit declassification boundary (SUB-015): only the advisory
  // crosses to the parent; guide highlights stay in the child Thread.
  projectResult: (report) =>
    Effect.succeed(
      DestinationResearchFindings.make({
        destination: report.destination,
        summary: report.advisory,
      }),
    ),
  policy: destinationResearchPolicy,
});

/**
 * Total mapping from every expected child Run failure to the declared Tool failure (SUB-028).
 *
 * @category utils
 * @since 0.1.0
 */
export const mapResearchChildFailure = (failure: {
  readonly _tag: string;
}): DestinationResearchFailed => DestinationResearchFailed.make({ childErrorTag: failure._tag });

/**
 * Runtime wiring: pair the immutable delegation with one explicit child Binding.
 *
 * @category layers
 * @since 0.1.0
 */
export const destinationResearchHandlersLayer = <ModelProvides, ModelRequires>(
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

// ---------------------------------------------------------------------------
// Travel Coordinator: the parent Agent that delegates destination research.
// ---------------------------------------------------------------------------

/**
 * The coordinator mission and candidate destinations.
 *
 * @category schemas
 * @since 0.1.0
 */
export class ResearchMission extends Schema.Class<ResearchMission>("ResearchMission")({
  request: Schema.NonEmptyString,
  candidates: Schema.Array(AirportCode).check(Schema.isMinLength(1)),
}) {}

/**
 * One researched destination and its bounded recommendation.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationRecommendation extends Schema.Class<DestinationRecommendation>(
  "DestinationRecommendation",
)({
  destination: AirportCode,
  summary: Schema.NonEmptyString,
}) {}

/**
 * The coordinator output containing researched destination recommendations.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DestinationShortlist extends Schema.Class<DestinationShortlist>(
  "DestinationShortlist",
)({
  recommendations: Schema.Array(DestinationRecommendation),
  nextAction: Schema.Literal("review"),
}) {}

/**
 * Parent-only transcript markers used to prove child context isolation (SUB-006/015).
 *
 * @category models
 * @since 0.1.0
 */
export const coordinatorConfidentialMarker = "coordinator-vault-7q42";
/**
 * Synthetic private mission marker used to detect leakage across the child boundary.
 *
 * @category models
 * @since 0.1.0
 */
export const missionConfidentialMarker = "traveler-dossier-19f";

/**
 * The destination research delegation tool exposed to the coordinator.
 *
 * @category constructors
 * @since 0.1.0
 */
export const TravelCoordinatorToolkit = Toolkit.make(destinationResearchDelegation.tool);

/**
 * A coordinator that delegates destination research and returns a bounded shortlist.
 *
 * @category constructors
 * @since 0.1.0
 */
export const TravelCoordinator = Agent.make("travel-coordinator", {
  input: ResearchMission,
  output: DestinationShortlist,
  instructions: [
    "You are the Effect Agent Travel Planner S1 delegation coordinator.",
    `Coordinator-only context: ${coordinatorConfidentialMarker}.`,
    "Call delegate_destination_research once per candidate in one Tool batch.",
    "Return only a JSON shortlist built from the delegated findings. This is read-only planning.",
  ].join("\n"),
  toolkit: TravelCoordinatorToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 3,
  }),
  description:
    "Coordinate bounded destination research through one declared attached delegation Tool.",
});

/**
 * The deterministic multi-destination mission submitted to the coordinator.
 *
 * @category models
 * @since 0.1.0
 */
export const researchMission = Schema.decodeSync(ResearchMission)({
  request: `Shortlist one September culture city; keep ${missionConfidentialMarker} inside the coordinator thread.`,
  candidates: ["LHR", "CDG"],
});

/**
 * The expected shortlist for the deterministic research mission.
 *
 * @category models
 * @since 0.1.0
 */
export const expectedDestinationShortlist = DestinationShortlist.make({
  recommendations: researchMission.candidates.map((destination) =>
    DestinationRecommendation.make({
      destination,
      summary: requireDestinationFacts(destination).advisory,
    }),
  ),
  nextAction: "review",
});

// ---------------------------------------------------------------------------
// Scripted turns (scenarios.ts conventions).
// ---------------------------------------------------------------------------

const scriptedUsage = { inputTokens: { total: 96 }, outputTokens: { total: 64 } };

/**
 * A tool call identifier, destination, and focus used to script coordinator delegation.
 *
 * @category models
 * @since 0.1.0
 */
export interface DestinationResearchCall {
  readonly id: string;
  readonly destination: string;
  readonly focus: string;
}

/**
 * One coordinator Turn that declares the given delegation Tool Calls in order.
 *
 * @category utils
 * @since 0.1.0
 */
export const coordinatorResearchTurn = (
  calls: ReadonlyArray<DestinationResearchCall>,
): ScriptedTurnInput => ({
  _tag: "Stream",
  parts: [
    ...calls.map((call) => ({
      type: "tool-call" as const,
      id: call.id,
      name: "delegate_destination_research",
      params: { destination: call.destination, focus: call.focus },
    })),
    { type: "finish" as const, reason: "tool-calls" as const, usage: scriptedUsage },
  ],
  termination: { _tag: "Complete" },
});

/**
 * The coordinator's final structured-output Turn.
 *
 * @category utils
 * @since 0.1.0
 */
export const coordinatorShortlistTurn = (shortlist: DestinationShortlist): ScriptedTurnInput => ({
  _tag: "Stream",
  parts: [
    { type: "text-start", id: "shortlist" },
    {
      type: "text-delta",
      id: "shortlist",
      delta: JSON.stringify(Schema.encodeSync(DestinationShortlist)(shortlist)),
    },
    { type: "text-end", id: "shortlist" },
    { type: "finish", reason: "stop", usage: scriptedUsage },
  ],
  termination: { _tag: "Complete" },
});

/**
 * Static researcher script for single-child tests: one guide lookup, then the report.
 *
 * @category utils
 * @since 0.1.0
 */
export const researcherHappyPathTurns = (
  destination: string,
): readonly [ScriptedTurnInput, ScriptedTurnInput] => [
  {
    _tag: "Stream",
    parts: [
      {
        type: "tool-call",
        id: `lookup-${destination}`,
        name: "lookup_destination",
        params: { destination },
      },
      { type: "finish", reason: "tool-calls", usage: scriptedUsage },
    ],
    termination: { _tag: "Complete" },
  },
  {
    _tag: "Stream",
    parts: [
      { type: "text-start", id: "destination-report" },
      {
        type: "text-delta",
        id: "destination-report",
        delta: encodedDestinationReport(destination),
      },
      { type: "text-end", id: "destination-report" },
      { type: "finish", reason: "stop", usage: scriptedUsage },
    ],
    termination: { _tag: "Complete" },
  },
];

// ---------------------------------------------------------------------------
// Destination-keyed researcher model for parallel-children tests.
// ---------------------------------------------------------------------------

/**
 * Deterministic controls for parallel researcher children whose completions
 * are released in a caller-selected order without clocks or sleeps.
 *
 * @category models
 * @since 0.1.0
 */
export interface DestinationResearcherControls {
  /** Await the first model request of the child researching this destination. */
  readonly awaitStarted: (destination: string) => Effect.Effect<void>;
  /** Allow the child researching this destination to produce its final report. */
  readonly release: (destination: string) => Effect.Effect<void>;
  /** JSON-encoded first-Turn child prompts in arrival order (isolation evidence). */
  readonly prompts: Effect.Effect<ReadonlyArray<string>>;
}

interface ResearcherGates {
  readonly started: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

const researcherLookupParts = (destination: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: `lookup-${destination}`,
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

/**
 * Build a deterministic researcher Model whose per-child behavior is keyed by
 * the destination named in the child's own prompt: Turn one records the
 * prompt, signals `started`, and calls the guide Tool; Turn two waits for the
 * caller's `release` before writing the report. Each child Run builds the
 * Model Layer inside its own scope.
 *
 * @category layers
 * @since 0.1.0
 */
export const makeDestinationResearcherModel = Effect.fnUntraced(function* (
  destinations: ReadonlyArray<string>,
) {
  const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
  const gates = new Map<string, ResearcherGates>();

  for (const destination of destinations) {
    gates.set(destination, {
      started: yield* Deferred.make<void>(),
      release: yield* Deferred.make<void>(),
    });
  }

  const gatesFor = (destination: string): Effect.Effect<ResearcherGates> =>
    Effect.suspend(() => {
      const entry = gates.get(destination);

      return entry === undefined
        ? // oxlint-disable-next-line effect/no-native-error -- Missing fixture gate is a native test assertion defect.
          Effect.die(new Error(`No researcher gates exist for destination ${destination}`))
        : Effect.succeed(entry);
    });

  const controls: DestinationResearcherControls = {
    awaitStarted: (destination) =>
      gatesFor(destination).pipe(Effect.flatMap((entry) => Deferred.await(entry.started))),
    release: (destination) =>
      gatesFor(destination).pipe(
        Effect.flatMap((entry) => Deferred.succeed(entry.release, undefined)),
        Effect.asVoid,
      ),
    prompts: Ref.get(prompts),
  };

  const model = Model.make(
    "scripted",
    "destination-researcher-scripted",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const promptJson = JSON.stringify(options.prompt.content);

                const destination = destinations.find((candidate) =>
                  promptJson.includes(candidate),
                );

                if (destination === undefined) {
                  return yield* Effect.die(
                    // oxlint-disable-next-line effect/no-native-error -- Deterministic model fixture assertion remains a native defect.
                    new Error("The researcher prompt names no scripted destination"),
                  );
                }
                const entry = yield* gatesFor(destination);
                const index = yield* Ref.getAndUpdate(turn, (value) => value + 1);

                if (index === 0) {
                  yield* Ref.update(prompts, (previous) => [...previous, promptJson]);
                  yield* Deferred.succeed(entry.started, undefined);

                  return Stream.fromIterable(researcherLookupParts(destination));
                }
                yield* Deferred.await(entry.release);

                return Stream.fromIterable(researcherReportParts(destination));
              }),
            ),
        });
      }),
    ),
  );

  return { controls, model };
});
