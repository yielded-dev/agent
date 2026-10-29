import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import { DurableWorkerBinding, type ResolvedBinding } from "@yielded/agent/agent-registration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  type DurableSubmitFailure,
  type DurableSubmitOptions,
  type Receipt,
} from "@yielded/agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointLocation,
} from "@yielded/agent/durable-failpoint";
import { DurableStep, DurableStepError } from "@yielded/agent/durable-step";
import { IdGenerator } from "@yielded/agent/id-generator";
import { ThreadId, RunId, ToolCallId, TurnId, type AgentId } from "@yielded/agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@yielded/agent/records";
import { childThreadIdFor } from "@yielded/agent/run-journal";
import { RunToolAuthorization } from "@yielded/agent/run-options";
import { layer as runStorageLayer, type StorageOwner } from "@yielded/agent/run-storage";
import type { SettlementPublisher } from "@yielded/agent/settlement-publisher";
import * as Subagent from "@yielded/agent/subagent";
import { SubagentPolicy } from "@yielded/agent/subagent";
import { SubagentReservationsMemoryLive } from "@yielded/agent/subagent-reservations";
import {
  ApprovalDecisionCommand,
  IdempotencyKey,
  Principal,
  ResolutionSafeToRetry,
  SubmissionLedger,
  SubmissionLookupById,
  UnknownResolutionCommand,
} from "@yielded/agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "@yielded/agent/testing/durable-failpoint-test-control";
import { ThreadStore, ThreadReader, ThreadStoreError } from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Model from "effect/ai/Model";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Cause from "effect/Cause";
import type * as CryptoDigest from "effect/crypto/Digest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

/** A failed coordinator recovery or failpoint coverage check. */
export class DurableCoordinatorConformanceViolation extends Schema.TaggedError<DurableCoordinatorConformanceViolation>()(
  "DurableCoordinatorConformanceViolation",
  { caseName: Schema.String, message: Schema.String },
) {}

/** The six coordinator scenario shapes in sweep order. */
const SCENARIOS = [
  "plain",
  "uncertain-tool",
  "durable-steps",
  "approval",
  "join",
  "delegation",
] as const;

type CoordinatorScenario = (typeof SCENARIOS)[number];

/**
 * Coordinator failpoint locations that none of the six scenario shapes can reach, recorded
 * honestly instead of silently claimed. These require operator, compaction, reservation,
 * background-worker, or Agent-update paths the shapes do not take. They are pinned in-process
 * by the P5/S2 suites (`packages/testing/test/durable-tools.test.ts` "resolveUnknown is idempotent across the
 * intent failpoint", `durable-runtime.test.ts` abort rows,
 * `durable-subagents.test.ts` abort propagation) and by the process-kill/eviction crash
 * matrices. The aggregate check requires the observed never-fired set to equal this list, so a
 * protocol change that silently stops exercising a location fails conformance.
 */
const UNREACHED_LOCATIONS: ReadonlyArray<DurableRuntimeFailpointLocation> = [
  // Active timeout recording is covered before/after append by durable-runtime.test.ts.
  "run:before-duration-append",
  "run:after-duration-append",
  "abort:after-intent",
  // Compaction requires a `contextTokenLimit` policy plus prior-Run history
  // none of the six scenario shapes carries; pinned in-process by the
  // RUN-026 rows in `packages/testing/test/durable-runtime.test.ts`
  // (compaction failpoint idempotence across re-drive).
  "compaction:before-canonical-append",
  "compaction:after-canonical-append",
  // These shapes use neither programmatic Tools nor grace finalization. Their reservations
  // are exercised before/after append in the public durable-runtime regression suite.
  "policy:before-reservation-append",
  "policy:after-reservation-append",
  "resolve:after-intent",
  // All six shapes retain their original Tool contracts, so none retires an unavailable
  // operation. The deployment continuity matrix in durable-runtime.test.ts exercises the
  // actual before/after-unavailable append boundaries, asserting absent/present canonical
  // ToolUnavailable results at the crash and one original-call settlement after re-drive.
  "tools:before-unavailable-append",
  "tools:after-unavailable-append",
  "subagent:after-child-abort-intent",
  // Background workers use retained delivery, source capacity, and child-origin paths absent
  // from these six attached/ordinary scenarios. The before/after creation and completion
  // boundaries are exercised by packages/effect-agent/test/durable/worker-host.test.ts.
  // Owner-wide stop is absent from these scenarios. The four boundaries and actual storage
  // restart are covered by packages/platform-node/test/background-workers.test.ts.
  "worker:before-stop-append",
  "worker:after-stop-append",
  "worker:before-stop-seal",
  "worker:after-stop-seal",
  "worker:before-source-append",
  "worker:after-source-append",
  "worker:before-origin-append",
  "worker:after-origin-append",
  "worker:before-completion-append",
  "worker:after-completion-append",
  // Root attached declarations keep their independent pool; nested shared subtree mutations
  // are covered by the same focused worker-host failpoint suite.
  "worker:before-subtree-append",
  "worker:after-subtree-append",
  // Atomic report publication has dedicated worker crash tests.
  "worker:before-report-append",
  "worker:after-report-append",
  // None of the six shapes emits Agent updates. Both publication boundaries are exercised separately
  // by packages/effect-agent/test/durable/worker-host.test.ts ("repairs an accepted parent update after ...").
  // Node restart/lost-ack coverage is in packages/platform-node/test/worker-updates.test.ts;
  // Cloudflare eviction/alarm recovery is in packages/platform-cloudflare/test/background-workers.test.ts.
  // These suites run independently; this sweep does not exercise the update boundaries.
  "update:before-canonical-append",
  "update:after-canonical-append",
];

// ---------------------------------------------------------------------------
// Deterministic fixtures (scripted prompt-shape models, agents, delegation)
// ---------------------------------------------------------------------------

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

const CHILD_DIGEST_STRINGS = {
  agent: "b".repeat(64),
  model: "c".repeat(64),
  tools: "d".repeat(64),
} as const;

const CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.agent),
  model: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.model),
  tools: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.tools),
});

const PRINCIPAL = Schema.decodeSync(Principal)("principal-certification");
const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallPart = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * Stateless scripted model that decides by PROMPT SHAPE instead of call count: while the
 * prompt carries no committed tool result the model declares `toolParts` (when given),
 * otherwise it answers with the final text. Deciding on the canonical prompt keeps every cell
 * deterministic regardless of where the injected fault fell — a re-invoked Turn re-declares
 * the same batch and a resumed batch flows into the final answer, so every scenario always
 * exercises its tool path and always converges.
 */
const promptShapeModel = (
  name: string,
  finalText: string,
  toolParts?: ReadonlyArray<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const hasToolResult = request.prompt.content.some((message) => message.role === "tool");

          const parts =
            toolParts === undefined || hasToolResult ? finalParts(finalText) : toolParts;

          return Stream.fromIterable(parts);
        },
      }),
    ),
  );

// Keep the fixture duration above the full eight-round recovery sweep so convergence does
// not depend on a fresh duration allowance per replacement Attempt. The durable-runtime
// duration-expiry cases exercise that policy separately.
const CERTIFICATION_MAX_DURATION = "10 minutes" as const;

const policy = AgentPolicy.make({
  maxTurns: 4,
  maxToolCalls: 4,
  maxDuration: CERTIFICATION_MAX_DURATION,
  toolConcurrency: 2,
});

const QuestionInput = Schema.Struct({ question: Schema.String });
const AnswerOutput = Schema.Struct({ answer: Schema.String });

/** plain / join: no tools — the pure Turn/submission/join seams. */
const plainDefinition = Agent.make("certify-plain", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

/** uncertain-tool: unannotated → fail-closed `uncertain`, enters the prepared/settled protocol. */
const Book = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});

const bookToolkit = Toolkit.make(Book);

const uncertainDefinition = Agent.make("certify-uncertain", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Book it.",
  toolkit: bookToolkit,
  policy,
});

/** durable-steps: declaring `DurableStep` as a dependency is what makes the Tool durable. */
const Itinerary = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});

const itineraryToolkit = Toolkit.make(Itinerary);

const stepsDefinition = Agent.make("certify-steps", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Reserve the itinerary.",
  toolkit: itineraryToolkit,
  policy,
});

/** approval: fail-closed — no `DurableApprovalResolver` Layer, so undecided approvals suspend. */
const BookApproval = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});

const approvalToolkit = Toolkit.make(BookApproval);

const approvalDefinition = Agent.make("certify-approval", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Book after approval.",
  toolkit: approvalToolkit,
  policy,
});

/** delegation: durable attached child plus an ordinary uncertain sibling in ONE batch. */
const childDefinition = Agent.make("certify-child", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: CERTIFICATION_MAX_DURATION,
    toolConcurrency: 1,
  }),
});

class CertifyDelegationFailed extends Schema.TaggedError<CertifyDelegationFailed>()(
  "CertifyDelegationFailed",
  { childErrorTag: Schema.String },
) {}

const researchDelegation = Subagent.make("delegate_research", {
  description: "Research one bounded question and return findings.",
  target: childDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: CertifyDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 2,
    maxConcurrency: 2,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: CERTIFICATION_MAX_DURATION,
  }),
});

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
});

const coordinatorDefinition = Agent.make("certify-coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate and look up, then answer as JSON.",
  toolkit: Toolkit.make(researchDelegation.tool, Lookup),
  policy: AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 3,
    maxDuration: CERTIFICATION_MAX_DURATION,
    toolConcurrency: 2,
  }),
});

const mapChildFailure = (failure: { readonly _tag: string }) =>
  CertifyDelegationFailed.make({ childErrorTag: failure._tag });

const DELEGATE_CALL = decodeToolCallId("delegate-1");

/** Fixture-only identity source consumed by the delegation Layer's ephemeral capture. */
const identifiers = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);

    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.getAndUpdate(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );

    return {
      nextThreadId: next(decodeThreadId, "certify-fixture-thread"),
      nextRunId: next(decodeRunId, "certify-fixture-run"),
      nextTurnId: next(decodeTurnId, "certify-fixture-turn"),
    };
  }),
);

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, identifiers);

// ---------------------------------------------------------------------------
// Coordinator scenario cells
// ---------------------------------------------------------------------------

interface CoordinatorCell {
  readonly bindings: ReadonlyArray<ResolvedBinding>;
  /** Idempotent submission batch — safe to replay verbatim after a submit-boundary fault. */
  readonly submit: Effect.Effect<ReadonlyArray<Receipt>, DurableSubmitFailure, DurableAgentRuntime>;
  /** All lanes of the cell in drive order, computed from the (possibly replayed) receipts. */
  readonly lanes: (receipts: ReadonlyArray<Receipt>) => ReadonlyArray<ThreadId>;
}

const submitOptionsFor = (slug: string, threadId: ThreadId): DurableSubmitOptions => ({
  threadId,
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(`certify-key-${slug}`),
  definitions: DIGESTS,
});

/** One single-agent cell: one lane, one Submission, one registered exact-digest binding. */
const makeSingleAgentCell = (
  definition: { readonly id: AgentId; readonly input: typeof QuestionInput },
  resolved: ResolvedBinding,
  slug: string,
): CoordinatorCell => {
  const threadId = decodeThreadId(`certify-${slug}`);

  const submit = Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    const receipt = yield* runtime.submit(
      { definition: { id: definition.id, input: definition.input } },
      { question: `certify ${slug}` },
      submitOptionsFor(slug, threadId),
    );

    return [receipt];
  });

  return {
    bindings: [resolved],
    submit,
    lanes: () => [threadId],
  };
};

const makeCell = Effect.fnUntraced(function* (scenario: CoordinatorScenario, slug: string) {
  switch (scenario) {
    case "plain": {
      const binding = Agent.withModel(
        plainDefinition,
        promptShapeModel("certify-plain", '{"answer":"done"}'),
      );

      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS);

      return makeSingleAgentCell(plainDefinition, resolved, slug);
    }
    case "uncertain-tool": {
      const binding = Agent.withModel(
        uncertainDefinition,
        promptShapeModel(
          "certify-uncertain",
          '{"answer":"booked"}',
          toolTurn(toolCallPart("book-1", "book", { ref: `r-${slug}` })),
        ),
      );

      const toolLayer = bookToolkit.toLayer({
        book: ({ ref }) => Effect.succeed({ confirmation: `confirmed-${ref}` }),
      });

      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS).pipe(
        Effect.provide(toolLayer),
      );

      return makeSingleAgentCell(uncertainDefinition, resolved, slug);
    }
    case "durable-steps": {
      const binding = Agent.withModel(
        stepsDefinition,
        promptShapeModel(
          "certify-steps",
          '{"answer":"reserved"}',
          toolTurn(toolCallPart("itinerary-1", "itinerary", { ref: `trip-${slug}` })),
        ),
      );

      const toolLayer = itineraryToolkit.toLayer({
        itinerary: Effect.fnUntraced(function* ({ ref }) {
          const step = yield* DurableStep;

          const flight = yield* step.do(
            "reserve-flight",
            Schema.String,
            Effect.succeed(`flight-${ref}`),
          );

          const lodging = yield* step.do(
            "reserve-lodging",
            Schema.String,
            Effect.succeed(`lodging-${ref}`),
          );

          return { state: `${flight}+${lodging}` };
        }),
      });

      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS).pipe(
        Effect.provide(toolLayer),
      );

      return makeSingleAgentCell(stepsDefinition, resolved, slug);
    }
    case "approval": {
      const binding = Agent.withModel(
        approvalDefinition,
        promptShapeModel(
          "certify-approval",
          '{"answer":"approved"}',
          toolTurn(toolCallPart("book-1", "book", { ref: `r-${slug}` })),
        ),
      );

      const toolLayer = approvalToolkit.toLayer({
        book: ({ ref }) => Effect.succeed({ confirmation: `confirmed-${ref}` }),
      });

      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS).pipe(
        Effect.provide(toolLayer),
      );

      return makeSingleAgentCell(approvalDefinition, resolved, slug);
    }
    case "join": {
      const binding = Agent.withModel(
        plainDefinition,
        promptShapeModel("certify-join", '{"answer":"host answer"}'),
      );

      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS);
      const threadId = decodeThreadId(`certify-${slug}`);

      const submitOne = Effect.fnUntraced(function* (key: string, question: string) {
        const runtime = yield* DurableAgentRuntime;

        return yield* runtime.submit(
          { definition: { id: plainDefinition.id, input: plainDefinition.input } },
          { question },
          {
            threadId,
            principal: PRINCIPAL,
            idempotencyKey: decodeIdempotencyKey(key),
            definitions: DIGESTS,
          },
        );
      });

      const cell: CoordinatorCell = {
        bindings: [resolved],
        submit: Effect.gen(function* () {
          const host = yield* submitOne(`certify-key-${slug}-host`, "host question");
          const queued = yield* submitOne(`certify-key-${slug}-queued`, "queued question");

          return [host, queued];
        }),
        lanes: () => [threadId],
      };

      return cell;
    }
    case "delegation": {
      const childBinding = Agent.withModel(
        childDefinition,
        promptShapeModel("certify-child", '{"answer":"child-answer"}'),
      );

      const parentBinding = Agent.withModel(
        coordinatorDefinition,
        promptShapeModel(
          "certify-parent",
          '{"report":"done"}',
          toolTurn(
            toolCallPart("delegate-1", "delegate_research", { topic: "paris" }),
            toolCallPart("lookup-1", "lookup", { key: "hotels" }),
          ),
        ),
      );

      const delegationLayer = Subagent.layer(researchDelegation, childBinding, {
        mapChildFailure,
      }).pipe(Layer.provide(delegationSupport));

      const lookupLayer = Toolkit.make(Lookup).toLayer({
        lookup: ({ key }) => Effect.succeed({ value: `found-${key}` }),
      });

      const parentResolved = yield* DurableWorkerBinding.make(parentBinding, DIGESTS).pipe(
        Effect.provide(Layer.mergeAll(delegationLayer, lookupLayer)),
      );

      const childResolved = yield* DurableWorkerBinding.make(childBinding, CHILD_DIGESTS);
      const threadId = decodeThreadId(`certify-${slug}`);

      const cell: CoordinatorCell = {
        bindings: [parentResolved, childResolved],
        submit: Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const receipt = yield* runtime.submit(
            { definition: { id: coordinatorDefinition.id, input: coordinatorDefinition.input } },
            { mission: "plan" },
            submitOptionsFor(slug, threadId),
          );

          return [receipt];
        }),
        lanes: (receipts) => {
          const parent = receipts.at(0);

          return parent === undefined
            ? [threadId]
            : [threadId, childThreadIdFor(parent.submissionId, DELEGATE_CALL)];
        },
      };

      return cell;
    }
  }
});

/** Maximum recovery/drive/unblock rounds before a cell is reported non-convergent. */
const MAX_REDRIVE_ROUNDS = 8;

/**
 * Verify one lane after convergence: canonical export + every lane Submission the ledger or
 * the log names (the same collection rule as the admin `verify` member), fed to the shared
 * invariant checker in convergence mode WITH the captured per-batch producer directory, so
 * retained history and active state are checked.
 */
const verifyLane = Effect.fnUntraced(function* (lane: ThreadId) {
  const store = yield* ThreadStore;

  if (store.verification === undefined)
    return yield* ThreadStoreError.make({
      operation: "coordinator conformance",
      message: "The adapter must provide bounded native Thread verification",
    });

  return yield* store.verification.verify({ threadId: lane, requireAllSettled: true });
});

const failureTagOf = <E>(cause: Cause.Cause<E>): string => {
  const failure = Cause.findErrorOption(cause);

  if (Option.isSome(failure)) {
    const error: unknown = failure.value;

    if (typeof error === "object" && error !== null && "_tag" in error) {
      return String(error._tag);
    }

    return String(error).slice(0, 256);
  }

  return "defect";
};

interface SweepOutcome {
  readonly failpointFired: boolean;
  readonly status: "converged" | "not-triggered" | "failed";
  readonly detail?: string;
}

/** Discover a clean path, or arm one location; both drives converge and verify real storage. */
const runSweepCell = Effect.fnUntraced(function* (
  scenario: CoordinatorScenario,
  location: DurableRuntimeFailpointLocation | undefined,
  reached: Set<DurableRuntimeFailpointLocation>,
) {
  const ledger = yield* SubmissionLedger;
  const control = yield* DurableRuntimeFailpointTestControl;
  const slug = `${scenario}-${location?.replaceAll(":", "-") ?? "discovery"}`;

  const failed = (detail: string, fired: boolean): SweepOutcome => ({
    failpointFired: fired,
    status: "failed",
    detail: detail.slice(0, 4_096),
  });

  const cell = yield* makeCell(scenario, slug);

  const runtime = yield* DurableAgentRuntime.pipe(
    Effect.provide(
      DurableAgentRuntime.layerWithBindings(cell.bindings).pipe(
        Layer.provide(RunToolAuthorization.allowAll),
      ),
    ),
  );

  const submit = cell.submit.pipe(Effect.provideService(DurableAgentRuntime, runtime));

  // One-shot arm: the fault fires at most once anywhere in the cell (initial drive OR a
  // re-drive round's public unblocking operation), modelling one crash at this boundary.
  const fired = yield* Ref.make(false);

  yield* control.setHandler((hit) =>
    Effect.suspend(() => {
      reached.add(hit);

      return hit !== location
        ? Effect.void
        : Ref.getAndSet(fired, true).pipe(
            Effect.flatMap((already) =>
              already
                ? Effect.void
                : Effect.fail(DurableRuntimeFailpointError.make({ location: hit })),
            ),
          );
    }),
  );

  // Submissions are idempotent (DUR-001): one replay recovers a submit-boundary fault.
  let receipts: ReadonlyArray<Receipt>;
  const firstSubmit = yield* Effect.exit(submit);

  if (Exit.isSuccess(firstSubmit)) {
    receipts = firstSubmit.value;
  } else {
    const secondSubmit = yield* Effect.exit(submit);

    if (Exit.isFailure(secondSubmit)) {
      yield* control.clear;

      return failed(
        `submission replay did not recover: ${failureTagOf(secondSubmit.cause)}`,
        yield* Ref.get(fired),
      );
    }
    receipts = secondSubmit.value;
  }
  const lanes = cell.lanes(receipts);

  const driveLane = (lane: ThreadId) => runtime.processThreadResolved(lane);

  const allSettled = Effect.gen(function* () {
    for (const receipt of receipts) {
      const snapshot = yield* ledger.lookup(
        SubmissionLookupById.make({ submissionId: receipt.submissionId }),
      );

      if (Option.isNone(snapshot) || snapshot.value.state !== "settled") return false;
    }

    return true;
  });

  // Re-drive to convergence using ONLY public operations: worker drives, recovery passes,
  // and the authorized DUR-017/approval unblocking paths chosen from `explainThread`.
  let converged = false;

  for (let round = 0; round < MAX_REDRIVE_ROUNDS && !converged; round++) {
    // A failed drive closes its scoped generation before recovery can select another owner.
    yield* Effect.exit(runtime.runRecovery());
    for (const lane of lanes) {
      yield* Effect.exit(driveLane(lane));
    }
    for (const lane of lanes) {
      const explains = yield* Effect.exit(runtime.explainThread(lane));

      if (Exit.isFailure(explains)) continue;
      for (const explanation of explains.value) {
        for (const unknown of explanation.evidence.unknownCalls) {
          if (unknown.resolved) continue;
          yield* Effect.exit(
            runtime.resolveUnknown(
              UnknownResolutionCommand.make({
                submissionId: explanation.submission.submissionId,
                toolCallId: unknown.toolCallId,
                author: "certification-runner",
                reason: `re-drive after injected fault at ${location}`,
                resolution: ResolutionSafeToRetry.make(),
              }),
            ),
          );
        }
        for (const pending of explanation.evidence.approvalsPending) {
          const decided = explanation.evidence.approvalDecisions.some(
            (decision) => decision.toolCallId === pending.toolCallId,
          );

          if (decided) continue;
          yield* Effect.exit(
            runtime.resolveApproval(
              ApprovalDecisionCommand.make({
                submissionId: explanation.submission.submissionId,
                toolCallId: pending.toolCallId,
                decision: "approved",
                resolver: "certification-runner",
                reason: `re-drive after injected fault at ${location}`,
              }),
            ),
          );
        }
      }
    }
    const settled = yield* Effect.exit(allSettled);

    converged = Exit.isSuccess(settled) && settled.value;
  }
  yield* control.clear;
  const wasFired = yield* Ref.get(fired);

  if (!converged) {
    return failed(`did not converge within ${MAX_REDRIVE_ROUNDS} re-drive rounds`, wasFired);
  }

  // Every lane must pass retained-history and active-state verification in convergence mode.
  const failedChecks: Array<string> = [];

  for (const lane of lanes) {
    const verdict = yield* Effect.exit(verifyLane(lane));

    if (Exit.isFailure(verdict)) {
      return failed(`lane ${lane} could not be verified: ${failureTagOf(verdict.cause)}`, wasFired);
    }
    for (const check of verdict.value.checks) {
      if (check.status === "failed") {
        failedChecks.push(
          `${lane}:${check.name}${check.detail === undefined ? "" : ` (${check.detail})`}`,
        );
      }
    }
  }
  if (failedChecks.length > 0) {
    return failed(`invariant checks failed: ${failedChecks.join("; ")}`, wasFired);
  }

  return {
    failpointFired: wasFired,
    status: wasFired ? "converged" : "not-triggered",
  } satisfies SweepOutcome;
});

const caseName = "coordinator recovery converges after injected faults";

/**
 * Exercise six deterministic scenarios over disposable, pristine storage. Discover each clean
 * path, arm every reached or unaccounted-for failpoint once, and require bounded recovery to
 * settled, natively verified history. The final never-fired set must match the known paths
 * outside these scenarios; every scenario must exercise at least one injected fault.
 *
 * Provide one combined adapter Layer with SubmissionLedger, ThreadStore, SettlementPublisher,
 * StorageOwner and Digest in a TestClock-backed environment. The helper owns its runtime Scope
 * and supplies a no-op WakeScheduler because it re-drives lanes explicitly. Injected failures
 * do not establish recovery after actual process loss or eviction.
 */
export const durableCoordinatorConformance: Effect.Effect<
  void,
  DurableCoordinatorConformanceViolation | ThreadStoreError,
  SubmissionLedger | ThreadStore | SettlementPublisher | StorageOwner | CryptoDigest.Digest
> = Effect.gen(function* () {
  const discoveries: Array<{
    readonly scenario: CoordinatorScenario;
    readonly outcome: SweepOutcome;
    readonly reached: Set<DurableRuntimeFailpointLocation>;
  }> = [];

  for (const scenario of SCENARIOS) {
    const reached = new Set<DurableRuntimeFailpointLocation>();
    const outcome = yield* runSweepCell(scenario, undefined, reached);

    discoveries.push({ scenario, outcome, reached });
  }
  const observed = new Set(discoveries.flatMap(({ reached }) => [...reached]));

  // Never silently omit a new location: arm it in every shape if neither clean discovery
  // nor the known never-fired set accounts for it. Recovery can expose further routes.
  const unaccounted = new Set(
    DurableRuntimeFailpointLocation.literals.filter(
      (location) => !observed.has(location) && !UNREACHED_LOCATIONS.includes(location),
    ),
  );

  const fired = new Set<DurableRuntimeFailpointLocation>();

  for (const { scenario, outcome, reached } of discoveries) {
    if (outcome.status === "failed") {
      return yield* DurableCoordinatorConformanceViolation.make({
        caseName,
        message: `${scenario} discovery failed: ${outcome.detail}`,
      });
    }
    const outcomes = new Map<DurableRuntimeFailpointLocation, SweepOutcome>();

    while (true) {
      // Schema order bounds this to one drive per location in each scenario.
      const location = DurableRuntimeFailpointLocation.literals.find(
        (candidate) =>
          !outcomes.has(candidate) && (reached.has(candidate) || unaccounted.has(candidate)),
      );

      if (location === undefined) break;
      const result = yield* runSweepCell(scenario, location, reached);

      outcomes.set(location, result);
      if (result.status === "failed") {
        return yield* DurableCoordinatorConformanceViolation.make({
          caseName,
          message: `${scenario} at ${location} failed: ${result.detail}`,
        });
      }
      if (result.failpointFired) fired.add(location);
    }
    if (![...outcomes.values()].some((result) => result.failpointFired)) {
      return yield* DurableCoordinatorConformanceViolation.make({
        caseName,
        message: `${scenario} did not fire any injected fault`,
      });
    }
  }

  const neverFired = DurableRuntimeFailpointLocation.literals
    .filter((location) => !fired.has(location))
    .sort();

  const expected = [...UNREACHED_LOCATIONS].sort();

  if (
    neverFired.length !== expected.length ||
    neverFired.some((location, index) => location !== expected[index])
  ) {
    return yield* DurableCoordinatorConformanceViolation.make({
      caseName,
      message: `expected never-fired locations [${expected.join(", ")}], got [${neverFired.join(", ")}]`,
    });
  }
}).pipe(
  Effect.provide(
    Layer.mergeAll(runStorageLayer, ThreadReader.layer()).pipe(
      Layer.provideMerge(
        Layer.mergeAll(
          WakeScheduler.layerNoop,
          DurableRuntimeFailpointTestControl.layer,
          ToolReconciler.uncertain,
          DurableRuntimeConfig.layer({
            deploymentId: Schema.decodeSync(DeploymentId)("deployment-certification"),
            producerId: Schema.decodeSync(ProducerId)("producer-certification"),
            settlementPollInterval: Duration.millis(50),
            abortPollInterval: Duration.millis(50),
          }),
        ),
      ),
    ),
  ),
);
