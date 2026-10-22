/**
 * Shared delegation policy, grants, budget ceilings, and typed Subagent failure contracts.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";

import { AgentId, ThreadId, DelegationId, RunId, ToolCallId, SubmissionId } from "./Identifiers.ts";

const Natural = Schema.Natural;

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);

const FiniteNonNegativeDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && !Duration.isNegative(duration),
    { expected: "a finite nonnegative duration" },
  ),
);

const SubagentPolicyFields = Schema.Struct({
  /** Total invocation slots, including reserved descendants, available through this delegation budget. */
  maxChildren: PositiveInt,
  /** Concurrently executing children per parent Run. */
  maxConcurrency: PositiveInt,
  /** Invocation slots held for the entire descendant subtree; omitted reserves zero. */
  descendantInvocations: Schema.optionalKey(Natural),
  /** Model turns reserved for each child invocation. */
  maxTurns: PositiveInt,
  /** Tool Calls reserved for each child invocation. */
  maxToolCalls: PositiveInt,
  /** Wall-clock duration reserved for each child invocation. */
  maxDuration: FinitePositiveDuration,
  /**
   * Time held back before the in-process parent's deadline; omitted reserves zero.
   * Does not apply to durable attached children or background workers.
   */
  parentReserveDuration: Schema.optionalKey(FiniteNonNegativeDuration),
  maxInputTokens: Schema.optionalKey(PositiveInt),
  maxOutputTokens: Schema.optionalKey(PositiveInt),
  maxCostMicrousd: Schema.optionalKey(Natural),
  maxResultBytes: Schema.optionalKey(PositiveInt),
});

type SubagentPolicyFields = typeof SubagentPolicyFields.Type;

/**
 * Inputs normalized and validated by `SubagentPolicy.make`.
 *
 * @category models
 * @since 0.1.0
 */
export type SubagentPolicyInput = Readonly<
  Omit<SubagentPolicyFields, "maxDuration" | "parentReserveDuration"> & {
    /** Finite, positive wall-clock duration accepted in any Effect Duration input form. */
    readonly maxDuration: Duration.Input;
    /** Finite, nonnegative time reserved for the in-process parent to finish. */
    readonly parentReserveDuration?: Duration.Input;
  }
>;

/**
 * Finite delegation bounds declared by one Subagent capability.
 * Structural limits are hard limits; token
 * and cost caps are optional and enforced only as honestly as provider
 * reporting allows.
 *
 * @category models
 * @since 0.1.0
 */
export class SubagentPolicy extends Schema.Class<SubagentPolicy>(
  "@effect-agent/capabilities/SubagentPolicy",
)(SubagentPolicyFields) {
  /** Normalize and validate finite delegation bounds, throwing on invalid input. */
  static override make(input: SubagentPolicyInput): SubagentPolicy {
    const { maxDuration, parentReserveDuration, ...bounds } = input;

    return super.make({
      ...bounds,
      maxDuration: Duration.fromInputUnsafe(maxDuration),
      ...(parentReserveDuration === undefined
        ? {}
        : { parentReserveDuration: Duration.fromInputUnsafe(parentReserveDuration) }),
    });
  }
}

/**
 * Tree authority ceiling. Each child exposes only its own Toolkit's permitted names.
 *
 * @category models
 * @since 0.1.0
 */
export class SubagentGrant extends Schema.Class<SubagentGrant>(
  "@effect-agent/capabilities/SubagentGrant",
)({
  allowedToolNames: Schema.Array(Schema.NonEmptyString).check(Schema.isMaxLength(128)),
  maxDepth: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })),
  /** Omitted permits either lifetime within the depth and reserved subtree budgets. */
  childLifetimes: Schema.optionalKey(
    Schema.Array(Schema.Literals(["attached", "background"])).check(Schema.isMaxLength(2)),
  ),
}) {}

/**
 * Intersect inherited authority; another delegation can never restore a removed capability.
 *
 * @category operations
 * @since 0.1.0
 */
export const narrowSubagentGrant = (
  declared: SubagentGrant,
  inherited?: SubagentGrant,
): SubagentGrant =>
  inherited === undefined
    ? declared
    : SubagentGrant.make({
        allowedToolNames: declared.allowedToolNames.filter((name) =>
          inherited.allowedToolNames.includes(name),
        ),
        maxDepth: Math.min(declared.maxDepth, inherited.maxDepth),
        childLifetimes: (declared.childLifetimes ?? ["attached", "background"]).filter((lifetime) =>
          (inherited.childLifetimes ?? ["attached", "background"]).includes(lifetime),
        ),
      });

/**
 * Finite delegable caps for one parent Run. Absent means not configured; present values are finite.
 *
 * @category models
 * @since 0.1.0
 */
export class SubagentDelegationCaps extends Schema.Class<SubagentDelegationCaps>(
  "@effect-agent/capabilities/SubagentDelegationCaps",
)({
  maxTotalChildInvocations: Schema.optionalKey(Natural),
  maxConcurrentChildren: Schema.optionalKey(Natural),
  maxTurns: Schema.optionalKey(Natural),
  maxToolCalls: Schema.optionalKey(Natural),
  maxDurationMillis: Schema.optionalKey(Natural),
  maxInputTokens: Schema.optionalKey(Natural),
  maxOutputTokens: Schema.optionalKey(Natural),
  maxCostMicrousd: Schema.optionalKey(Natural),
  maxResultBytes: Schema.optionalKey(Natural),
}) {}

const AmountFields = {
  turns: Natural,
  toolCalls: Natural,
  durationMillis: Natural,
  inputTokens: Natural,
  outputTokens: Natural,
  costMicrousd: Natural,
  resultBytes: Natural,
} as const;

/**
 * Exact amounts across every reservable delegation dimension.
 *
 * @category models
 * @since 0.1.0
 */
export class SubagentReservationAmounts extends Schema.Class<SubagentReservationAmounts>(
  "@effect-agent/capabilities/SubagentReservationAmounts",
)(AmountFields) {}

/**
 * One child reservation against the parent Run's shared delegation pool.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubagentBudgetReservation = Schema.Struct({
  caps: SubagentDelegationCaps,
  allocation: SubagentReservationAmounts,
  /** Slots reserved for every descendant input beneath this input; absent reserves none. */
  descendantInvocations: Schema.optionalKey(Natural),
});

/**
 * Decoded value of {@link SubagentBudgetReservation}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubagentBudgetReservation = typeof SubagentBudgetReservation.Type;

/**
 * Root-relative delegation depth; the direct child of a top-level run has depth 1.
 *
 * @category schemas
 * @since 0.1.0
 */
export const DelegationDepth = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
/**
 * Decoded value of {@link DelegationDepth}.
 *
 * @category models
 * @since 0.1.0
 */
export type DelegationDepth = typeof DelegationDepth.Type;

/**
 * Marks a Tool implementing the idempotent Subagent establishment protocol.
 *
 * @category services
 * @since 0.1.0
 */
export const DelegationTool = Context.Reference<boolean>("@effect-agent/core/DelegationTool", {
  defaultValue: () => false,
});

/**
 * Marks a host-managed idempotent worker operation, independently of its Tool name.
 *
 * @category services
 * @since 0.1.0
 */
export const WorkerOperationTool = Context.Reference<boolean>(
  "@effect-agent/core/WorkerOperationTool",
  {
    defaultValue: () => false,
  },
);

/**
 * Identifies the background launch operation within a generated management Toolkit.
 *
 * @category services
 * @since 0.1.0
 */
export const BackgroundSpawnTool = Context.Reference<boolean>(
  "@effect-agent/core/BackgroundSpawnTool",
  {
    defaultValue: () => false,
  },
);

/**
 * Declaration metadata and inherited authority determine the effective visible Tool set.
 *
 * @category guards
 * @since 0.1.0
 */
export const isSubagentToolAllowed = (
  grant: SubagentGrant | undefined,
  depth: number,
  name: string,
  annotations: Context.Context<never>,
): boolean =>
  grant === undefined ||
  (grant.allowedToolNames.includes(name) &&
    (Context.get(annotations, BackgroundSpawnTool)
      ? depth < grant.maxDepth &&
        (grant.childLifetimes ?? ["attached", "background"]).includes("background")
      : Context.get(annotations, DelegationTool)
        ? depth < grant.maxDepth &&
          (grant.childLifetimes ?? ["attached", "background"]).includes("attached")
        : true));

/**
 * Persisted preparation classification, checked against the resolved Tool before replay.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ToolExecutionKind = Schema.Literals(["ordinary", "delegation", "orchestration"]);
/**
 * Decoded value of {@link ToolExecutionKind}.
 *
 * @category models
 * @since 0.1.0
 */
export type ToolExecutionKind = typeof ToolExecutionKind.Type;

/**
 * Resolve only trusted declaration metadata; names never authorize automatic replay.
 *
 * @category operations
 * @since 0.1.0
 */
export const getToolExecutionKind = (annotations: Context.Context<never>): ToolExecutionKind =>
  Context.get(annotations, WorkerOperationTool)
    ? "orchestration"
    : Context.get(annotations, DelegationTool)
      ? "delegation"
      : "ordinary";

/**
 * Immutable lineage from a child Thread to the parent identity that established it.
 *
 * @category models
 * @since 0.1.0
 */
export class SubagentParentLink extends Schema.Class<SubagentParentLink>("SubagentParentLink")({
  delegationId: DelegationId,
  parentAgentId: AgentId,
  parentThreadId: ThreadId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  depth: DelegationDepth,
}) {}

/**
 * Classification of one bounded durable delegation failure.
 * `"child-failed"` and `"child-aborted"` project the
 * child's canonical failed/aborted Settlement; `"child-compatibility"`
 * projects the framework's `ChildCompatibilityFailure` child Settlement (the
 * stored child Binding digest was unavailable — recovery never substituted
 * current code); `"establishment-denied"` is a fail-closed coordinator
 * refusal (lineage/digest verification, divergent replay); and
 * `"declaration-unavailable"` is retained for decoding failures recorded by
 * versions that required an explicit durable digest declaration.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubagentExecutionFailureClassification = Schema.Literals([
  "child-failed",
  "child-aborted",
  "child-compatibility",
  "establishment-denied",
  "declaration-unavailable",
]);

/**
 * Decoded value of {@link SubagentExecutionFailureClassification}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubagentExecutionFailureClassification =
  typeof SubagentExecutionFailureClassification.Type;

/**
 * Maximum string length of a retained Subagent failure tag: 256 UTF-16 code units.
 *
 * @category operations
 * @since 0.1.0
 */
export const maxErrorTagLength = 256;
const BoundedErrorTag = Schema.NonEmptyString.check(Schema.isMaxLength(maxErrorTagLength));

/**
 * Bounded framework projection of a durable child failure, shared by attached joins and reports.
 * A failed or aborted durable child
 * joins its parent Tool Call as exactly this typed failure: a classification,
 * the child references, and the coordinator's bounded `{errorTag, message}`
 * projection — never a raw Cause, stack, provider response, secret, or child
 * payload. The typed child failure union does not survive a durable
 * Settlement, so `mapChildFailure` remains the ephemeral-path contract;
 * Schema-declared durable domain-failure mapping is a recorded later
 * extension.
 *
 * @category errors
 * @since 0.1.0
 */
export class SubagentExecutionFailure extends Schema.TaggedError<SubagentExecutionFailure>()(
  "SubagentExecutionFailure",
  {
    delegationId: DelegationId,
    targetAgentId: AgentId,
    classification: SubagentExecutionFailureClassification,
    /** Child references, present once establishment reached a child identity. */
    childThreadId: Schema.optionalKey(ThreadId),
    childSubmissionId: Schema.optionalKey(SubmissionId),
    childRunId: Schema.optionalKey(RunId),
    errorTag: BoundedErrorTag,
    message: Schema.String.check(Schema.isMaxLength(4 * 1024)),
  },
) {}
