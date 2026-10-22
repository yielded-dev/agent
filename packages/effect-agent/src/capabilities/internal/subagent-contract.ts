import * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";

import * as FailureDiagnostic from "../../core/FailureDiagnostic.ts";
import { AgentId, DelegationId } from "../../core/Identifiers.ts";
import type { SubagentPolicy } from "../../core/SubagentContract.ts";
import { SubagentDelegationCaps, SubagentReservationAmounts } from "../../core/SubagentContract.ts";

export {
  /**
   * Finite delegation bounds declared by one Subagent capability.
   * Structural limits are hard limits; token
   * and cost caps are optional and enforced only as honestly as provider
   * reporting allows.
   *
   * @category models
   * @since 0.1.0
   */
  SubagentPolicy,
  /**
   * Inputs normalized and validated by `SubagentPolicy.make`.
   *
   * @category models
   * @since 0.1.0
   */
  type SubagentPolicyInput,
} from "../../core/SubagentContract.ts";

const BoundedFailureText = Schema.String.check(Schema.isMaxLength(4 * 1024));

/**
 * Delegation preflight denied before any child started.
 * No reservation, identity, or event exists for the
 * denied invocation; retry requires a new authorized parent Tool Call.
 *
 * @category errors
 * @since 0.1.0
 */
export class SubagentPrestartDenied extends Schema.TaggedError<SubagentPrestartDenied>()(
  "SubagentPrestartDenied",
  {
    delegationId: DelegationId,
    targetAgentId: AgentId,
    reason: Schema.Literals(["nested-delegation", "grant-violation", "budget-conflict"]),
    message: BoundedFailureText,
  },
) {}

/**
 * Input or result projection failed its Schema or bounds.
 * Fail closed: the message is a fixed description and
 * never carries the raw child value.
 *
 * @category errors
 * @since 0.1.0
 */
export class SubagentProjectionFailure extends Schema.TaggedError<SubagentProjectionFailure>()(
  "SubagentProjectionFailure",
  {
    delegationId: DelegationId,
    stage: Schema.Literals(["input", "result"]),
    message: BoundedFailureText,
    /** Original validation error; detailed diagnostics are never part of the safe message. */
    cause: Schema.optionalKey(FailureDiagnostic.Value),
    stack: Schema.optionalKey(Schema.String),
  },
) {}

export {
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
  SubagentExecutionFailureClassification,
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
  SubagentExecutionFailure,
  /**
   * Maximum string length of a retained Subagent failure tag: 256 UTF-16 code units.
   *
   * @category operations
   * @since 0.1.0
   */
  maxErrorTagLength,
} from "../../core/SubagentContract.ts";

const millisOfMaxDuration = (policy: SubagentPolicy): number =>
  Math.max(1, Math.ceil(Duration.toMillis(policy.maxDuration)));

/**
 * Derive the parent-Run delegation caps registered with
 * `SubagentReservations` from one delegation policy: the per-invocation
 * bounds scaled by `maxChildren` plus the invocation and concurrency limits.
 *
 * @category operations
 * @since 0.1.0
 */
export const delegationCapsFromPolicy = (policy: SubagentPolicy): SubagentDelegationCaps =>
  SubagentDelegationCaps.make({
    maxTotalChildInvocations: policy.maxChildren,
    maxConcurrentChildren: policy.maxConcurrency,
    maxTurns: policy.maxChildren * policy.maxTurns,
    maxToolCalls: policy.maxChildren * policy.maxToolCalls,
    maxDurationMillis: policy.maxChildren * millisOfMaxDuration(policy),
    ...(policy.maxInputTokens === undefined
      ? {}
      : { maxInputTokens: policy.maxChildren * policy.maxInputTokens }),
    ...(policy.maxOutputTokens === undefined
      ? {}
      : { maxOutputTokens: policy.maxChildren * policy.maxOutputTokens }),
    ...(policy.maxCostMicrousd === undefined
      ? {}
      : { maxCostMicrousd: policy.maxChildren * policy.maxCostMicrousd }),
    ...(policy.maxResultBytes === undefined
      ? {}
      : { maxResultBytes: policy.maxChildren * policy.maxResultBytes }),
  });

/**
 * Derive the all-or-nothing per-invocation reservation from one delegation policy.
 *
 * @category operations
 * @since 0.1.0
 */
export const delegationAllocationFromPolicy = (
  policy: SubagentPolicy,
): SubagentReservationAmounts =>
  SubagentReservationAmounts.make({
    turns: policy.maxTurns,
    toolCalls: policy.maxToolCalls,
    durationMillis: millisOfMaxDuration(policy),
    inputTokens: policy.maxInputTokens ?? 0,
    outputTokens: policy.maxOutputTokens ?? 0,
    costMicrousd: policy.maxCostMicrousd ?? 0,
    resultBytes: policy.maxResultBytes ?? 0,
  });
