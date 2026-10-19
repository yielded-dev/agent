import * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";

import * as FailureDiagnostic from "../../core/FailureDiagnostic.ts";
import { AgentId, DelegationId } from "../../core/Identifiers.ts";
import type { SubagentPolicy } from "../../core/SubagentContract.ts";
import { SubagentDelegationCaps, SubagentReservationAmounts } from "../../core/SubagentContract.ts";

export { SubagentPolicy, type SubagentPolicyInput } from "../../core/SubagentContract.ts";

const BoundedFailureText = Schema.String.check(Schema.isMaxLength(4 * 1024));

/**
 * Delegation preflight denied before any child started.
 * No reservation, identity, or event exists for the
 * denied invocation; retry requires a new authorized parent Tool Call.
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
  SubagentExecutionFailureClassification,
  SubagentExecutionFailure,
  maxErrorTagLength,
} from "../../core/SubagentContract.ts";

const millisOfMaxDuration = (policy: SubagentPolicy): number =>
  Math.max(1, Math.ceil(Duration.toMillis(policy.maxDuration)));

/**
 * Derive the parent-Run delegation caps registered with
 * `SubagentReservations` from one delegation policy: the per-invocation
 * bounds scaled by `maxChildren` plus the invocation and concurrency limits.
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

/** Derive the all-or-nothing per-invocation reservation from one delegation policy. */
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
