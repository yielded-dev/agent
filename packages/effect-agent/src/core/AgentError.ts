/**
 * Typed failures for Agent input, output, model execution, Tool execution, authorization, and
 * runtime boundaries.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

import * as FailureDiagnostic from "./FailureDiagnostic.ts";
import { ToolCallId } from "./Identifiers.ts";

/**
 * Input projection, encoding, or host preparation failed during a Run.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentInputError extends Schema.TaggedError<AgentInputError>()("AgentInputError", {
  message: Schema.String,
}) {}

/**
 * The current Definition's input Schema cannot interpret this input.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentInputDecodeError extends Schema.TaggedError<AgentInputDecodeError>()(
  "AgentInputDecodeError",
  { message: Schema.String },
) {}

/**
 * A dispatch or local result exceeds canonical capacity; retain room for a bounded failure.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentPersistenceCapacityError extends Schema.TaggedError<AgentPersistenceCapacityError>()(
  "AgentPersistenceCapacityError",
  { message: Schema.String.check(Schema.isMaxLength(4_096)) },
) {}

/**
 * Final model output failed to decode through the agent definition's output Schema.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentOutputError extends Schema.TaggedError<AgentOutputError>()("AgentOutputError", {
  message: Schema.String,
}) {}

/**
 * An application-selected run disposition failed its definition-owned Schema boundary.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentRunDispositionError extends Schema.TaggedError<AgentRunDispositionError>()(
  "AgentRunDispositionError",
  {
    cause: Schema.Defect(),
    message: Schema.String,
  },
) {}

/**
 * The finite policy dimension a run exhausted; shared with durable settlement metadata.
 *
 * @category schemas
 * @since 0.1.0
 */
export const PolicyLimit = Schema.Literals([
  "turns",
  "tool-calls",
  "duration",
  "usage",
  "tokens",
  "cost",
  "repeated-failures",
]);

/**
 * Decoded value of {@link PolicyLimit}.
 *
 * @category models
 * @since 0.1.0
 */
export type PolicyLimit = typeof PolicyLimit.Type;

/**
 * A run exhausted one of its finite policy limits.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentPolicyError extends Schema.TaggedError<AgentPolicyError>()("AgentPolicyError", {
  limit: PolicyLimit,
  message: Schema.String,
}) {
  /** Prior failed completion when exhaustion prevents correction; private, excluded from JSON. */
  declare readonly cause?: unknown;
}

/**
 * A native Effect AI Tool approval was explicitly denied before its Handler started.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentApprovalDenied extends Schema.TaggedError<AgentApprovalDenied>()(
  "AgentApprovalDenied",
  {
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {
  /** Decode the native Effect AI Tool Call ID at the core error boundary. */
  static override make(input: {
    readonly toolCallId: string;
    readonly toolName: string;
    readonly message: string;
  }): AgentApprovalDenied {
    return super.make({
      ...input,
      toolCallId: Schema.decodeSync(ToolCallId)(input.toolCallId),
    });
  }
}

/**
 * A host denied current execution authority for a Tool Call before its Handler started.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentToolAuthorizationDenied extends Schema.TaggedError<AgentToolAuthorizationDenied>()(
  "AgentToolAuthorizationDenied",
  {
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
    /** Optional original policy evidence, excluded from model-facing ToolCallFailed data. */
    cause: Schema.optionalKey(FailureDiagnostic.Cause),
    stack: Schema.optionalKey(Schema.String),
  },
) {}

/**
 * The host could not complete its authority check; this is not a policy denial.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentToolAuthorizationCheckError extends Schema.TaggedError<AgentToolAuthorizationCheckError>()(
  "AgentToolAuthorizationCheckError",
  {
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    /** Safe summary. The original cause is private diagnostic evidence. */
    message: Schema.String,
    check: Schema.optionalKey(Schema.String),
    callId: Schema.optionalKey(Schema.String),
    cause: FailureDiagnostic.Cause,
    stack: Schema.optionalKey(Schema.String),
  },
) {}

/**
 * A native Effect AI Tool approval has no decision and the ephemeral Run cannot proceed.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentApprovalPending extends Schema.TaggedError<AgentApprovalPending>()(
  "AgentApprovalPending",
  {
    approvalId: Schema.NonEmptyString,
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {
  /** Decode the native Effect AI Tool Call ID at the core error boundary. */
  static override make(input: {
    readonly approvalId: string;
    readonly toolCallId: string;
    readonly toolName: string;
    readonly message: string;
  }): AgentApprovalPending {
    return super.make({
      ...input,
      toolCallId: Schema.decodeSync(ToolCallId)(input.toolCallId),
    });
  }
}

/**
 * A model response sequence violated an agent-loop protocol invariant.
 *
 * @category errors
 * @since 0.1.0
 */
export class ModelProtocolError extends Schema.TaggedError<ModelProtocolError>()(
  "ModelProtocolError",
  {
    message: Schema.String,
  },
) {}

/**
 * A run was interrupted without manufacturing a successful result.
 *
 * @category errors
 * @since 0.1.0
 */
export class AgentInterrupted extends Schema.TaggedError<AgentInterrupted>()("AgentInterrupted", {
  message: Schema.String,
}) {}

/**
 * The provider rejected a prompt that exceeds the model context window and
 * compaction recovery is unavailable or was already attempted once. This is
 * an expected failure: the engine classifies the provider error rather than
 * surfacing an opaque transport failure.
 *
 * @category errors
 * @since 0.1.0
 */
export class ContextOverflowError extends Schema.TaggedError<ContextOverflowError>()(
  "ContextOverflowError",
  {
    message: Schema.String,
    retried: Schema.Boolean,
  },
) {}

/**
 * Local context preparation could not fit a legal prompt inside its target before provider I/O.
 *
 * @category errors
 * @since 0.1.0
 */
export class ContextBudgetError extends Schema.TaggedError<ContextBudgetError>()(
  "ContextBudgetError",
  {
    message: Schema.String,
    estimatedTokens: Schema.Natural,
    targetTokens: Schema.Natural,
    completionReserveTokens: Schema.Natural,
  },
) {}

/**
 * Schema for framework-owned agent errors; application and Effect AI failures remain separate.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AgentError = Schema.Union([
  AgentInputDecodeError,
  AgentPersistenceCapacityError,
  AgentInputError,
  AgentOutputError,
  AgentRunDispositionError,
  AgentPolicyError,
  AgentApprovalDenied,
  AgentToolAuthorizationDenied,
  AgentToolAuthorizationCheckError,
  AgentApprovalPending,
  ModelProtocolError,
  AgentInterrupted,
  ContextOverflowError,
  ContextBudgetError,
]);

/**
 * Decoded value of {@link AgentError}.
 *
 * @category models
 * @since 0.1.0
 */
export type AgentError = typeof AgentError.Type;
