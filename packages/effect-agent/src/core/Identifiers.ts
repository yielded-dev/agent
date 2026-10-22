/**
 * Branded identities for Agents, Threads, Runs, Tool Calls, and durable work records.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

/**
 * Stable identity of an agent definition, distinct from a display name.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AgentId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/AgentId"));

/**
 * Decoded value of {@link AgentId}.
 *
 * @category models
 * @since 0.1.0
 */
export type AgentId = typeof AgentId.Type;

/**
 * Identity shared by runs that participate in one thread history.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ThreadId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/ThreadId"));

/**
 * Decoded value of {@link ThreadId}.
 *
 * @category models
 * @since 0.1.0
 */
export type ThreadId = typeof ThreadId.Type;

/**
 * Identity of one accepted input submission.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubmissionId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/SubmissionId"),
);

/**
 * Decoded value of {@link SubmissionId}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubmissionId = typeof SubmissionId.Type;

/**
 * Durable identity returned once ledger admission and Thread readiness are committed.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ReceiptId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/ReceiptId"));

/**
 * Decoded value of {@link ReceiptId}.
 *
 * @category models
 * @since 0.1.0
 */
export type ReceiptId = typeof ReceiptId.Type;

/**
 * Identity of the single durable terminal outcome owed to one accepted Submission.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SettlementId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/SettlementId"),
);

/**
 * Decoded value of {@link SettlementId}.
 *
 * @category models
 * @since 0.1.0
 */
export type SettlementId = typeof SettlementId.Type;

/**
 * Identity of one execution ownership period for accepted work.
 *
 * @category schemas
 * @since 0.1.0
 */
export const AttemptId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/AttemptId"));

/**
 * Decoded value of {@link AttemptId}.
 *
 * @category models
 * @since 0.1.0
 */
export type AttemptId = typeof AttemptId.Type;

/**
 * Identity of one logical agent run.
 *
 * @category schemas
 * @since 0.1.0
 */
export const RunId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/RunId"));

/**
 * Decoded value of {@link RunId}.
 *
 * @category models
 * @since 0.1.0
 */
export type RunId = typeof RunId.Type;

/**
 * Identity of one model turn within a run.
 *
 * @category schemas
 * @since 0.1.0
 */
export const TurnId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/TurnId"));

/**
 * Decoded value of {@link TurnId}.
 *
 * @category models
 * @since 0.1.0
 */
export type TurnId = typeof TurnId.Type;

/**
 * Identity used to correlate a Tool Call with progress and its terminal outcome.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ToolCallId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/ToolCallId"));

/**
 * Decoded value of {@link ToolCallId}.
 *
 * @category models
 * @since 0.1.0
 */
export type ToolCallId = typeof ToolCallId.Type;

/**
 * Stable identity of one configured Subagent capability.
 *
 * @category schemas
 * @since 0.1.0
 */
export const DelegationId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/DelegationId"),
);

/**
 * Decoded value of {@link DelegationId}.
 *
 * @category models
 * @since 0.1.0
 */
export type DelegationId = typeof DelegationId.Type;

/**
 * Opaque position in a semantic event sequence.
 *
 * @category schemas
 * @since 0.1.0
 */
export const EventOffset = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/EventOffset"),
);

/**
 * Decoded value of {@link EventOffset}.
 *
 * @category models
 * @since 0.1.0
 */
export type EventOffset = typeof EventOffset.Type;
