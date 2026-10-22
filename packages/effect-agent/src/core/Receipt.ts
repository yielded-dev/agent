/**
 * Durable admission identities and statuses for accepted input. A Receipt identifies work without
 * granting permission to observe or control it.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

import { ReceiptId, SubmissionId, ThreadId } from "./Identifiers.ts";

// Keep the established brand and Schema identities when sharing these values inward.
/**
 * Caller-supplied deduplication key, scoped to one Thread and authenticated principal.
 *
 * @category schemas
 * @since 0.1.0
 */
export const IdempotencyKey = Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
  Schema.brand("@effect-agent/thread/IdempotencyKey"),
);

/**
 * Decoded value of {@link IdempotencyKey}.
 *
 * @category models
 * @since 0.1.0
 */
export type IdempotencyKey = typeof IdempotencyKey.Type;

/**
 * Stable host-authenticated admission principal; the established durable brand is preserved.
 *
 * @category schemas
 * @since 0.1.0
 */
export const Principal = Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
  Schema.brand("@effect-agent/thread/Principal"),
);

/**
 * Decoded value of {@link Principal}.
 *
 * @category models
 * @since 0.1.0
 */
export type Principal = typeof Principal.Type;

/**
 * Thread-local FIFO position allocated once at admission.
 *
 * @category schemas
 * @since 0.1.0
 */
export const QueueSequence = Schema.Natural.pipe(
  Schema.brand("@effect-agent/thread/QueueSequence"),
);

/**
 * Decoded value of {@link QueueSequence}.
 *
 * @category models
 * @since 0.1.0
 */
export type QueueSequence = typeof QueueSequence.Type;

/**
 * One accepted input whose admission, Thread materialization and readiness are committed.
 * A Receipt identifies an observation target; possession grants no authorization.
 *
 * @category models
 * @since 0.1.0
 */
export class Receipt extends Schema.Class<Receipt>("@effect-agent/thread/Receipt")({
  receiptId: ReceiptId,
  submissionId: SubmissionId,
  threadId: ThreadId,
  queueSequence: QueueSequence,
}) {}

/**
 * The input joined this host Submission; cancelling it must never silently cancel the host.
 *
 * @category errors
 * @since 0.1.0
 */
export class JoinedToHost extends Schema.TaggedError<JoinedToHost>()("JoinedToHost", {
  submissionId: SubmissionId,
  hostSubmissionId: SubmissionId,
}) {}
