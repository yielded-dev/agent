/**
 * Typed pending or settled status for accepted input. Canonical settlement is the authority for
 * completion.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

import { Settlement } from "./SubmissionLedger.ts";

/**
 * A Submission still owes its canonical Settlement, including while suspended.
 *
 * @category models
 * @since 0.1.0
 */
export class PendingSubmission extends Schema.TaggedClass<PendingSubmission>()("pending", {}) {}

/**
 * The existing durable Settlement is the only authority for completion.
 *
 * @category models
 * @since 0.1.0
 */
export class SettledSubmission extends Schema.TaggedClass<SettledSubmission>()("settled", {
  settlement: Settlement,
}) {}

/**
 * Whether accepted input still owes settlement or has its authoritative terminal outcome.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SubmissionStatus = Schema.Union([PendingSubmission, SettledSubmission]);
/**
 * Decoded value of {@link SubmissionStatus}.
 *
 * @category models
 * @since 0.1.0
 */
export type SubmissionStatus = typeof SubmissionStatus.Type;
