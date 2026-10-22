/**
 * Best-effort text and reasoning drafts for live observers, independent of canonical Thread
 * history.
 *
 * @since 0.1.0
 */

import * as Response from "effect/ai/Response";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

import { AttemptId, RunId, SubmissionId, ThreadId, TurnId } from "./Identifiers.ts";

const attemptIdentity = {
  threadId: ThreadId,
  submissionId: SubmissionId,
  attemptId: AttemptId,
};

const modelIdentity = {
  ...attemptIdentity,
  runId: RunId,
  turnId: TurnId,
  generation: Schema.Int.check(Schema.isGreaterThan(0)),
};

/**
 * Best-effort text and reasoning drafts, never canonical history. Native parts omit provider metadata.
 * Discard retracts one generation; AttemptEnded closes every draft for that Attempt.
 *
 * @category schemas
 * @since 0.1.0
 */
export const Event = Schema.Union([
  Schema.TaggedStruct("Text", {
    ...modelIdentity,
    part: Schema.Union([
      Response.TextStartPart,
      Response.TextDeltaPart,
      Response.TextEndPart,
      Response.ReasoningStartPart,
      Response.ReasoningDeltaPart,
      Response.ReasoningEndPart,
    ]),
  }),
  Schema.TaggedStruct("Discard", modelIdentity),
  Schema.TaggedStruct("AttemptEnded", attemptIdentity),
]);

/**
 * Decoded value of {@link Event}.
 *
 * @category models
 * @since 0.1.0
 */
export type Event = typeof Event.Type;

/**
 * Synchronous best-effort sink for provisional text events. Implementations must accept into
 * bounded storage or drop without performing I/O.
 *
 * @category models
 * @since 0.1.0
 */
export interface PublisherService {
  /** Synchronously accept into bounded storage or drop; never perform I/O. Observer throws are ignored. */
  readonly offerUnsafe: (event: Event) => boolean;
}

/**
 * Default disables draft allocation. Loss may include Discard or AttemptEnded.
 *
 * @category operations
 * @since 0.1.0
 */
export const noopPublisher: PublisherService = { offerUnsafe: () => false };

/**
 * Optional provisional-text publisher; the default drops events and disables draft allocation.
 * Draft delivery is never canonical history.
 *
 * @category services
 * @since 0.1.0
 */
export const Publisher = Context.Reference<PublisherService>(
  "@effect-agent/core/ProvisionalText/Publisher",
  { defaultValue: () => noopPublisher },
);
