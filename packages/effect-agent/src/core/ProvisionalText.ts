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
 * Best-effort drafts, never canonical history. Emitted native parts omit provider metadata.
 * Discard retracts one generation; AttemptEnded closes every draft for that Attempt.
 */
export const Event = Schema.Union([
  Schema.TaggedStruct("Text", {
    ...modelIdentity,
    part: Schema.Union([Response.TextStartPart, Response.TextDeltaPart, Response.TextEndPart]),
  }),
  Schema.TaggedStruct("Discard", modelIdentity),
  Schema.TaggedStruct("AttemptEnded", attemptIdentity),
]);

export type Event = typeof Event.Type;

export interface PublisherService {
  /** Synchronously accept into bounded storage or drop; never perform I/O. Observer throws are ignored. */
  readonly offerUnsafe: (event: Event) => boolean;
}

/** Default disables draft allocation. Loss may include Discard or AttemptEnded. */
export const noopPublisher: PublisherService = { offerUnsafe: () => false };

export const Publisher = Context.Reference<PublisherService>(
  "@effect-agent/core/ProvisionalText/Publisher",
  { defaultValue: () => noopPublisher },
);
