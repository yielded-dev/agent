import { ThreadId } from "@yielded/agent/identifiers";
import { Event } from "@yielded/agent/provisional-text";
import { Schema } from "effect";

export const MAX_FRAME_BYTES = 32 * 1_024;
export const MAX_ID_LENGTH = 256;
export const MAX_DELTA_LENGTH = 4_096;
export const BUFFER_SIZE = 32;
export const MAX_OBSERVERS = 8;

const streamId = Schema.NonEmptyString.check(Schema.isMaxLength(MAX_ID_LENGTH));

export const Request = Schema.Struct({ schemaVersion: Schema.Literal(1) });

/** Disposable connection framing; sequences have no relationship to canonical sequences. */
export const Frame = Schema.Union([
  Schema.TaggedStruct("Reset", {
    schemaVersion: Schema.Literal(1),
    streamId,
    threadId: ThreadId,
    sequence: Schema.Literal(0),
  }),
  Schema.TaggedStruct("Event", {
    schemaVersion: Schema.Literal(1),
    streamId,
    sequence: Schema.Int.check(Schema.isGreaterThan(0)),
    event: Event,
  }),
]);

export type Frame = typeof Frame.Type;

/** Bounds are checked before queueing; provider metadata is deliberately absent. */
export const isBoundedEvent = (event: Event): boolean => {
  const ids: string[] = [event.threadId, event.submissionId, event.attemptId];

  if (event._tag !== "AttemptEnded") ids.push(event.runId, event.turnId);
  if (ids.some((id) => id.length > MAX_ID_LENGTH)) return false;

  return (
    event._tag !== "Text" ||
    (event.part.id.length <= MAX_ID_LENGTH &&
      Object.keys(event.part.metadata).length === 0 &&
      (event.part.type !== "text-delta" || event.part.delta.length <= MAX_DELTA_LENGTH))
  );
};
