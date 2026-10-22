import { ThreadId } from "@yielded/agent/identifiers";
import { Event } from "@yielded/agent/provisional-text";
import * as Schema from "effect/Schema";

import { HostProtocolError } from "./host-protocol-error.ts";

/** @internal */
export const MAX_FRAME_BYTES = 32 * 1_024;
/** @internal */
export const MAX_ID_LENGTH = 256;
/** @internal */
export const MAX_DELTA_LENGTH = 4_096;
/** @internal */
export const BUFFER_SIZE = 32;
/** @internal */
export const MAX_OBSERVERS = 8;
/** @internal */
export const MAX_DRAFT_PARTS = 32;
/** @internal */
export const MAX_DRAFT_LENGTH = 2 * 1_024 * 1_024;

const streamId = Schema.NonEmptyString.check(Schema.isMaxLength(MAX_ID_LENGTH));

/** @internal */
export const Request = Schema.Struct({ schemaVersion: Schema.Literal(1) });

/**
 * Reset replaces all drafts with a snapshot, streamed as native Text parts before live deltas.
 * Each snapshot starts a fresh streamId. Its sequences are unrelated to canonical sequences.
 *
 * @category schemas
 * @since 0.1.0
 */
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

/**
 * Versioned provisional text frame; reset starts a fresh sequence independent of canonical history.
 *
 * @category models
 * @since 0.1.0
 */
export type Frame = typeof Frame.Type;

/** Bounds are checked before queueing; provider metadata is deliberately absent.
 * @internal
 */
export const isBoundedEvent = (event: Event): boolean => {
  const ids: string[] = [event.threadId, event.submissionId, event.attemptId];

  if (event._tag !== "AttemptEnded") ids.push(event.runId, event.turnId);
  if (ids.some((id) => id.length > MAX_ID_LENGTH)) return false;

  return (
    event._tag !== "Text" ||
    (event.part.id.length <= MAX_ID_LENGTH &&
      Object.keys(event.part.metadata).length === 0 &&
      ((event.part.type !== "text-delta" && event.part.type !== "reasoning-delta") ||
        event.part.delta.length <= MAX_DELTA_LENGTH))
  );
};

/** @internal */
export const liveTextProtocolFailure = () =>
  HostProtocolError.make({
    message: "Provisional text is incomplete or invalid; discard drafts and read canonical records",
  });
