import { type ThreadId } from "@yielded/agent/identifiers";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { HostProtocolError } from "./internal/host-protocol-error.ts";
import {
  Frame as LiveTextFrame,
  isBoundedEvent,
  liveTextProtocolFailure,
  MAX_DRAFT_PARTS,
  MAX_FRAME_BYTES,
} from "./internal/live-text-protocol.ts";

export { HostProtocolError } from "./internal/host-protocol-error.ts";
export { Frame as LiveTextFrame } from "./internal/live-text-protocol.ts";

const decodeLiveTextFrame = Schema.decodeUnknownSync(Schema.fromJsonString(LiveTextFrame));

/**
 * Validate untrusted NDJSON at the final consumer, including frame bounds, thread,
 * sequence and text lifecycle. Stream loss invalidates all provisional drafts.
 */
export const decodeLiveText = <E, R>(
  threadId: ThreadId,
  source: Stream.Stream<Uint8Array, E, R>,
): Stream.Stream<LiveTextFrame, E | HostProtocolError, R> =>
  Stream.suspend(() => {
    const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
    let pending = "";
    let pendingBytes = 0;
    let expected = 0;
    let streamId: string | undefined;
    const models = new Map<string, { attemptId: string; parts: Set<string> }>();

    const accept = Effect.fnUntraced(function* (frame: LiveTextFrame) {
      if (frame._tag === "Reset") {
        if (frame.streamId === streamId || frame.threadId !== threadId)
          return yield* liveTextProtocolFailure();
        streamId = frame.streamId;
        expected = 1;
        models.clear();

        return true;
      }
      if (
        streamId === undefined ||
        frame.streamId !== streamId ||
        frame.sequence !== expected++ ||
        frame.event.threadId !== threadId ||
        !isBoundedEvent(frame.event)
      )
        return yield* liveTextProtocolFailure();
      const event = frame.event;

      if (event._tag === "AttemptEnded") {
        for (const [key, model] of models)
          if (model.attemptId === event.attemptId) models.delete(key);

        return true;
      }
      const key = JSON.stringify([event.attemptId, event.runId, event.turnId, event.generation]);

      if (event._tag === "Discard") {
        models.delete(key);

        return true;
      }
      let model = models.get(key);

      const partId = JSON.stringify([
        event.part.type.startsWith("reasoning-") ? "reasoning" : "text",
        event.part.id,
      ]);

      if (event.part.type === "text-start" || event.part.type === "reasoning-start") {
        if (model?.parts.has(partId)) return yield* liveTextProtocolFailure();
        if (
          [...models.values()].reduce((size, value) => size + value.parts.size, 0) >=
          MAX_DRAFT_PARTS
        )
          return yield* liveTextProtocolFailure();
        if (model === undefined) {
          model = { attemptId: event.attemptId, parts: new Set() };
          models.set(key, model);
        }
        model.parts.add(partId);

        return true;
      }
      if (!model?.parts.has(partId)) return yield* liveTextProtocolFailure();
      if (event.part.type === "text-end" || event.part.type === "reasoning-end") {
        model.parts.delete(partId);
        if (model.parts.size === 0) models.delete(key);
      }

      return true;
    });

    return source.pipe(
      Stream.mapEffect(
        Effect.fnUntraced(function* (chunk) {
          if (!(chunk instanceof Uint8Array)) return yield* liveTextProtocolFailure();
          const frames: LiveTextFrame[] = [];

          for (let offset = 0; offset < chunk.byteLength;) {
            const newline = chunk.indexOf(10, offset);
            const end = newline < 0 ? chunk.byteLength : newline;

            pendingBytes += end - offset;
            if (pendingBytes > MAX_FRAME_BYTES) return yield* liveTextProtocolFailure();
            pending += yield* Effect.try({
              try: () => decoder.decode(chunk.subarray(offset, end), { stream: newline < 0 }),
              catch: liveTextProtocolFailure,
            });
            offset = end + 1;
            if (newline < 0) break;

            const frame = yield* Effect.try({
              try: () => decodeLiveTextFrame(pending),
              catch: liveTextProtocolFailure,
            });

            pending = "";
            pendingBytes = 0;
            if (yield* accept(frame)) frames.push(frame);
          }

          return frames;
        }),
      ),
      Stream.flatMap(Stream.fromArray),
      // This connection is open-ended. Clean EOF is also loss of provisional state.
      Stream.concat(Stream.fail(liveTextProtocolFailure())),
    );
  });
