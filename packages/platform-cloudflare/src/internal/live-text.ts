import { type ThreadId } from "@yielded/agent/identifiers";
import { Publisher, type Event } from "@yielded/agent/provisional-text";
import { Cause, Context, Effect, Exit, Layer, Queue, Schema, Scope, Stream } from "effect";
import * as Response from "effect/ai/Response";

import { HostProtocolError } from "../CloudflareThreadClient.ts";
import {
  BUFFER_SIZE,
  Frame,
  isBoundedEvent,
  MAX_DELTA_LENGTH,
  MAX_DRAFT_LENGTH,
  MAX_DRAFT_PARTS,
  MAX_FRAME_BYTES,
  MAX_ID_LENGTH,
  MAX_OBSERVERS,
} from "./live-text-protocol.ts";

type TextEvent = Extract<Event, { readonly _tag: "Text" }>;

interface Draft {
  readonly start: TextEvent;
  text: string;
  ended: boolean;
}

interface Observer {
  readonly threadId: ThreadId;
  readonly queue: Queue.Queue<Event | undefined, HostProtocolError>;
  snapshot: boolean;
  streamId: string;
  sequence: number;
}

/** One Object incarnation; owns drafts and subscriptions independently of RPC event scopes. */
export class LiveTextHub extends Context.Service<
  LiveTextHub,
  {
    readonly open: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadableStream<Uint8Array>, HostProtocolError>;
  }
>()("@effect-agent/platform-cloudflare/LiveTextHub") {
  static readonly layer: Layer.Layer<LiveTextHub> = Layer.effectContext(
    Effect.gen(function* () {
      const owner = yield* Effect.scope;
      const observers = new Set<Observer>();
      const drafts = new Map<string, Draft>();
      let length = 0;

      let unavailable:
        | { readonly error: HostProtocolError; readonly attemptId: Event["attemptId"] }
        | undefined;

      const encode = Schema.encodeEffect(Schema.fromJsonString(Frame));
      const encoder = new TextEncoder();

      yield* Effect.addFinalizer(() => Effect.sync(() => drafts.clear()));

      const fail = (attemptId: Event["attemptId"]) => {
        const error = HostProtocolError.make({
          message:
            "Provisional text exceeds its draft bounds or is invalid; read canonical records",
        });

        unavailable = { error, attemptId };
        drafts.clear();
        length = 0;
        for (const observer of observers) {
          observer.snapshot = true;
          Queue.failCauseUnsafe(observer.queue, Cause.fail(error));
          Queue.shutdownUnsafe(observer.queue);
        }

        return false;
      };

      const offerUnsafe = (event: Event): boolean => {
        // Retain drafts even without subscribers. Invalid input disables previews until an
        // offending Attempt ends; canonical execution never depends on the observer's state.
        if (event._tag === "AttemptEnded" && event.attemptId === unavailable?.attemptId)
          unavailable = undefined;
        if (unavailable !== undefined) return false;
        if (!isBoundedEvent(event)) return fail(event.attemptId);

        if (event._tag === "Text") {
          const key = JSON.stringify([
            event.threadId,
            event.submissionId,
            event.attemptId,
            event.runId,
            event.turnId,
            event.generation,
            event.part.id,
          ]);

          const draft = drafts.get(key);

          if (event.part.type === "text-start") {
            if (draft !== undefined || drafts.size >= MAX_DRAFT_PARTS) return fail(event.attemptId);
            drafts.set(key, { start: event, text: "", ended: false });
          } else {
            // After invalidation, never expose an orphaned suffix as a complete prefix.
            if (draft === undefined) return false;
            if (draft.ended) return fail(event.attemptId);
            if (event.part.type === "text-delta") {
              if (length + event.part.delta.length > MAX_DRAFT_LENGTH) return fail(event.attemptId);
              draft.text += event.part.delta;
              length += event.part.delta.length;
            } else {
              draft.ended = true;
            }
          }
        } else {
          for (const [key, draft] of drafts) {
            const start = draft.start;

            if (
              start.threadId === event.threadId &&
              start.submissionId === event.submissionId &&
              start.attemptId === event.attemptId &&
              (event._tag === "AttemptEnded" ||
                (start.runId === event.runId &&
                  start.turnId === event.turnId &&
                  start.generation === event.generation))
            ) {
              drafts.delete(key);
              length -= draft.text.length;
            }
          }
        }

        // No snapshot copying, encoding, I/O or awaited work on the producer's path.
        for (const observer of observers) {
          if (observer.threadId !== event.threadId) continue;
          if (!Queue.offerUnsafe(observer.queue, event)) observer.snapshot = true;
        }

        return true;
      };

      const open = Effect.fnUntraced(function* (threadId: ThreadId) {
        if (unavailable !== undefined) return yield* unavailable.error;
        if (threadId.length > MAX_ID_LENGTH || observers.size >= MAX_OBSERVERS)
          return yield* HostProtocolError.make({
            message: "Provisional text observation capacity exceeded",
          });
        const scope = yield* Scope.fork(owner);
        const queue = yield* Queue.dropping<Event | undefined, HostProtocolError>(BUFFER_SIZE);

        const observer: Observer = {
          threadId,
          queue,
          snapshot: true,
          streamId: "",
          sequence: 0,
        };

        const close = Scope.close(scope, Exit.void);

        yield* Scope.addFinalizer(
          scope,
          Effect.sync(() => {
            observer.snapshot = true;
            observers.delete(observer);
            Queue.shutdownUnsafe(queue);
          }),
        );
        observers.add(observer);
        Queue.offerUnsafe(queue, undefined);

        const frame = (event: Event): Frame => ({
          _tag: "Event",
          schemaVersion: 1,
          streamId: observer.streamId,
          sequence: ++observer.sequence,
          event,
        });

        const snapshot = function* (): Generator<Frame> {
          // Capture immutable strings once, then encode bounded slices on consumer pulls.
          // Queue drain and capture are synchronous so no delta can straddle the snapshot.
          while (Queue.sizeUnsafe(queue) > 0) Queue.takeUnsafe(queue);

          const current = Array.from(drafts.values())
            .filter((draft) => draft.start.threadId === threadId)
            .map((draft) => ({ ...draft }));

          observer.snapshot = false;
          observer.streamId = crypto.randomUUID();
          observer.sequence = 0;
          yield {
            _tag: "Reset",
            schemaVersion: 1,
            streamId: observer.streamId,
            threadId,
            sequence: 0,
          };
          for (const draft of current) {
            yield frame(draft.start);
            for (let offset = 0; offset < draft.text.length; offset += MAX_DELTA_LENGTH) {
              yield frame({
                ...draft.start,
                part: Response.makePart("text-delta", {
                  id: draft.start.part.id,
                  delta: draft.text.slice(offset, offset + MAX_DELTA_LENGTH),
                }),
              });
            }
            if (draft.ended)
              yield frame({
                ...draft.start,
                part: Response.makePart("text-end", { id: draft.start.part.id }),
              });
          }
        };

        const toBytes = Effect.fnUntraced(function* (value: Frame) {
          const json = yield* encode(value).pipe(
            Effect.mapError(() =>
              HostProtocolError.make({ message: "Provisional text frame is invalid" }),
            ),
          );

          const bytes = encoder.encode(json + "\n");

          if (bytes.byteLength > MAX_FRAME_BYTES)
            return yield* HostProtocolError.make({
              message: "Provisional text frame exceeds its wire bound",
            });

          return bytes;
        });

        const frames = Stream.fromEffectRepeat(Queue.take(queue)).pipe(
          Stream.flatMap((event) =>
            observer.snapshot || event === undefined
              ? Stream.fromIterable(snapshot(), { chunkSize: 1 }).pipe(
                  Stream.takeWhile(() => !observer.snapshot),
                )
              : Stream.succeed(frame(event)),
          ),
          Stream.mapEffect(toBytes),
          Stream.ensuring(close),
        );

        return yield* Stream.toReadableStreamEffect(frames);
      });

      return Context.make(LiveTextHub, { open }).pipe(Context.add(Publisher, { offerUnsafe }));
    }),
  );
}
