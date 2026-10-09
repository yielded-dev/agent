import { type ThreadId } from "@yielded/agent/identifiers";
import { Publisher, type Event } from "@yielded/agent/provisional-text";
import { Context, Effect, Exit, Layer, Queue, Schema, Scope, Stream } from "effect";

import { HostProtocolError } from "../CloudflareThreadClient.ts";
import {
  BUFFER_SIZE,
  Frame,
  isBoundedEvent,
  MAX_FRAME_BYTES,
  MAX_ID_LENGTH,
  MAX_OBSERVERS,
} from "./live-text-protocol.ts";

interface Observer {
  readonly threadId: ThreadId;
  readonly streamId: string;
  readonly queue: Queue.Queue<Frame>;
  sequence: number;
}

/** One Object incarnation; owns subscriptions independently of individual RPC event scopes. */
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
      const encode = Schema.encodeSync(Schema.fromJsonString(Frame));
      const encoder = new TextEncoder();

      const offerUnsafe = (event: Event): boolean => {
        if (observers.size === 0) return false;
        const bounded = isBoundedEvent(event);
        let delivered = false;

        // Fixed consumer count and queue capacities. No encoding, I/O or awaited work here.
        for (const observer of observers) {
          if (observer.threadId !== event.threadId) continue;
          const sequence = ++observer.sequence;

          if (!bounded) continue; // The next frame exposes this gap, including lost invalidations.
          delivered =
            Queue.offerUnsafe(observer.queue, {
              _tag: "Event",
              schemaVersion: 1,
              streamId: observer.streamId,
              sequence,
              event,
            }) || delivered;
        }

        return delivered;
      };

      const open = (threadId: ThreadId) =>
        Effect.gen(function* () {
          if (threadId.length > MAX_ID_LENGTH || observers.size >= MAX_OBSERVERS)
            return yield* HostProtocolError.make({
              message: "Provisional text observation capacity exceeded",
            });
          const scope = yield* Scope.fork(owner);
          const queue = yield* Queue.sliding<Frame>(BUFFER_SIZE);

          const observer: Observer = {
            threadId,
            streamId: crypto.randomUUID(),
            queue,
            sequence: 0,
          };

          const close = Scope.close(scope, Exit.void);

          yield* Scope.addFinalizer(
            scope,
            Effect.sync(() => {
              observers.delete(observer);
              Queue.shutdownUnsafe(queue);
            }),
          );
          observers.add(observer);

          const toBytes = Effect.fnUntraced(function* (frame: Frame) {
            const value = encoder.encode(encode(frame) + "\n");

            if (value.byteLength > MAX_FRAME_BYTES)
              return yield* HostProtocolError.make({
                message: "Provisional text frame exceeds its wire bound",
              });

            return value;
          });

          const resetFrame: Frame = {
            _tag: "Reset",
            schemaVersion: 1,
            streamId: observer.streamId,
            threadId,
            sequence: 0,
          };

          const frames = Stream.fromEffectRepeat(Queue.take(queue)).pipe(
            Stream.prepend([resetFrame]),
            Stream.mapEffect(toBytes),
            Stream.ensuring(close),
          );

          return yield* Stream.toReadableStreamEffect(frames);
        });

      return Context.make(LiveTextHub, { open }).pipe(Context.add(Publisher, { offerUnsafe }));
    }),
  );
}
