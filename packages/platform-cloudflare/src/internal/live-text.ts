import { type ThreadId } from "@yielded/agent/identifiers";
import { Publisher, type Event } from "@yielded/agent/provisional-text";
import { Context, Effect, Exit, Layer, Queue, Schema, Scope } from "effect";

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

          let closed = false;
          let output: ReadableByteStreamController | undefined;
          const close = Scope.close(scope, Exit.void);

          yield* Scope.addFinalizer(
            scope,
            Effect.sync(() => {
              closed = true;
              observers.delete(observer);
              Queue.shutdownUnsafe(queue);
              // Native cancellation can have closed the controller already.
              try {
                output?.close();
              } catch {
                /* Already closed or cancelled. */
              }
            }),
          );
          observers.add(observer);

          const bytes = (frame: Frame) => {
            // Byte-stream enqueue transfers ownership: each pull gets its own buffer.
            const value = encoder.encode(encode(frame) + "\n");

            if (value.byteLength > MAX_FRAME_BYTES)
              throw HostProtocolError.make({
                message: "Provisional text frame exceeds its wire bound",
              });

            return value;
          };

          return new ReadableStream({
            type: "bytes",
            start(controller) {
              output = controller;
              controller.enqueue(
                bytes({
                  _tag: "Reset",
                  schemaVersion: 1,
                  streamId: observer.streamId,
                  threadId,
                  sequence: 0,
                }),
              );
            },
            async pull(controller) {
              try {
                // The stream owns this pull; Scope shutdown wakes any pending Queue.take.
                const frame = await Effect.runPromise(Queue.take(queue));

                if (!closed) controller.enqueue(bytes(frame));
              } catch (cause) {
                if (!closed) controller.error(cause);
                await Effect.runPromise(close);
              }
            },
            cancel() {
              return Effect.runPromise(close);
            },
          });
        });

      return Context.make(LiveTextHub, { open }).pipe(Context.add(Publisher, { offerUnsafe }));
    }),
  );
}
