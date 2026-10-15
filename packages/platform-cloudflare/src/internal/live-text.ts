import { type ThreadId } from "@yielded/agent/identifiers";
import { Publisher, type Event } from "@yielded/agent/provisional-text";
import { RpcTarget } from "cloudflare:workers";
import { Context, Effect, Layer, Schema } from "effect";
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
  Observation,
} from "./live-text-protocol.ts";

class ObservationLifetime extends RpcTarget {
  readonly #close: () => void;

  constructor(close: () => void) {
    super();
    this.#close = close;
  }

  [Symbol.dispose]() {
    this.#close();
  }
}

type TextEvent = Extract<Event, { readonly _tag: "Text" }>;

interface Draft {
  readonly start: TextEvent;
  text: string;
  ended: boolean;
}

interface Observer {
  readonly threadId: ThreadId;
  readonly offer: (event: Event) => void;
  readonly close: (error?: HostProtocolError) => void;
}

/** One Object incarnation; owns drafts and subscriptions independently of RPC event scopes. */
export class LiveTextHub extends Context.Service<
  LiveTextHub,
  {
    readonly open: (
      threadId: ThreadId,
    ) => Effect.Effect<typeof Observation.Type, HostProtocolError>;
  }
>()("@effect-agent/platform-cloudflare/LiveTextHub") {
  static readonly layer: Layer.Layer<LiveTextHub> = Layer.effectContext(
    Effect.gen(function* () {
      const observers = new Set<Observer>();
      const drafts = new Map<string, Draft>();
      let length = 0;
      let stopped = false;

      let unavailable:
        | { readonly error: HostProtocolError; readonly attemptId: Event["attemptId"] }
        | undefined;

      const encode = Schema.encodeSync(Frame);
      const encoder = new TextEncoder();

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          stopped = true;
          drafts.clear();
          for (const observer of observers) observer.close();
        }),
      );

      const fail = (attemptId: Event["attemptId"]) => {
        const error = HostProtocolError.make({
          message:
            "Provisional text exceeds its draft bounds or is invalid; read canonical records",
        });

        unavailable = { error, attemptId };
        drafts.clear();
        length = 0;
        for (const observer of observers) observer.close(error);

        return false;
      };

      const offerUnsafe = (event: Event): boolean => {
        // Retain drafts even without subscribers. Invalid input disables previews until an
        // offending Attempt ends; canonical execution never depends on the observer's state.
        if (stopped) return false;
        if (event._tag === "AttemptEnded" && event.attemptId === unavailable?.attemptId)
          unavailable = undefined;
        if (unavailable !== undefined) return false;
        if (!isBoundedEvent(event)) {
          // Drop an oversized terminal frame without disabling an already-ended Attempt.
          return event._tag === "AttemptEnded" ? false : fail(event.attemptId);
        }

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

        // Fulfill native demand inline. A slow reader only fills its bounded buffer;
        // canonical execution never waits for a pull or a transport acknowledgement.
        for (const observer of observers) {
          if (observer.threadId === event.threadId) observer.offer(event);
        }

        return true;
      };

      const open = Effect.fnUntraced(function* (threadId: ThreadId) {
        if (stopped)
          return yield* HostProtocolError.make({ message: "Provisional text owner is closed" });
        if (unavailable !== undefined) return yield* unavailable.error;
        if (threadId.length > MAX_ID_LENGTH || observers.size >= MAX_OBSERVERS)
          return yield* HostProtocolError.make({
            message: "Provisional text observation capacity exceeded",
          });
        const buffer: Event[] = [];
        let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
        let pending: Generator<Frame> | undefined;
        let snapshotNeeded = true;
        let streamId = "";
        let sequence = 0;
        let closed = false;

        const dispose = () => {
          closed = true;
          buffer.length = 0;
          pending = undefined;
          observers.delete(observer);
        };

        const observer: Observer = {
          threadId,
          offer(event) {
            if (buffer.length === BUFFER_SIZE) snapshotNeeded = true;
            else buffer.push(event);
            flush();
          },
          close(error) {
            if (closed) return;
            dispose();
            if (error === undefined) controller?.close();
            else controller?.error(error);
          },
        };

        const frame = (event: Event): Frame => ({
          _tag: "Event",
          schemaVersion: 1,
          streamId,
          sequence: ++sequence,
          event,
        });

        const snapshot = function* (): Generator<Frame> {
          // Capture immutable strings once, then encode bounded slices on consumer pulls.
          // Buffer drain and capture are synchronous so no delta can straddle the snapshot.
          buffer.length = 0;

          const current = Array.from(drafts.values())
            .filter((draft) => draft.start.threadId === threadId)
            .map((draft) => ({ ...draft }));

          snapshotNeeded = false;
          streamId = crypto.randomUUID();
          sequence = 0;
          yield {
            _tag: "Reset",
            schemaVersion: 1,
            streamId,
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

        const flush = () => {
          if (closed || controller === undefined) return;
          // Native demand and one queued Frame bound this loop. There is no
          // scheduled consumer fiber between an offered part and its enqueue.
          while (controller.desiredSize !== null && controller.desiredSize > 0) {
            if (snapshotNeeded) pending = snapshot();
            let value: Frame;

            if (pending !== undefined) {
              const next = pending.next();

              if (next.done) {
                pending = undefined;
                continue;
              }
              value = next.value;
            } else {
              const event = buffer.shift();

              if (event === undefined) return;
              value = frame(event);
            }

            let json: string;

            try {
              json = JSON.stringify(encode(value));
            } catch {
              observer.close(
                HostProtocolError.make({ message: "Provisional text frame is invalid" }),
              );

              return;
            }
            const bytes = encoder.encode(json + "\n");

            if (bytes.byteLength > MAX_FRAME_BYTES) {
              observer.close(
                HostProtocolError.make({
                  message: "Provisional text frame exceeds its wire bound",
                }),
              );

              return;
            }
            controller.enqueue(bytes);
          }
        };

        const stream = new ReadableStream<Uint8Array>({
          start(value) {
            controller = value;
            observers.add(observer);
            flush();
          },
          pull: flush,
          cancel: dispose,
        });

        // An idle RPC stream may not receive source cancellation until its next
        // enqueue. The returned capability also releases it when the caller's
        // RPC scope ends, without requiring another Frame or producer work.
        return Observation.make({
          stream,
          lifetime: new ObservationLifetime(() => observer.close()),
        });
      });

      return Context.make(LiveTextHub, { open }).pipe(Context.add(Publisher, { offerUnsafe }));
    }),
  );
}
