import { type ThreadId } from "@yielded/agent/identifiers";
import { Publisher, Event } from "@yielded/agent/provisional-text";
import { RpcTarget } from "cloudflare:workers";
import * as Response from "effect/ai/Response";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import { HostProtocolError } from "./host-protocol-error.ts";
import { Observation } from "./live-text-observation.ts";
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
  readonly offer: (payload: Uint8Array) => void;
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
      const encodeEvent = Schema.encodeSync(Event);
      const encodeHeader = Schema.encodeSync(Frame.members[1].mapFields(Struct.omit(["event"])));
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
            event.part.type.startsWith("reasoning-") ? "reasoning" : "text",
            event.part.id,
          ]);

          const draft = drafts.get(key);

          if (event.part.type === "text-start" || event.part.type === "reasoning-start") {
            if (draft !== undefined || drafts.size >= MAX_DRAFT_PARTS) return fail(event.attemptId);
            drafts.set(key, {
              start: {
                ...event,
                part: Response.makePart(event.part.type, { id: event.part.id }),
              },
              text: "",
              ended: false,
            });
          } else {
            // After invalidation, never expose an orphaned suffix as a complete prefix.
            if (draft === undefined) return false;
            if (draft.ended) return fail(event.attemptId);
            if (event.part.type === "text-delta" || event.part.type === "reasoning-delta") {
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
        // Own one encoded payload per offer. Observers copy it into their own frames;
        // neither later caller mutation nor a reader can mutate another reader's bytes.
        let payload: Uint8Array | HostProtocolError | undefined;

        for (const observer of observers) {
          if (observer.threadId !== event.threadId) continue;
          if (payload === undefined) {
            try {
              payload = encoder.encode(JSON.stringify(encodeEvent(event)));
            } catch {
              payload = HostProtocolError.make({ message: "Provisional text frame is invalid" });
            }
          }
          if (payload instanceof HostProtocolError) observer.close(payload);
          else observer.offer(payload);
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
        const buffer: Uint8Array[] = [];
        let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
        let pending: Generator<Frame> | undefined;
        let snapshotNeeded = true;
        let streamId = "";
        let sequence = 0;
        let closed = false;
        let keepalive: ReturnType<typeof setInterval> | undefined;

        const dispose = () => {
          closed = true;
          clearInterval(keepalive);
          buffer.length = 0;
          pending = undefined;
          observers.delete(observer);
        };

        const observer: Observer = {
          threadId,
          offer(payload) {
            if (buffer.length === BUFFER_SIZE) snapshotNeeded = true;
            else buffer.push(payload);
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
                part: Response.makePart(
                  draft.start.part.type === "reasoning-start" ? "reasoning-delta" : "text-delta",
                  {
                    id: draft.start.part.id,
                    delta: draft.text.slice(offset, offset + MAX_DELTA_LENGTH),
                  },
                ),
              });
            }
            if (draft.ended)
              yield frame({
                ...draft.start,
                part: Response.makePart(
                  draft.start.part.type === "reasoning-start" ? "reasoning-end" : "text-end",
                  { id: draft.start.part.id },
                ),
              });
          }
        };

        const flush = () => {
          if (closed || controller === undefined) return;
          // Native demand and one queued Frame bound this loop. There is no
          // scheduled consumer fiber between an offered part and its enqueue.
          while (controller.desiredSize !== null && controller.desiredSize > 0) {
            if (snapshotNeeded) pending = snapshot();
            let value: Frame | undefined;
            let payload: Uint8Array | undefined;

            if (pending !== undefined) {
              const next = pending.next();

              if (next.done) {
                pending = undefined;
                continue;
              }
              value = next.value;
            } else {
              payload = buffer.shift();

              if (payload === undefined) return;
            }

            let bytes: Uint8Array;

            try {
              if (value !== undefined) {
                bytes = encoder.encode(JSON.stringify(encode(value)) + "\n");
              } else if (payload !== undefined) {
                const header = encodeHeader({
                  _tag: "Event",
                  schemaVersion: 1,
                  streamId,
                  sequence: ++sequence,
                });

                // Both fragments have passed their parts of the unchanged Frame schema.
                const prefix = encoder.encode(`${JSON.stringify(header).slice(0, -1)},"event":`);

                bytes = new Uint8Array(prefix.byteLength + payload.byteLength + 2);
                bytes.set(prefix);
                bytes.set(payload, prefix.byteLength);
                bytes[bytes.length - 2] = 125; // }
                bytes[bytes.length - 1] = 10; // newline
              } else return;
            } catch {
              observer.close(
                HostProtocolError.make({ message: "Provisional text frame is invalid" }),
              );

              return;
            }
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
            // Write only between complete frames, before RPC can split their bytes.
            // Idle writes expose disconnected consumers without growing the native queue.
            keepalive = setInterval(() => {
              if (closed || value.desiredSize === null || value.desiredSize <= 0) return;
              value.enqueue(new Uint8Array([10]));
            }, 15_000);
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
