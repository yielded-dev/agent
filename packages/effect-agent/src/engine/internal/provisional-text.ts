import * as Response from "effect/ai/Response";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import type { AttemptId, RunId, SubmissionId, ThreadId, TurnId } from "../../core/Identifiers.ts";
import * as ProvisionalText from "../../core/ProvisionalText.ts";

export interface ModelHandle {
  readonly offerUnsafe: (part: Extract<ProvisionalText.Event, { _tag: "Text" }>["part"]) => boolean;
  readonly discard: () => void;
  readonly close: () => void;
}

export interface Attempt {
  readonly threadId: ThreadId;
  readonly openModel: (runId: RunId, turnId: TurnId) => ModelHandle | undefined;
  readonly close: () => void;
}

/** Bound only from a trusted Claim, inside captured registration and per-Attempt services. */
export const CurrentAttempt = Context.Reference<Attempt | undefined>(
  "@effect-agent/engine/ProvisionalText/CurrentAttempt",
  { defaultValue: () => undefined },
);

const makeAttempt = (
  publisher: ProvisionalText.PublisherService,
  threadId: ThreadId,
  submissionId: SubmissionId,
  attemptId: AttemptId,
): Attempt => {
  const identity = { threadId, submissionId, attemptId };
  const active = new Set<ModelHandle>();
  let closed = false;
  let generation = 0;

  const offerUnsafe = (event: ProvisionalText.Event): boolean => {
    try {
      return publisher.offerUnsafe(event);
    } catch {
      return false;
    }
  };

  return {
    threadId,
    openModel: (runId, turnId) => {
      if (closed) return undefined;

      const model = { ...identity, runId, turnId, generation: ++generation };
      let open = true;

      const handle: ModelHandle = {
        offerUnsafe: (part) => {
          if (!open || closed) return false;

          // processModelPart retains the original. Copy only native text fields so a
          // publisher cannot mutate that trace or observe provider metadata.
          const detached =
            part.type === "text-delta"
              ? Response.makePart("text-delta", { id: part.id, delta: part.delta })
              : Response.makePart(part.type, { id: part.id });

          return offerUnsafe({ _tag: "Text", ...model, part: detached });
        },
        discard: () => {
          if (!open) return;
          handle.close();
          offerUnsafe({ _tag: "Discard", ...model });
        },
        close: () => {
          if (!open) return;
          open = false;
          active.delete(handle);
        },
      };

      active.add(handle);

      return handle;
    },
    close: () => {
      if (closed) return;
      closed = true;
      for (const handle of active) handle.close();
      offerUnsafe({ _tag: "AttemptEnded", ...identity });
    },
  };
};

/** Disabled observation acquires no Scope, counters, handles, or event payloads. */
export const withAttempt = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  threadId: ThreadId,
  submissionId: SubmissionId,
  attemptId: AttemptId,
): Effect.Effect<A, E, R> =>
  Effect.flatMap(ProvisionalText.Publisher, (publisher) =>
    publisher === ProvisionalText.noopPublisher
      ? Effect.provideService(effect, CurrentAttempt, undefined)
      : Effect.scoped(
          Effect.acquireRelease(
            Effect.sync(() => makeAttempt(publisher, threadId, submissionId, attemptId)),
            (attempt) => Effect.sync(attempt.close),
          ).pipe(
            Effect.flatMap((attempt) => Effect.provideService(effect, CurrentAttempt, attempt)),
          ),
        ),
  );
