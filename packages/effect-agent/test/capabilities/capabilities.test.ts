import { describe, expect, it } from "@effect/vitest";
import { ThreadId, RunId } from "@yielded/agent/identifiers";
import {
  ThreadAppend,
  Store as ConversationStore,
  layerMemory,
  toPrompt,
} from "@yielded/agent/thread";
import * as Prompt from "effect/ai/Prompt";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

const threadId = Schema.decodeSync(ThreadId)("trip-1");
const runId = Schema.decodeSync(RunId)("run-1");

const textMessage = (role: "system" | "user" | "assistant", content: string): Prompt.Message => {
  const [message] = Prompt.make([{ role, content }]).content;

  if (message === undefined) {
    throw new Error("Expected Prompt.make to preserve its single input message");
  }

  return message;
};

describe("capability contracts", () => {
  it.effect("releases an interrupted history transaction without publishing its suffix", () =>
    Effect.gen(function* () {
      const threads = yield* ConversationStore;

      yield* threads.create(threadId);

      const base = yield* threads.append(
        threadId,
        ThreadAppend.make({ message: textMessage("user", "base") }),
      );

      const history = Prompt.fromMessages([
        ...toPrompt(base).content,
        textMessage("assistant", "suffix"),
      ]);

      const clock = yield* Clock.Clock;

      {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let finalized = false;

        const timestamp = Effect.acquireUseRelease(
          Effect.void,
          () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          () =>
            Effect.sync(() => {
              finalized = true;
            }),
        );

        const record = threads
          .recordHistory(threadId, runId, history)
          .pipe(Effect.provideService(Clock.Clock, { ...clock, currentTimeMillis: timestamp }));

        const fiber = yield* record.pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        expect(finalized).toBe(true);
        expect(yield* threads.snapshot(threadId)).toEqual(base);
      }
      const committed = yield* threads.recordHistory(threadId, runId, history);

      expect(committed.nextSequence).toBe(2);
      expect(committed.messages).toHaveLength(2);
    }).pipe(Effect.provide(layerMemory)),
  );
});
