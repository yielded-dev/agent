import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { AttemptId, RunId, SubmissionId, ThreadId, TurnId } from "@yielded/agent/identifiers";
import { Context, Effect, Layer, Stream } from "effect";
import * as Response from "effect/ai/Response";
import { expect, it } from "vite-plus/test";

import {
  CurrentAttempt,
  withAttempt,
} from "../../effect-agent/src/engine/internal/provisional-text.ts";
import { LiveTextHub } from "../src/internal/live-text.ts";
import { decodeThreadId, plannerDefinition, submitOptions } from "./fixtures.ts";
import { allSettled, drainAlarmsUntil, runClient } from "./harness.ts";

// Temporary reproduction of review 5473378123, written before the fix.
it("reopens watchText after an admitted 257-character Submission settles", async () => {
  const thread = `preview-bound-${crypto.randomUUID()}`.padEnd(220, "x");
  const receipt = await runClient(
    CloudflareThreadClient.use((client) =>
      client.submit(
        { definition: plannerDefinition },
        { question: "complete the accepted input", ref: thread },
        submitOptions(thread, "preview-bound"),
      ),
    ),
  );

  expect(receipt.submissionId.length).toBe(257);
  await drainAlarmsUntil(thread, allSettled(thread));
  const frames = await runClient(
    CloudflareThreadClient.use((client) =>
      client.watchText(decodeThreadId(thread)).pipe(Stream.take(1), Stream.runCollect),
    ),
  );

  expect(frames.map((frame) => frame._tag)).toEqual(["Reset"]);
});

for (const publishText of [false, true]) {
  it(`releases the shared hub after an oversized Attempt ${publishText ? "with" : "without"} text`, () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(LiveTextHub.layer);
          const hub = Context.get(context, LiveTextHub);
          const thread = ThreadId.make("x".repeat(220));
          const submission = SubmissionId.make(`${"s".repeat(36)}:${thread}`);
          const sibling = ThreadId.make("short-thread");

          yield* withAttempt(
            Effect.gen(function* () {
              const attempt = yield* CurrentAttempt;

              if (attempt === undefined) throw new Error("The Attempt did not acquire Publisher");
              if (!publishText) return;
              const model = attempt.openModel(RunId.make("run"), TurnId.make("turn"));

              if (model === undefined) throw new Error("The Attempt did not open its model");
              expect(model.offerUnsafe(Response.makePart("text-start", { id: "text" }))).toBe(
                false,
              );
              expect(yield* hub.open(sibling).pipe(Effect.result)).toMatchObject({
                _tag: "Failure",
                failure: { _tag: "HostProtocolError" },
              });
            }),
            thread,
            submission,
            AttemptId.make("oversized-attempt"),
          ).pipe(Effect.provide(context));

          const stream = yield* hub.open(sibling);

          yield* Effect.promise(() => stream.cancel());
        }),
      ),
    ));
}
