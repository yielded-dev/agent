import { assert, it } from "@effect/vitest";
import { Effect, Fiber, Redacted, Result } from "effect";
import { FetchHttpClient } from "effect/http";
import { TestClock } from "effect/testing";

import { request } from "../live/request.ts";

const evidence = {
  phase: "idle",
  started: false,
  attempts: 0,
  receipt: null,
  closed: true,
  scrapeAttempts: 0,
  loginRequests: 0,
  failure: null,
  browserDiagnostics: [],
};

const options = {
  operation: "run",
  origin: "https://checkout.test",
  token: Redacted.make("control-secret"),
  runId: "checkout-test",
} as const;

// Edge propagation cannot be induced reliably in the live gate. Exercise its HTTP boundary.
it.effect("repeats only a run that received an unstamped edge 404", () =>
  Effect.gen(function* () {
    let calls = 0;

    const fiber = yield* request(options).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, async () => {
        calls++;

        return calls === 1
          ? new Response("error code: 1042\n", { status: 404 })
          : Response.json(evidence, { headers: { "x-checkout-run-id": "checkout-test" } });
      }),
      Effect.forkChild,
    );

    yield* TestClock.adjust("2 seconds");
    assert.strictEqual((yield* Fiber.join(fiber)).status, 200);
    assert.strictEqual(calls, 2);
  }),
);

it.effect("never repeats stamped 404s or 502s and rejects a wrong deployment stamp", () =>
  Effect.gen(function* () {
    for (const response of [
      new Response("Not found", { status: 404, headers: { "x-checkout-run-id": "checkout-test" } }),
      Response.json(
        { ...evidence, failure: "BrowserSessionError" },
        {
          status: 502,
          headers: { "x-checkout-run-id": "checkout-test" },
        },
      ),
      Response.json(evidence, { headers: { "x-checkout-run-id": "different-run" } }),
    ]) {
      let calls = 0;

      const result = yield* request(options).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, async () => {
          calls++;

          return response;
        }),
        Effect.result,
      );

      assert.strictEqual(calls, 1);
      if (response.status === 502) {
        assert.isTrue(Result.isSuccess(result));
        if (Result.isSuccess(result)) assert.strictEqual(result.success.status, 502);
      } else assert.isTrue(Result.isFailure(result));
    }
  }),
);
