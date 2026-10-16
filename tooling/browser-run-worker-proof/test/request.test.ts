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

it.effect("keeps answered and uncertain run failures terminal", () =>
  Effect.gen(function* () {
    for (const response of [
      new Response("Not found", { status: 404, headers: { "x-checkout-run-id": "checkout-test" } }),
      new Response("Not found", { status: 404, headers: { "x-checkout-run-id": "different-run" } }),
      new Response("unavailable", { status: 502 }),
      Response.json(
        { ...evidence, failure: "BrowserSessionError" },
        {
          status: 502,
          headers: { "x-checkout-run-id": "checkout-test" },
        },
      ),
      Response.json(evidence),
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
      if (response.status === 502 && response.headers.has("x-checkout-run-id")) {
        assert.isTrue(Result.isSuccess(result));
        if (Result.isSuccess(result)) assert.strictEqual(result.success.status, 502);
      } else assert.isTrue(Result.isFailure(result));
    }
  }),
);

it.effect("bounds missing-route retries and never retries a lost response", () =>
  Effect.gen(function* () {
    let calls = 0;

    const fiber = yield* request(options).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, async () => {
        calls++;

        return new Response("error code: 1042\n", { status: 404 });
      }),
      Effect.result,
      Effect.forkChild,
    );

    yield* TestClock.adjust("1 minute");
    assert.isTrue(Result.isFailure(yield* Fiber.join(fiber)));
    assert.isAbove(calls, 1);
    assert.isAtMost(calls, 30);

    calls = 0;

    const lost = yield* request(options).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, async () => {
        calls++;
        throw new TypeError("connection lost");
      }),
      Effect.result,
    );

    assert.isTrue(Result.isFailure(lost));
    assert.strictEqual(calls, 1);
  }),
);

it.effect("retains safe failure diagnostics without publishing unexpected payloads", () =>
  Effect.gen(function* () {
    const result = yield* request(options).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(
        FetchHttpClient.Fetch,
        async () =>
          new Response("private-payload credential account-identifier", {
            status: 403,
            headers: {
              "cf-ray": "a4b8f7533a65ef2c-PDX",
              "content-type": "text/plain; charset=UTF-8",
              "set-cookie": "private-cookie",
              location: "https://private.test",
            },
          }),
      ),
      Effect.result,
    );

    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result)) {
      assert.include(JSON.stringify(result.failure), "a4b8f7533a65ef2c-PDX");
      assert.notInclude(JSON.stringify(result.failure), "private");
      assert.notInclude(JSON.stringify(result.failure), "credential");
      assert.notInclude(JSON.stringify(result.failure), "account-identifier");
    }
  }),
);
