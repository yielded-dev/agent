import { Clock, Effect, Option, type Redacted, Schedule, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";

import { Evidence, workerRunHeader } from "../src/proof.ts";

const ResponseDiagnostic = Schema.Struct({
  status: Schema.Int,
  cfRay: Schema.NullOr(Schema.String.check(Schema.isPattern(/^[a-f0-9]{16}-[A-Z]{3}$/))),
  stampPresent: Schema.Boolean,
  stampMatched: Schema.Boolean,
  bodyExcerpt: Schema.String,
});

export const Failure = Schema.Struct({
  stage: Schema.String,
  status: Schema.NullOr(Schema.Int),
  response: Schema.optionalKey(ResponseDiagnostic),
});

export class GateFailure extends Schema.TaggedError<GateFailure>()(
  "CheckoutGateFailure",
  Failure.fields,
) {}

export const fail = (
  stage: string,
  status: number | null = null,
  response?: typeof ResponseDiagnostic.Type,
) => GateFailure.make({ stage, status, ...(response === undefined ? {} : { response }) });

// Unknown bodies may echo secrets or account details. Retain only recognized response text.
const bodyExcerpt = (body: string) =>
  body === "Not found" || body === "Unauthorized" || /^error code: \d{4}\s*$/.test(body)
    ? body.trim()
    : "[redacted]";

export const request = Effect.fnUntraced(function* (options: {
  readonly operation: "run" | "evidence" | "close";
  readonly origin: string;
  readonly token: Redacted.Redacted<string>;
  readonly runId: string;
}) {
  const { operation, origin, token, runId } = options;
  const client = yield* HttpClient.HttpClient;
  const deadline = (yield* Clock.monotonicTimeNanos) + 60_000_000_000n;

  const attempt = Effect.gen(function* () {
    const response = yield* client
      .execute(
        HttpClientRequest.make(operation === "evidence" ? "GET" : "POST")(
          origin + "/" + operation,
        ).pipe(HttpClientRequest.bearerToken(token)),
      )
      .pipe(
        Effect.timeout(operation === "run" ? "8 minutes" : "1 minute"),
        Effect.mapError(() => fail(operation + ":transport-or-timeout")),
      );

    const stamp = response.headers[workerRunHeader];

    const metadata = {
      status: response.status,
      cfRay: Option.getOrNull(
        Schema.decodeOption(ResponseDiagnostic.fields.cfRay)(response.headers["cf-ray"] ?? null),
      ),
      stampPresent: stamp !== undefined,
      stampMatched: stamp === runId,
    };

    const body = yield* response.text.pipe(
      Effect.timeout("10 seconds"),
      Effect.mapError(() =>
        fail(operation + ":transport-or-timeout", response.status, {
          ...metadata,
          bodyExcerpt: "[response body unavailable]",
        }),
      ),
    );

    const diagnostic = { ...metadata, bodyExcerpt: bodyExcerpt(body) };

    // Only this response proves the one-shot request never reached our receiver.
    // Any stamp (even the wrong deployment), transport loss, or 5xx is terminal for /run.
    if (operation === "run" && response.status === 404 && stamp === undefined)
      return yield* fail("run:not-dispatched", response.status, diagnostic);
    if (response.status !== 200 && !(operation === "run" && response.status === 502))
      return yield* fail(operation, response.status, diagnostic);
    if (stamp !== runId)
      return yield* fail(operation + ":worker-identity", response.status, diagnostic);

    const evidence = yield* Schema.decodeEffect(Schema.fromJsonString(Evidence))(body).pipe(
      Effect.mapError(() => fail(operation + ":invalid-evidence", response.status, diagnostic)),
    );

    return { status: response.status, evidence, response: diagnostic };
  });

  // Bound admission retries, without interrupting a checkout that has reached the Worker.
  return yield* attempt.pipe(
    Effect.retry({
      schedule: Schedule.spaced("2 seconds"),
      while: (error) =>
        error.stage === "run:not-dispatched"
          ? Clock.monotonicTimeNanos.pipe(Effect.map((now) => now + 2_000_000_000n < deadline))
          : false,
    }),
  );
});
