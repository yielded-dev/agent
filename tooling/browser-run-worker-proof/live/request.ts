import { Clock, Effect, Option, type Redacted, Schedule, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";

import { Evidence, WorkerFailure, workerRunHeader } from "../src/proof.ts";

const ResponseDiagnostic = Schema.Struct({
  headers: Schema.Record(Schema.String, Schema.String),
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

const safeHeaders = {
  "cf-ray": Schema.String.check(Schema.isPattern(/^[a-f0-9]{16}-[A-Z]{3}$/)),
  "cf-cache-status": Schema.Literals([
    "HIT",
    "MISS",
    "DYNAMIC",
    "BYPASS",
    "EXPIRED",
    "STALE",
    "REVALIDATED",
    "UPDATING",
    "NONE",
    "UNKNOWN",
  ]),
  "content-type": Schema.String.check(
    Schema.isPattern(/^(text\/(plain|html)|application\/json)(;\s*charset=utf-8)?$/i),
  ),
  "content-length": Schema.String.check(Schema.isPattern(/^\d{1,12}$/)),
  server: Schema.Literal("cloudflare"),
};

// Unknown bodies may echo secrets or account details. Retain only recognized response text.
const bodyExcerpt = (body: string) => {
  const known = ["Not found", "Unauthorized", "Attempt already started; never replay it"];

  if (known.includes(body) || /^error code: \d{4}\s*$/.test(body) || body === "")
    return body.slice(0, 256);
  const evidence = Schema.decodeOption(Schema.fromJsonString(Evidence))(body);

  if (Option.isSome(evidence))
    return JSON.stringify({
      phase: evidence.value.phase,
      failure: Option.getOrElse(
        Schema.decodeUnknownOption(Schema.NullOr(WorkerFailure))(evidence.value.failure),
        () => "worker-failure",
      ),
      started: evidence.value.started,
      attempts: evidence.value.attempts,
      closed: evidence.value.closed,
    }).slice(0, 256);

  return "[unrecognized response body redacted]";
};

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
    const headers: Record<string, string> = {};

    for (const [name, schema] of Object.entries(safeHeaders)) {
      const raw = response.headers[name];

      if (raw === undefined) continue;
      const value = Schema.decodeOption(schema)(raw);

      if (Option.isSome(value)) headers[name] = value.value;
    }
    if (stamp !== undefined) headers[workerRunHeader] = stamp === runId ? runId : "[unexpected]";

    const body = yield* response.text.pipe(
      Effect.timeout("10 seconds"),
      Effect.mapError(() =>
        fail(operation + ":transport-or-timeout", response.status, {
          headers,
          bodyExcerpt: "[response body unavailable]",
        }),
      ),
    );

    const diagnostic = { headers, bodyExcerpt: bodyExcerpt(body) };

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
