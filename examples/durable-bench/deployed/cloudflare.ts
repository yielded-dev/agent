import {
  Config,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
  Schema,
  Schedule,
  Stream,
  type Duration,
} from "effect";
import { Ndjson } from "effect/encoding";

import {
  DriverRequest,
  DriverRecovery,
  MeasureOutcome,
  type MeasureRequest,
  type StoredMeasureResponse,
} from "./model.ts";
import { BenchError, redact } from "./platform.ts";
import { errorText, Failure, UnknownOutcome } from "./worker/protocol.ts";

/** A transient transport failure; only callers with an idempotent operation may retry. */
export class RequestError extends Schema.TaggedError<RequestError>()("RequestError", {
  message: Schema.String,
}) {}

export const retryRequest = <A, E extends { readonly _tag: string }, R>(
  operation: Effect.Effect<A, E, R>,
) =>
  operation.pipe(
    Effect.retry({
      times: 3,
      schedule: Schedule.exponential("1 second"),
      while: (error) => error._tag === "RequestError",
    }),
  );

const transientStatus = (status: number) => status === 408 || status === 429 || status >= 500;

const nativeFailure = (cause: unknown, secrets: readonly string[]) => {
  const detail = Schema.decodeUnknownOption(
    Schema.Struct({
      name: Schema.optionalKey(Schema.String),
      code: Schema.optionalKey(Schema.String),
    }),
  )(cause);

  return redact(
    [...(Option.isSome(detail) ? [detail.value.name, detail.value.code] : []), errorText(cause)]
      .filter((part) => part !== undefined)
      .join(": "),
    secrets,
  ).slice(0, 1200);
};

const Workers = Schema.Array(Schema.Struct({ id: Schema.String }));

const Namespaces = Schema.Array(
  Schema.Struct({ name: Schema.String, script: Schema.optionalKey(Schema.String) }),
);

const BucketPage = Schema.Struct({
  buckets: Schema.Array(Schema.Struct({ name: Schema.NonEmptyString })),
});

const connect = Effect.gen(function* () {
  const prefix = yield* Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,23}$/)),
    "DURABLE_BENCH_PREFIX",
  ).pipe(Config.withDefault("durable-bench"));

  const accountId = Redacted.value(yield* Config.Redacted("CLOUDFLARE_ACCOUNT_ID"));
  const apiToken = Redacted.value(yield* Config.Redacted("CLOUDFLARE_API_TOKEN"));

  const api = Effect.fnUntraced(
    function* <S extends Schema.Top & { readonly DecodingServices: never }>(
      route: string,
      schema: S,
      body?: unknown,
    ) {
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/${route}`, {
            method: body === undefined ? "GET" : "POST",
            signal,
            headers: { authorization: `Bearer ${apiToken}`, "content-type": "application/json" },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          }),
        catch: (cause) =>
          RequestError.make({
            message: `Cloudflare API request failed: ${nativeFailure(cause, [apiToken, accountId])}`,
          }),
      });

      if (!response.ok)
        return yield* (transientStatus(response.status) ? RequestError : BenchError).make({
          message: `Cloudflare API returned HTTP ${response.status}.`,
        });

      const json = yield* Effect.tryPromise({
        try: () => response.json(),
        catch: (cause) =>
          RequestError.make({
            message: `Cannot read Cloudflare API JSON: ${nativeFailure(cause, [apiToken, accountId])}`,
          }),
      });

      const envelope = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ success: Schema.Boolean, result: Schema.Unknown }),
      )(json).pipe(
        Effect.mapError(() => BenchError.make({ message: "Unexpected Cloudflare API response." })),
      );

      if (!envelope.success)
        return yield* BenchError.make({ message: "Cloudflare API rejected the operation." });

      return yield* Schema.decodeUnknownEffect(schema)(envelope.result).pipe(
        Effect.mapError(() => BenchError.make({ message: "Unexpected Cloudflare API result." })),
      );
    },
    (operation, _route, _schema, body?: unknown) => {
      const timed = operation.pipe(
        Effect.timeoutOrElse({
          duration: "60 seconds",
          orElse: () => RequestError.make({ message: "Cloudflare API timed out." }),
        }),
      );

      return body === undefined ? timed.pipe(retryRequest) : timed;
    },
  );

  const account = yield* api("", Schema.Struct({ id: Schema.String, name: Schema.String }));

  if (account.id !== accountId)
    return yield* BenchError.make({
      message: "The token cannot verify the configured Cloudflare account.",
    });

  const domain = yield* api(
    "workers/subdomain",
    Schema.Struct({ subdomain: Schema.NonEmptyString }),
  );

  const listNamespaces = Effect.gen(function* () {
    const namespaces: (typeof Namespaces.Type)[number][] = [];

    for (let page = 1; page <= 100; page++) {
      const rows = yield* api(
        `workers/durable_objects/namespaces?page=${page}&per_page=100`,
        Namespaces,
      );

      namespaces.push(...rows);
      if (rows.length < 100) return namespaces;
    }

    return yield* BenchError.make({
      message: "Cloudflare namespace pagination exceeded its bound.",
    });
  });

  /** Reserve production headroom; count the entire account, mutate only our owned stacks. */
  const namespaceHeadroom = Effect.fnUntraced(function* (workers: readonly string[]) {
    const namespaces = yield* listNamespaces;

    if (workers.some((worker) => !/-(yielded|pi|tardie|effect-ai-floor|driver)$/.test(worker)))
      return yield* BenchError.make({ message: "Unknown benchmark Worker target" });

    const required = workers.flatMap((worker) => {
      const target = worker.endsWith("-effect-ai-floor")
        ? "effect-ai-floor"
        : worker.split("-").at(-1);

      const classes =
        target === "effect-ai-floor"
          ? ["EffectAiFloorDO"]
          : target === "yielded"
            ? ["YieldedDO"]
            : target === "pi"
              ? ["PiDO"]
              : target === "tardie"
                ? ["ActorDO", "ThreadDO"]
                : target === "driver"
                  ? ["DriverMeasurement"]
                  : [];

      return classes.map((name) => `${worker}_${name}`);
    });

    const existing = new Set(namespaces.map((namespace) => namespace.name));
    const additional = required.filter((name) => !existing.has(name)).length;
    const free = 500 - namespaces.length - additional;

    const owned = namespaces.filter((namespace) => namespace.name.startsWith(prefix + "-")).length;

    return {
      existing: namespaces.length,
      additional,
      free,
      owned,
      projectedOwned: owned + additional,
    };
  });

  const resources = Effect.fnUntraced(function* (workerName?: string, cpu = false) {
    // Prefixes may contain hyphens, so match the complete names emitted by run/deploy.
    const generatedName = new RegExp(
      `^${prefix}-(?:shared-[a-f0-9]{8}-(?:driver|provider|cpu)|[a-z0-9]+-[a-f0-9]{8}(?:-[a-z0-9-]+)?)$`,
    );

    const matches = (name: string) =>
      workerName === undefined
        ? generatedName.test(name)
        : name === workerName || name.startsWith(workerName + "-");

    const workers = (yield* api("workers/scripts", Workers))
      .filter((w) => matches(w.id))
      .map((w) => w.id);

    const namespaces = (yield* listNamespaces)
      .filter((n) => matches(n.name.split("_", 1)[0] ?? "") || matches(n.script ?? ""))
      .map((n) => `${n.script ?? ""}/${n.name}`);

    const buckets: string[] = [];

    if (cpu && workerName === undefined) {
      let startAfter = "";

      for (;;) {
        const page = yield* api(
          `r2/buckets?per_page=1000&start_after=${encodeURIComponent(startAfter)}`,
          BucketPage,
        );

        buckets.push(...page.buckets.map((bucket) => bucket.name).filter(matches));
        if (page.buckets.length < 1000) break;
        const next = page.buckets.at(-1)!.name;

        if (next <= startAfter)
          return yield* BenchError.make({
            message: "Cloudflare bucket pagination did not advance.",
          });
        startAfter = next;
      }
    }

    return {
      verified: workers.length === 0 && namespaces.length === 0 && buckets.length === 0,
      workers,
      namespaces,
      buckets,
    };
  });

  return {
    prefix,
    accountId,
    apiToken,
    accountName: account.name,
    subdomain: domain.subdomain,
    api,
    resources,
    namespaceHeadroom,
  };
});

export class Cloudflare extends Context.Service<Cloudflare, Effect.Success<typeof connect>>()(
  "durable-bench/Cloudflare",
) {
  static readonly layer = Layer.effect(Cloudflare, connect);
}

/** One transport attempt. Measurement, build and import inputs are never automatically replayed. */
const fetchResponse = Effect.fnUntraced(function* (
  url: URL | string,
  token: string,
  body?: unknown,
  timeout: Duration.Input = "3 minutes",
) {
  const response = yield* Effect.tryPromise({
    try: (signal) =>
      fetch(url, {
        method: body === undefined ? "GET" : "POST",
        signal,
        cache: "no-store",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          "cache-control": "no-store",
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      }),
    catch: (cause) =>
      RequestError.make({
        message: `Controller → Worker ${new URL(url).pathname} response was lost; request outcome is unknown: ${nativeFailure(cause, [token])}`,
      }),
  }).pipe(
    Effect.timeoutOrElse({
      duration: timeout,
      orElse: () => RequestError.make({ message: "Worker request timed out; outcome unknown." }),
    }),
  );

  if (!response.ok) {
    const text = yield* Effect.tryPromise({
      try: () => response.text(),
      catch: (cause) =>
        RequestError.make({
          message: `Cannot read Worker failure: ${nativeFailure(cause, [token])}`,
        }),
    });

    if (new URL(url).pathname === "/measure") {
      const sent = Schema.decodeUnknownOption(DriverRequest)(body);
      const failure = Schema.decodeOption(Schema.fromJsonString(Failure))(text);

      if (Option.isSome(sent) && Option.isSome(failure)) {
        if (failure.value.sample !== sent.value.query.sample)
          return yield* BenchError.make({
            message: "Worker failure named a different sample; refusing retirement or recovery.",
          });
        if (failure.value.transport !== undefined) return yield* failure.value.transport;
        if (failure.value.unknownOutcome !== undefined) return yield* failure.value.unknownOutcome;

        return yield* BenchError.make({
          message: redact(failure.value.error, [token]).slice(0, 1200),
        });
      }
    }

    return yield* (transientStatus(response.status) ? RequestError : BenchError).make({
      message: `Worker ${new URL(url).pathname} returned HTTP ${response.status}; the input will not be retried. ${redact(text, [token]).slice(0, 1200)}`,
    });
  }

  return response;
});

export const request = Effect.fnUntraced(
  function* <S extends Schema.Top & { readonly DecodingServices: never }>(
    url: URL | string,
    token: string,
    schema: S,
    body?: unknown,
    timeout: Duration.Input = "3 minutes",
  ) {
    const response = yield* fetchResponse(url, token, body, timeout);

    const raw = yield* Effect.tryPromise({
      try: () => response.json(),
      catch: (cause) =>
        RequestError.make({
          message: `Controller → Worker ${new URL(url).pathname} returned unreadable JSON; outcome unknown: ${nativeFailure(cause, [token])}`,
        }),
    });

    const value = yield* Schema.decodeUnknownEffect(schema)(raw).pipe(
      Effect.mapError(() =>
        BenchError.make({ message: "Worker response failed schema validation." }),
      ),
    );

    return { value, colo: response.headers.get("cf-ray")?.split("-").at(-1) ?? null };
  },
  (operation, _url, _token, _schema, body?: unknown, timeout: Duration.Input = "3 minutes") => {
    const timed = operation.pipe(
      Effect.timeoutOrElse({
        duration: timeout,
        orElse: () => RequestError.make({ message: "Worker request timed out; outcome unknown." }),
      }),
    );

    return body === undefined ? timed.pipe(retryRequest) : timed;
  },
);

/** A lost controller response is reconciled with the Driver's durable receipt, never guessed. */
export const measureRequest = Effect.fnUntraced(function* (
  url: string,
  token: string,
  input: MeasureRequest,
) {
  const address = new URL(url);

  address.searchParams.set("requestId", input.requestId);

  const send = () =>
    request(address, token, MeasureOutcome, input).pipe(
      Effect.catchTag("RequestError", Effect.succeed),
    );

  const first = yield* send();

  const validate = (value: MeasureOutcome) =>
    value.requestId === input.requestId && value.build === input.expectedDriverBuild
      ? Effect.succeed(value)
      : BenchError.make({ message: "Driver outcome request ID or build mismatch." });

  if (!Schema.is(RequestError)(first)) {
    const value = yield* validate(first.value);

    if (value._tag === "Complete")
      return { outcome: value.outcome, colo: first.colo, recovery: undefined };
  }

  const initialError = Schema.is(RequestError)(first)
    ? first.message
    : "Driver acknowledged a pending measurement.";

  let acknowledged = !Schema.is(RequestError)(first) && first.value._tag === "Running";
  let polls = 0;
  let resubmissions = 0;

  return yield* Effect.gen(function* () {
    for (;;) {
      yield* Effect.sleep(Math.min(8, 2 ** Math.min(polls, 3)) * 1000);
      polls++;

      let response = yield* request(address, token, MeasureOutcome, undefined, "10 seconds").pipe(
        Effect.catchTag("RequestError", Effect.succeed),
      );

      if (Schema.is(RequestError)(response)) continue;
      let value = yield* validate(response.value);

      if (value._tag === "NotReceived") {
        if (acknowledged)
          return yield* BenchError.make({
            message: "Driver lost an acknowledged request record; refusing resubmission.",
          });
        // The strongly consistent same-ID Object proved non-receipt. Concurrent delayed
        // arrivals also deduplicate at that Object; this is never a new input identity.
        resubmissions++;
        response = yield* send();
        if (Schema.is(RequestError)(response)) continue;
        value = yield* validate(response.value);
      }
      if (value._tag === "Running") acknowledged = true;
      if (value._tag !== "Complete") continue;

      return {
        outcome: value.outcome,
        colo: response.colo,
        recovery: DriverRecovery.make({
          requestId: input.requestId,
          polls,
          resubmissions,
          error: initialError,
        }),
      };
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: "5 minutes",
      orElse: () =>
        UnknownOutcome.make({
          message: `Driver outcome remains unknown after ${polls} polls for ${input.requestId}; ${initialError}`,
        }),
    }),
  );
});

/** Persisted failures keep the existing target failure classification and recovery rules. */
export const measureResult = Effect.fnUntraced(function* (
  outcome: StoredMeasureResponse,
  sample: string,
) {
  const value = outcome.body;

  if (value.ok) {
    if (outcome.status !== 200)
      return yield* BenchError.make({ message: "Driver success had an invalid HTTP status." });

    return value;
  }
  if (value.sample !== sample)
    return yield* BenchError.make({ message: "Driver failure named a different sample." });
  if (value.transport !== undefined) return yield* value.transport;
  if (value.unknownOutcome !== undefined) return yield* value.unknownOutcome;

  return yield* BenchError.make({ message: value.error });
});

export const requestEvents = <S extends Schema.Top & { readonly DecodingServices: never }>(
  url: URL | string,
  token: string,
  schema: S,
  body: unknown,
) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const response = yield* fetchResponse(url, token, body, "10 minutes");
      const stream = response.body;

      if (!stream)
        return yield* BenchError.make({ message: "Worker response has no event stream." });

      return Stream.fromReadableStream({
        evaluate: () => stream,
        onError: () =>
          BenchError.make({
            message: "History build response was lost; input will not be replayed.",
          }),
      }).pipe(
        Stream.pipeThroughChannel(Ndjson.decodeSchema(schema)({ ignoreEmptyLines: true })),
        Stream.mapError((cause) =>
          cause instanceof BenchError
            ? cause
            : BenchError.make({ message: "Invalid history build event." }),
        ),
      );
    }),
  );
