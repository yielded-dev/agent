import { Config, Context, Effect, Layer, Redacted, Schema, Stream, type Duration } from "effect";
import { Ndjson } from "effect/encoding";

import { BenchError, redact } from "./platform.ts";

const Workers = Schema.Array(Schema.Struct({ id: Schema.String }));

const Namespaces = Schema.Array(
  Schema.Struct({ name: Schema.String, script: Schema.optionalKey(Schema.String) }),
);

const connect = Effect.gen(function* () {
  const prefix = yield* Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,23}$/)),
    "DURABLE_BENCH_PREFIX",
  ).pipe(Config.withDefault("durable-bench"));

  const accountId = Redacted.value(yield* Config.Redacted("CLOUDFLARE_ACCOUNT_ID"));
  const apiToken = Redacted.value(yield* Config.Redacted("CLOUDFLARE_API_TOKEN"));

  const api = <S extends Schema.Top & { readonly DecodingServices: never }>(
    route: string,
    schema: S,
    body?: unknown,
  ) =>
    Effect.gen(function* () {
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/${route}`, {
            method: body === undefined ? "GET" : "POST",
            signal,
            headers: { authorization: `Bearer ${apiToken}`, "content-type": "application/json" },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          }),
        catch: () => new BenchError({ message: "Cloudflare API request failed." }),
      }).pipe(Effect.timeout("60 seconds"));

      if (!response.ok)
        return yield* new BenchError({
          message: `Cloudflare API returned HTTP ${response.status}.`,
        });

      const json = yield* Effect.tryPromise({
        try: () => response.json(),
        catch: () => new BenchError({ message: "Invalid Cloudflare API JSON." }),
      });

      const envelope = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ success: Schema.Boolean, result: Schema.Unknown }),
      )(json).pipe(
        Effect.mapError(() => new BenchError({ message: "Unexpected Cloudflare API response." })),
      );

      if (!envelope.success)
        return yield* new BenchError({ message: "Cloudflare API rejected the operation." });

      return yield* Schema.decodeUnknownEffect(schema)(envelope.result).pipe(
        Effect.mapError(() => new BenchError({ message: "Unexpected Cloudflare API result." })),
      );
    });

  const account = yield* api("", Schema.Struct({ id: Schema.String, name: Schema.String }));

  if (account.id !== accountId)
    return yield* new BenchError({
      message: "The token cannot verify the configured Cloudflare account.",
    });

  const domain = yield* api(
    "workers/subdomain",
    Schema.Struct({ subdomain: Schema.NonEmptyString }),
  );

  const resources = Effect.fnUntraced(function* (workerName?: string) {
    // Prefixes may contain hyphens, so match the complete names emitted by run/deploy.
    const generatedName = new RegExp(
      `^${prefix}-(?:shared-[a-f0-9]{8}-(?:driver|provider)|[a-z0-9]+-[a-f0-9]{8})$`,
    );

    const matches = (name: string) =>
      workerName === undefined ? generatedName.test(name) : name === workerName;

    const workers = (yield* api("workers/scripts", Workers))
      .filter((w) => matches(w.id))
      .map((w) => w.id);

    const namespaces: string[] = [];

    for (let page = 1; page <= 100; page++) {
      const rows = yield* api(
        `workers/durable_objects/namespaces?page=${page}&per_page=100`,
        Namespaces,
      );

      namespaces.push(
        ...rows
          // Namespace names are <worker>_<class>, including when script is absent.
          .filter((n) => matches(n.name.split("_", 1)[0] ?? "") || matches(n.script ?? ""))
          .map((n) => `${n.script ?? ""}/${n.name}`),
      );
      if (rows.length < 100)
        return { verified: workers.length === 0 && namespaces.length === 0, workers, namespaces };
    }

    return yield* new BenchError({
      message: "Cloudflare namespace pagination exceeded its bound.",
    });
  });

  return {
    prefix,
    accountId,
    apiToken,
    accountName: account.name,
    subdomain: domain.subdomain,
    api,
    resources,
  };
});

export class Cloudflare extends Context.Service<Cloudflare, Effect.Success<typeof connect>>()(
  "durable-bench/Cloudflare",
) {
  static readonly layer = Layer.effect(Cloudflare, connect);
}

/** One transport attempt. Measurement, build and import inputs are never automatically replayed. */
const fetchResponse = (
  url: URL | string,
  token: string,
  body?: unknown,
  timeout: Duration.Input = "3 minutes",
) =>
  Effect.gen(function* () {
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
      catch: () =>
        new BenchError({
          message: "Worker response was lost; request outcome is unknown and will not be retried.",
        }),
    }).pipe(Effect.timeout(timeout));

    if (!response.ok) {
      const text = yield* Effect.tryPromise({
        try: () => response.text(),
        catch: () => new BenchError({ message: "Cannot read Worker failure." }),
      });

      return yield* new BenchError({
        message: `Worker returned HTTP ${response.status}; the input will not be retried. ${redact(text, [token]).slice(0, 1200)}`,
      });
    }

    return response;
  });

export const request = <S extends Schema.Top & { readonly DecodingServices: never }>(
  url: URL | string,
  token: string,
  schema: S,
  body?: unknown,
  timeout: Duration.Input = "3 minutes",
) =>
  Effect.gen(function* () {
    const response = yield* fetchResponse(url, token, body, timeout);

    const raw = yield* Effect.tryPromise({
      try: () => response.json(),
      catch: () => new BenchError({ message: "Worker returned invalid JSON." }),
    });

    const value = yield* Schema.decodeUnknownEffect(schema)(raw).pipe(
      Effect.mapError(
        () => new BenchError({ message: "Worker response failed schema validation." }),
      ),
    );

    return { value, colo: response.headers.get("cf-ray")?.split("-").at(-1) ?? null };
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
        return yield* new BenchError({ message: "Worker response has no event stream." });

      return Stream.fromReadableStream({
        evaluate: () => stream,
        onError: () =>
          new BenchError({
            message: "History build response was lost; input will not be replayed.",
          }),
      }).pipe(
        Stream.pipeThroughChannel(Ndjson.decodeSchema(schema)({ ignoreEmptyLines: true })),
        Stream.mapError((cause) =>
          cause instanceof BenchError
            ? cause
            : new BenchError({ message: "Invalid history build event." }),
        ),
      );
    }),
  );
