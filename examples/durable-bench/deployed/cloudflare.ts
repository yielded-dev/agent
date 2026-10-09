import { Config, Context, Effect, Layer, Redacted, Schema, type Duration } from "effect";

import { BenchError, redact } from "./platform.ts";

const Workers = Schema.Array(Schema.Struct({ id: Schema.String }));

const Namespaces = Schema.Array(
  Schema.Struct({ name: Schema.String, script: Schema.optionalKey(Schema.String) }),
);

const connect = Effect.gen(function* () {
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

  const resources = Effect.fnUntraced(function* (prefix = "cold-storage") {
    const workers = (yield* api("workers/scripts", Workers))
      .filter((w) => w.id.startsWith(prefix))
      .map((w) => w.id);

    const namespaces: string[] = [];

    for (let page = 1; page <= 100; page++) {
      const rows = yield* api(
        `workers/durable_objects/namespaces?page=${page}&per_page=100`,
        Namespaces,
      );

      namespaces.push(
        ...rows
          .filter((n) => n.name.startsWith(prefix) || n.script?.startsWith(prefix))
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

/** One transport attempt. Measurement and import calls are never automatically replayed. */
export const request = <S extends Schema.Top & { readonly DecodingServices: never }>(
  url: URL | string,
  token: string,
  schema: S,
  body?: unknown,
  timeout: Duration.Input = "3 minutes",
) =>
  Effect.gen(function* () {
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        fetch(url, {
          method: body === undefined ? "GET" : "POST",
          signal,
          keepalive: false,
          cache: "no-store",
          headers: {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
            "cache-control": "no-store",
          },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        }),
      catch: (cause) => {
        const name = cause instanceof Error ? cause.name : "unknown";

        const code =
          cause instanceof Error && "code" in cause && typeof cause.code === "string"
            ? `/${cause.code}`
            : "";

        return new BenchError({
          message: `Worker response was lost (${name}${code}); request outcome is unknown and will not be retried.`,
        });
      },
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
