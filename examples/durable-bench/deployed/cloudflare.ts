import {
  Config,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
  Schema,
  Stream,
  type Duration,
} from "effect";
import { Ndjson } from "effect/encoding";

import { DriverRequest } from "./model.ts";
import { BenchError, redact } from "./platform.ts";
import { Failure } from "./worker/protocol.ts";

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

  const api = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(route: string, schema: S, body?: unknown) {
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/${route}`, {
          method: body === undefined ? "GET" : "POST",
          signal,
          headers: { authorization: `Bearer ${apiToken}`, "content-type": "application/json" },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        }),
      catch: () => BenchError.make({ message: "Cloudflare API request failed." }),
    }).pipe(Effect.timeout("60 seconds"));

    if (!response.ok)
      return yield* BenchError.make({
        message: `Cloudflare API returned HTTP ${response.status}.`,
      });

    const json = yield* Effect.tryPromise({
      try: () => response.json(),
      catch: () => BenchError.make({ message: "Invalid Cloudflare API JSON." }),
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
  });

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

    if (workers.some((worker) => !/-(yielded|pi|tardie|effect-ai-floor)$/.test(worker)))
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
                : [];

      return classes.map((name) => `${worker}_${name}`);
    });

    const existing = new Set(namespaces.map((namespace) => namespace.name));
    const additional = required.filter((name) => !existing.has(name)).length;
    const free = 500 - namespaces.length - additional;

    if (free < 100)
      return yield* BenchError.make({
        message: `Durable Object namespace headroom: account has ${namespaces.length}/500; this run needs ${additional} additional namespaces and would leave ${free} free. At least 100 must remain free. Tear down unused bench pools with their owning prefixes before retrying.`,
      });

    return { existing: namespaces.length, additional, free };
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
    catch: () =>
      BenchError.make({
        message: "Worker response was lost; request outcome is unknown and will not be retried.",
      }),
  }).pipe(Effect.timeout(timeout));

  if (!response.ok) {
    const text = yield* Effect.tryPromise({
      try: () => response.text(),
      catch: () => BenchError.make({ message: "Cannot read Worker failure." }),
    });

    if (new URL(url).pathname === "/measure") {
      const sent = Schema.decodeUnknownOption(DriverRequest)(body);
      const failure = Schema.decodeOption(Schema.fromJsonString(Failure))(text);

      if (
        Option.isSome(sent) &&
        Option.isSome(failure) &&
        failure.value.sample === sent.value.query.sample &&
        failure.value.transport !== undefined
      )
        return yield* failure.value.transport;
    }

    return yield* BenchError.make({
      message: `Worker ${new URL(url).pathname} returned HTTP ${response.status}; the input will not be retried. ${redact(text, [token]).slice(0, 1200)}`,
    });
  }

  return response;
});

export const request = Effect.fnUntraced(function* <
  S extends Schema.Top & { readonly DecodingServices: never },
>(
  url: URL | string,
  token: string,
  schema: S,
  body?: unknown,
  timeout: Duration.Input = "3 minutes",
) {
  const response = yield* fetchResponse(url, token, body, timeout);

  const raw = yield* Effect.tryPromise({
    try: () => response.json(),
    catch: () => BenchError.make({ message: "Worker returned invalid JSON." }),
  });

  const value = yield* Schema.decodeUnknownEffect(schema)(raw).pipe(
    Effect.mapError(() =>
      BenchError.make({ message: "Worker response failed schema validation." }),
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
