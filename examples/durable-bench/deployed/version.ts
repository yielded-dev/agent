import { Resource } from "alchemy";
import * as Provider from "alchemy/Provider";
import { Config, Effect, Redacted, Schema } from "effect";

import { BenchError } from "./platform.ts";

const Version = Schema.Struct({
  worker: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
  activation: Schema.NonEmptyString,
});

type Version = typeof Version.Type;
type Deployment = Resource<"durable-bench/Deployment", Version, Version>;

/** Activates an already uploaded version; the Worker resource owns its lifetime. */
export const Deployment = Resource<Deployment>("durable-bench/Deployment");

export const deploymentProvider = () =>
  Provider.succeed(Deployment, {
    read: ({ output }) => Effect.succeed(output),
    reconcile: Effect.fnUntraced(function* ({ news }) {
      const value = yield* Schema.decodeUnknownEffect(Version)(news);
      const prefix = yield* Config.NonEmptyString("DURABLE_BENCH_NAME");

      if (!value.worker.startsWith(prefix + "-"))
        return yield* new BenchError({ message: "Version activation is outside this stack." });
      const account = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");
      const token = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");

      const response = yield* Effect.tryPromise({
        try: (signal) =>
          fetch(
            `https://api.cloudflare.com/client/v4/accounts/${account}/workers/scripts/${encodeURIComponent(value.worker)}/deployments`,
            {
              method: "POST",
              signal,
              headers: {
                authorization: `Bearer ${Redacted.value(token)}`,
                "content-type": "application/json",
              },
              body: JSON.stringify({
                strategy: "percentage",
                versions: [{ version_id: value.version, percentage: 100 }],
              }),
            },
          ),
        catch: () => new BenchError({ message: "Worker version activation request failed." }),
      }).pipe(Effect.timeout("60 seconds"));

      if (!response.ok)
        return yield* new BenchError({
          message: `Worker version activation returned HTTP ${response.status}.`,
        });

      const body = yield* Effect.tryPromise({
        try: () => response.json(),
        catch: () => new BenchError({ message: "Invalid Worker deployment response." }),
      });

      yield* Schema.decodeUnknownEffect(Schema.Struct({ success: Schema.Literal(true) }))(body);

      return value;
    }),
    // Deleting a deployment resource must not delete its parent's stored Objects.
    delete: () => Effect.void,
  });
