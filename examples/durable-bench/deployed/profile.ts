import { join } from "node:path";

import { Clock, Console, Effect, FileSystem, Schema, Semaphore } from "effect";

import { Cloudflare, request } from "./cloudflare.ts";
import { type Profile, type ProfileType } from "./model.ts";
import { BenchError, redact } from "./platform.ts";
import { ProfileTarget, type Query } from "./worker/protocol.ts";

const Settings = Schema.Struct({
  bindings: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      type: Schema.String,
      namespace_id: Schema.optionalKey(Schema.String),
    }),
  ),
});

const Capture = Schema.Struct({
  duration_ms: Schema.Int.check(Schema.isBetween({ minimum: 1000, maximum: 50_000 })),
  profile_type: Schema.Literals(["cpu", "heap"]),
  namespace_id: Schema.NonEmptyString,
  actor_id: ProfileTarget.fields.actorId,
});

/** Space captures by five minutes before warming the next profiling Object. */
export const profileBatches = Effect.fnUntraced(function* (size: number) {
  const lock = yield* Semaphore.make(1);

  if (size > 1)
    return yield* BenchError.make({
      message:
        "Select one profile type per run; captures must be spaced at least five minutes apart.",
    });
  let finishedAt = 0;

  return <A, E, R>(batch: Effect.Effect<A, E, R>) =>
    lock.withPermit(
      Effect.gen(function* () {
        while (finishedAt > 0) {
          const remainingMs = 300_000 - ((yield* Clock.currentTimeMillis) - finishedAt);

          if (remainingMs <= 0) break;
          yield* Console.error(
            "Waiting five minutes between profile captures before warming the next Object…",
          );
          yield* Effect.sleep(Math.min(remainingMs, 60_000));
        }

        return yield* batch.pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              finishedAt = yield* Clock.currentTimeMillis;
            }),
          ),
        );
      }),
    );
});

/** Resolve the warmed Object's own version immediately before requesting its capture. */
export const prepareProfile = Effect.fnUntraced(function* (options: {
  readonly worker: string;
  readonly endpoint: URL;
  readonly token: string;
  readonly query: Query;
  readonly output: string;
  readonly sourceMap: string;
  /** Called synchronously after fetch is issued, not an acknowledgement of profiler attachment. */
  readonly onIssued?: (request: { versionId: string; requestedMs: number }) => void;
}) {
  const cloud = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;

  return Effect.fnUntraced(function* (type: typeof ProfileType.Type, durationMs: number) {
    const target = (yield* request(options.endpoint, options.token, ProfileTarget)).value;
    const worker = encodeURIComponent(options.worker);
    const settings = yield* cloud.api(`workers/scripts/${worker}/settings`, Settings);

    const namespace = settings.bindings.find(
      (binding) => binding.type === "durable_object_namespace" && binding.name === target.binding,
    )?.namespace_id;

    if (!namespace || target.build !== options.query.expectedBuild)
      return yield* BenchError.make({
        message: "Cannot resolve the profiling Object's namespace/build.",
      });

    const body = yield* Schema.encodeEffect(Schema.fromJsonString(Capture))({
      duration_ms: durationMs,
      profile_type: type === "memory" ? "heap" : "cpu",
      namespace_id: namespace,
      actor_id: target.actorId,
    });

    const transportError = (cause: unknown) =>
      BenchError.make({ message: `Cloudflare ${type} profile request failed.`, cause });

    const requestedMs = yield* Clock.currentTimeMillis;

    const bytes = yield* Effect.acquireUseRelease(
      Effect.sync(() => new AbortController()),
      (controller) =>
        Effect.gen(function* () {
          const response = yield* Effect.tryPromise({
            try: () => {
              const pending = fetch(
                `https://api.cloudflare.com/client/v4/accounts/${cloud.accountId}/workers/workers/${worker}/versions/${encodeURIComponent(target.versionId)}/profile`,
                {
                  method: "POST",
                  signal: controller.signal,
                  headers: {
                    authorization: `Bearer ${cloud.apiToken}`,
                    "content-type": "application/json",
                  },
                  body,
                },
              );

              options.onIssued?.({ versionId: target.versionId, requestedMs });

              return pending;
            },
            catch: transportError,
          });

          if (!response.ok) {
            const retryAfter = response.headers.get("retry-after");

            const text = yield* Effect.tryPromise({
              try: () => response.text(),
              catch: transportError,
            });

            return yield* BenchError.make({
              message: `Cloudflare ${type} profile returned HTTP ${response.status}${retryAfter ? ` (Retry-After: ${retryAfter})` : ""}: ${redact(text, [cloud.apiToken, cloud.accountId, cloud.accountName]).slice(0, 1000)}`,
            });
          }

          return new Uint8Array(
            yield* Effect.tryPromise({
              try: () => response.arrayBuffer(),
              catch: transportError,
            }),
          );
        }),
      (controller) => Effect.sync(() => controller.abort()),
    ).pipe(Effect.timeout("65 seconds"));

    if (bytes.length === 0)
      return yield* BenchError.make({ message: `Cloudflare returned an empty ${type} profile.` });
    const receivedMs = yield* Clock.currentTimeMillis;
    const query = options.query;

    const file = join(
      options.output,
      `${query.object}-${query.target}-${query.expectedBuild}-${type}.pprof`,
    );

    yield* fs.writeFile(file + ".tmp", bytes, { mode: 0o600 });
    yield* fs.rename(file + ".tmp", file);

    return {
      type,
      target: query.target,
      object: query.object,
      history: query.history,
      ttftMs: query.ttftMs,
      build: query.expectedBuild,
      sample: query.sample,
      durationMs,
      versionId: target.versionId,
      requestedMs,
      receivedMs,
      file,
      sourceMap: options.sourceMap,
    } satisfies Profile;
  });
});
