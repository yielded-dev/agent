import { join } from "node:path";

import { Effect, Exit, Fiber, FileSystem, Schema } from "effect";

import { Cloudflare, request } from "../cloudflare.ts";
import { BenchError, hash, privateDirectory, redact } from "../platform.ts";
import { ProfileIdentity, type ProfileCapture, type Query } from "./protocol.ts";

const Settings = Schema.Struct({
  bindings: Schema.Array(
    Schema.Struct({
      type: Schema.String,
      name: Schema.String,
      namespace_id: Schema.optionalKey(Schema.String),
    }),
  ),
});

// The production endpoint rate-limits captures; reserve starts across concurrent Objects.
let nextCaptureAt = 0;

const reserveCapture = Effect.sync(() => {
  const now = Date.now();
  const start = Math.max(now, nextCaptureAt);

  nextCaptureAt = start + 15000;

  return start - now;
});

/** Binary production profiling API. Capture retries are read-only; turns never retry. */
export const profiler = Effect.fnUntraced(function* (
  worker: string,
  endpoint: string,
  expectedBuild: string,
  token: string,
  query: Query,
  mode: "object" | "fresh",
) {
  const cloud = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;
  const url = new URL("/profile-id", endpoint);

  for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

  const identity = (yield* request(url, token, ProfileIdentity, {}, "30 seconds", expectedBuild))
    .value;

  if (identity.isolate.build !== expectedBuild || (mode === "fresh" && identity.initialized))
    return yield* new BenchError({
      message: "Profile target build mismatch or fresh ThreadObject already initialized",
    });
  const settings = yield* cloud.api(`workers/scripts/${worker}/settings`, Settings);

  const namespace = settings.bindings.find(
    (binding) => binding.name === "YIELDED" && binding.type === "durable_object_namespace",
  )?.namespace_id;

  if (!namespace) return yield* new BenchError({ message: "Missing profile namespace" });
  const directory = join(privateDirectory, "profiles", query.object);

  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  const route = `https://api.cloudflare.com/client/v4/accounts/${cloud.accountId}/workers/workers/${worker}/versions/${identity.version}/profile`;
  const preflight: { status: number; message: string }[] = [];

  // An idle target can disappear after a successful availability check. Metadata-only
  // traffic keeps this same isolate loaded without opening the deferred ThreadObject.
  const waitActive = Effect.fnUntraced(function* (milliseconds: number) {
    const until = Date.now() + milliseconds;

    for (;;) {
      const seen = (yield* request(url, token, ProfileIdentity, {}, "30 seconds", expectedBuild))
        .value;

      if (seen.isolate.id !== identity.isolate.id)
        return yield* new BenchError({
          message: "Profile isolate changed during preparation; no input was sent",
        });
      const remaining = until - Date.now();

      if (remaining <= 0) return;
      yield* Effect.sleep(Math.min(1000, remaining));
    }
  });

  const capture = Effect.fnUntraced(function* (file: string) {
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        fetch(route, {
          method: "POST",
          signal,
          headers: {
            authorization: `Bearer ${cloud.apiToken}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            duration_ms: 20000,
            profile_type: "cpu",
            namespace_id: namespace,
            actor_id: identity.actorId,
          }),
        }),
      catch: () => new BenchError({ message: "Profile API transport failed" }),
    }).pipe(Effect.timeout("55 seconds"));

    const bytes = new Uint8Array(
      yield* Effect.tryPromise({
        try: () => response.arrayBuffer(),
        catch: () => new BenchError({ message: "Profile response was lost" }),
      }),
    );

    yield* fs.writeFile(join(directory, file), bytes, { mode: 0o600 });

    const result = {
      status: response.status,
      bytes: bytes.length,
      sha256: hash(bytes),
      file: join(directory, file),
      message: response.ok
        ? "ok"
        : redact(new TextDecoder().decode(bytes), [
            cloud.accountId,
            cloud.apiToken,
            cloud.accountName,
            cloud.subdomain,
            token,
          ]).slice(0, 1200),
      retryAfter: Number(response.headers.get("retry-after") ?? 15),
    };

    yield* fs.writeFileString(join(directory, file + ".json"), JSON.stringify(result) + "\n", {
      mode: 0o600,
    });

    return result;
  });

  const before = Effect.gen(function* () {
    const sentinel = new URL(url);

    sentinel.pathname = "/sentinel";
    yield* request(
      sentinel,
      token,
      Schema.Struct({ ok: Schema.Literal(true), result: Schema.Number }),
      {},
      "30 seconds",
      expectedBuild,
    );
  });

  // New isolates need time to become discoverable by the production profiler.
  // Keep the unopened shell active instead of spending capture quota on 404s.
  if (mode === "fresh") {
    yield* before;
    yield* waitActive(45000);
  }

  // Start the real capture directly. Early read-only failures can retry while the
  // Object stays active; no framework input is sent until a capture remains open.
  for (let attempt = 0; attempt < 8; attempt++) {
    yield* waitActive(yield* reserveCapture);
    yield* before;
    const active = yield* capture(`attempt-${attempt}.pprof.gz`).pipe(Effect.forkScoped);

    const early = yield* Effect.raceFirst(
      Fiber.await(active),
      Effect.sleep("2 seconds").pipe(Effect.as(undefined)),
    );

    if (early === undefined) {
      const record = Fiber.join(active).pipe(
        Effect.map((result): ProfileCapture => ({
          target: query.target,
          object: query.object,
          mode,
          sample: query.sample,
          status: result.status,
          bytes: result.bytes,
          sha256: result.sha256,
          file: result.file,
          preflight: [...preflight],
        })),
      );

      return { record, before };
    }
    if (Exit.isFailure(early)) return yield* Effect.failCause(early.cause);
    const result = early.value;

    preflight.push({
      status: result.status,
      message: result.status === 200 ? "Capture ended before input dispatch" : result.message,
    });
    if (result.status !== 200 && result.status !== 404 && result.status !== 429)
      return yield* new BenchError({
        message: `Profile preparation HTTP ${result.status}: ${result.message}`,
      });
    yield* waitActive(
      Math.max(15, Number.isFinite(result.retryAfter) ? result.retryAfter : 15) * 1000,
    );
  }

  return yield* new BenchError({
    message: "No active profile capture after eight read-only attempts; no input was sent",
  });
});
