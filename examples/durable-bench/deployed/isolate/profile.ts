import { join } from "node:path";

import { Effect, FileSystem, Schema } from "effect";

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

  const capture = Effect.fnUntraced(function* (duration: number, file: string) {
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
            duration_ms: duration,
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

    return {
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
      retryAfter: Number(response.headers.get("retry-after") ?? 5),
    };
  });

  // Recent-execution discovery is delayed. Validate availability before appending the
  // one 250-turn measurement; failed preflights do not initialize the deferred shell.
  let available = false;

  for (let attempt = 0; attempt < 8; attempt++) {
    yield* request(url, token, ProfileIdentity, {}, "30 seconds", expectedBuild);
    const result = yield* capture(1000, `preflight-${attempt}.bin`);

    preflight.push({ status: result.status, message: result.message });
    if (result.status === 200) {
      available = true;
      break;
    }
    if (result.status !== 404 && result.status !== 429)
      return yield* new BenchError({
        message: `Profile preflight HTTP ${result.status}: ${result.message}`,
      });
    yield* Effect.sleep(Math.min(30, Math.max(5, result.retryAfter)) * 1000);
  }
  if (!available)
    return yield* new BenchError({
      message: "Profile target was not discoverable after eight preflights",
    });

  const record = capture(20000, "cold-first-request.pprof.gz").pipe(
    Effect.map((result): ProfileCapture => ({
      target: query.target,
      object: query.object,
      mode,
      sample: query.sample,
      status: result.status,
      bytes: result.bytes,
      sha256: result.sha256,
      file: result.file,
      preflight,
    })),
  );

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

  return { record, before };
});
