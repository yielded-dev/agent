import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { convertV4MiniflareOptions, Log, LogLevel, Miniflare } from "miniflare";

import { BenchError } from "./platform.ts";
import { type BulkFixture, ImportResult, RestoreAck } from "./worker/protocol.ts";

/** Correctness only: restore disposable copies through each frozen arm before deployment. */
export const checkFixture = Effect.fnUntraced(function* (
  bundle: { readonly file: string; readonly sha256: string },
  fixture: BulkFixture,
  chunks: readonly string[],
) {
  const fs = yield* FileSystem.FileSystem;

  if (fixture.target !== "yielded")
    return yield* BenchError.make({
      message: "Native layout preflight requires a Yielded fixture.",
    });
  const script = yield* fs.readFileString(bundle.file);
  const token = "local-fixture-compatibility";

  const fail = (cause: unknown) =>
    BenchError.make({
      message: `Fixture compatibility preflight failed: ${cause instanceof Error ? cause.message : String(cause)}`,
    });

  const runtime = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            modules: true,
            script,
            log: new Log(LogLevel.NONE),
            compatibilityDate: "2026-08-18",
            compatibilityFlags: ["nodejs_compat", "global_fetch_strictly_public"],
            bindings: {
              BENCH_TOKEN: token,
              BUILD: bundle.sha256,
              CPU: "false",
              PROVIDER_URL: "https://fixture-validation.invalid/v1",
            },
            durableObjects: { YIELDED: { className: "YieldedDO", useSQLite: true } },
          }),
        ),
      catch: fail,
    }),
    (runtime) => Effect.promise(() => runtime.dispose()),
  );

  yield* Effect.tryPromise({ try: () => runtime.ready, catch: fail });

  const invoke = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(path: string, schema: S, body: string) {
    const url = new URL(path, "https://fixture-validation");

    for (const [name, value] of Object.entries({
      target: fixture.target,
      object: fixture.threadId,
      storageObject: fixture.threadId,
      history: fixture.history,
      sample: "import",
      expectedBuild: bundle.sha256,
      ttftMs: 0,
      chunkDelayMs: 0,
      textStreaming: false,
    }))
      url.searchParams.set(name, String(value));

    const response = yield* Effect.tryPromise({
      try: () =>
        runtime.dispatchFetch(url.toString(), {
          method: "POST",
          headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
          body,
        }),
      catch: fail,
    });

    const text = yield* Effect.tryPromise({ try: () => response.text(), catch: fail });

    if (!response.ok)
      return yield* BenchError.make({
        message: `Fixture ${fixture.history} ${path} refused: ${text.slice(0, 2000)}`,
      });

    return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text);
  });

  yield* invoke("/restore/start", RestoreAck, JSON.stringify(fixture));
  for (const chunk of chunks)
    yield* invoke("/restore/rows", RestoreAck, yield* fs.readFileString(chunk));
  const result = yield* invoke("/restore/finalize", ImportResult, "{}");

  if (
    result.identity.build !== bundle.sha256 ||
    result.fingerprint !== fixture.fingerprint ||
    !result.compatibility
  )
    return yield* BenchError.make({
      message: "Native fixture preflight did not attest this build and transcript.",
    });

  return result.compatibility;
}, Effect.scoped);
