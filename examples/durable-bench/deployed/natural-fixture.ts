import { join } from "node:path";

import { Console, Effect, FileSystem, Schema } from "effect";
import { convertV4MiniflareOptions, Log, LogLevel, Miniflare } from "miniflare";

import { build } from "./build.ts";
import { BenchError, execute, git, read, repository, save, workspace } from "./platform.ts";
import { ColdResult, Metrics, SeedResult, type Target } from "./worker/protocol.ts";

// Same prefix/run/hash/cell/replica shape and 50-character identity as the natural matrix.
export const NATURAL_THREAD = "durable-bench-cache-00000000-00000000-h1000-d0-o00";

/** Fixture generation only: run the deployed protocol locally; never report local timings. */
export const naturalFixture = Effect.fnUntraced(function* (
  target: Target,
  output: string,
  ref?: string,
  formatKey?: string,
) {
  const fs = yield* FileSystem.FileSystem;

  if (yield* fs.exists(output + ".json")) return;
  if (yield* fs.exists(output))
    return yield* BenchError.make({
      message: "Fixture directory exists without metadata; preserve it and choose a new directory.",
    });
  const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "durable-bench-natural-" });

  const compiled = yield* build(join(scratch, "target"), "target", ref, target);

  const provider = yield* build(join(scratch, "provider"), "provider");
  const targetScript = yield* fs.readFileString(compiled.file);
  const providerScript = yield* fs.readFileString(provider.file);
  const token = "local-fixture-only";
  const revision = compiled.sha256;
  const persist = join(scratch, "persist");

  const { fingerprint, metrics } = yield* Effect.gen(function* () {
    const mf = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          new Miniflare(
            convertV4MiniflareOptions({
              resourcePersistencePath: persist,
              log: new Log(LogLevel.NONE),
              workers: [
                {
                  modules: true,
                  script: targetScript,
                  compatibilityDate: "2026-08-18",
                  compatibilityFlags: ["nodejs_compat", "global_fetch_strictly_public"],
                  outboundService: "provider",
                  bindings: {
                    BENCH_TOKEN: token,
                    BUILD: revision,
                    CPU: "false",
                    PROVIDER_URL: "https://provider/v1",
                  },
                  durableObjects: Object.fromEntries(
                    Object.entries(
                      target === "yielded"
                        ? { YIELDED: "YieldedDO" }
                        : target === "pi"
                          ? { PI: "PiDO" }
                          : { ACTORS: "ActorDO", THREADS: "ThreadDO" },
                    ).map(([name, className]) => [name, { className, useSQLite: true }]),
                  ),
                },
                {
                  name: "provider",
                  modules: true,
                  script: providerScript,
                  compatibilityDate: "2026-08-18",
                  compatibilityFlags: ["nodejs_compat"],
                  bindings: { BENCH_TOKEN: token, BUILD: revision },
                },
              ],
            }),
          ),
        catch: (cause) =>
          BenchError.make({
            message: `Could not start the natural fixture builder: ${cause instanceof Error ? cause.message : "unknown error"}`,
          }),
      }),
      (instance) => Effect.promise(() => instance.dispose()),
    );

    yield* Effect.tryPromise({
      try: () => mf.ready,
      catch: (cause) =>
        BenchError.make({
          message: `Natural fixture startup failed: ${cause instanceof Error ? cause.message : "unknown error"}`,
        }),
    });

    const invoke = Effect.fnUntraced(function* <
      S extends Schema.Top & { readonly DecodingServices: never },
    >(path: string, schema: S, body?: unknown, sample = "import") {
      const url = new URL(path, "https://bench");

      for (const [key, value] of Object.entries({
        target,
        object: NATURAL_THREAD,
        history: 1000,
        sample,
        expectedBuild: revision,
        ttftMs: 0,
        chunkDelayMs: 0,
        textStreaming: false,
      }))
        url.searchParams.set(key, String(value));

      const response = yield* Effect.tryPromise({
        try: () =>
          mf.dispatchFetch(url.toString(), {
            method: body === undefined ? "GET" : "POST",
            headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          }),
        catch: () => BenchError.make({ message: `Natural fixture ${path} failed.` }),
      });

      const text = yield* Effect.tryPromise({
        try: () => response.text(),
        catch: () => BenchError.make({ message: "Cannot read natural fixture response." }),
      });

      if (!response.ok)
        return yield* BenchError.make({
          message: `Natural fixture ${path}: ${text.slice(0, 2000)}`,
        });

      return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text);
    });

    let fingerprint = "";

    for (let from = 0; from < 1000; from += 50) {
      if (from > 0) yield* invoke("/cold", ColdResult, {});
      const seeded = yield* invoke("/seed", SeedResult, { from, to: from + 50, startedMs: 1 });

      fingerprint = seeded.fingerprint;
      yield* Console.error(`Natural ${target} fixture: ${from + 50}/1000 turns`);
    }
    const metrics = yield* invoke("/metrics?quiescent=true", Metrics, undefined, "h999");

    return { fingerprint, metrics };
  }).pipe(Effect.scoped);

  const metadata =
    target === "yielded" && ref
      ? yield* Schema.decodeEffect(
          Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
        )(yield* git(["show", `${ref}:packages/effect-agent/package.json`]))
      : yield* read(
          join(
            target === "yielded" ? repository : workspace,
            target === "yielded"
              ? "packages/effect-agent/package.json"
              : `third-party/node_modules/${target === "pi" ? "@earendil-works/pi-durable" : "tardie"}/package.json`,
          ),
          Schema.Struct({ version: Schema.String }),
        );

  // The runtime scope closes before copying so SQLite and its WAL are flushed.
  yield* fs.makeDirectory(join(output, ".."), { recursive: true });

  const copied = yield* execute([
    "exec",
    "cp",
    ...(process.platform === "darwin" ? ["-cR"] : ["-R"]),
    persist,
    output,
  ]);

  if (copied.code !== 0)
    return yield* BenchError.make({ message: "Cannot save the natural fixture." });
  yield* save(output + ".json", {
    target,
    version: metadata.version,
    turns: 1000,
    threadId: NATURAL_THREAD,
    workload: "natural-id50-v1",
    formatKey,
    fingerprint,
    tables: metrics.tables,
    bytes: metrics.bytes,
  });
});
