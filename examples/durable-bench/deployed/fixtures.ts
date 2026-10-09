import { builtinModules } from "node:module";
import { fileURLToPath } from "node:url";

import { Effect, FileSystem, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Log, LogLevel, Miniflare } from "miniflare";

import {
  type BulkFixture,
  Counts,
  expectedSeed,
  History,
  SqlDump,
  TARGETS,
  type Target,
} from "./worker/protocol.ts";

export class FixtureError extends Schema.TaggedError<FixtureError>()("FixtureError", {
  message: Schema.String,
}) {}

const root = fileURLToPath(new URL("../", import.meta.url));

const failure = (message: string) => (cause: unknown) =>
  new FixtureError({
    message: `${message}: ${cause instanceof Error ? cause.message : String(cause)}`,
  });

const boundary = <A>(message: string, run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: failure(message) });

const Meta = Schema.Struct({ version: Schema.String, fingerprint: Schema.String, tables: Counts });

const Export = Schema.Struct({
  tables: Counts,
  fingerprint: Schema.optionalKey(Schema.String),
  archive: Schema.optionalKey(Schema.Array(Schema.Json)),
  dump: Schema.optionalKey(SqlDump),
});

/** Seed once; use bulk import only when its complete table counts reproduce the source. */
export const prepareFixtures = Effect.fn("durableBench.prepareFixtures")(
  function* (options: {
    readonly sizes: readonly number[];
    readonly targets?: readonly Target[];
    readonly directory?: string;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const sizes = [
      ...new Set(yield* Schema.decodeUnknownEffect(Schema.Array(History))(options.sizes)),
    ].sort((a, b) => a - b);

    const targets = options.targets ?? TARGETS;
    const directory = options.directory ?? `${root}fixtures`;
    const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "durable-bench-transfer-" });
    const script = `${scratch}/fixture.mjs`;

    yield* boundary("Build local fixture transfer", () =>
      build({
        entryPoints: [fileURLToPath(new URL("./worker/fixture-worker.ts", import.meta.url))],
        outfile: script,
        bundle: true,
        format: "esm",
        platform: "neutral",
        target: "es2024",
        conditions: ["workerd", "worker", "browser", "import"],
        mainFields: ["module", "main"],
        external: ["cloudflare:*", "node:*", ...builtinModules],
        logLevel: "silent",
      }),
    );

    const scriptContent = yield* fs.readFileString(script);

    const read = Effect.fnUntraced(function* (
      target: Target,
      persist: string,
      path: string,
      body?: BulkFixture,
      actor = false,
    ) {
      const className =
        target === "yielded"
          ? path === "/import"
            ? "NormalizedYieldedDO"
            : "YieldedDO"
          : target === "pi"
            ? "PiDO"
            : actor
              ? "ActorDO"
              : "ThreadDO";

      return yield* Effect.gen(function* () {
        const mf = yield* Effect.acquireRelease(
          boundary("Open local transfer Worker", async () => {
            const mf = new Miniflare(
              convertV4MiniflareOptions({
                modules: true,
                script: scriptContent,
                compatibilityDate: "2026-08-18",
                compatibilityFlags: ["nodejs_compat"],
                log: new Log(LogLevel.NONE),
                handleStructuredLogs: () => {},
                durableObjects: {
                  OBJECT: { className, useSQLite: true },
                  YIELDED: { className: "NormalizedYieldedDO", useSQLite: true },
                },
                bindings: {
                  BUILD: "local-fixture",
                  BENCH_TOKEN: "local-fixture",
                  PROVIDER_URL: "http://unused/v1",
                },
                resourcePersistencePath: persist,
              }),
            );

            return mf;
          }),
          (mf) => Effect.promise(() => mf.dispose()),
        );

        yield* boundary("Start local transfer Worker", () => mf.ready);

        return yield* boundary("Transfer local fixture", async () => {
          const response = await mf.dispatchFetch(
            `http://fixture${path}?target=${target}&actor=${actor}`,
            body === undefined ? {} : { method: "POST", body: JSON.stringify(body) },
          );

          if (!response.ok) throw new Error(await response.text());

          return Schema.decodeUnknownSync(Export)(await response.json());
        });
      }).pipe(Effect.scoped);
    });

    const result: BulkFixture[] = [];

    for (const target of targets) {
      const present = yield* Effect.forEach(sizes, (size) =>
        fs.exists(`${directory}/${target}-${size}.json`),
      );

      const missing = sizes.filter((_size, index) => !present[index]);

      if (missing.length) {
        if (directory !== `${root}fixtures`)
          return yield* new FixtureError({ message: `Missing seed fixture in ${directory}` });

        const code = yield* spawner.exitCode(
          ChildProcess.make("vp", ["run", "seed", target, ...missing.map(String)], {
            cwd: root,
            stdout: "inherit",
            stderr: "inherit",
          }),
        );

        if (Number(code) !== 0)
          return yield* new FixtureError({ message: `Seed ${target} failed with exit ${code}` });
      }
      for (const history of sizes) {
        const source = `${directory}/${target}-${history}`;

        const meta = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Meta))(
          yield* fs.readFileString(`${source}.json`),
        );

        if (
          (target === "pi" && meta.version !== "1.0.4") ||
          (target === "tardie" && meta.version !== "0.44.0")
        )
          return yield* new FixtureError({
            message: `Unexpected ${target} fixture version ${meta.version}`,
          });
        const snapshot = `${scratch}/${target}-${history}`;

        yield* fs.copy(source, snapshot);
        const exported = yield* read(target, snapshot, "/export");

        if (
          exported.fingerprint !== meta.fingerprint ||
          (expectedSeed[history] !== undefined && meta.fingerprint !== expectedSeed[history])
        )
          return yield* new FixtureError({
            message: `Local ${target}/${history} fingerprint mismatch`,
          });
        if (
          Object.keys(exported.tables).sort().join("\n") !==
          Object.keys(meta.tables).sort().join("\n")
        )
          return yield* new FixtureError({
            message: `Local ${target}/${history} table inventory changed after seed`,
          });
        for (const [name, count] of Object.entries(meta.tables))
          if (exported.tables[name] !== count)
            return yield* new FixtureError({
              message: `Local ${target}/${history} table ${name} changed after seed`,
            });

        let fixture: BulkFixture = {
          version: 1,
          target,
          history,
          sourceVersion: meta.version,
          fingerprint: meta.fingerprint,
          mode: "import",
          tables: exported.tables,
          ...(exported.archive === undefined ? {} : { archive: exported.archive }),
          ...(exported.dump === undefined ? {} : { thread: exported.dump }),
        };

        if (target === "yielded") {
          const normalized = yield* read(
            target,
            `${scratch}/normalized-${history}`,
            "/import",
            fixture,
          );

          if (normalized.fingerprint !== fixture.fingerprint)
            return yield* new FixtureError({ message: "Canonical import changed transcript" });

          const differences = Object.keys({ ...fixture.tables, ...normalized.tables })
            .filter((table) => fixture.tables[table] !== normalized.tables[table])
            .map(
              (table) =>
                `${table} ${fixture.tables[table] ?? "absent"} -> ${normalized.tables[table] ?? "absent"}`,
            );

          if (differences.length > 0) {
            const { archive: _archive, ...source } = fixture;

            fixture = {
              ...source,
              mode: "replay",
              fallbackReason: `Canonical import changes source table counts: ${differences.join(", ")}`,
            };
          }
        }
        if (target === "tardie") {
          const actor = yield* read(target, snapshot, "/export", undefined, true);

          if (!actor.dump)
            return yield* new FixtureError({ message: "Missing Tardie Actor fixture" });
          fixture = { ...fixture, actor: actor.dump };
        }
        result.push(fixture);
      }
    }

    return result;
  },
  Effect.scoped,
  Effect.mapError((cause) =>
    cause instanceof FixtureError ? cause : failure("Prepare local fixtures")(cause),
  ),
);
