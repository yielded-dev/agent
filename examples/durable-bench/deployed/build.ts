import { builtinModules } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect, FileSystem, Schema, Semaphore } from "effect";
import { ChildProcess } from "effect/process";
import { build as bundle, type Plugin } from "esbuild";

import {
  BenchError,
  directory,
  execute,
  git,
  hash,
  read,
  redact,
  repository,
  save,
  workspace,
} from "./platform.ts";

const Package = Schema.Struct({ name: Schema.String });
const Catalog = Schema.Struct({ catalog: Schema.Record(Schema.String, Schema.String) });

const SourceMap = Schema.Struct({
  sources: Schema.Array(Schema.String),
});

const dependencyLock = Semaphore.makeUnsafe(1);

// Reuse an installation only when the ref's complete lockfile matches.
const dependenciesFor = Effect.fnUntraced(function* (revision: string) {
  const fs = yield* FileSystem.FileSystem;
  const lock = yield* git(["show", `${revision}:bun.lock`]);
  const harnessLock = (yield* fs.readFileString(join(repository, "bun.lock"))).trim();

  const catalog = yield* Schema.decodeEffect(Schema.fromJsonString(Catalog))(
    yield* git(["show", `${revision}:package.json`]),
  );

  const expectedEffect = catalog.catalog.effect;

  if (!expectedEffect)
    return yield* new BenchError({ message: "Ref has no Effect catalog version." });
  if (lock === harnessLock) return { root: repository, expectedEffect };

  const root = join(
    tmpdir(),
    "durable-bench-dependencies",
    hash(repository).slice(0, 12),
    hash(lock),
  );

  yield* dependencyLock.withPermit(
    Effect.gen(function* () {
      if (!(yield* fs.exists(join(root, ".git")))) {
        yield* fs.makeDirectory(join(root, ".."), { recursive: true });
        yield* git(["worktree", "add", "--detach", root, revision]);
      }
      const receipt = join(root, "node_modules", ".bench-frozen-install");

      if (!(yield* fs.exists(receipt))) {
        const installed = yield* execute(["install", "--frozen-lockfile"], root);

        if (installed.code !== 0)
          return yield* new BenchError({
            message: `Dependency install failed: ${redact(installed.output).slice(-3000)}`,
          });
        yield* fs.writeFileString(receipt, hash(lock));
      }
    }),
  );

  return { root, expectedEffect };
});

const entries = {
  driver: "driver.ts",
  provider: "provider.ts",
  tail: "tail.ts",
  target: "../third-party/src/deployed/index.ts",
  restore: "worker/restore-worker.ts",
};

/** A ref supplies framework sources and, when its lock differs, its frozen dependencies. */
export const build = Effect.fnUntraced(function* (
  output: string,
  entry: keyof typeof entries,
  ref?: string,
) {
  const fs = yield* FileSystem.FileSystem;
  let revision = yield* git(["rev-parse", "HEAD"]);
  let sourceRoot: string | undefined;
  let dependencyRoot = repository;
  let expectedEffect: string | undefined;
  const packages = new Set<string>();

  yield* fs.makeDirectory(output, { recursive: true, mode: 0o700 });
  if (ref) {
    revision = yield* git(["rev-parse", "--verify", "--end-of-options", `${ref}^{commit}`]);
    const dependencies = yield* dependenciesFor(revision);

    dependencyRoot = dependencies.root;
    expectedEffect = dependencies.expectedEffect;
    sourceRoot = join(output, "source");
    yield* fs.makeDirectory(sourceRoot, { recursive: true, mode: 0o700 });

    const archive = ChildProcess.make("vp", ["exec", "git", "archive", revision, "packages"], {
      cwd: repository,
    });

    const unpack = ChildProcess.make("vp", ["exec", "tar", "-x", "-C", sourceRoot]);
    const child = yield* archive.pipe(ChildProcess.pipeTo(unpack));
    const exitCode = yield* child.exitCode;

    if (exitCode !== 0)
      return yield* new BenchError({
        message: "Could not unpack the requested framework revision.",
      });
    // Framework links override workspace links; dependencies come from this revision's lockfile.
    yield* fs.symlink(join(dependencyRoot, "node_modules"), join(output, "node_modules"));
    for (const name of yield* fs.readDirectory(join(sourceRoot, "packages"))) {
      const packageRoot = join(sourceRoot, "packages", name);

      if (!(yield* fs.exists(join(packageRoot, "package.json")))) continue;
      const metadata = yield* read(join(packageRoot, "package.json"), Package);
      const link = join(sourceRoot, "node_modules", metadata.name);

      yield* fs.makeDirectory(join(link, ".."), { recursive: true });
      yield* fs.symlink(packageRoot, link);
      const dependencies = join(dependencyRoot, "packages", name, "node_modules");

      if (yield* fs.exists(dependencies))
        yield* fs.symlink(dependencies, join(packageRoot, "node_modules"));
      packages.add(metadata.name);
    }
  }
  const refRoot = sourceRoot;

  const resolver: Plugin = {
    name: "durable-bench-dependencies",
    setup(bundler) {
      bundler.onResolve({ filter: /.*/ }, async (args) => {
        // Tardie has an intentionally isolated pre-release Effect graph. Do not upgrade it.
        if (
          args.importer.startsWith(join(workspace, "third-party/node_modules")) ||
          args.importer === join(workspace, "third-party/src/deployed/tardie.ts")
        )
          return;

        const relativeShared =
          /(?:^|\/)node_modules\/(effect|@effect\/[^/]+)\/dist\/(.+)\.js$/.exec(args.path);

        const specifier = relativeShared
          ? relativeShared[1] + (relativeShared[2] === "index" ? "" : "/" + relativeShared[2])
          : args.path;

        const localPackage = [...packages].some(
          (name) => specifier === name || specifier.startsWith(name + "/"),
        );

        const sharedEffect =
          specifier === "effect" ||
          specifier.startsWith("effect/") ||
          specifier.startsWith("@effect/") ||
          specifier === "effect-cf" ||
          specifier.startsWith("effect-cf/");

        if (args.pluginData?.resolved || !refRoot || (!localPackage && !sharedEffect)) return;

        const roots = localPackage
          ? [refRoot]
          : [
              join(dependencyRoot, "examples/durable-bench"),
              join(dependencyRoot, "packages/platform-cloudflare"),
              join(dependencyRoot, "packages/effect-agent"),
              dependencyRoot,
            ];

        for (const resolveDir of roots) {
          const result = await bundler.resolve(specifier, {
            resolveDir,
            kind: args.kind,
            pluginData: { resolved: true },
          });

          if (result.errors.length === 0) return result;
        }

        return { errors: [{ text: `Cannot resolve ${args.path} in revision dependencies` }] };
      });
    },
  };

  const outfile = join(output, `${entry}.mjs`);

  yield* Effect.tryPromise({
    try: () =>
      bundle({
        entryPoints: [join(directory, entries[entry])],
        outfile,
        bundle: true,
        // Remove source-path comments so equal bundles hash equally across run directories.
        minifyWhitespace: true,
        sourcemap: entry === "target" || entry === "restore" ? "linked" : false,
        format: "esm",
        platform: "neutral",
        target: "es2024",
        conditions: ["workerd", "worker", "browser", "import"],
        mainFields: ["module", "main"],
        // Bun lists ws as built-in; workerd needs its browser export bundled.
        external: ["cloudflare:*", "node:*", ...builtinModules.filter((name) => name !== "ws")],
        logLevel: "silent",
        plugins: [resolver],
      }),
    catch: (cause) =>
      new BenchError({
        message: `Cannot build ${entry}${ref ? " at the requested ref" : ""}: ${redact(cause instanceof Error ? cause.message : "Unknown build error").slice(0, 2000)}`,
      }),
  });

  if (expectedEffect !== undefined && (entry === "target" || entry === "restore")) {
    const map = yield* read(outfile + ".map", SourceMap);

    const versions = [
      ...new Set(
        map.sources.flatMap((source) => {
          const match = /\/effect@([^/]+)\/node_modules\/effect\//.exec(source);

          return match ? [match[1]] : [];
        }),
      ),
    ];

    const frameworkRoots = [
      ...new Set(
        map.sources
          .filter((source) => /\/effect@[^/]+\/node_modules\/effect\//.test(source))
          .map((source) => source.split("/node_modules/effect/")[0]),
      ),
    ];

    const isolatedVendorEffect = map.sources.some((source) =>
      source.includes("/third-party/node_modules/effect/"),
    )
      ? (yield* read(
          join(workspace, "third-party/node_modules/effect/package.json"),
          Schema.Struct({ version: Schema.String }),
        )).version
      : undefined;

    if (frameworkRoots.length !== 1 || versions.length !== 1 || versions[0] !== expectedEffect)
      return yield* new BenchError({
        message: `Effect bundle mismatch: expected ${expectedEffect}, found ${versions.join(",")}.`,
      });
    yield* save(outfile + ".dependencies.json", {
      revision,
      expectedEffect,
      versions,
      frameworkRoots,
      isolatedVendorEffect,
      dependencyRoot,
    });
  }

  return { file: outfile, revision, sha256: hash(yield* fs.readFile(outfile)) };
}, Effect.scoped);

export const ensureVendor = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  const { dependencies } = yield* read(
    join(workspace, "third-party/package.json"),
    Schema.Struct({ dependencies: Schema.Record(Schema.String, Schema.String) }),
  );

  const installed = Effect.gen(function* () {
    for (const [name, version] of Object.entries(dependencies)) {
      const file = join(workspace, "third-party/node_modules", name, "package.json");

      if (!(yield* fs.exists(file))) return false;
      if ((yield* read(file, Schema.Struct({ version: Schema.String }))).version !== version)
        return false;
    }

    return true;
  });

  if (!(yield* installed)) {
    const install = yield* execute(["run", "-F", "@yielded/agent-example-durable-bench", "vendor"]);

    if (install.code !== 0 || !(yield* installed))
      return yield* new BenchError({
        message: "Install the durable-bench vendor dependencies first.",
      });
  }
});
