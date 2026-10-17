import { builtinModules } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Effect, FileSystem, Schema } from "effect";
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
import type { Target } from "./worker/protocol.ts";

const Package = Schema.Struct({ name: Schema.String });
const Catalog = Schema.Struct({ catalog: Schema.Record(Schema.String, Schema.String) });

const SourceMap = Schema.Struct({
  sources: Schema.Array(Schema.String),
});

// Frozen revisions include dependency patch bytes that the lockfile does not identify.
const dependenciesFor = Effect.fnUntraced(function* (revision: string) {
  const fs = yield* FileSystem.FileSystem;

  const catalog = yield* Schema.decodeEffect(Schema.fromJsonString(Catalog))(
    yield* git(["show", `${revision}:package.json`]),
  );

  const expectedEffect = catalog.catalog.effect;

  if (!expectedEffect)
    return yield* BenchError.make({ message: "Ref has no Effect catalog version." });

  const root = join(
    tmpdir(),
    "durable-bench-dependencies",
    hash(repository).slice(0, 12),
    revision,
  );

  const receipt = join(root, "node_modules", ".bench-frozen-install");

  const installed = Effect.gen(function* () {
    return (yield* fs.exists(receipt)) && (yield* fs.readFileString(receipt)) === revision;
  });

  const cacheHit = yield* installed;

  if (!cacheHit) {
    yield* fs.makeDirectory(join(root, ".."), { recursive: true });

    const temporary = yield* Effect.acquireRelease(
      fs.makeTempDirectory({ directory: join(root, "..") }),
      (path) => fs.remove(path, { recursive: true, force: true }).pipe(Effect.orDie),
    );

    const archive = ChildProcess.make("vp", ["exec", "git", "archive", revision], {
      cwd: repository,
    });

    const unpack = ChildProcess.make("vp", ["exec", "tar", "-x", "-C", temporary]);
    const child = yield* archive.pipe(ChildProcess.pipeTo(unpack));

    if ((yield* child.exitCode) !== 0)
      return yield* BenchError.make({ message: "Could not unpack dependency sources." });
    const install = yield* execute(["install", "--frozen-lockfile"], temporary);

    if (install.code !== 0)
      return yield* BenchError.make({
        message: `Dependency install failed: ${redact(install.output).slice(-3000)}`,
      });
    yield* fs.writeFileString(join(temporary, "node_modules", ".bench-frozen-install"), revision);
    // Only complete installations are visible. A competing winner is immutable;
    // the scoped temporary directory discards the losing installation.
    yield* fs
      .rename(temporary, root)
      .pipe(
        Effect.catchTag("PlatformError", (error) =>
          installed.pipe(
            Effect.flatMap((complete) => (complete ? Effect.void : Effect.fail(error))),
          ),
        ),
      );
  }

  return { root, expectedEffect, cacheHit };
});

const entries = {
  driver: "driver.ts",
  provider: "provider.ts",
  tail: "tail.ts",
  yielded: "../third-party/src/deployed/index.ts",
  pi: "../third-party/src/deployed/pi-entry.ts",
  tardie: "../third-party/src/deployed/tardie-entry.ts",
  "effect-ai-floor": "../third-party/src/deployed/floor-entry.ts",
  "restore-yielded": "worker/restore-yielded.ts",
  "restore-pi": "worker/restore-pi.ts",
};

/** A ref supplies framework sources and its complete frozen dependency installation. */
export const build = Effect.fnUntraced(function* (
  output: string,
  entry: "driver" | "provider" | "tail" | "target" | "restore",
  ref?: string,
  target?: Target,
) {
  const fs = yield* FileSystem.FileSystem;
  const harnessRevision = yield* git(["rev-parse", "HEAD"]);
  const isTarget = entry === "target" || entry === "restore";

  if (isTarget && target === undefined)
    return yield* BenchError.make({ message: "Target builds require an explicit target." });

  // Controls, including every transitive support dependency, belong to the harness,
  // independently of the tested Yielded revision. Both arms take this same path.
  const sourceRef = isTarget
    ? target === "yielded"
      ? (ref ?? harnessRevision)
      : harnessRevision
    : undefined;

  let revision = harnessRevision;
  let dependencyCacheHit = false;
  let sourceRoot: string | undefined;
  let dependencyRoot = repository;
  let expectedEffect: string | undefined;
  const packages = new Set<string>();

  yield* fs.makeDirectory(output, { recursive: true, mode: 0o700 });
  if (sourceRef) {
    revision = yield* git(["rev-parse", "--verify", "--end-of-options", `${sourceRef}^{commit}`]);
    const dependencies = yield* dependenciesFor(revision);

    dependencyRoot = dependencies.root;
    dependencyCacheHit = dependencies.cacheHit;
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
      return yield* BenchError.make({
        message: "Could not unpack the requested framework revision.",
      });
    // Framework links override workspace links; dependencies come from the pinned installation.
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
  let entryDirectory = directory;
  let vendorRoot = join(workspace, "third-party");

  if (isTarget) {
    const harnessRoot = join(output, "harness");

    yield* fs.makeDirectory(harnessRoot, { recursive: true });

    const archive = ChildProcess.make(
      "vp",
      ["exec", "git", "archive", harnessRevision, "examples/durable-bench"],
      { cwd: repository },
    );

    const child = yield* archive.pipe(
      ChildProcess.pipeTo(ChildProcess.make("vp", ["exec", "tar", "-x", "-C", harnessRoot])),
    );

    if ((yield* child.exitCode) !== 0)
      return yield* BenchError.make({ message: "Could not freeze harness sources." });
    const frozenWorkspace = join(harnessRoot, "examples/durable-bench");

    entryDirectory = join(frozenWorkspace, "deployed");
    vendorRoot = join(frozenWorkspace, "third-party");
    yield* fs.symlink(
      join(workspace, "third-party/node_modules"),
      join(vendorRoot, "node_modules"),
    );
    yield* fs.symlink(
      join(dependencyRoot, "examples/durable-bench/node_modules"),
      join(frozenWorkspace, "node_modules"),
    );
  }
  const refRoot = sourceRoot;

  const resolver: Plugin = {
    name: "durable-bench-dependencies",
    setup(bundler) {
      bundler.onResolve({ filter: /.*/ }, async (args) => {
        // Tardie has an intentionally isolated pre-release Effect graph. Do not upgrade it.
        if (
          args.importer.startsWith(join(workspace, "third-party/node_modules")) ||
          args.importer.startsWith(join(vendorRoot, "node_modules")) ||
          args.importer === join(vendorRoot, "src/deployed/tardie.ts")
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

  const selected =
    entry === "target" || entry === "restore"
      ? target === "yielded"
        ? entry === "restore"
          ? "restore-yielded"
          : "yielded"
        : target === "pi"
          ? entry === "restore"
            ? "restore-pi"
            : "pi"
          : target === "tardie"
            ? "tardie"
            : "effect-ai-floor"
      : entry;

  const outfile = join(output, `${entry}.mjs`);

  yield* Effect.tryPromise({
    try: () =>
      bundle({
        entryPoints: [join(entryDirectory, entries[selected])],
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
      BenchError.make({
        message: `Cannot build ${entry}${ref ? " at the requested ref" : ""}: ${redact(cause instanceof Error ? cause.message : "Unknown build error").slice(0, 2000)}`,
      }),
  });

  if (expectedEffect !== undefined && (entry === "target" || entry === "restore")) {
    const map = yield* read(outfile + ".map", SourceMap);

    const frameworkRoots = [
      ...new Set(
        map.sources
          .filter((source) => /\/effect@[^/]+\/node_modules\/effect\//.test(source))
          .map((source) => source.split("/node_modules/effect/")[0]),
      ),
    ];

    // Bun's shared-store directory may append a content hash to the version.
    // Verify package metadata while still rejecting multiple physical Effect roots.
    const versions = [
      ...new Set(
        yield* Effect.forEach(frameworkRoots, (root) =>
          read(
            resolve(output, root, "node_modules/effect/package.json"),
            Schema.Struct({ version: Schema.String }),
          ).pipe(Effect.map((metadata) => metadata.version)),
        ),
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
      return yield* BenchError.make({
        message: `Effect bundle mismatch: expected ${expectedEffect}, found ${versions.join(",")}.`,
      });
    yield* save(outfile + ".dependencies.json", {
      revision,
      harnessRevision,
      target,
      vendorLockSha256: hash(yield* fs.readFile(join(workspace, "third-party/bun.lock"))),
      materialization: "commit-archive",
      dependencyCacheHit,
      expectedEffect,
      versions,
      frameworkRoots,
      isolatedVendorEffect,
      dependencyRoot,
    });
  }

  return {
    file: outfile,
    revision,
    harnessRevision,
    dependencyCacheHit,
    sha256: hash(yield* fs.readFile(outfile)),
  };
}, Effect.scoped);

export const ensureVendor = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  for (const name of ["package.json", "bun.lock"]) {
    const path = `examples/durable-bench/third-party/${name}`;

    if (
      (yield* fs.readFileString(join(repository, path))).trim() !==
      (yield* git(["show", `HEAD:${path}`]))
    )
      return yield* BenchError.make({
        message: "Vendor inputs must match the frozen harness commit.",
      });
  }
  const install = yield* execute(["run", "-F", "@yielded/agent-example-durable-bench", "vendor"]);

  if (install.code !== 0)
    return yield* BenchError.make({ message: "Frozen vendor install failed." });
});
