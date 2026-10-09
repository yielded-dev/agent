import { builtinModules } from "node:module";
import { join } from "node:path";

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
  workspace,
} from "./platform.ts";

const Package = Schema.Struct({ name: Schema.String });

const entries = {
  driver: "driver.ts",
  provider: "provider.ts",
  target: "../third-party/src/deployed/index.ts",
  production: "../src/yielded.ts",
  bare: "isolate/bare.ts",
  yielded: "isolate/yielded.ts",
  pi: "../third-party/src/deployed/isolate-pi.ts",
};

/** A ref supplies framework sources; both builds use this checkout's harness and pinned dependencies. */
export const build = Effect.fnUntraced(function* (
  output: string,
  entry: keyof typeof entries,
  ref?: string,
  extraPlugins: readonly Plugin[] = [],
) {
  const fs = yield* FileSystem.FileSystem;
  let revision = yield* git(["rev-parse", "HEAD"]);
  let sourceRoot: string | undefined;
  const packages = new Set<string>();

  yield* fs.makeDirectory(output, { recursive: true, mode: 0o700 });
  if (ref) {
    revision = yield* git(["rev-parse", "--verify", "--end-of-options", `${ref}^{commit}`]);
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
    // Framework links override workspace links; all third-party packages retain the installed catalog versions.
    yield* fs.symlink(join(repository, "node_modules"), join(output, "node_modules"));
    for (const name of yield* fs.readDirectory(join(sourceRoot, "packages"))) {
      const packageRoot = join(sourceRoot, "packages", name);

      if (!(yield* fs.exists(join(packageRoot, "package.json")))) continue;
      const metadata = yield* read(join(packageRoot, "package.json"), Package);
      const link = join(sourceRoot, "node_modules", metadata.name);

      yield* fs.makeDirectory(join(link, ".."), { recursive: true });
      yield* fs.symlink(packageRoot, link);
      const dependencies = join(repository, "packages", name, "node_modules");

      if (yield* fs.exists(dependencies))
        yield* fs.symlink(dependencies, join(packageRoot, "node_modules"));
      packages.add(metadata.name);
    }
  }
  const refRoot = sourceRoot;

  const resolver: Plugin = {
    name: "durable-bench-dependencies",
    setup(bundler) {
      bundler.onResolve({ filter: /^[^./]/ }, (args) => {
        const localPackage = [...packages].some(
          (name) => args.path === name || args.path.startsWith(name + "/"),
        );

        if (args.pluginData?.resolved || !refRoot || !localPackage) return;

        return bundler.resolve(args.path, {
          resolveDir: refRoot,
          kind: args.kind,
          pluginData: { resolved: true },
        });
      });
    },
  };

  const outfile = join(output, `${entry}.mjs`);

  const compiled = yield* Effect.tryPromise({
    try: () =>
      bundle({
        entryPoints: [join(directory, entries[entry])],
        outfile,
        bundle: true,
        format: "esm",
        platform: "neutral",
        target: "es2024",
        conditions: ["workerd", "worker", "browser", "import"],
        mainFields: ["module", "main"],
        external: ["cloudflare:*", "node:*", ...builtinModules],
        logLevel: "silent",
        plugins: [resolver, ...extraPlugins],
        metafile: true,
      }),
    catch: (cause) =>
      new BenchError({
        message: `Cannot build ${entry}${ref ? " at the requested ref" : ""}: ${redact(cause instanceof Error ? cause.message : "Unknown build error").slice(0, 2000)}`,
      }),
  });

  yield* fs.writeFileString(join(output, `${entry}-meta.json`), JSON.stringify(compiled.metafile));

  return { file: outfile, revision, sha256: hash(yield* fs.readFile(outfile)) };
}, Effect.scoped);

export const ensureVendor = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  const versions = [
    ["tardie", "0.44.0"],
    ["@earendil-works/pi-durable", "1.0.4"],
  ] as const;

  const installed = Effect.gen(function* () {
    for (const [name, version] of versions) {
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
