import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Console, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { Command, Flag } from "effect/cli";
import { ChildProcess } from "effect/process";

import type { Target } from "../../../bench/targets.ts";
import { REFERENCE_ALGORITHM, REFERENCE_CHECKSUM, REFERENCE_ITERATIONS } from "./contracts.ts";
import { locationHint, referenceSource, wrapper } from "./wrapper.ts";

const sizes = [50, 250, 1000];
const samples = 3;
const benchDirectory = fileURLToPath(new URL("../../../", import.meta.url));

class BuildError extends Schema.TaggedError<BuildError>()("BuildError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

const FileIdentity = Schema.Struct({
  sha256: Schema.String,
  rawBytes: Schema.Int,
  gzipBytes: Schema.Int,
});

const PackageIdentity = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  packageJson: Schema.String,
  packageJsonSha256: Schema.String,
});

const EffectIdentity = Schema.Struct({
  ...PackageIdentity.fields,
  // Hash every runtime JS file, including relative paths, so same-version patches are distinct.
  runtimeSha256: Schema.String,
});

const Manifest = Schema.Struct({
  target: Schema.Literals(["yielded", "pi", "tardie"]),
  label: Schema.Literals(["pinned", "base", "head", "control", "pi", "tardie"]),
  repositoryCommit: Schema.String,
  effect: EffectIdentity,
  targetPackage: PackageIdentity,
  fixtureHashes: Schema.Record(Schema.String, Schema.String),
  bench: FileIdentity,
  wrapper: FileIdentity,
  sizes: Schema.Array(Schema.Int),
  samples: Schema.Int,
  locationHint: Schema.Literal("wnam"),
  reference: Schema.Struct({
    algorithm: Schema.Literal(REFERENCE_ALGORITHM),
    iterations: Schema.Literal(REFERENCE_ITERATIONS),
    checksum: Schema.Literal(REFERENCE_CHECKSUM),
    sourceSha256: Schema.String,
  }),
});

const sha256 = (bytes: Uint8Array | string): string =>
  createHash("sha256").update(bytes).digest("hex");

const fileIdentity = (bytes: Uint8Array) => ({
  sha256: sha256(bytes),
  rawBytes: bytes.byteLength,
  gzipBytes: gzipSync(bytes, { level: 9 }).byteLength,
});

// prepare() owns its esbuild options and relative paths. This single-purpose CLI
// temporarily enters the original bench cwd and never changes installed packages.
const prepareOriginal = (target: Target) =>
  Effect.tryPromise({
    try: async () => {
      const previous = process.cwd();

      process.chdir(benchDirectory);
      try {
        // esbuild captures cwd on import, so targets must also load from the bench cwd.
        const { prepare, TARGETS } = await import("../../../bench/targets.ts");

        return { original: await prepare(target), definition: TARGETS[target] };
      } finally {
        process.chdir(previous);
      }
    },
    catch: (cause) => new BuildError({ message: `prepare(${target}) failed`, cause }),
  });

const packageIdentity = Effect.fnUntraced(function* (file: string) {
  const fs = yield* FileSystem.FileSystem;
  const packageJson = yield* fs.realPath(file);
  const bytes = yield* fs.readFileString(packageJson);

  const metadata = yield* Schema.decodeEffect(
    Schema.fromJsonString(
      Schema.Struct({
        name: Schema.String,
        version: Schema.String,
      }),
    ),
  )(bytes);

  return { ...metadata, packageJson, packageJsonSha256: sha256(bytes) };
});

const effectIdentity = Effect.fnUntraced(function* (entry: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const packageJson = yield* Effect.try({
    try: () => createRequire(entry).resolve("effect/package.json"),
    catch: (cause) => new BuildError({ message: `Cannot resolve Effect for ${entry}`, cause }),
  });

  const identity = yield* packageIdentity(packageJson);
  const runtime = path.join(path.dirname(identity.packageJson), "dist");

  const files = (yield* fs.readDirectory(runtime, { recursive: true }))
    .filter((file) => file.endsWith(".js"))
    .sort();

  const hashes = yield* Effect.forEach(
    files,
    Effect.fnUntraced(function* (file) {
      return `${file}\0${sha256(yield* fs.readFile(path.join(runtime, file)))}`;
    }),
  );

  return { ...identity, runtimeSha256: sha256(hashes.join("\n")) };
});

export const command = Command.make(
  "cf-bench-build",
  {
    target: Flag.Literals("target", ["yielded", "pi", "tardie"]),
    label: Flag.Literals("label", ["pinned", "base", "head", "control", "pi", "tardie"]),
    outputDir: Flag.String("output-dir").pipe(Flag.withSchema(Schema.NonEmptyString)),
  },
  Effect.fnUntraced(function* ({ target, label, outputDir }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    // Resolve before prepare() changes cwd, preserving the caller's relative output path.
    const output = path.resolve(outputDir);
    const repository = path.resolve(benchDirectory, "../..");

    const git = yield* ChildProcess.make("git", ["rev-parse", "HEAD"], {
      cwd: repository,
      stdout: "pipe",
      stderr: "pipe",
    });

    const [commit, gitError, exitCode] = yield* Effect.all(
      [
        Stream.mkString(Stream.decodeText(git.stdout)),
        Stream.mkString(Stream.decodeText(git.stderr)),
        git.exitCode,
      ],
      { concurrency: "unbounded" },
    );

    if (exitCode !== 0)
      return yield* new BuildError({ message: `git rev-parse HEAD: ${gitError}` });
    const { original, definition } = yield* prepareOriginal(target);
    const entry = path.join(benchDirectory, definition.entry);

    // Pi has no Effect dependency; record the parent installation used by the build CLI.
    const effect = yield* effectIdentity(
      target === "pi" ? path.join(benchDirectory, "src/yielded.ts") : entry,
    );

    const targetPackage = yield* packageIdentity(
      path.resolve(benchDirectory, definition.packageJson),
    );

    const fixtureFiles = [
      "src/plan.ts",
      "src/serve.ts",
      definition.entry,
      "bench/targets.ts",
      "bench/seed.ts",
    ];

    const fixtureHashes: Record<string, string> = {};

    for (const file of fixtureFiles)
      fixtureHashes[file] = sha256(yield* fs.readFile(path.join(benchDirectory, file)));
    if (target === "tardie") {
      const naming = "third-party/node_modules/tardie/src/platform/cloudflare/objects.ts";

      fixtureHashes[naming] = sha256(yield* fs.readFile(path.join(benchDirectory, naming)));
    }
    yield* fs.makeDirectory(output, { recursive: true });
    yield* fs.copyFile(original, path.join(output, "bench.mjs"));
    yield* fs.writeFileString(path.join(output, "worker.mjs"), wrapper(target, sizes, samples));
    const bench = fileIdentity(yield* fs.readFile(path.join(output, "bench.mjs")));

    if (bench.sha256 !== sha256(yield* fs.readFile(original))) {
      return yield* new BuildError({ message: "bench.mjs differs from prepare() output" });
    }

    const manifest = yield* Schema.encodeEffect(Schema.fromJsonString(Manifest))({
      target,
      label,
      repositoryCommit: commit.trim(),
      effect,
      targetPackage,
      fixtureHashes,
      bench,
      wrapper: fileIdentity(yield* fs.readFile(path.join(output, "worker.mjs"))),
      sizes,
      samples,
      locationHint,
      reference: {
        algorithm: REFERENCE_ALGORITHM,
        iterations: REFERENCE_ITERATIONS,
        checksum: REFERENCE_CHECKSUM,
        sourceSha256: sha256(referenceSource),
      },
    });

    yield* fs.writeFileString(path.join(output, "manifest.json"), manifest + "\n");
    yield* Console.log(path.join(output, "manifest.json"));
  }),
).pipe(
  Command.withDescription(
    "Copy the original durable bench bundle and add an unbundled Cloudflare wrapper (50,250,1000; 3 samples; wnam).",
  ),
);

if (import.meta.main) {
  command.pipe(
    Command.run({ version: "1.0.0" }),
    Effect.scoped,
    Effect.provide(BunServices.layer),
    BunRuntime.runMain,
  );
}
