import { createHash } from "node:crypto";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem } from "effect";
import { build, version as esbuildVersion } from "esbuild";

const [checkoutArg, outputArg, fixtureRootArg, mode = "original"] = process.argv.slice(2);
if (!checkoutArg || !outputArg || !fixtureRootArg || !["original", "cpu"].includes(mode)) {
  throw new Error("stage <checkout> <new-output> <fixture-source-checkout> [original|cpu]");
}
const checkout = resolve(checkoutArg);
const output = resolve(outputArg);
const fixtureRoot = resolve(fixtureRootArg);
const { stageCheckout } = await import(join(fixtureRoot, "scripts/runtime-benchmark.ts"));
const { withPublishManifests } = await import(join(fixtureRoot, "scripts/release-publish.ts"));
const { BenchmarkError } = await import(join(fixtureRoot, "tooling/runtime-benchmark/src/contracts.ts"));
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const files = ["contracts.ts", "fixture.ts", "worker.ts", "evidence.ts", "seeds.ts", "ids.ts", "history.ts", "settlement.ts", "cpu-profile.ts", "steady-state.ts"];

NodeRuntime.runMain(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  if (yield* fs.exists(output)) return yield* BenchmarkError.make({ message: `Refusing existing output ${output}` });
  const fixtures = yield* fs.makeTempDirectoryScoped({ prefix: "kom433-fixture-" });
  yield* Effect.tryPromise({
    try: () => build({
      entryPoints: files.map((file) => join(fixtureRoot, "tooling/runtime-benchmark/src", file)),
      outdir: fixtures, bundle: false, platform: "node", format: "esm", target: "node24",
      sourcemap: false, minify: false, logLevel: "silent",
    }),
    catch: (cause) => BenchmarkError.make({ message: "Cannot transpile shared benchmark fixture", cause }),
  });
  if (mode === "cpu") {
    yield* fs.copyFile(fileURLToPath(new URL("./cpu-capture.mjs", import.meta.url)), join(fixtures, "cpu-profile.js"));
    const file = join(fixtures, "steady-state.js");
    const original = yield* fs.readFileString(file);
    const field = "samplingIntervalMicros: profiler === void 0 ? null : STEADY_STATE.samplingIntervalMicros";
    if (!original.includes(field)) return yield* BenchmarkError.make({ message: "Resident fixture sampling field changed" });
    yield* fs.writeFileString(file, original.replace(field, "samplingIntervalMicros: null"));
  }
  const staged = yield* stageCheckout(checkout, "base", fixtures);
  yield* withPublishManifests(staged.stage, () => Effect.gen(function* () {
    yield* fs.makeDirectory(output, { recursive: true });
    for (const entry of ["packages", "fixture", "package.json"]) yield* fs.copy(join(staged.stage, entry), join(output, entry));
    yield* fs.makeDirectory(join(output, "node_modules"));
    for (const entry of yield* fs.readDirectory(join(staged.stage, "node_modules"))) {
      if (entry === "@yielded" || entry === "@effect-agent" || entry === "effect-agent") continue;
      yield* fs.symlink(yield* fs.realPath(join(staged.stage, "node_modules", entry)), join(output, "node_modules", entry));
    }
    for (const directory of yield* fs.readDirectory(join(output, "packages"))) {
      const destination = join(output, "packages", directory);
      const manifest = JSON.parse(yield* fs.readFileString(join(destination, "package.json")));
      const link = join(output, "node_modules", manifest.name);
      yield* fs.makeDirectory(resolve(link, ".."), { recursive: true });
      yield* fs.symlink(destination, link);
    }
  }));
  const fixtureFiles = [];
  for (const file of (yield* fs.readDirectory(join(output, "fixture"))).sort()) {
    fixtureFiles.push({ file, sha256: hash(yield* fs.readFile(join(output, "fixture", file))) });
  }
  const identity = { ...staged.revision, checkout, fixtureRoot, mode, esbuildVersion, stagingRuntime: { bun: process.versions.bun ?? null, nodeCompatibility: process.version }, fixtureFiles, capturedAt: new Date().toISOString() };
  yield* fs.writeFileString(join(output, "identity.json"), JSON.stringify(identity, null, 2) + "\n");
  yield* Console.log(JSON.stringify(identity));
}).pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
