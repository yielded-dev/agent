import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { builtinModules } from "node:module";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { build, version as esbuildVersion } from "esbuild";
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../../..");
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const BuildError = Schema.TaggedError()("RealTurnBuildError", { message: Schema.String });

NodeRuntime.runMain(Effect.tryPromise({ try: async () => {
  const output = join(process.env.EVAL_COST_PRIVATE ?? "/private/tmp/effect-eval-cost-20261008", "turn-bundle");
  mkdirSync(output, { recursive: true });
  const files = ["examples/durable-bench/src/yielded.ts", "examples/durable-bench/src/plan.ts", "examples/durable-bench/src/serve.ts"];
  const fixtureFiles = files.map((path) => ({ path, sha256: hash(readFileSync(join(root, path))) }));
  const fixtureSha256 = hash(JSON.stringify(fixtureFiles));
  const patchSha256 = hash(readFileSync(join(root, "patches/effect@4.0.0.patch")));
  const buildId = `effect@4.0.0+patch:${patchSha256}`;
  const compiled = await build({
    entryPoints: [join(here, "worker.ts")], outfile: join(output, "worker.mjs"), bundle: true, format: "esm", platform: "neutral", target: "es2024",
    conditions: ["workerd", "worker", "browser", "import"], mainFields: ["module", "main"], external: ["cloudflare:*", "node:*", ...builtinModules],
    logLevel: "error", metafile: true, define: { BUILD_ID: JSON.stringify(buildId), FIXTURE_SHA: JSON.stringify(fixtureSha256) },
  });
  const bundle = readFileSync(join(output, "worker.mjs"));
  const identity = { name: "pin", repositoryCommit: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
    effectVersion: "4.0.0", buildId, patchSha256, fixtureSha256, fixtureFiles, wrapperSha256: hash(readFileSync(join(here, "worker.ts"))),
    bundleSha256: hash(bundle), bundleBytes: bundle.length, esbuildVersion, minify: false, target: "es2024", output };
  mkdirSync(join(here, "build-identities"), { recursive: true });
  writeFileSync(join(here, "build-identities/all.json"), JSON.stringify([identity], null, 2) + "\n");
  writeFileSync(join(here, "build-identities/inputs.json"), JSON.stringify(Object.keys(compiled.metafile.inputs).sort().map((path) => ({ path, sha256: hash(readFileSync(path)) })), null, 2) + "\n");
  console.log(JSON.stringify({ bundleSha256: identity.bundleSha256, bundleBytes: identity.bundleBytes, fixtureSha256 }));
}, catch: (cause) => new BuildError({ message: String(cause) }) }));
