import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync, readdirSync, realpathSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { execFileSync } from "node:child_process";
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";
import { build, version as esbuildVersion } from "esbuild";

// Task-local adaptation of the KOM-433 bundle builder. Filesystem, compiler and
// git calls below form its executable boundary; no local timings are collected.
const BuildError = Schema.TaggedError()("CalibrationBuildError", { message: Schema.String });
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../../..");
const privateRoot = process.env.EVAL_COST_PRIVATE ?? "/private/tmp/effect-eval-cost-20261008";
const hash = (data) => createHash("sha256").update(data).digest("hex");
const git = (cwd, ...args) => execFileSync("git", args, { cwd, encoding: "utf8" }).trim();
const json = (path, value) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, JSON.stringify(value, null, 2) + "\n"); };
const walk = (dir) => readdirSync(dir, { withFileTypes: true }).flatMap((entry) => entry.isDirectory() ? walk(join(dir, entry.name)) : [join(dir, entry.name)]);

const counterBanner = `globalThis.__evalCost = { active: false, evaluations: 0, inlineSuccesses: 0, allocations: 0, operations: {}, constructors: {},
  reset() { this.evaluations = this.inlineSuccesses = this.allocations = 0; this.operations = {}; this.constructors = {}; this.active = true; },
  evaluation(op) { if (!this.active) return; this.evaluations++; this.operations[op] = (this.operations[op] ?? 0) + 1; },
  allocation(op) { if (!this.active) return; this.allocations++; this.constructors[op] = (this.constructors[op] ?? 0) + 1; },
  inline() { if (this.active) this.inlineSuccesses++; },
  result() { this.active = false; return { evaluations: this.evaluations, inlineSuccesses: this.inlineSuccesses, allocations: this.allocations, operations: this.operations, constructors: this.constructors }; }
};`;

export const compile = Effect.tryPromise({
  try: async () => {
    const revision = git(root, "rev-parse", "HEAD");
    const patchSha256 = hash(readFileSync(join(root, "patches/effect@4.0.0.patch")));
    const fixtureSha256 = hash(["cases.ts", "worker.ts"].map((file) => file + "\n" + readFileSync(join(here, file), "utf8")).join("\n"));
    const identities = [];
    for (const [name, checkout, commit] of [
      ["pin", null, null],
      ["base", join(privateRoot, "merge-base"), "757821fe99b7179f907d6d1a34a4e86de4173112"],
      ["head", join(privateRoot, "upstream"), "01c6222ccf74390848595633ef23410cbfa6983b"],
    ]) {
      let effectRoot = join(root, "node_modules/effect");
      if (checkout) {
        if (git(checkout, "rev-parse", "HEAD") !== commit) throw new Error("Upstream revision mismatch");
        const source = join(checkout, "packages/effect/src");
        effectRoot = join(privateRoot, "variants", name);
        const entries = walk(source).filter((file) => file.endsWith(".ts") && !file.endsWith(".d.ts"));
        await build({ entryPoints: entries, outbase: source, outdir: join(effectRoot, "dist"), bundle: false, format: "esm", platform: "neutral", target: "es2022", logLevel: "silent" });
        for (const file of walk(join(effectRoot, "dist"))) {
          const text = readFileSync(file, "utf8").replace(/(from\s*["'][^"']+|import\s*\(\s*["'][^"']+)\.ts(["'])/g, "$1.js$2");
          writeFileSync(file, text);
        }
        const manifest = JSON.parse(readFileSync(join(checkout, "packages/effect/package.json"), "utf8"));
        json(join(effectRoot, "package.json"), { name: "effect", type: "module", version: manifest.version, exports: manifest.publishConfig.exports });
        json(join(here, "build-identities", `${name}-patch.json`), { commit, patchSha256, sourcePatch: git(checkout, "diff", "--", "packages/effect/src/ai/LanguageModel.ts") });
      }
      effectRoot = realpathSync(effectRoot);
      const manifest = JSON.parse(readFileSync(join(effectRoot, "package.json"), "utf8"));
      const output = join(privateRoot, "bundles", name);
      mkdirSync(output, { recursive: true });
      const resolveEffect = {
        name: "selected-effect-dist",
        setup(builder) {
          builder.onResolve({ filter: /^effect(?:\/|$)/ }, ({ path }) => {
            const key = path === "effect" ? "." : "." + path.slice(6);
            const target = manifest.exports[key] ?? manifest.exports["./*"]?.replace("*", key.slice(2));
            if (typeof target !== "string") throw new Error(`Unsupported Effect export ${path}`);
            return { path: resolve(effectRoot, target) };
          });
        },
      };
      const shared = { bundle: true, format: "esm", platform: "neutral", target: "es2022", conditions: ["workerd", "worker", "browser", "import"], mainFields: ["module", "main"], nodePaths: [join(root, "node_modules")], external: ["cloudflare:*", "node:*"], logLevel: "silent" };
      const buildId = name === "pin" ? `effect@${manifest.version}+patch:${patchSha256}` : `${commit}+patch:${patchSha256}`;
      const timing = await build({ ...shared, entryPoints: [join(here, "worker.ts")], outfile: join(output, "worker.mjs"), metafile: true, minify: false, plugins: [resolveEffect], define: { BUILD_ID: JSON.stringify(buildId), FIXTURE_SHA: JSON.stringify(fixtureSha256) } });
      const sites = [];
      const instrument = {
        name: "kom433-primitive-counter",
        setup(builder) {
          builder.onLoad({ filter: /\/internal\/(?:core|effect)\.js$/ }, ({ path }) => {
            if (!path.startsWith(effectRoot)) return;
            let text = readFileSync(path, "utf8");
            const isCore = path.endsWith("/core.js");
            const names = isCore ? ["PrimitiveImpl", "ExitPrimitive", "Success", "Failure", "WithFiber", "WithFiberSucceed"] : ["AsyncImpl", "IteratorImpl", "ContImpl", "OnFailureImpl", "OnSuccessAndFailureImpl", "MatchImpl", "OnExitImpl", "Sync", "Suspend"];
            for (const ctor of names) {
              const expression = new RegExp(`(?:const|let) ${ctor}\\d* = function\\s*\\([^)]*\\)\\s*\\{`, "g");
              const matches = [...text.matchAll(expression)];
              if (matches.length === 0 && ["Success", "Failure", "WithFiber", "WithFiberSucceed", "Sync", "Suspend"].includes(ctor)) continue;
              if (matches.length !== 1) throw new Error(`Constructor count ${ctor}: ${matches.length}`);
              const label = ["PrimitiveImpl", "ExitPrimitive"].includes(ctor) ? `"${ctor}:" + options.op` : JSON.stringify(ctor);
              text = text.replace(matches[0][0], matches[0][0] + `\n globalThis.__evalCost.allocation(${label});`);
              sites.push({ file: path.slice(effectRoot.length + 1), constructor: ctor });
            }
            if (!isCore) {
              const marker = name === "head" ? /if \(cache\.tracerContext === (?:void 0|undefined) && current instanceof ContImpl\)/ : /current = cache\.tracerContext/;
              if ([...text.matchAll(new RegExp(marker, "g"))].length !== 1) throw new Error("Ambiguous interpreter counter site");
              text = text.replace(marker, (match) => `globalThis.__evalCost.evaluation(current["~effect/Effect/identifier"] ?? "unknown");\n${match}`);
              text = text.replace("succeedWith(value) {", "succeedWith(value) { globalThis.__evalCost.inline();");
            }
            return { contents: text, loader: "js" };
          });
        },
      };
      await build({ ...shared, entryPoints: [join(here, "cases.ts")], outfile: join(output, "counted.mjs"), plugins: [resolveEffect, instrument], banner: { js: counterBanner } });
      const counted = await import(pathToFileURL(join(output, "counted.mjs")).href);
      const counts = [];
      const lowIterations = (name) => name === "allocate" ? 1_048_576 : name === "sql" ? 4096 : ["stream8", "fn-traced", "scope", "span"].includes(name) ? 16384 : 65536;
      for (const caseName of counted.cases) for (const mode of ["plain", "effect"]) for (const iterations of [0, 1000, 10000, lowIterations(caseName), lowIterations(caseName) * 4]) {
        globalThis.__evalCost.reset();
        const checksum = counted.runCase(caseName, mode, iterations, { exec: (_query, value) => ({ one: () => ({ n: value }) }) });
        counts.push({ case: caseName, mode, iterations, checksum, ...globalThis.__evalCost.result() });
      }
      for (const row of counts.filter((row) => row.mode === "effect")) {
        const plain = counts.find((other) => other.case === row.case && other.mode === "plain" && other.iterations === row.iterations);
        if (row.checksum !== plain.checksum) throw new Error(`Plain/Effect mismatch: ${name}/${row.case}/${row.iterations}`);
      }
      if (sites.length < 9 || counts.find((row) => row.case === "sync" && row.mode === "effect" && row.iterations === 10000).evaluations < 10000) throw new Error("Missing primitive instrumentation");
      const bundle = readFileSync(join(output, "worker.mjs"));
      const identity = { name, repositoryCommit: revision, effectCommit: commit, effectVersion: manifest.version, buildId, patchSha256, fixtureSha256, bundleSha256: hash(bundle), bundleBytes: bundle.length, esbuildVersion, minify: false, target: "es2022", countedSites: sites, output };
      identities.push(identity);
      json(join(here, "build-identities", `${name}.json`), identity);
      json(join(here, "counts", `${name}.json`), counts);
      json(join(here, "build-identities", `${name}-inputs.json`), Object.keys(timing.metafile.inputs).map((path) => path.replace(privateRoot, "<private-build>").replace(root, "<repository>")));
      console.log(JSON.stringify({ build: name, bundleSha256: identity.bundleSha256, bundleBytes: identity.bundleBytes, checksums: "matched", countedCases: counts.length }));
    }
    json(join(here, "build-identities", "all.json"), identities);
  },
  catch: (cause) => new BuildError({ message: String(cause) }),
});

if (import.meta.main) NodeRuntime.runMain(compile);
