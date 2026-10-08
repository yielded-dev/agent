import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { build, version as esbuildVersion } from "esbuild";
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { instrumentSource } from "./instrument-build.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../../..");
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const git = (...args) => execFileSync("vp", ["exec", "git", ...args], { cwd: root, encoding: "utf8", maxBuffer: 8 * 1024 * 1024 }).trim();
const BuildError = Schema.TaggedError()("WakeDeferBuildError", { message: Schema.String });

export const program = Effect.tryPromise({
  try: async () => {
    const mode = process.argv.slice(2).filter((arg) => arg !== "--")[0] ?? "baseline";
    if (mode === "--help") { console.log("vp exec node examples/durable-bench/results/wake-defer/build.mjs [baseline|ab] — local build only"); return; }
    if (!["baseline", "ab"].includes(mode)) throw new Error("Build mode must be baseline or ab");
    const privateDirectory = mkdtempSync("/private/tmp/wake-defer-build-");
    chmodSync(privateDirectory, 0o700);
    const identitiesPath = join(here, "build-identities");
    mkdirSync(identitiesPath, { recursive: true });
    const allFile = join(identitiesPath, "all.json");
    const identities = existsSync(allFile) ? JSON.parse(readFileSync(allFile, "utf8")) : [];
    const repositoryCommit = git("rev-parse", "HEAD");
    const dirtyStatus = git("status", "--short");
    const sourceRevision = git("rev-parse", "origin/dan/cf-latency-breakdown");
    for (const [name, entry] of Object.entries({ [`network-${mode}`]: "network/worker.ts", provider: "network/provider.ts", driver: "network/driver.ts" })) {
      const snapshots = new Map();
      const output = join(privateDirectory, `${name}.mjs`);
      const compiled = await build({
        absWorkingDir: root, entryPoints: [join(here, entry)], outfile: output, bundle: true,
        format: "esm", platform: "neutral", target: "es2024", minify: false,
        conditions: ["workerd", "worker", "browser", "import"], mainFields: ["module", "main"],
        external: ["cloudflare:*", "node:*", ...builtinModules], logLevel: "error", metafile: true,
        plugins: [{ name: "wake-defer-observation-only", setup(bundler) {
          // Reuse the installed catalog-pinned native adapter. No manifest edits or vendor install.
          bundler.onResolve({ filter: /^@effect\/ai-openai-compat$/ }, (args) => {
            if (args.pluginData?.providerResolved) return;
            return bundler.resolve(args.path, { resolveDir: join(root, "examples/browser-speed"), kind: args.kind, pluginData: { providerResolved: true } });
          });
          bundler.onLoad({ filter: /\.(ts|mjs)$/ }, ({ path }) => {
            if (path.includes("/node_modules/")) return;
            const original = readFileSync(path, "utf8");
            const transformed = instrumentSource(path, original, here, mode);
            snapshots.set(path, { path: relative(root, path), original, ...transformed });
            return { contents: transformed.source, loader: path.endsWith(".ts") ? "ts" : "js" };
          });
        } }],
      });
      for (const [path, snapshot] of snapshots) if (readFileSync(path, "utf8") !== snapshot.original)
        throw new Error(`Source changed during compilation: ${snapshot.path}; rebuild after edits stop`);
      const bundle = readFileSync(output);
      const inputs = Object.keys(compiled.metafile.inputs).sort().map((path) => {
        const absolute = resolve(root, path);
        const snapshot = snapshots.get(absolute);
        return { path: relative(root, absolute), sha256: hash(snapshot?.original ?? readFileSync(absolute)), compiledSha256: hash(snapshot?.source ?? readFileSync(absolute)) };
      });
      const sourceSnapshot = [...snapshots.values()].sort((a, b) => a.path.localeCompare(b.path));
      const sourceBytes = Buffer.from(JSON.stringify(sourceSnapshot));
      const fixtureFiles = ["examples/durable-bench/src/plan.ts", "bun.lock", "package.json"].map((path) => ({ path, sha256: hash(readFileSync(join(root, path))) }));
      const identity = { name, mode, repositoryCommit, dirtyStatus, reusedFrom: { revision: sourceRevision, path: "examples/durable-bench/results/cf-latency" },
        bundleSha256: hash(bundle), bundleBytes: bundle.length, gzipBytes: gzipSync(bundle).length,
        inputsSha256: hash(JSON.stringify(inputs)), sourcesSha256: hash(sourceBytes), fixtureFiles,
        expectedSeeds: { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" },
        instrumentation: sourceSnapshot.filter((s) => s.changes.length).map(({ path, changes }) => ({ path, changes })),
        esbuildVersion, target: "es2024", minify: false, output,
        effectVersion: JSON.parse(readFileSync(join(root, "node_modules/effect/package.json"), "utf8")).version };
      const stem = join(identitiesPath, `${name}-${identity.bundleSha256}`);
      writeFileSync(`${stem}.mjs.gz`, gzipSync(bundle));
      writeFileSync(`${stem}-inputs.json.gz`, gzipSync(JSON.stringify(inputs)));
      writeFileSync(`${stem}-sources.json.gz`, gzipSync(sourceBytes));
      writeFileSync(`${stem}.json`, JSON.stringify(identity, null, 2) + "\n");
      const previous = identities.findIndex((item) => item.name === name);
      if (previous >= 0) identities.splice(previous, 1);
      identities.push(identity);
      writeFileSync(allFile, JSON.stringify(identities, null, 2) + "\n");
      console.log(JSON.stringify({ name, repositoryCommit, bundleBytes: identity.bundleBytes, bundleSha256: identity.bundleSha256, instrumentedFiles: identity.instrumentation.length }));
    }
  },
  catch: (cause) => new BuildError({ message: String(cause) }),
});
if (import.meta.url === `file://${process.argv[1]}`) NodeRuntime.runMain(program);
