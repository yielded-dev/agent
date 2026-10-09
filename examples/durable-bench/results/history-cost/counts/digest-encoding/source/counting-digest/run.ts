// Adapted from counting-hydration/run.ts: offline operation counts, no timing claims.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { resolve } from "node:path";

import { build } from "esbuild";
import { convertV4MiniflareOptions, Log, LogLevel, Miniflare } from "miniflare";

import { instrument } from "./instrument.ts";

const args = process.argv.slice(2).filter((arg) => arg !== "--");
const option = (name: string) => {
  const at = args.indexOf(name);
  const value = at < 0 ? undefined : args[at + 1];
  if (value === undefined || value.startsWith("--")) throw new Error(`Missing ${name}`);
  args.splice(at, 2);
  return value;
};
const artifacts = resolve(option("--out-dir"));
const original = resolve(option("--fixtures-dir"));
const archiveDirectory = resolve(option("--archive-dir"));
const candidateRunContextPath = resolve(option("--candidate-run-context"));
const candidateSourceSha256 = option("--candidate-sha256");
const target = "yielded";
const variant = args.shift();
if (variant !== "baseline" && variant !== "digest-encoding")
  throw new Error("Expected baseline or digest-encoding");
const sizes = args.map(Number);
if (!sizes.length || sizes.some((size) => ![50, 250, 1000, 3500].includes(size)))
  throw new Error("Specify retained fixture sizes: 50 250 1000 3500");
const sha256 = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
if (sha256(readFileSync(candidateRunContextPath)) !== candidateSourceSha256)
  throw new Error("Candidate source SHA-256 differs from pinned snapshot");
const baseline = "07f0272e7ba49a494064b6b74c6318b55514ae19";
const checkout = resolve("../..");
const git = (...gitArgs: string[]) =>
  execFileSync("git", ["-C", checkout, ...gitArgs], { encoding: "utf8" });
if (git("rev-parse", "HEAD").trim() !== baseline) throw new Error("Baseline revision mismatch");
git("diff", "--exit-code", baseline, "--", "packages", "examples/durable-bench/src");
const recordsPath = resolve(checkout, "packages/effect-agent/src/durable/Records.ts");
const baselineRecordsSha256 = sha256(readFileSync(recordsPath));
mkdirSync(artifacts, { recursive: true });
const bundle = `${artifacts}/${target}-${variant}.mjs`;
await build({
  stdin: {
    contents: 'export { YieldedDO, countWorker as default } from "./src/yielded.ts";',
    resolveDir: process.cwd(),
    loader: "ts",
  },
  outfile: bundle,
  bundle: true,
  format: "esm",
  platform: "neutral",
  target: "es2024",
  conditions: ["workerd", "worker", "browser", "import"],
  mainFields: ["module", "main"],
  external: ["cloudflare:*", "node:*", ...builtinModules],
  logLevel: "error",
  banner: { js: readFileSync("counting-digest/probe.js", "utf8") },
  // Scope hooks are identical on both sides and add no Effect operations.
  plugins: [instrument(variant, candidateRunContextPath, true)],
});
const bundleSha256 = sha256(readFileSync(bundle));
const start = async (persist: string) => {
  const mf = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: readFileSync(bundle, "utf8"),
      compatibilityDate: "2026-08-18",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { THREADS: { className: "YieldedDO", useSQLite: true } },
      resourcePersistencePath: persist,
      log: new Log(LogLevel.NONE),
    }),
  );
  await mf.ready;
  return mf;
};
const call = async (mf: Miniflare, path: string, body: unknown) => {
  const response = await mf.dispatchFetch(`http://counts${path}`, {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
};
for (const size of sizes) {
  const source = `${original}/${target}-${size}`;
  const metadataBytes = readFileSync(`${source}.json`);
  const meta = JSON.parse(metadataBytes.toString());
  const archivePath = `${archiveDirectory}/archive-${size}.json`;
  if (!existsSync(archivePath))
    throw new Error("Retained archive missing; refusing setup writes to shared inputs");
  const archiveBytes = readFileSync(archivePath);
  const exported = JSON.parse(archiveBytes.toString());
  if (exported.fingerprint !== meta.fingerprint)
    throw new Error("Retained archive fingerprint mismatch");
  const runDir = mkdtempSync(`${artifacts}/work-${target}-${variant}-${size}-`);
  try {
    let normalized;
    const importer = await start(runDir);
    try {
      normalized = await call(importer, "/import", exported.archive);
      if (normalized.fingerprint !== meta.fingerprint)
        throw new Error("Import fingerprint mismatch");
    } finally {
      await importer.dispose();
    }
    const mf = await start(runDir);
    try {
      const result = await call(mf, "/measure", { id: "count-0", text: "turn count-0 tools=8" });
      if (result.seedFingerprint !== meta.fingerprint)
        throw new Error("Model-visible seed fingerprint mismatch");
      const output = {
        target,
        variant,
        size,
        baseline,
        candidateSourceSha256,
        baselineRecordsSha256,
        bundleSha256,
        supplementalDigestScope: true,
        source,
        archiveSha256: sha256(archiveBytes),
        metadataSha256: sha256(metadataBytes),
        meta,
        normalized,
        ...result,
      };
      writeFileSync(
        `${artifacts}/${target}-${variant}-${size}.json`,
        JSON.stringify(output, null, 2),
      );
      const first = result.modelSnapshots[0];
      console.log(
        JSON.stringify({
          target,
          variant,
          size,
          fingerprint: result.fingerprint,
          firstModelEffects: first.effectEvaluations,
          firstModelSchemaNodes: first.schemaNodeCalls,
          digestEffects: first["historyDigest.effectEvaluations"],
          digestSchemaNodes: first["historyDigest.schemaNodeCalls"],
          fullTurnEffects: result.counts.effectEvaluations,
          fullTurnSchemaNodes: result.counts.schemaNodeCalls,
        }),
      );
    } finally {
      await mf.dispose();
    }
  } finally {
    rmSync(runDir, { recursive: true, force: true });
  }
}
