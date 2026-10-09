// Adapted from bench/targets.ts: task-local, offline operation-count capture.
import { cpSync, mkdirSync, readFileSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { builtinModules } from "node:module";
import { resolve } from "node:path";

import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare, Log, LogLevel } from "miniflare";

import { instrument } from "./instrument.ts";

// Vite Task does not forward arbitrary environment variables: use explicit flags.
const args = process.argv.slice(2).filter((a) => a !== "--");
const option = (name: string, fallback?: string) => {
  const at = args.indexOf(name);
  if (at < 0) {
    if (fallback !== undefined) return fallback;
    throw new Error(`Missing ${name}`);
  }
  const value = args[at + 1];
  if (value === undefined || value.startsWith("--")) throw new Error(`Missing value for ${name}`);
  args.splice(at, 2);
  return value;
};
const artifacts = resolve(option("--out-dir"));
const original = resolve(option("--fixtures-dir", "fixtures"));
const candidateRecordsPath = resolve(
  option("--candidate-records", "counting/candidate-Records.ts"),
);
const terminalReadsSqlPath = resolve(
  option("--sql-source", "counting/terminal-reads-SqlThreadNativeReads.ts"),
);
const target = args[0] ?? "yielded";
const variant = args[1] ?? "baseline";
const sizes = args.slice(2).map(Number);
if (!sizes.length) sizes.push(50, 250);
mkdirSync(artifacts, { recursive: true });
const bundle = `${artifacts}/${target}-${variant}.mjs`;
const entry = target === "yielded" ? "src/yielded.ts" : "third-party/src/pi.ts";
await build({
  stdin: {
    contents: `export { ${target === "yielded" ? "YieldedDO" : "PiDO"}, countWorker as default } from "./${entry}";`,
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
  banner: { js: readFileSync("counting/probe.js", "utf8") },
  plugins: [instrument(variant, candidateRecordsPath, terminalReadsSqlPath)],
});
const start = async (persist: string) => {
  const mf = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: readFileSync(bundle, "utf8"),
      compatibilityDate: "2026-08-18",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: {
        [target === "yielded" ? "THREADS" : "PI"]: {
          className: target === "yielded" ? "YieldedDO" : "PiDO",
          useSQLite: true,
        },
      },
      resourcePersistencePath: persist,
      log: new Log(LogLevel.NONE),
    }),
  );
  await mf.ready;
  return mf;
};
const call = async (mf: Miniflare, path: string, body?: unknown) => {
  const response = await mf.dispatchFetch(
    `http://counts${path}`,
    body === undefined ? {} : { method: "POST", body: JSON.stringify(body) },
  );
  if (!response.ok) throw new Error(await response.text());
  return response.json();
};
for (const size of sizes) {
  const source = `${original}/${target}-${size}`;
  const meta = JSON.parse(readFileSync(`${source}.json`, "utf8"));
  const runDir = `${artifacts}/work-${target}-${variant}-${size}-${Date.now()}`;
  let normalized;
  if (target === "yielded") {
    const archivePath = `${artifacts}/archive-${size}.json`;
    if (!existsSync(archivePath)) {
      const copied = runDir + "-source";
      cpSync(source, copied, { recursive: true });
      const mf = await start(copied);
      try {
        const exported = await call(mf, "/export");
        if (exported.fingerprint !== meta.fingerprint)
          throw new Error("Export fingerprint mismatch");
        writeFileSync(archivePath, JSON.stringify(exported));
      } finally {
        await mf.dispose();
        rmSync(copied, { recursive: true, force: true });
      }
    }
    const exported = JSON.parse(readFileSync(archivePath, "utf8"));
    const mf = await start(runDir);
    try {
      normalized = await call(mf, "/import", exported.archive);
      if (normalized.fingerprint !== meta.fingerprint)
        throw new Error("Import fingerprint mismatch");
    } finally {
      await mf.dispose();
    }
  } else cpSync(source, runDir, { recursive: true });
  const mf = await start(runDir);
  try {
    const result = await call(mf, "/measure", { id: "count-0", text: "turn count-0 tools=8" });
    if (result.seedFingerprint !== meta.fingerprint)
      throw new Error("Model-visible seed fingerprint mismatch");
    const output = {
      target,
      variant,
      size,
      baseline: "07f0272e7ba49a494064b6b74c6318b55514ae19",
      candidateRevision:
        variant === "terminal-reads" ? "4417a0955e9c61cd3a45ac4cf4ad1e5c8f431228" : undefined,
      source,
      meta,
      normalized,
      ...result,
    };
    writeFileSync(
      `${artifacts}/${target}-${variant}-${size}.json`,
      JSON.stringify(output, null, 2),
    );
    console.log(
      JSON.stringify({
        target,
        variant,
        size,
        fingerprint: result.fingerprint,
        counts: result.counts,
        visits: result.visits,
      }),
    );
  } finally {
    await mf.dispose();
    rmSync(runDir, { recursive: true, force: true });
  }
}
