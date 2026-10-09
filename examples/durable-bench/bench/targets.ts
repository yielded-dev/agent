import { readFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { join, resolve } from "node:path";

import { build } from "esbuild";
import { convertV4MiniflareOptions, Log, LogLevel, Miniflare } from "miniflare";

export const SIZES = [50, 250, 1000, 3500] as const;

const readVersion = (packageJson: string): string =>
  (JSON.parse(readFileSync(packageJson, "utf8")) as { version: string }).version;

const thirdParty = (name: string) => join("third-party", "node_modules", name, "package.json");

export const TARGETS = {
  yielded: {
    entry: "src/yielded.ts",
    objects: { THREADS: "YieldedDO" },
    packageJson: "../../packages/effect-agent/package.json",
  },
  "yielded-inline": {
    entry: "src/yielded-inline.ts",
    objects: { THREADS: "YieldedDO" },
    packageJson: "../../packages/effect-agent/package.json",
  },
  pi: {
    entry: "third-party/src/pi.ts",
    objects: { PI: "PiDO" },
    packageJson: thirdParty("@earendil-works/pi-durable"),
  },
  tardie: {
    entry: "third-party/src/tardie.ts",
    objects: { ACTORS: "ActorDO", THREADS: "ThreadDO" },
    packageJson: thirdParty("tardie"),
  },
} as const;

export const version = (name: Target): string => readVersion(TARGETS[name].packageJson);

export type Target = keyof typeof TARGETS;

export const target = (name: string | undefined): Target => {
  if (name === "yielded" || name === "yielded-inline" || name === "pi" || name === "tardie")
    return name;

  throw new Error("target: yielded | yielded-inline | pi | tardie");
};

export const fixture = (name: Target, turns: number) => `fixtures/${name}-${turns}`;

const bundles = new Map<Target, Promise<string>>();

const bundle = (name: Target): Promise<string> => {
  const cached = bundles.get(name);

  if (cached !== undefined) return cached;

  const pending = (async () => {
    const outfile = `dist/${name}.mjs`;

    await build({
      entryPoints: [TARGETS[name].entry],
      outfile,
      bundle: true,
      format: "esm",
      platform: "neutral",
      target: "es2024",
      conditions: ["workerd", "worker", "browser", "import"],
      mainFields: ["module", "main"],
      external: ["cloudflare:*", "node:*", ...builtinModules],
      logLevel: "error",
    });

    return resolve(outfile);
  })();

  bundles.set(name, pending);

  return pending;
};

export const start = async (name: Target, persist: string): Promise<Miniflare> => {
  const mf = new Miniflare(
    convertV4MiniflareOptions({
      modules: [{ type: "ESModule", path: await bundle(name) }],
      compatibilityDate: "2026-08-18",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: Object.fromEntries(
        Object.entries(TARGETS[name].objects).map(([binding, className]) => [
          binding,
          { className, useSQLite: true },
        ]),
      ),
      resourcePersistencePath: persist,
      log: new Log(LogLevel.NONE),
      handleStructuredLogs: () => {},
    }),
  );

  await mf.ready;

  return mf;
};

export const call = async <T>(mf: Miniflare, path: string, body?: unknown): Promise<T> => {
  const response = await mf.dispatchFetch(
    `http://bench${path}`,
    body === undefined ? {} : { method: "POST", body: JSON.stringify(body) },
  );

  if (!response.ok) throw new Error(`${path} ${response.status}: ${await response.text()}`);

  return (await response.json()) as T;
};
