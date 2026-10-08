import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { cpus, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

export const SIZES = [50, 250, 1000, 3500] as const;
export const MEASURED_TURNS = 10;
export const SAMPLES = 3;
export const CPU_MAX = 0.08;
export const CPU_WINDOW_MS = 1000;

const readVersion = (packageJson: string): string =>
  (JSON.parse(readFileSync(packageJson, "utf8")) as { version: string }).version;

const thirdParty = (name: string) => join("third-party", "node_modules", name, "package.json");

export const TARGETS = {
  yielded: {
    entry: "src/yielded.ts",
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
  if (name === "yielded" || name === "pi" || name === "tardie") return name;

  throw new Error("target: yielded | pi | tardie");
};

export const fixture = (name: Target, turns: number) => `fixtures/${name}-${turns}`;

export const stage = (name: Target, turns: number): string => {
  const dir = mkdtempSync(join(tmpdir(), "durable-bench-"));

  cpSync(fixture(name, turns), dir, { recursive: true });
  for (const file of readdirSync(dir, { recursive: true, withFileTypes: true })) {
    if (file.isFile()) readFileSync(join(file.parentPath, file.name));
  }

  return dir;
};

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

export const prepare = (name: Target): Promise<string> => bundle(name);

export const start = async (name: Target, persist: string): Promise<Miniflare> => {
  const mf = new Miniflare(
    convertV4MiniflareOptions({
      modulesRoot: "/",
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

const ticks = () =>
  cpus().reduce(
    (sum, { times }) => {
      const total = times.user + times.nice + times.sys + times.irq + times.idle;

      return { busy: sum.busy + total - times.idle, total: sum.total + total };
    },
    { busy: 0, total: 0 },
  );

export const busy = async (windowMs = CPU_WINDOW_MS): Promise<number> => {
  const before = ticks();

  await sleep(windowMs);
  const after = ticks();

  return (after.busy - before.busy) / (after.total - before.total);
};

export const quiet = async (max = CPU_MAX): Promise<number> => {
  let quietest = 1;

  for (let attempt = 0; attempt < 15; attempt++) {
    const value = await busy();

    quietest = Math.min(quietest, value);
    if (value <= max) return value;
    process.stderr.write(
      `cpu ${(value * 100).toFixed(0)}% > ${(max * 100).toFixed(0)}%, waiting\n`,
    );
  }
  process.stderr.write(`cpu stayed at ${(quietest * 100).toFixed(0)}%; measuring anyway\n`);

  return quietest;
};

export const options = (
  argv: readonly string[],
): {
  readonly positional: readonly string[];
  readonly samples: number;
  readonly cpuMax: number;
} => {
  const positional: string[] = [];
  let samples = Number(process.env.SAMPLES ?? SAMPLES);
  let cpuMax = Number(process.env.CPU_MAX ?? CPU_MAX);

  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];

    if (arg === "--samples") {
      samples = Number(argv[index + 1]);
      index += 1;
      continue;
    }
    if (arg === "--cpu-max") {
      cpuMax = Number(argv[index + 1]);
      index += 1;
      continue;
    }
    if (arg !== undefined) positional.push(arg);
  }

  return { positional, samples, cpuMax };
};

export const rss = (): number => {
  const rows = execFileSync("ps", ["-A", "-o", "pid=,ppid=,rss="], { encoding: "utf8" })
    .trim()
    .split("\n")
    .map((line) => line.trim().split(/\s+/).map(Number));

  const tree = new Set([process.pid]);

  for (let grew = true; grew;) {
    grew = false;
    for (const row of rows) {
      const pid = row[0];
      const ppid = row[1];

      if (pid === undefined || ppid === undefined) continue;
      if (tree.has(ppid) && !tree.has(pid)) {
        tree.add(pid);
        grew = true;
      }
    }
  }

  return (
    rows
      .filter((row) => {
        const pid = row[0];

        return pid !== undefined && pid !== process.pid && tree.has(pid);
      })
      .reduce((sum, row) => sum + (row[2] ?? 0), 0) / 1024
  );
};
