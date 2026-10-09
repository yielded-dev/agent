import { appendFileSync, mkdirSync, rmSync } from "node:fs";
import { loadavg } from "node:os";

import { MEASURED_TOOLS, turn, type Stats } from "../src/plan.ts";
import {
  call,
  MEASURED_TURNS,
  options,
  prepare,
  quiet,
  rss,
  SIZES,
  stage,
  start,
  target,
  version,
} from "./targets.ts";

const parsed = options(process.argv.slice(2));
const name = target(parsed.positional[0]);
const sizes = parsed.positional.length > 1 ? parsed.positional.slice(1).map(Number) : [...SIZES];
const { samples, cpuMax } = parsed;

if (sizes.some((size) => !Number.isInteger(size) || size < 1)) {
  throw new Error("sizes must be positive integers");
}
if (!Number.isInteger(samples) || samples < 1 || !(cpuMax >= 0)) {
  throw new Error(
    "usage: bench <yielded|yielded-inline|pi|tardie> [turns...] --samples N --cpu-max 0.08",
  );
}

await prepare(name);
mkdirSync("results", { recursive: true });

for (const size of sizes) {
  for (let sample = 0; sample < samples; sample++) {
    const dir = stage(name, size);
    const cpu = await quiet(cpuMax);
    const load = loadavg()[0];
    let at = performance.now();
    const mf = await start(name, dir);
    const startup = performance.now() - at;

    at = performance.now();
    await call(mf, "/wake");
    const open = performance.now() - at;
    const turns: number[] = [];

    for (let i = 0; i < MEASURED_TURNS; i++) {
      at = performance.now();
      await call(mf, "/turn", turn(`m${i}`, MEASURED_TOOLS));
      turns.push(performance.now() - at);
    }
    const memory = rss();
    const stats = await call<Stats>(mf, "/stats");

    await mf.dispose();
    rmSync(dir, { recursive: true, force: true });

    const line = {
      target: name,
      executionPath: name === "yielded" ? "rpc-alarm" : "direct-turn",
      version: version(name),
      turns: size,
      sample,
      startup,
      open,
      turn: turns,
      rss: memory,
      ...stats,
      cpu,
      load,
      at: new Date().toISOString(),
    };

    appendFileSync("results/results.jsonl", `${JSON.stringify(line)}\n`);
    process.stderr.write(
      `${name} ${size} #${sample} open ${open.toFixed(0)}ms turn ${turns.map((value) => value.toFixed(0)).join(",")}ms\n`,
    );
  }
}
