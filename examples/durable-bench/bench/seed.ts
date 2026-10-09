import { cpSync, mkdirSync, rmSync, writeFileSync } from "node:fs";

import { history, type Stats } from "../src/plan.ts";
import { call, fixture, SIZES, start, target, version } from "./targets.ts";

const BATCH = 50;

const [nameArg, ...sizeArgs] = process.argv.slice(2).filter((arg: string) => arg !== "--");
const name = target(nameArg);
const sizes = sizeArgs.length ? sizeArgs.map(Number) : [...SIZES];
const work = `fixtures/${name}-work`;

rmSync(work, { recursive: true, force: true });
mkdirSync(work, { recursive: true });

let done = 0;
let digest = "";

for (const size of sizes) {
  const started = performance.now();

  while (done < size) {
    const turns = history(done, Math.min(done + BATCH, size));
    const mf = await start(name, work);

    if (done === 0) await call(mf, "/setup");
    digest = await call<string>(mf, "/seed", turns);
    await mf.dispose();
    done += turns.length;
    process.stderr.write(`\r${name} ${done}/${size}`);
  }
  const mf = await start(name, work);
  const stats = await call<Stats>(mf, "/stats");

  await mf.dispose();
  const out = fixture(name, size);

  rmSync(out, { recursive: true, force: true });
  cpSync(work, out, { recursive: true });

  const meta = {
    target: name,
    version: version(name),
    turns: size,
    fingerprint: digest,
    seedMs: Math.round(performance.now() - started),
    ...stats,
  };

  writeFileSync(`${out}.json`, JSON.stringify(meta, null, 2));
  process.stderr.write(`\r${JSON.stringify(meta)}\n`);
}
rmSync(work, { recursive: true, force: true });
