import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve, join } from "node:path";

const args = process.argv.slice(2);
if (args[0] === "--") args.shift();
const [stageArg, optionsFile] = args;
if (!stageArg || !optionsFile) throw new Error("cpu-worker <stage> <worker-options.json>");
const stage = resolve(stageArg);
const options = readFileSync(optionsFile, "utf8");
const child = spawn(process.execPath, [join(stage, "fixture/worker.js")], {
  cwd: stage,
  env: { ...process.env, NODE_ENV: "production", RUNTIME_BENCHMARK_OPTIONS: options },
  stdio: "inherit",
});
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => child.kill(signal));
child.on("error", (error) => { console.error(error); process.exitCode = 1; });
child.on("exit", (code, signal) => { process.exitCode = code ?? (signal ? 1 : 0); });
