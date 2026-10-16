import { spawnSync } from "node:child_process";
import { createHash, webcrypto } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { Session } from "node:inspector/promises";
import { SourceMap } from "node:module";
import { cpus } from "node:os";
import { resolve } from "node:path";
import vm from "node:vm";

const cpu = () => {
  const usage = process.threadCpuUsage();

  return (usage.user + usage.system) / 1000;
};

const save = (path, value) => writeFileSync(path, JSON.stringify(value, null, 2) + "\n");
const args = process.argv.slice(2);

if (args[0] === "--child") {
  const [, arm, mode] = args;
  const code = readFileSync(`dist/${arm}.mjs`, "utf8");

  const sandbox = {
    console,
    crypto: webcrypto,
    URL,
    URLSearchParams,
    TextEncoder,
    TextDecoder,
    ReadableStream,
    WritableStream,
    TransformStream,
    Request,
    Response,
    Headers,
    AbortController,
    AbortSignal,
    performance,
    queueMicrotask,
    structuredClone,
    Buffer,
    atob,
    btoa,
    DOMException,
    FormData,
    Blob,
    File,
    process: { env: {}, versions: process.versions },
    fetch: () => {
      throw new Error("Unexpected evaluation I/O");
    },
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
  };

  const context = vm.createContext(sandbox);
  const before = cpu();
  const mod = new vm.SourceTextModule(code, { context, identifier: `target://${arm}` });
  const cache = new Map();

  await mod.link(async (specifier) => {
    if (cache.has(specifier)) return cache.get(specifier);
    class NativeBase {}

    const values = specifier.startsWith("cloudflare:")
      ? {
          DurableObject: NativeBase,
          RpcTarget: NativeBase,
          RpcStub: NativeBase,
          WorkerEntrypoint: NativeBase,
          WorkflowEntrypoint: NativeBase,
          NonRetryableError: class extends Error {},
          env: {},
          exports: {},
          withEnv: (_, run) => run(),
        }
      : await import(specifier);

    const linked = new vm.SyntheticModule(
      Object.keys(values),
      function () {
        for (const key of Object.keys(values)) this.setExport(key, values[key]);
      },
      { context, identifier: specifier },
    );

    cache.set(specifier, linked);

    return linked;
  });
  const parseLinkMs = cpu() - before;
  const session = new Session();

  if (mode !== "timing") {
    session.connect();
    await session.post("Profiler.enable");
    if (mode === "coverage")
      await session.post("Profiler.startPreciseCoverage", { callCount: true, detailed: true });
    else {
      await session.post("Profiler.setSamplingInterval", { interval: 100 });
      await session.post("Profiler.start");
    }
  }
  const start = cpu();

  await mod.evaluate();
  const evaluationMs = cpu() - start;
  let evidence;

  if (mode === "coverage") evidence = await session.post("Profiler.takePreciseCoverage");
  if (mode === "profile") evidence = (await session.post("Profiler.stop")).profile;
  if (mode !== "timing") session.disconnect();
  console.log(JSON.stringify({ arm, parseLinkMs, evaluationMs, evidence }));
} else {
  const index = args.indexOf("--out");

  if (index < 0 || !args[index + 1]) throw new Error("Usage: local --out <private-directory>");
  const out = resolve(args[index + 1]);

  if (args.includes("--reuse")) {
    const previous = JSON.parse(readFileSync(`${out}/summary.json`, "utf8"));

    for (const arm of ["yielded", "pi"]) {
      const hash = createHash("sha256")
        .update(readFileSync(`dist/${arm}.mjs`))
        .digest("hex");

      if (previous.arms[arm].sha256 !== hash)
        throw new Error("Saved capture belongs to another bundle");
    }
  }

  mkdirSync(out, { recursive: true });
  const runs = { yielded: [], pi: [] };

  const launch = (arm, mode, sample) => {
    if (args.includes("--reuse"))
      return JSON.parse(readFileSync(`${out}/${arm}-${mode}-${sample}.json`, "utf8"));

    const result = spawnSync(
      process.execPath,
      ["--experimental-vm-modules", "--no-warnings", import.meta.filename, "--child", arm, mode],
      { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
    );

    if (result.status !== 0) throw new Error(result.stderr);
    const value = JSON.parse(result.stdout);

    save(`${out}/${arm}-${mode}-${sample}.json`, value);

    return value;
  };

  for (let sample = 0; sample < 12; sample++) {
    for (const arm of sample % 2 ? ["pi", "yielded"] : ["yielded", "pi"])
      runs[arm].push(launch(arm, "timing", sample));
  }

  const summary = {
    node: process.version,
    v8: process.versions.v8,
    cpu: cpus()[0].model,
    arms: {},
  };

  for (const arm of ["yielded", "pi"]) {
    const code = readFileSync(`dist/${arm}.mjs`, "utf8");
    const map = new SourceMap(JSON.parse(readFileSync(`dist/${arm}.mjs.map`, "utf8")));
    const starts = [0];

    for (let i = 0; i < code.length; i++) if (code[i] === "\n") starts.push(i + 1);

    const mapped = (offset) => {
      let lo = 0,
        hi = starts.length - 1;

      while (lo < hi) {
        const mid = Math.ceil((lo + hi) / 2);

        if (starts[mid] <= offset) lo = mid;
        else hi = mid - 1;
      }

      return map.findEntry(lo, offset - starts[lo]);
    };

    const countRuns = [];

    for (let sample = 0; sample < 3; sample++) {
      const coverage = launch(arm, "coverage", sample).evidence.result.find(
        (x) => x.url === `target://${arm}`,
      );

      const functions = coverage.functions
        .filter((f) => f.ranges[0].count > 0)
        .map((f) => ({
          ...mapped(f.ranges[0].startOffset),
          name: f.functionName,
          count: f.ranges[0].count,
        }));

      save(`${out}/${arm}-functions-${sample}.json`, functions);

      const count = (name, file) =>
        functions
          .filter((f) => name.test(f.name) && file.test(f.originalSource ?? ""))
          .reduce((sum, f) => sum + f.count, 0);

      countRuns.push({
        classes: count(/^makeClass\d*$/, /\/Schema\.[jt]s$/),
        structs: count(/^Struct\d*$/, /\/Schema\.[jt]s$/),
        wrappers: count(/^make\d*$/, /\/internal\/schema\/make\.[jt]s$/),
      });
    }
    const groups = {};

    for (let sample = 0; sample < 7; sample++) {
      const profile = launch(arm, "profile", sample).evidence;
      const nodes = new Map(profile.nodes.map((n) => [n.id, n]));

      for (let i = 0; i < profile.samples.length; i++) {
        const frame = nodes.get(profile.samples[i]).callFrame;

        const location =
          frame.url === `target://${arm}`
            ? map.findEntry(frame.lineNumber, frame.columnNumber)
            : {};

        const source = location.originalSource ?? "";

        const group = /\/(Schema|SchemaAST)\.[jt]s$|\/internal\/schema\//.test(source)
          ? "Schema/AST"
          : frame.url === `target://${arm}`
            ? "other bundle code"
            : "runtime/profiler/GC";

        groups[group] = (groups[group] ?? 0) + profile.timeDeltas[i] / 1000 / 7;
      }
    }
    summary.arms[arm] = {
      sha256: createHash("sha256").update(code).digest("hex"),
      evaluationMeanMs: runs[arm].reduce((n, r) => n + r.evaluationMs, 0) / runs[arm].length,
      timings: runs[arm].map((r) => r.evaluationMs),
      countRuns,
      sampledSelfMs: groups,
    };
  }
  save(`${out}/summary.json`, summary);
  console.log(JSON.stringify(summary, null, 2));
}
