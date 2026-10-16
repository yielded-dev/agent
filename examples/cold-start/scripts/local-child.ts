/** Native measurement boundary. Do not import Effect here: it is the code under measurement. */
import { webcrypto } from "node:crypto";
import { readFileSync } from "node:fs";
import { Session } from "node:inspector/promises";
import { SourceMap } from "node:module";
import { cpus } from "node:os";
import vm from "node:vm";

export const executable = () => process.execPath;

export const environment = () => ({
  node: process.version,
  v8: process.versions.v8,
  cpu: cpus()[0].model,
});

export const sourceMap = (payload: {
  readonly version: number;
  readonly sources: readonly string[];
  readonly sourcesContent: readonly string[];
  readonly names: readonly string[];
  readonly mappings: string;
  readonly file?: string;
  readonly sourceRoot?: string;
}) =>
  new SourceMap({
    ...payload,
    sources: [...payload.sources],
    sourcesContent: [...payload.sourcesContent],
    names: [...payload.names],
    file: payload.file ?? "",
    sourceRoot: payload.sourceRoot ?? "",
  });

const cpu = () => {
  const usage = process.threadCpuUsage();

  return (usage.user + usage.system) / 1000;
};

const capture = async (arm: string, mode: string) => {
  if (
    (arm !== "yielded" && arm !== "pi") ||
    (mode !== "timing" && mode !== "coverage" && mode !== "profile")
  ) {
    throw new Error("Expected a framework arm and timing, coverage, or profile mode");
  }
  const code = readFileSync(`dist/${arm}.mjs`, "utf8");

  const context = vm.createContext({
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
  });

  const before = cpu();
  const mod = new vm.SourceTextModule(code, { context, identifier: `target://${arm}` });
  const cache = new Map<string, vm.SyntheticModule>();

  await mod.link(async (specifier) => {
    const cached = cache.get(specifier);

    if (cached) return cached;
    class NativeBase {}

    const values: Record<string, unknown> = specifier.startsWith("cloudflare:")
      ? {
          DurableObject: NativeBase,
          RpcTarget: NativeBase,
          RpcStub: NativeBase,
          WorkerEntrypoint: NativeBase,
          WorkflowEntrypoint: NativeBase,
          NonRetryableError: class extends Error {},
          env: {},
          exports: {},
          withEnv: (_: unknown, run: () => unknown) => run(),
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

  try {
    if (mode !== "timing") {
      session.connect();
      await session.post("Profiler.enable");
      if (mode === "coverage") {
        await session.post("Profiler.startPreciseCoverage", { callCount: true, detailed: true });
      } else {
        await session.post("Profiler.setSamplingInterval", { interval: 100 });
        await session.post("Profiler.start");
      }
    }
    const start = cpu();

    await mod.evaluate();
    const evaluationMs = cpu() - start;

    const evidence =
      mode === "coverage"
        ? await session.post("Profiler.takePreciseCoverage")
        : mode === "profile"
          ? (await session.post("Profiler.stop")).profile
          : undefined;

    console.log(JSON.stringify({ arm, parseLinkMs, evaluationMs, evidence }));
  } finally {
    if (mode !== "timing") session.disconnect();
  }
};

if (import.meta.main) {
  try {
    await capture(process.argv[2], process.argv[3]);
  } catch (cause) {
    console.error(cause);
    process.exitCode = 1;
  }
}
