import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

import { FIXTURES, Role, SLUG } from "./contracts.ts";

export default Alchemy.Stack(
  SLUG,
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const role = yield* Config.schema(Role, "BENCH_ROLE");

    const bindings =
      role === "pi" ? ["PI"] : role === "tardie" ? ["THREADS", "ACTORS"] : ["THREADS"];

    const objects = Object.fromEntries(
      FIXTURES.flatMap(({ size, sample }) =>
        bindings.map((binding) => [
          `${binding}_${size}_${sample}`,
          Cloudflare.DurableObject(`${SLUG}-${binding}-${size}-${sample}`, {
            className: `${binding === "ACTORS" ? "A" : "C"}${size}_${sample}`,
          }),
        ]),
      ),
    );

    const worker = yield* Cloudflare.Worker(`${SLUG}-worker`, {
      name: yield* Config.NonEmptyString("BENCH_WORKER_NAME"),
      main: yield* Config.NonEmptyString("BENCH_BUNDLE"),
      bundle: false,
      rules: [{ globs: ["bench.mjs"] }],
      compatibility: { date: "2026-08-18", flags: ["nodejs_compat"] },
      limits: { cpuMs: 300_000 },
      workersDev: { enabled: true, previewsEnabled: false },
      observability: {
        enabled: true,
        headSamplingRate: 1,
        logs: { enabled: true, invocationLogs: true },
        traces: { enabled: false },
      },
      env: {
        ...objects,
        BENCH_ROLE: role,
        BENCH_TOKEN: Config.Redacted("BENCH_TOKEN"),
        BENCH_GENERATION: yield* Config.NonEmptyString("BENCH_GENERATION"),
        BENCH_VERSION: Cloudflare.VersionMetadata(),
      },
    });

    return {
      workerName: worker.workerName,
      url: worker.url,
      namespaces: worker.durableObjectNamespaces,
    };
  }),
);
