import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

export default Alchemy.Stack(
  "rpc-overhead",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const worker = yield* Cloudflare.Worker("rpc-overhead-driver", {
      name: yield* Config.NonEmptyString("RPC_OVERHEAD_NAME"),
      main: yield* Config.NonEmptyString("RPC_OVERHEAD_BUNDLE"),
      bundle: false,
      rules: [],
      workersDev: { enabled: true, previewsEnabled: false },
      placement: { region: "aws:us-west-1" },
      compatibility: {
        date: "2026-08-18",
        flags: ["nodejs_compat", "global_fetch_strictly_public"],
      },
      limits: { cpuMs: 300_000 },
      observability: {
        enabled: true,
        headSamplingRate: 1,
        logs: { enabled: true, invocationLogs: true },
        traces: { enabled: false },
      },
      env: {
        BENCH_TOKEN: Config.Redacted("RPC_OVERHEAD_TOKEN"),
        BUILD: yield* Config.NonEmptyString("RPC_OVERHEAD_BUILD"),
        MICRO: Cloudflare.DurableObject("rpc-overhead-micro", { className: "MicroDO" }),
        THREADS: Cloudflare.DurableObject("rpc-overhead-threads", { className: "BenchThreadDO" }),
      },
    });

    return { driver: worker.url };
  }),
);
