import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

export default Alchemy.Stack(
  "cold-bisect",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const kind = yield* Config.NonEmptyString("DURABLE_BENCH_KIND");
    const name = yield* Config.NonEmptyString("DURABLE_BENCH_NAME");
    const bundle = yield* Config.NonEmptyString("DURABLE_BENCH_BUNDLE");
    const build = yield* Config.NonEmptyString("DURABLE_BENCH_BUILD");
    const token = Config.Redacted("DURABLE_BENCH_TOKEN");
    const cpu = yield* Config.String("DURABLE_BENCH_CPU").pipe(Config.withDefault("false"));
    const env = { BENCH_TOKEN: token, BUILD: build, VERSION: Cloudflare.VersionMetadata() };

    const options = {
      bundle: false,
      rules: [],
      workersDev: { enabled: true, previewsEnabled: false },
      compatibility: {
        date: "2026-08-18",
        flags: ["nodejs_compat", "global_fetch_strictly_public"],
      },
      limits: { cpuMs: 300_000 },
      observability: {
        enabled: cpu === "true",
        headSamplingRate: 1,
        logs: { enabled: cpu === "true", invocationLogs: cpu === "true" },
        traces: { enabled: false },
      },
    };

    if (kind === "infrastructure") {
      const prefix = yield* Config.NonEmptyString("DURABLE_BENCH_INFRA_PREFIX");

      const provider = yield* Cloudflare.Worker("cold-bisect-provider", {
        ...options,
        name: prefix + "-provider",
        main: bundle + "/provider.mjs",
        env,
      });

      const driver = yield* Cloudflare.Worker("cold-bisect-driver", {
        ...options,
        name: prefix + "-driver",
        main: bundle + "/driver.mjs",
        placement: { region: "aws:us-west-1" },
        env: { ...env, WORKERS_SUBDOMAIN: yield* Config.NonEmptyString("DURABLE_BENCH_SUBDOMAIN") },
      });

      return { driver: driver.url, provider: provider.url };
    }

    const worker = yield* Cloudflare.Worker("cold-bisect-target", {
      ...options,
      name,
      main: bundle,
      placement: { region: "aws:us-west-1" },
      env: {
        ...env,
        CPU: cpu,
        PROVIDER_URL: yield* Config.NonEmptyString("DURABLE_BENCH_PROVIDER"),
        ...(kind === "startup-yielded"
          ? {
              THREADS: Cloudflare.DurableObject("cold-bisect-yielded", {
                className: "YieldedDO",
              }),
            }
          : kind === "startup-pi"
            ? {
                THREADS: Cloudflare.DurableObject("cold-bisect-pi", { className: "PiDO" }),
              }
            : {
                ...(kind === "target" || kind === "yielded"
                  ? {
                      YIELDED: Cloudflare.DurableObject("cold-bisect-yielded", {
                        className: "YieldedDO",
                      }),
                    }
                  : {}),
                ...(kind === "bare"
                  ? { BARE: Cloudflare.DurableObject("cold-bisect-bare", { className: "BareDO" }) }
                  : {}),
                ...(kind === "target" || kind === "pi"
                  ? {
                      PI: Cloudflare.DurableObject("cold-bisect-pi", { className: "PiDO" }),
                    }
                  : {}),
                ...(kind === "target"
                  ? {
                      ACTORS: Cloudflare.DurableObject("cold-bisect-actors", {
                        className: "ActorDO",
                      }),
                      THREADS: Cloudflare.DurableObject("cold-bisect-threads", {
                        className: "ThreadDO",
                      }),
                    }
                  : {}),
              }),
      },
    });

    return { target: worker.url };
  }),
);
