import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect, Schema } from "effect";

export default Alchemy.Stack(
  "durable-bench",
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

    const resourcePrefix = yield* Config.NonEmptyString("DURABLE_BENCH_PREFIX").pipe(
      Config.withDefault("durable-bench"),
    );

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

      const provider = yield* Cloudflare.Worker("durable-bench-provider", {
        ...options,
        name: prefix + "-provider",
        main: bundle + "/provider.mjs",
        env,
      });

      const driver = yield* Cloudflare.Worker("durable-bench-driver", {
        ...options,
        name: prefix + "-driver",
        main: bundle + "/driver.mjs",
        placement: { region: "aws:us-west-1" },
        env: {
          ...env,
          BENCH_PREFIX: resourcePrefix,
          WORKERS_SUBDOMAIN: yield* Config.NonEmptyString("DURABLE_BENCH_SUBDOMAIN"),
        },
      });

      return { driver: driver.url, provider: provider.url };
    }

    const names = yield* Config.schema(
      Schema.fromJsonString(
        Schema.NonEmptyArray(
          Schema.String.check(Schema.isPattern(new RegExp(`^${name}(?:-[a-z0-9-]+)?$`))),
        ).check(Schema.isUnique()),
      ),
      "DURABLE_BENCH_WORKERS",
    ).pipe(Config.withDefault([name]));

    const provider = yield* Config.NonEmptyString("DURABLE_BENCH_PROVIDER");

    const workers = yield* Effect.forEach(
      names,
      Effect.fnUntraced(function* (workerName) {
        const worker = yield* Cloudflare.Worker(
          workerName === name ? "durable-bench-target" : `durable-bench-target-${workerName}`,
          {
            ...options,
            name: workerName,
            main: bundle,
            rules: [{ globs: ["*.mjs.map"] }],
            placement: { region: "aws:us-west-1" },
            env: {
              ...env,
              CPU: cpu,
              PROVIDER_URL: provider,
              YIELDED: Cloudflare.DurableObject("durable-bench-yielded", {
                className: "YieldedDO",
              }),
              PI: Cloudflare.DurableObject("durable-bench-pi", { className: "PiDO" }),
              ACTORS: Cloudflare.DurableObject("durable-bench-actors", { className: "ActorDO" }),
              THREADS: Cloudflare.DurableObject("durable-bench-threads", { className: "ThreadDO" }),
            },
          },
        );

        return { name: workerName, url: worker.url };
      }),
      { concurrency: 6 },
    );

    return { targets: Object.fromEntries(workers.map((worker) => [worker.name, worker.url])) };
  }),
);
