import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Clock, Config, Console, Effect, Exit, Layer, Schema } from "effect";
import * as HttpClient from "effect/http/HttpClient";

import { WorkerBuild } from "./model.ts";

// Record API wall time without URLs, headers, account identifiers, or response bodies.
const deploymentHttp = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;

    return HttpClient.transform(client, (response, request) =>
      Effect.gen(function* () {
        const startedMs = yield* Clock.currentTimeMillis;

        const category =
          request.method === "PUT" && /\/scripts\/[^/]+$/.test(new URL(request.url).pathname)
            ? "upload"
            : "api";

        return yield* response.pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              yield* Console.error(
                `BENCH_DEPLOY_HTTP ${JSON.stringify({ category, ...(category === "upload" ? { worker: new URL(request.url).pathname.split("/").at(-1), ok: Exit.isSuccess(exit) && exit.value.status >= 200 && exit.value.status < 300 } : {}), startedMs, elapsedMs: (yield* Clock.currentTimeMillis) - startedMs })}`,
              );
            }),
          ),
        );
      }),
    );
  }),
);

export default Alchemy.Stack(
  "durable-bench",
  {
    providers: Cloudflare.providers().pipe(Layer.provideMerge(deploymentHttp)),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const kind = yield* Config.NonEmptyString("DURABLE_BENCH_KIND");
    const name = yield* Config.NonEmptyString("DURABLE_BENCH_NAME");
    const bundle = yield* Config.NonEmptyString("DURABLE_BENCH_BUNDLE");
    const build = yield* Config.NonEmptyString("DURABLE_BENCH_BUILD");
    const token = Config.Redacted("DURABLE_BENCH_TOKEN");
    const cpu = yield* Config.String("DURABLE_BENCH_CPU").pipe(Config.withDefault("false"));
    const writes = yield* Config.String("DURABLE_BENCH_WRITES").pipe(Config.withDefault("false"));

    const resourcePrefix = yield* Config.NonEmptyString("DURABLE_BENCH_PREFIX").pipe(
      Config.withDefault("durable-bench"),
    );

    const env = { BENCH_TOKEN: token, BUILD: build, VERSION: Cloudflare.VersionMetadata() };

    const options = {
      bundle: false,
      // Profiles retain their maps locally; only executable modules need uploading.
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
        observability: { enabled: false },
        main: bundle + "/provider.mjs",
        env,
      });

      const driver = yield* Cloudflare.Worker("durable-bench-driver", {
        ...options,
        name: prefix + "-driver",
        observability: { enabled: false },
        main: bundle + "/driver.mjs",
        placement: { region: "aws:us-west-1" },
        env: {
          ...env,
          MEASUREMENTS: Cloudflare.DurableObject("durable-bench-measurements", {
            className: "DriverMeasurement",
          }),
          BENCH_PREFIX: resourcePrefix,
          WORKERS_SUBDOMAIN: yield* Config.NonEmptyString("DURABLE_BENCH_SUBDOMAIN"),
        },
      });

      if (cpu !== "true") return { driver: driver.url, provider: provider.url };

      const records = yield* Cloudflare.R2.Bucket("durable-bench-cpu-records", {
        name: prefix + "-cpu",
        forceDestroy: true,
      });

      const tail = yield* Cloudflare.Worker("durable-bench-cpu", {
        ...options,
        name: prefix + "-cpu",
        main: bundle + "/tail.mjs",
        observability: { enabled: false },
        env: { ...env, RECORDS: records },
      });

      return { driver: driver.url, provider: provider.url, tail: tail.url };
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
    const infrastructurePrefix = yield* Config.NonEmptyString("DURABLE_BENCH_INFRA_PREFIX");

    const workerBuilds: Readonly<Record<string, WorkerBuild>> = yield* Config.schema(
      Schema.fromJsonString(
        Schema.Record(Schema.String, WorkerBuild).check(
          Schema.makeFilter(
            (assignments) =>
              Object.keys(assignments).length === names.length &&
              names.every((worker) => assignments[worker] !== undefined),
            { message: "Every target Worker requires an explicit build assignment." },
          ),
        ),
      ),
      "DURABLE_BENCH_WORKER_BUILDS",
    );

    const workers = yield* Effect.forEach(
      Object.entries(workerBuilds),
      Effect.fnUntraced(function* ([workerName, assigned]) {
        const target = yield* Schema.decodeUnknownEffect(
          Schema.Literals(["yielded", "pi", "tardie", "effect-ai-floor"]),
        )(
          workerName.endsWith("-effect-ai-floor")
            ? "effect-ai-floor"
            : workerName.split("-").at(-1),
        ).pipe(Effect.orDie);

        const worker = yield* Cloudflare.Worker(
          workerName === name ? "durable-bench-target" : `durable-bench-target-${workerName}`,
          {
            ...options,
            name: workerName,
            ...(cpu === "true" ? { tailConsumers: [infrastructurePrefix + "-cpu"] } : {}),
            main: assigned.bundle,
            placement: { region: "aws:us-west-1" },
            env: {
              ...env,
              BUILD: assigned.build,
              CPU: cpu,
              WRITES: writes,
              PROVIDER_URL: provider,
              ...(target === "effect-ai-floor"
                ? {
                    FLOOR: Cloudflare.DurableObject("durable-bench-floor", {
                      className: "EffectAiFloorDO",
                    }),
                  }
                : {}),
              ...(target === "yielded"
                ? {
                    YIELDED: Cloudflare.DurableObject("durable-bench-yielded", {
                      className: "YieldedDO",
                    }),
                  }
                : {}),
              ...(target === "pi"
                ? { PI: Cloudflare.DurableObject("durable-bench-pi", { className: "PiDO" }) }
                : {}),
              ...(target === "tardie"
                ? {
                    ACTORS: Cloudflare.DurableObject("durable-bench-actors", {
                      className: "ActorDO",
                    }),
                    THREADS: Cloudflare.DurableObject("durable-bench-threads", {
                      className: "ThreadDO",
                    }),
                  }
                : {}),
            },
          },
        );

        return { name: workerName, url: worker.url };
      }),
      { concurrency: names.length },
    );

    return { targets: Object.fromEntries(workers.map((worker) => [worker.name, worker.url])) };
  }),
);
